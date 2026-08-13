package dataplane

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"time"

	"github.com/nosway/namrbd/gateway/auth"
	"github.com/nosway/namrbd/gateway/service"
	"github.com/nosway/namrbd/internal/structuredlog"
	"github.com/nosway/namrbd/protocol/wirev1"
	"github.com/nosway/namrbd/protocol/wirev2"
)

type Config struct {
	PathID                 uint32
	GatewayID              string
	MaxIOSize              uint32
	MaxZeroLikeIOSize      uint32
	MaxSegments            uint32
	MaxInflightRequests    uint32
	MaxInflightBytes       uint64
	UseWireV2              bool
	TraceCompletedRequests bool
	TokenVerifier          auth.TokenIssuer
	SessionDerivationKey   []byte
	Sessions               *SessionTable
}

const DefaultMaxIOSize uint32 = 4*1024*1024 - 64*1024
const DefaultMaxZeroLikeIOSize uint32 = 256 * 1024 * 1024

type Server struct {
	svc *service.Service
	cfg Config
}

func New(svc *service.Service, cfg Config) *Server {
	if cfg.MaxIOSize == 0 {
		cfg.MaxIOSize = DefaultMaxIOSize
	}
	if cfg.MaxZeroLikeIOSize == 0 {
		cfg.MaxZeroLikeIOSize = DefaultMaxZeroLikeIOSize
	}
	if cfg.MaxSegments == 0 {
		cfg.MaxSegments = 32
	}
	if cfg.MaxInflightRequests == 0 {
		cfg.MaxInflightRequests = 128
	}
	if cfg.MaxInflightBytes == 0 {
		cfg.MaxInflightBytes = 8 * 1024 * 1024
	}
	if cfg.UseWireV2 && cfg.Sessions == nil {
		cfg.Sessions = NewSessionTable()
	}
	return &Server{svc: svc, cfg: cfg}
}

func (s *Server) maxLogicalLengthForOp(op uint32) uint32 {
	switch op {
	case wirev1.OpDiscard, wirev1.OpWriteZeroes:
		return s.cfg.MaxZeroLikeIOSize
	default:
		return s.cfg.MaxIOSize
	}
}

func (s *Server) validateLogicalLength(op uint32, length uint32) error {
	maxLength := s.maxLogicalLengthForOp(op)
	if maxLength == 0 || length <= maxLength {
		return nil
	}
	return fmt.Errorf("dataplane op=%d length=%d exceeds max=%d", op, length, maxLength)
}

func (s *Server) pathCapability() wirev1.PathCapability {
	return wirev1.PathCapability{
		MaxIOSize:           s.cfg.MaxIOSize,
		MaxSegments:         s.cfg.MaxSegments,
		SupportedOpsMask:    supportedOpsMask(),
		MaxInflightRequests: s.cfg.MaxInflightRequests,
		MaxInflightBytes:    s.cfg.MaxInflightBytes,
		MaxZeroLikeIOSize:   s.cfg.MaxZeroLikeIOSize,
	}
}

func (s *Server) Serve(l net.Listener) error {
	for {
		conn, err := l.Accept()
		if err != nil {
			return err
		}
		go s.handleConn(conn)
	}
}

// RevokeSessionsForVolume removes all sessions for the given volume (e.g. after detach or generation bump).
func (s *Server) RevokeSessionsForVolume(volumeID uint64) {
	if s.cfg.Sessions != nil {
		s.cfg.Sessions.RevokeByVolume(volumeID)
	}
}

func (s *Server) handleConn(conn net.Conn) {
	defer conn.Close()
	_ = conn.SetReadDeadline(time.Now().Add(30 * time.Second))
	first6 := make([]byte, 6)
	if _, err := io.ReadFull(conn, first6); err != nil {
		if !errors.Is(err, io.EOF) {
			log.Printf("dataplane read prefix: %v", err)
		}
		return
	}
	magic := binary.LittleEndian.Uint32(first6[0:4])
	if magic != wirev1.Magic {
		log.Printf("dataplane bad magic: %x", magic)
		return
	}
	version := binary.LittleEndian.Uint16(first6[4:6])
	if version == 2 && s.cfg.UseWireV2 && s.cfg.TokenVerifier != nil && len(s.cfg.SessionDerivationKey) > 0 {
		s.handleConnV2(conn, first6)
		return
	}
	s.handleConnV1(conn, first6)
}

func (s *Server) handleConnV1(conn net.Conn, first6 []byte) {
	r := io.MultiReader(bytes.NewReader(first6), conn)
	c := wirev1.NewConn(&readWriter{r: r, w: conn}, s.cfg.MaxIOSize+24)
	for {
		_ = conn.SetReadDeadline(time.Now().Add(30 * time.Second))
		_ = conn.SetWriteDeadline(time.Time{})
		h, payload, err := c.ReadFrame()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return
			}
			log.Printf("dataplane read error: %v", err)
			return
		}
		resp, respPayload := s.handleRequest(h, payload)
		wire, err := wirev1.EncodeResponseMessage(resp, respPayload)
		if err != nil {
			log.Printf("dataplane encode response error: %v", err)
			return
		}
		_ = conn.SetReadDeadline(time.Time{})
		_ = conn.SetWriteDeadline(time.Now().Add(30 * time.Second))
		if _, err := conn.Write(wire); err != nil {
			log.Printf("dataplane write error: %v", err)
			return
		}
	}
}

type readWriter struct {
	r io.Reader
	w io.Writer
}

func (r *readWriter) Read(p []byte) (n int, err error)  { return r.r.Read(p) }
func (r *readWriter) Write(p []byte) (n int, err error) { return r.w.Write(p) }

func (s *Server) handleConnV2(conn net.Conn, first6 []byte) {
	r := io.MultiReader(bytes.NewReader(first6), conn)
	maxPayload := s.cfg.MaxIOSize + 24
	h, payload, _, err := wirev2.ReadFrame(r, maxPayload)
	if err != nil {
		log.Printf("dataplane v2 read HELLO: %v", err)
		s.sendAuthErr(conn, wirev2.ErrBadAuthTag)
		return
	}
	if h.Op != wirev2.OpHello || h.SessionID != 0 || h.SeqNo != 0 {
		log.Printf("dataplane v2 expected HELLO, got op=%d session=%d seq=%d", h.Op, h.SessionID, h.SeqNo)
		s.sendAuthErr(conn, wirev2.ErrSessionClosed)
		return
	}
	hello, err := wirev2.DecodeHelloPayload(payload)
	if err != nil {
		log.Printf("dataplane v2 HELLO payload: %v", err)
		s.sendAuthErr(conn, wirev2.ErrBadAuthTag)
		return
	}
	verified, err := s.cfg.TokenVerifier.VerifyDataplaneToken(hello.Token)
	if err != nil {
		if errors.Is(err, auth.ErrTokenExpired) {
			s.sendAuthErr(conn, wirev2.ErrTokenExpired)
		} else {
			s.sendAuthErr(conn, wirev2.ErrBadAuthTag)
		}
		return
	}
	st, err := s.svc.VolumeState(uint64(verified.Claims.VolumeID))
	if err != nil {
		log.Printf("dataplane v2 volume state lookup failed: %v", err)
		s.sendAuthErr(conn, wirev2.ErrSessionClosed)
		return
	}
	if err := s.validateHelloSession(hello, verified, st); err != nil {
		log.Printf("dataplane v2 HELLO validation failed: %v", err)
		s.sendAuthErr(conn, wirev2.ErrSessionClosed)
		return
	}
	serverNonce := mustRandomBase64URL(16)
	sess := &Session{
		VolumeID:     uint64(verified.Claims.VolumeID),
		AttachmentID: verified.Claims.AttachmentID,
		Generation:   verified.Claims.Generation,
		HostID:       verified.Claims.HostID,
		DeviceID:     verified.Claims.DeviceID,
		PathID:       s.cfg.PathID,
		ExpiresAt:    verified.ExpiresAt,
	}
	s.cfg.Sessions.Add(sess)
	// Use the configured session derivation key when present; token fallback preserves compatibility.
	derivationKey := s.cfg.SessionDerivationKey
	if len(derivationKey) == 0 {
		derivationKey = []byte(hello.Token)
	}
	sess.SessionKey = auth.DeriveSessionKey(derivationKey, hello.Token, hello.ClientNonce, serverNonce, sess.ID)
	sessionID := sess.ID

	ackPayload, _ := wirev2.EncodeHelloAckPayload(&wirev2.HelloAckPayload{
		SessionID:           sessionID,
		ServerNonce:         serverNonce,
		SelectedAuth:        auth.AuthModeTokenHMACV1,
		ExpiresAt:           verified.ExpiresAt.Format(time.RFC3339),
		PathID:              s.cfg.PathID,
		MaxInflightRequests: s.cfg.MaxInflightRequests,
		MaxInflightBytes:    s.cfg.MaxInflightBytes,
		MaxIOSize:           s.cfg.MaxIOSize,
		MaxZeroLikeIOSize:   s.cfg.MaxZeroLikeIOSize,
	})
	ackH := &wirev2.Header{
		Op: wirev2.OpHelloAck, RequestID: h.RequestID, VolumeID: uint64(verified.Claims.VolumeID), Generation: verified.Claims.Generation,
		SessionID: sessionID, SeqNo: 0,
	}
	if err := wirev2.WriteFrame(conn, ackH, ackPayload, nil); err != nil {
		log.Printf("dataplane v2 write HELLO_ACK: %v", err)
		return
	}

	for {
		_ = conn.SetReadDeadline(time.Now().Add(30 * time.Second))
		_ = conn.SetWriteDeadline(time.Time{})
		h, payload, tag, err := wirev2.ReadFrame(conn, maxPayload)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return
			}
			log.Printf("dataplane v2 read: %v", err)
			return
		}
		sess := s.cfg.Sessions.Get(h.SessionID)
		if sess == nil {
			s.sendAuthErr(conn, wirev2.ErrSessionClosed)
			return
		}
		if status, ok := s.sessionStillCurrent(sess); !ok {
			s.cfg.Sessions.Remove(sess.ID)
			s.sendAuthErr(conn, status)
			return
		}
		if h.Op != wirev2.OpHello && h.Op != wirev2.OpHelloAck {
			if h.AuthLen != wirev2.AuthTagSize || len(tag) != wirev2.AuthTagSize {
				s.sendAuthErr(conn, wirev2.ErrBadAuthTag)
				return
			}
			if err := wirev2.VerifyAuthTag(sess.SessionKey, &h, payload, tag); err != nil {
				log.Printf("dataplane v2 auth_tag verify: %v", err)
				s.sendAuthErr(conn, wirev2.ErrBadAuthTag)
				return
			}
			if err := sess.Seq.CheckNext(h.SeqNo); err != nil {
				log.Printf("dataplane v2 replay or out-of-order seq: %v", err)
				s.sendAuthErr(conn, wirev2.ErrReplayDetected)
				return
			}
		}
		resp, respPayload := s.handleRequestV2(h, payload, sess)
		respTag := wirev2.ComputeAuthTag(sess.SessionKey, &resp.Base, respPayload)
		_ = conn.SetReadDeadline(time.Time{})
		_ = conn.SetWriteDeadline(time.Now().Add(30 * time.Second))
		if err := wirev2.WriteResponseFrame(conn, &resp, respPayload, respTag); err != nil {
			log.Printf("dataplane v2 write response: %v", err)
			return
		}
	}
}

func (s *Server) validateHelloSession(hello *wirev2.HelloPayload, verified *auth.VerifiedToken, st service.VolumeState) error {
	claims := verified.Claims
	if claims.GatewayID != "" && s.cfg.GatewayID != "" && claims.GatewayID != s.cfg.GatewayID {
		return errors.New("gateway_id mismatch")
	}
	if st.AttachmentID != claims.AttachmentID || st.Generation != claims.Generation {
		return errors.New("attachment or generation mismatch")
	}
	if st.AttachedHostID != claims.HostID || st.AttachedDeviceID != claims.DeviceID {
		return errors.New("metadata host/device mismatch")
	}
	if hello.HostID != claims.HostID || hello.DeviceID != claims.DeviceID {
		return errors.New("hello host/device mismatch")
	}
	if hello.RequestedPathID != s.cfg.PathID {
		return errors.New("requested path mismatch")
	}
	if len(claims.AllowedPathIDs) > 0 && !containsPathID(claims.AllowedPathIDs, s.cfg.PathID) {
		return errors.New("path not allowed by token")
	}
	if !containsAuthMode(hello.SupportedAuth, auth.AuthModeTokenHMACV1) {
		return errors.New("unsupported auth mode")
	}
	return nil
}

func (s *Server) sessionStillCurrent(sess *Session) (int32, bool) {
	st, err := s.svc.VolumeState(sess.VolumeID)
	if err != nil {
		log.Printf("dataplane v2 refresh volume state failed: %v", err)
		return wirev2.ErrSessionClosed, false
	}
	if st.Generation != sess.Generation {
		return wirev2.ErrGenerationMatch, false
	}
	if st.AttachmentID != sess.AttachmentID || st.AttachedHostID != sess.HostID || st.AttachedDeviceID != sess.DeviceID {
		return wirev2.ErrSessionClosed, false
	}
	return 0, true
}

func containsPathID(pathIDs []uint32, pathID uint32) bool {
	for _, allowed := range pathIDs {
		if allowed == pathID {
			return true
		}
	}
	return false
}

func containsAuthMode(modes []string, want string) bool {
	for _, mode := range modes {
		if mode == want {
			return true
		}
	}
	return false
}

func (s *Server) handleRequestV2(h wirev2.Header, payload []byte, sess *Session) (wirev2.ResponseHeader, []byte) {
	start := time.Now()
	resp := wirev2.ResponseHeader{
		Base: wirev2.Header{
			Op:         responseOpV2(h.Op),
			Flags:      h.Flags,
			RequestID:  h.RequestID,
			VolumeID:   sess.VolumeID,
			Generation: sess.Generation,
			SessionID:  sess.ID,
			SeqNo:      h.SeqNo,
		},
		StatusCode: 0,
		PathID:     s.cfg.PathID,
	}
	var out []byte
	logicalLength := uint64(h.LengthBytes)
	payloadBytes := len(payload)
	switch h.Op {
	case wirev2.OpRead:
		if err := s.validateLogicalLength(wirev1.OpRead, h.LengthBytes); err != nil {
			resp.StatusCode = wirev1.ErrInvalidRange
			break
		}
		data, err := s.svc.Read(context.Background(), sess.VolumeID, h.OffsetBytes, uint64(h.LengthBytes))
		if err != nil {
			resp.StatusCode = mapErrorV2(err)
			break
		}
		out = data
	case wirev2.OpWrite:
		wt, data, err := wirev1.DecodeWritePayload(payload)
		if err != nil {
			resp.StatusCode = wirev2.ErrBadAuthTag
			break
		}
		_ = wt
		logicalLength = uint64(len(data))
		if err := s.validateLogicalLength(wirev1.OpWrite, uint32(len(data))); err != nil {
			resp.StatusCode = wirev1.ErrInvalidRange
			break
		}
		if err := s.svc.Write(context.Background(), sess.VolumeID, h.OffsetBytes, uint64(len(data)), data); err != nil {
			resp.StatusCode = mapErrorV2(err)
			break
		}
	case wirev2.OpFlush:
		if err := s.svc.Flush(context.Background(), sess.VolumeID); err != nil {
			resp.StatusCode = mapErrorV2(err)
			break
		}
	case wirev2.OpDiscard:
		if err := s.validateLogicalLength(wirev1.OpDiscard, h.LengthBytes); err != nil {
			resp.StatusCode = wirev1.ErrInvalidRange
			break
		}
		if err := s.svc.Discard(context.Background(), sess.VolumeID, h.OffsetBytes, uint64(h.LengthBytes)); err != nil {
			resp.StatusCode = mapErrorV2(err)
			break
		}
	case wirev2.OpWriteZeroes:
		if err := s.validateLogicalLength(wirev1.OpWriteZeroes, h.LengthBytes); err != nil {
			resp.StatusCode = wirev1.ErrInvalidRange
			break
		}
		if err := s.svc.Zero(context.Background(), sess.VolumeID, h.OffsetBytes, uint64(h.LengthBytes)); err != nil {
			resp.StatusCode = mapErrorV2(err)
			break
		}
	case wirev2.OpHeartbeat:
	case wirev2.OpPathProbe, wirev2.OpGetVolumeInfo:
		out = wirev1.EncodePathCapabilityPayload(s.pathCapability())
	default:
		resp.StatusCode = wirev2.ErrBadAuthTag
	}
	resp.BackendLatencyUS = uint32(time.Since(start).Microseconds())
	resp.GatewayLatencyUS = resp.BackendLatencyUS
	s.traceRequestCompleted(h.Op, 2, h.RequestID, sess.VolumeID, sess.Generation, h.OffsetBytes, logicalLength, payloadBytes, resp.StatusCode, resp.BackendLatencyUS, resp.GatewayLatencyUS)
	return resp, out
}

func responseOpV2(op uint32) uint32 {
	switch op {
	case wirev2.OpRead:
		return wirev1.OpReadResp
	case wirev2.OpWrite:
		return wirev1.OpWriteResp
	case wirev2.OpFlush:
		return wirev1.OpFlushResp
	case wirev2.OpDiscard:
		return wirev1.OpDiscardResp
	case wirev2.OpWriteZeroes:
		return wirev1.OpWriteZeroesResp
	default:
		return op
	}
}

func mapErrorV2(err error) int32 {
	if status, ok := mapSBSError(err); ok {
		return status
	}
	switch {
	case errors.Is(err, service.ErrVolumeNotFound):
		return wirev1.ErrNoSuchVolume
	case errors.Is(err, service.ErrBadAlignment), errors.Is(err, service.ErrDiscardAlignment), errors.Is(err, service.ErrOutOfRange), errors.Is(err, service.ErrBadDataLength):
		return wirev1.ErrInvalidRange
	case errors.Is(err, service.ErrAttachConflict), errors.Is(err, service.ErrDetachConflict), errors.Is(err, service.ErrHostIDRequired):
		return wirev1.ErrUnauthorized
	default:
		return int32(14)
	}
}

func (s *Server) sendAuthErr(conn net.Conn, code int32) {
	r := &wirev2.ResponseHeader{
		Base:       wirev2.Header{Op: wirev2.OpAuthErr},
		StatusCode: code,
	}
	_ = wirev2.WriteResponseFrame(conn, r, nil, nil)
}

func mustRandomBase64URL(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		fallback := fmt.Sprintf("fallback-%d-%d", time.Now().UnixNano(), os.Getpid())
		return base64.RawURLEncoding.EncodeToString([]byte(fallback))
	}
	return base64.RawURLEncoding.EncodeToString(b)
}

func (s *Server) handleRequest(h wirev1.Header, payload []byte) (wirev1.ResponseHeader, []byte) {
	start := time.Now()
	resp := wirev1.ResponseHeader{
		Base: wirev1.Header{
			Op:         responseOp(h.Op),
			Flags:      h.Flags,
			RequestID:  h.RequestID,
			VolumeID:   h.VolumeID,
			Generation: h.Generation,
		},
		StatusCode: wirev1.StatusOK,
		PathID:     s.cfg.PathID,
	}
	var out []byte
	var requestErr error
	logicalLength := uint64(h.LengthBytes)
	payloadBytes := len(payload)

	switch h.Op {
	case wirev1.OpRead:
		if err := s.validateLogicalLength(h.Op, h.LengthBytes); err != nil {
			requestErr = err
			resp.StatusCode = wirev1.ErrInvalidRange
			break
		}
		data, err := s.svc.Read(context.Background(), h.VolumeID, h.OffsetBytes, uint64(h.LengthBytes))
		if err != nil {
			requestErr = err
			resp.StatusCode = mapError(err)
			break
		}
		out = data
	case wirev1.OpWrite:
		_, data, err := wirev1.DecodeWritePayload(payload)
		if err != nil {
			requestErr = err
			resp.StatusCode = wirev1.ErrInternal
			break
		}
		logicalLength = uint64(len(data))
		if err := s.validateLogicalLength(h.Op, uint32(len(data))); err != nil {
			requestErr = err
			resp.StatusCode = wirev1.ErrInvalidRange
			break
		}
		if err := s.svc.Write(context.Background(), h.VolumeID, h.OffsetBytes, uint64(len(data)), data); err != nil {
			requestErr = err
			resp.StatusCode = mapError(err)
			break
		}
	case wirev1.OpFlush:
		if err := s.svc.Flush(context.Background(), h.VolumeID); err != nil {
			requestErr = err
			resp.StatusCode = mapError(err)
			break
		}
	case wirev1.OpDiscard:
		if err := s.validateLogicalLength(h.Op, h.LengthBytes); err != nil {
			requestErr = err
			resp.StatusCode = wirev1.ErrInvalidRange
			break
		}
		if err := s.svc.Discard(context.Background(), h.VolumeID, h.OffsetBytes, uint64(h.LengthBytes)); err != nil {
			requestErr = err
			resp.StatusCode = mapError(err)
			break
		}
	case wirev1.OpWriteZeroes:
		if err := s.validateLogicalLength(h.Op, h.LengthBytes); err != nil {
			requestErr = err
			resp.StatusCode = wirev1.ErrInvalidRange
			break
		}
		if err := s.svc.Zero(context.Background(), h.VolumeID, h.OffsetBytes, uint64(h.LengthBytes)); err != nil {
			requestErr = err
			resp.StatusCode = mapError(err)
			break
		}
	case wirev1.OpHeartbeat:
	case wirev1.OpPathProbe, wirev1.OpGetVolumeInfo:
		out = wirev1.EncodePathCapabilityPayload(s.pathCapability())
	default:
		requestErr = fmt.Errorf("unsupported dataplane opcode %d", h.Op)
		resp.StatusCode = wirev1.ErrInternal
	}

	resp.BackendLatencyUS = uint32(time.Since(start).Microseconds())
	resp.GatewayLatencyUS = resp.BackendLatencyUS
	if resp.StatusCode != wirev1.StatusOK {
		structuredlog.Error("gateway.dataplane", "dataplane_request_failed", requestErr,
			structuredlog.F("gateway_id", s.cfg.GatewayID),
			structuredlog.F("path_id", s.cfg.PathID),
			structuredlog.F("op", h.Op),
			structuredlog.F("request_id", h.RequestID),
			structuredlog.F("volume_id", fmt.Sprintf("%08x", h.VolumeID)),
			structuredlog.F("generation", h.Generation),
			structuredlog.F("offset_bytes", h.OffsetBytes),
			structuredlog.F("length_bytes", h.LengthBytes),
			structuredlog.F("payload_bytes", len(payload)),
			structuredlog.F("status_code", resp.StatusCode),
			structuredlog.F("status_name", wireStatusName(resp.StatusCode)),
			structuredlog.F("backend_latency_us", resp.BackendLatencyUS),
		)
	}
	s.traceRequestCompleted(h.Op, 1, h.RequestID, h.VolumeID, h.Generation, h.OffsetBytes, logicalLength, payloadBytes, resp.StatusCode, resp.BackendLatencyUS, resp.GatewayLatencyUS)
	return resp, out
}

func (s *Server) traceRequestCompleted(op uint32, wireVersion int, requestID, volumeID, generation, offsetBytes, lengthBytes uint64, payloadBytes int, statusCode int32, backendLatencyUS, gatewayLatencyUS uint32) {
	if !s.cfg.TraceCompletedRequests || statusCode != wirev1.StatusOK {
		return
	}
	structuredlog.Info("gateway.dataplane", "dataplane_request_completed",
		structuredlog.F("gateway_id", s.cfg.GatewayID),
		structuredlog.F("path_id", s.cfg.PathID),
		structuredlog.F("wire_version", wireVersion),
		structuredlog.F("op", dataplaneOpName(op)),
		structuredlog.F("request_id", requestID),
		structuredlog.F("volume_id", fmt.Sprintf("%08x", volumeID)),
		structuredlog.F("generation", generation),
		structuredlog.F("offset_bytes", offsetBytes),
		structuredlog.F("length_bytes", lengthBytes),
		structuredlog.F("payload_bytes", payloadBytes),
		structuredlog.F("status_code", statusCode),
		structuredlog.F("status_name", wireStatusName(statusCode)),
		structuredlog.F("backend_latency_us", backendLatencyUS),
		structuredlog.F("gateway_latency_us", gatewayLatencyUS),
	)
}

func supportedOpsMask() uint64 {
	var v uint64
	for _, op := range []uint32{
		wirev1.OpRead,
		wirev1.OpWrite,
		wirev1.OpFlush,
		wirev1.OpDiscard,
		wirev1.OpWriteZeroes,
		wirev1.OpHeartbeat,
		wirev1.OpPathProbe,
		wirev1.OpGetVolumeInfo,
	} {
		v |= 1 << op
	}
	return v
}

func responseOp(op uint32) uint32 {
	switch op {
	case wirev1.OpRead:
		return wirev1.OpReadResp
	case wirev1.OpWrite:
		return wirev1.OpWriteResp
	case wirev1.OpFlush:
		return wirev1.OpFlushResp
	case wirev1.OpDiscard:
		return wirev1.OpDiscardResp
	case wirev1.OpWriteZeroes:
		return wirev1.OpWriteZeroesResp
	default:
		return op
	}
}

func dataplaneOpName(op uint32) string {
	switch op {
	case wirev1.OpRead:
		return "read"
	case wirev1.OpWrite:
		return "write"
	case wirev1.OpFlush:
		return "flush"
	case wirev1.OpDiscard:
		return "discard"
	case wirev1.OpWriteZeroes:
		return "write_zeroes"
	case wirev1.OpHeartbeat:
		return "heartbeat"
	case wirev1.OpPathProbe:
		return "path_probe"
	case wirev1.OpGetVolumeInfo:
		return "get_volume_info"
	case wirev2.OpHello:
		return "hello"
	case wirev2.OpHelloAck:
		return "hello_ack"
	case wirev2.OpAuthErr:
		return "auth_err"
	default:
		return fmt.Sprintf("op_%d", op)
	}
}

func mapError(err error) int32 {
	if status, ok := mapSBSError(err); ok {
		return status
	}
	switch {
	case errors.Is(err, service.ErrVolumeNotFound):
		return wirev1.ErrNoSuchVolume
	case errors.Is(err, service.ErrBadAlignment), errors.Is(err, service.ErrDiscardAlignment), errors.Is(err, service.ErrOutOfRange), errors.Is(err, service.ErrBadDataLength):
		return wirev1.ErrInvalidRange
	case errors.Is(err, service.ErrAttachConflict), errors.Is(err, service.ErrDetachConflict), errors.Is(err, service.ErrHostIDRequired):
		return wirev1.ErrUnauthorized
	default:
		return wirev1.ErrInternal
	}
}

func wireStatusName(status int32) string {
	switch status {
	case wirev1.StatusOK:
		return "ok"
	case wirev1.ErrBadMagic:
		return "bad_magic"
	case wirev1.ErrUnsupportedVersion:
		return "unsupported_version"
	case wirev1.ErrUnauthorized:
		return "unauthorized"
	case wirev1.ErrNoSuchVolume:
		return "no_such_volume"
	case wirev1.ErrGenerationMismatch:
		return "generation_mismatch"
	case wirev1.ErrInvalidRange:
		return "invalid_range"
	case wirev1.ErrPathDraining:
		return "path_draining"
	case wirev1.ErrNoHealthyReplica:
		return "no_healthy_replica"
	case wirev1.ErrQuorumFailed:
		return "quorum_failed"
	case wirev1.ErrTimeout:
		return "timeout"
	case wirev1.ErrRetryable:
		return "retryable"
	case wirev1.ErrBusy:
		return "busy"
	case wirev1.ErrChecksum:
		return "checksum"
	case wirev1.ErrInternal:
		return "internal"
	default:
		return "unknown"
	}
}

func mapSBSError(err error) (int32, bool) {
	var sbsErr *service.SBSError
	if !errors.As(err, &sbsErr) {
		return 0, false
	}
	switch sbsErr.Code {
	case service.SBSErrorCodeNotFound:
		return wirev1.ErrNoSuchVolume, true
	case service.SBSErrorCodeBadRequest, service.SBSErrorCodeIdempotencyConflict:
		return wirev1.ErrInvalidRange, true
	case service.SBSErrorCodeStaleGeneration:
		return wirev1.ErrGenerationMismatch, true
	case service.SBSErrorCodeAttachmentMismatch:
		return wirev1.ErrUnauthorized, true
	case service.SBSErrorCodeUnavailable:
		if sbsErr.Retryable {
			return wirev1.ErrRetryable, true
		}
		return wirev1.ErrNoHealthyReplica, true
	case service.SBSErrorCodeTimeout:
		return wirev1.ErrTimeout, true
	case service.SBSErrorCodeInternal:
		return wirev1.ErrInternal, true
	default:
		return wirev1.ErrInternal, true
	}
}

func ParsePathCapability(payload []byte) (wirev1.PathCapability, error) {
	return wirev1.DecodePathCapabilityPayload(payload)
}

func EncodeZeroWriteTag() wirev1.WriteTag {
	return wirev1.WriteTag{}
}

func DecodeUint64LE(b []byte) uint64 {
	return binary.LittleEndian.Uint64(b)
}
