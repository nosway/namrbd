package wirev2

import (
	"encoding/binary"
	"fmt"
	"io"
)

const (
	// MinFrameLen is the minimum frame size (header only, no payload/auth).
	MinFrameLen = HeaderSizeV2
)

// ReadFrame reads one wire v2 frame from r (header + payload + auth_tag) and verifies header CRC.
func ReadFrame(r io.Reader, maxPayloadLen uint32) (h Header, payload []byte, tag []byte, err error) {
	headerBuf := make([]byte, HeaderSizeV2)
	if _, err = io.ReadFull(r, headerBuf); err != nil {
		return Header{}, nil, nil, err
	}
	h = parseHeader(headerBuf)
	if h.Magic != MagicV2 || h.Version != VersionV2 {
		return h, nil, nil, ErrBadMagicValue
	}
	if h.LengthBytes > maxPayloadLen {
		return h, nil, nil, fmt.Errorf("%w: length=%d max=%d", ErrFrameTooLarge, h.LengthBytes, maxPayloadLen)
	}
	tailLen := int(h.LengthBytes) + int(h.AuthLen)
	var payloadAndTag []byte
	if tailLen > 0 {
		payloadAndTag = make([]byte, tailLen)
		if _, err = io.ReadFull(r, payloadAndTag); err != nil {
			return h, nil, nil, err
		}
	}
	fullFrame := append(headerBuf, payloadAndTag...)
	h2, p2, t2, err := DecodeMessage(fullFrame)
	if err != nil {
		return h, nil, nil, err
	}
	h = h2
	payload = p2
	if len(t2) > 0 {
		tag = t2
	}
	return h, payload, tag, nil
}

// WriteFrame writes one wire v2 frame to w.
func WriteFrame(w io.Writer, h *Header, payload []byte, tag []byte) error {
	frame, err := EncodeMessage(h, payload, tag)
	if err != nil {
		return err
	}
	_, err = w.Write(frame)
	return err
}

// ErrFrameTooLarge is returned when payload length exceeds limit.
var ErrFrameTooLarge = fmt.Errorf("frame too large")

// WriteResponseFrame writes a response frame (response header + payload + optional auth_tag).
func WriteResponseFrame(w io.Writer, r *ResponseHeader, payload []byte, tag []byte) error {
	frame, err := EncodeResponseMessage(r, payload, tag)
	if err != nil {
		return err
	}
	_, err = w.Write(frame)
	return err
}

// ParseVersion peeks at the first 6 bytes (magic + version) to detect wire version.
// Returns 1 or 2 and the 6 bytes consumed, or 0 and error.
func ParseVersion(r io.Reader) (version uint16, first6 []byte, err error) {
	first6 = make([]byte, 6)
	if _, err = io.ReadFull(r, first6); err != nil {
		return 0, nil, err
	}
	magic := binary.LittleEndian.Uint32(first6[0:4])
	if magic != MagicV2 {
		return 0, first6, ErrBadMagicValue
	}
	version = binary.LittleEndian.Uint16(first6[4:6])
	return version, first6, nil
}
