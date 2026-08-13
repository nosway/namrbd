//go:build linux

package netlinkclient

import (
	"encoding/binary"
	"fmt"
	"sync/atomic"
	"syscall"

	"github.com/nosway/namrbd/control/netlinktlv"
)

const (
	nlmsgHdrLen = 16

	genlIDCtrl uint16 = 0x10

	nlmsgNoop  uint16 = 1
	nlmsgError uint16 = 2
	nlmsgDone  uint16 = 3

	nlmFRequest uint16 = 0x0001
	nlmFAck     uint16 = 0x0004

	ctrlCmdGetFamily uint8 = 3

	ctrlAttrFamilyID   uint16 = 1
	ctrlAttrFamilyName uint16 = 2
)

type linuxClient struct {
	fd       int
	seq      uint32
	familyID uint16
}

func Dial() (Client, error) {
	fd, err := syscall.Socket(syscall.AF_NETLINK, syscall.SOCK_RAW, syscall.NETLINK_GENERIC)
	if err != nil {
		return nil, err
	}
	addr := &syscall.SockaddrNetlink{Family: syscall.AF_NETLINK}
	if err := syscall.Bind(fd, addr); err != nil {
		syscall.Close(fd)
		return nil, err
	}
	c := &linuxClient{fd: fd}
	fid, err := c.resolveFamily(netlinktlv.FamilyName)
	if err != nil {
		syscall.Close(fd)
		return nil, err
	}
	c.familyID = fid
	return c, nil
}

func (c *linuxClient) Close() error {
	return syscall.Close(c.fd)
}

func (c *linuxClient) CreateDevice() (netlinktlv.CreateDeviceResponse, error) {
	payload, err := c.request(c.familyID, netlinktlv.CmdCreateDevice, netlinktlv.FamilyVersion, nil, true)
	if err != nil {
		return netlinktlv.CreateDeviceResponse{}, err
	}
	return netlinktlv.DecodeCreateDeviceResponse(payload)
}

func (c *linuxClient) DestroyDevice(deviceID uint32) error {
	attr, err := netlinktlv.EncodeDestroyDeviceRequest(netlinktlv.DestroyDeviceRequest{DeviceID: deviceID})
	if err != nil {
		return err
	}
	_, err = c.request(c.familyID, netlinktlv.CmdDestroyDevice, netlinktlv.FamilyVersion, attr, false)
	return err
}

func (c *linuxClient) ConfigREST(req netlinktlv.ConfigRESTRequest) error {
	attr, err := netlinktlv.EncodeConfigREST(req)
	if err != nil {
		return err
	}
	_, err = c.request(c.familyID, netlinktlv.CmdConfigREST, netlinktlv.FamilyVersion, attr, false)
	return err
}

func (c *linuxClient) AttachVolume(req netlinktlv.AttachRequest) error {
	attr, err := netlinktlv.EncodeAttachRequest(req)
	if err != nil {
		return err
	}
	_, err = c.request(c.familyID, netlinktlv.CmdAttach, netlinktlv.FamilyVersion, attr, false)
	return err
}

func (c *linuxClient) DetachVolume(req netlinktlv.DetachRequest) error {
	attr, err := netlinktlv.EncodeDetachRequest(req)
	if err != nil {
		return err
	}
	_, err = c.request(c.familyID, netlinktlv.CmdDetach, netlinktlv.FamilyVersion, attr, false)
	return err
}

func (c *linuxClient) AttachManifest(req netlinktlv.AttachManifestRequest) error {
	attr, err := netlinktlv.EncodeAttachManifestRequest(req)
	if err != nil {
		return err
	}
	_, err = c.request(c.familyID, netlinktlv.CmdAttachManifest, netlinktlv.FamilyVersion, attr, false)
	return err
}

func (c *linuxClient) ReconfigureDataPaths(req netlinktlv.AttachManifestRequest) error {
	attr, err := netlinktlv.EncodeAttachManifestRequest(req)
	if err != nil {
		return err
	}
	_, err = c.request(c.familyID, netlinktlv.CmdReconfigureDataPaths, netlinktlv.FamilyVersion, attr, false)
	return err
}

func (c *linuxClient) DetachLocal(req netlinktlv.DetachLocalRequest) error {
	attr, err := netlinktlv.EncodeDetachLocalRequest(req)
	if err != nil {
		return err
	}
	_, err = c.request(c.familyID, netlinktlv.CmdDetachLocal, netlinktlv.FamilyVersion, attr, false)
	return err
}

func (c *linuxClient) UpdatePathPlan(req netlinktlv.UpdatePathPlanRequest) error {
	attr, err := netlinktlv.EncodeUpdatePathPlanRequest(req)
	if err != nil {
		return err
	}
	_, err = c.request(c.familyID, netlinktlv.CmdUpdatePathPlan, netlinktlv.FamilyVersion, attr, false)
	return err
}

func (c *linuxClient) ResizeDevice(req netlinktlv.ResizeDeviceRequest) error {
	attr, err := netlinktlv.EncodeResizeDeviceRequest(req)
	if err != nil {
		return err
	}
	_, err = c.request(c.familyID, netlinktlv.CmdResizeDevice, netlinktlv.FamilyVersion, attr, false)
	return err
}

func (c *linuxClient) GetStatus(deviceID uint32) (netlinktlv.DeviceStatus, error) {
	attr, err := netlinktlv.EncodeGetStatusRequest(deviceID)
	if err != nil {
		return netlinktlv.DeviceStatus{}, err
	}
	payload, err := c.request(c.familyID, netlinktlv.CmdGetStatus, netlinktlv.FamilyVersion, attr, true)
	if err != nil {
		return netlinktlv.DeviceStatus{}, err
	}
	return netlinktlv.DecodeDeviceStatus(payload)
}

func (c *linuxClient) ListDevices() ([]netlinktlv.DeviceStatus, error) {
	payload, err := c.request(c.familyID, netlinktlv.CmdListDevices, netlinktlv.FamilyVersion, nil, true)
	if err != nil {
		return nil, err
	}
	resp, err := netlinktlv.DecodeListDevices(payload)
	if err != nil {
		return nil, err
	}
	return resp.Devices, nil
}

func (c *linuxClient) resolveFamily(name string) (uint16, error) {
	nameAttr := encodeAttr(ctrlAttrFamilyName, append([]byte(name), 0))
	payload, err := c.request(genlIDCtrl, ctrlCmdGetFamily, 1, nameAttr, true)
	if err != nil {
		return 0, err
	}
	attrs, err := parseAttrs(payload)
	if err != nil {
		return 0, err
	}
	raw, ok := attrs[ctrlAttrFamilyID]
	if !ok || len(raw) != 2 {
		return 0, fmt.Errorf("family %q not found", name)
	}
	return binary.LittleEndian.Uint16(raw), nil
}

func (c *linuxClient) request(nlmsgType uint16, cmd uint8, version uint8, attrs []byte, expectReply bool) ([]byte, error) {
	seq := atomic.AddUint32(&c.seq, 1)
	flags := nlmFRequest | nlmFAck

	genl := []byte{cmd, version, 0, 0}
	payload := append(genl, attrs...)
	hdr := make([]byte, nlmsgHdrLen)
	binary.LittleEndian.PutUint32(hdr[0:4], uint32(nlmsgHdrLen+len(payload)))
	binary.LittleEndian.PutUint16(hdr[4:6], nlmsgType)
	binary.LittleEndian.PutUint16(hdr[6:8], flags)
	binary.LittleEndian.PutUint32(hdr[8:12], seq)
	binary.LittleEndian.PutUint32(hdr[12:16], uint32(syscall.Getpid()))

	wire := append(hdr, payload...)
	if err := syscall.Sendto(c.fd, wire, 0, &syscall.SockaddrNetlink{Family: syscall.AF_NETLINK}); err != nil {
		return nil, err
	}

	var replyPayload []byte
	var sawAck bool

	for {
		msgs, err := c.recv()
		if err != nil {
			return nil, err
		}
		for _, m := range msgs {
			if m.nlmsgSeq != seq {
				continue
			}
			if m.nlmsgType == nlmsgError {
				if m.errno == 0 {
					sawAck = true
					continue
				}
				return nil, syscall.Errno(-m.errno)
			}
			if len(m.payload) >= 4 {
				replyPayload = append([]byte(nil), m.payload[4:]...)
			}
		}

		if expectReply {
			if replyPayload != nil {
				return replyPayload, nil
			}
			if sawAck {
				return nil, fmt.Errorf("no reply payload received")
			}
			continue
		}

		if sawAck {
			return nil, nil
		}
	}
}

type recvMsg struct {
	nlmsgType uint16
	nlmsgSeq  uint32
	errno     int32
	payload   []byte
}

func (c *linuxClient) recv() ([]recvMsg, error) {
	buf := make([]byte, 32*1024)
	n, _, err := syscall.Recvfrom(c.fd, buf, 0)
	if err != nil {
		return nil, err
	}
	buf = buf[:n]
	var out []recvMsg
	for len(buf) >= nlmsgHdrLen {
		l := int(binary.LittleEndian.Uint32(buf[0:4]))
		t := binary.LittleEndian.Uint16(buf[4:6])
		seq := binary.LittleEndian.Uint32(buf[8:12])
		if l < nlmsgHdrLen || l > len(buf) {
			return nil, fmt.Errorf("bad nlmsg length=%d available=%d", l, len(buf))
		}
		msg := buf[:l]
		payload := msg[nlmsgHdrLen:]
		r := recvMsg{nlmsgType: t, nlmsgSeq: seq}
		if t == nlmsgError {
			if len(payload) < 4 {
				return nil, fmt.Errorf("short NLMSG_ERROR")
			}
			r.errno = int32(binary.LittleEndian.Uint32(payload[:4]))
		} else if t != nlmsgNoop && t != nlmsgDone {
			r.payload = append([]byte(nil), payload...)
		}
		out = append(out, r)
		buf = buf[align4(l):]
	}
	return out, nil
}

func parseAttrs(data []byte) (map[uint16][]byte, error) {
	out := make(map[uint16][]byte)
	i := 0
	for i < len(data) {
		if len(data)-i < 4 {
			return nil, fmt.Errorf("short attr header")
		}
		l := int(binary.LittleEndian.Uint16(data[i : i+2]))
		t := binary.LittleEndian.Uint16(data[i+2 : i+4])
		if l < 4 || i+l > len(data) {
			return nil, fmt.Errorf("bad attr len=%d", l)
		}
		out[t] = append([]byte(nil), data[i+4:i+l]...)
		i += align4(l)
	}
	return out, nil
}

func encodeAttr(typ uint16, payload []byte) []byte {
	l := 4 + len(payload)
	b := make([]byte, align4(l))
	binary.LittleEndian.PutUint16(b[0:2], uint16(l))
	binary.LittleEndian.PutUint16(b[2:4], typ)
	copy(b[4:], payload)
	return b
}

func align4(n int) int {
	return (n + 3) &^ 3
}
