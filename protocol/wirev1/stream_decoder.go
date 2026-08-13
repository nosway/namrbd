package wirev1

import "fmt"

var ErrFrameTooLarge = fmt.Errorf("frame too large")

// StreamDecoder incrementally parses wire messages from a byte stream.
type StreamDecoder struct {
	buf           []byte
	maxPayloadLen uint32
}

func NewStreamDecoder(maxPayloadLen uint32) *StreamDecoder {
	return &StreamDecoder{maxPayloadLen: maxPayloadLen}
}

func (d *StreamDecoder) Feed(chunk []byte) {
	if len(chunk) == 0 {
		return
	}
	d.buf = append(d.buf, chunk...)
}

// Next returns one complete message if available.
// ok=false means more bytes are needed.
func (d *StreamDecoder) Next() (h Header, payload []byte, ok bool, err error) {
	if len(d.buf) < int(HeaderSize) {
		return Header{}, nil, false, nil
	}

	probe := parseHeader(d.buf[:HeaderSize])
	if probe.Magic != Magic {
		// Attempt resync by skipping one byte.
		d.buf = d.buf[1:]
		return Header{}, nil, false, ErrBadMagicValue
	}
	if probe.Version != VersionV1 {
		d.buf = d.buf[1:]
		return Header{}, nil, false, ErrBadVersionValue
	}
	if probe.HeaderLen != HeaderSize {
		d.buf = d.buf[1:]
		return Header{}, nil, false, ErrBadHeaderLen
	}
	if !opUsesLengthAsRangeWithoutRequestPayload(probe.Op) &&
		probe.LengthBytes > d.maxPayloadLen {
		return Header{}, nil, false, fmt.Errorf("%w: got=%d limit=%d", ErrFrameTooLarge, probe.LengthBytes, d.maxPayloadLen)
	}

	frameLen := int(HeaderSize) + int(probe.LengthBytes)
	if opUsesLengthAsRangeWithoutRequestPayload(probe.Op) {
		frameLen = int(HeaderSize)
	}
	if len(d.buf) < frameLen {
		return Header{}, nil, false, nil
	}

	frame := d.buf[:frameLen]
	d.buf = d.buf[frameLen:]
	h, payload, err = DecodeMessage(frame)
	if err != nil {
		return Header{}, nil, false, err
	}
	return h, payload, true, nil
}

func (d *StreamDecoder) Buffered() int {
	return len(d.buf)
}
