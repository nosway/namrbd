package wirev1

import (
	"errors"
	"fmt"
	"io"
)

type Conn struct {
	rw    io.ReadWriter
	dec   *StreamDecoder
	rbuf  []byte
	wbuf  []byte
	limit uint32
}

func NewConn(rw io.ReadWriter, maxPayloadLen uint32) *Conn {
	return &Conn{
		rw:    rw,
		dec:   NewStreamDecoder(maxPayloadLen),
		rbuf:  make([]byte, 64*1024),
		limit: maxPayloadLen,
	}
}

func (c *Conn) WriteFrame(h Header, payload []byte) error {
	if uint32(len(payload)) > c.limit {
		return fmt.Errorf("%w: got=%d limit=%d", ErrFrameTooLarge, len(payload), c.limit)
	}
	wire, err := EncodeMessage(h, payload)
	if err != nil {
		return err
	}
	c.wbuf = wire
	for len(c.wbuf) > 0 {
		n, werr := c.rw.Write(c.wbuf)
		if n > 0 {
			c.wbuf = c.wbuf[n:]
		}
		if werr != nil {
			return werr
		}
	}
	return nil
}

func (c *Conn) ReadFrame() (Header, []byte, error) {
	for {
		h, payload, ok, err := c.dec.Next()
		if err != nil {
			return Header{}, nil, err
		}
		if ok {
			return h, payload, nil
		}

		n, rerr := c.rw.Read(c.rbuf)
		if n > 0 {
			c.dec.Feed(c.rbuf[:n])
			continue
		}
		if rerr != nil {
			if errors.Is(rerr, io.EOF) && c.dec.Buffered() > 0 {
				return Header{}, nil, ErrShortMessage
			}
			return Header{}, nil, rerr
		}
	}
}
