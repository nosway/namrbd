package wirev1

import (
	"net"
	"testing"
)

func TestConnReadWriteFrame(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()

	server := NewConn(a, 1<<20)
	client := NewConn(b, 1<<20)

	done := make(chan error, 1)
	go func() {
		h := NewRequestHeader(OpWrite, 77, 11, 9, 4096, FlagChecksum)
		done <- client.WriteFrame(h, []byte("payload"))
	}()

	h, p, err := server.ReadFrame()
	if err != nil {
		t.Fatalf("ReadFrame failed: %v", err)
	}
	if h.RequestID != 77 || string(p) != "payload" {
		t.Fatalf("unexpected frame: header=%+v payload=%q", h, p)
	}
	if err := <-done; err != nil {
		t.Fatalf("WriteFrame failed: %v", err)
	}
}

func TestConnWriteFrame_TooLarge(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()

	c := NewConn(a, 2)
	if err := c.WriteFrame(NewRequestHeader(OpWrite, 1, 1, 1, 0, 0), []byte("abc")); err == nil {
		t.Fatalf("expected frame-too-large error")
	}
}
