package wirev1

import (
	"context"
	"errors"
	"testing"
)

func TestMuxHelloHandler(t *testing.T) {
	m := NewMux()
	m.RegisterHello(0x1001, func(ctx context.Context, hdr Header, body Hello) ([]byte, error) {
		if body.HostID != 7 {
			t.Fatalf("unexpected host_id=%d", body.HostID)
		}
		return []byte("ok"), nil
	})

	h := NewRequestHeader(0x1001, 1, 2, 3, 0, 0)
	out, err := m.Handle(context.Background(), Frame{
		Header:  h,
		Payload: EncodeHelloPayload(Hello{HostID: 7}),
	})
	if err != nil {
		t.Fatalf("Handle failed: %v", err)
	}
	if string(out) != "ok" {
		t.Fatalf("unexpected handler output=%q", out)
	}
}

func TestMuxPathCapabilityHandler(t *testing.T) {
	m := NewMux()
	called := false
	m.RegisterPathCapability(0x1002, func(ctx context.Context, hdr Header, body PathCapability) ([]byte, error) {
		called = true
		if body.MaxIOSize != 4096 {
			t.Fatalf("unexpected MaxIOSize=%d", body.MaxIOSize)
		}
		return nil, nil
	})

	_, err := m.Handle(context.Background(), Frame{
		Header:  NewRequestHeader(0x1002, 1, 1, 1, 0, 0),
		Payload: EncodePathCapabilityPayload(PathCapability{MaxIOSize: 4096}),
	})
	if err != nil {
		t.Fatalf("Handle failed: %v", err)
	}
	if !called {
		t.Fatalf("handler was not called")
	}
}

func TestMuxErrorDetailHandler(t *testing.T) {
	m := NewMux()
	sentinel := errors.New("expected")
	m.RegisterErrorDetail(0x1003, func(ctx context.Context, hdr Header, body ErrorDetail) ([]byte, error) {
		if body.DetailCode != 99 {
			t.Fatalf("unexpected DetailCode=%d", body.DetailCode)
		}
		return nil, sentinel
	})

	payload, err := EncodeErrorDetailPayload(ErrorDetail{DetailCode: 99, Message: "m"})
	if err != nil {
		t.Fatalf("EncodeErrorDetailPayload failed: %v", err)
	}
	_, err = m.Handle(context.Background(), Frame{
		Header:  NewRequestHeader(0x1003, 1, 1, 1, 0, 0),
		Payload: payload,
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("expected sentinel error, got %v", err)
	}
}
