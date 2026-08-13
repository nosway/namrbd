package wirev1

import (
	"context"
	"fmt"
	"sync"
)

type Frame struct {
	Header  Header
	Payload []byte
}

type FrameHandler func(ctx context.Context, f Frame) (respPayload []byte, err error)

// Mux dispatches decoded frames to opcode-specific handlers.
type Mux struct {
	mu       sync.RWMutex
	handlers map[uint32]FrameHandler
}

func NewMux() *Mux {
	return &Mux{handlers: make(map[uint32]FrameHandler)}
}

func (m *Mux) Handle(ctx context.Context, f Frame) ([]byte, error) {
	m.mu.RLock()
	h := m.handlers[f.Header.Op]
	m.mu.RUnlock()
	if h == nil {
		return nil, fmt.Errorf("no handler registered for opcode=0x%04X", f.Header.Op)
	}
	return h(ctx, f)
}

func (m *Mux) Register(op uint32, h FrameHandler) {
	m.mu.Lock()
	m.handlers[op] = h
	m.mu.Unlock()
}

type HelloHandler func(ctx context.Context, hdr Header, body Hello) ([]byte, error)

func (m *Mux) RegisterHello(op uint32, h HelloHandler) {
	m.Register(op, func(ctx context.Context, f Frame) ([]byte, error) {
		body, err := DecodeHelloPayload(f.Payload)
		if err != nil {
			return nil, err
		}
		return h(ctx, f.Header, body)
	})
}

type PathCapabilityHandler func(ctx context.Context, hdr Header, body PathCapability) ([]byte, error)

func (m *Mux) RegisterPathCapability(op uint32, h PathCapabilityHandler) {
	m.Register(op, func(ctx context.Context, f Frame) ([]byte, error) {
		body, err := DecodePathCapabilityPayload(f.Payload)
		if err != nil {
			return nil, err
		}
		return h(ctx, f.Header, body)
	})
}

type ErrorDetailHandler func(ctx context.Context, hdr Header, body ErrorDetail) ([]byte, error)

func (m *Mux) RegisterErrorDetail(op uint32, h ErrorDetailHandler) {
	m.Register(op, func(ctx context.Context, f Frame) ([]byte, error) {
		body, err := DecodeErrorDetailPayload(f.Payload)
		if err != nil {
			return nil, err
		}
		return h(ctx, f.Header, body)
	})
}
