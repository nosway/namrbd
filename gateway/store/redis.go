//go:build legacy_redis
// +build legacy_redis

package store

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"
)

type RedisStore struct {
	Addr    string
	Timeout time.Duration
}

func NewRedisStore(addr string, timeout time.Duration) *RedisStore {
	return &RedisStore{Addr: addr, Timeout: timeout}
}

func (r *RedisStore) Get(ctx context.Context, key string) ([]byte, bool, error) {
	resp, err := r.call(ctx, "GET", key)
	if err != nil {
		return nil, false, err
	}
	if resp == nil {
		return nil, false, nil
	}
	return resp, true, nil
}

func (r *RedisStore) Set(ctx context.Context, key string, value []byte) error {
	_, err := r.call(ctx, "SET", key, string(value))
	return err
}

func (r *RedisStore) Put(ctx context.Context, key string, value []byte) error {
	return r.Set(ctx, key, value)
}

func (r *RedisStore) Delete(ctx context.Context, key string) error {
	_, err := r.call(ctx, "DEL", key)
	return err
}

func (r *RedisStore) List(ctx context.Context, prefix, cursor string, limit int) ([]string, string, error) {
	if limit <= 0 {
		limit = 128
	}
	if cursor == "" {
		cursor = "0"
	}
	resp, err := r.callValue(ctx, "SCAN", cursor, "MATCH", prefix+"*", "COUNT", strconv.Itoa(limit))
	if err != nil {
		return nil, "", err
	}
	if len(resp.Array) != 2 {
		return nil, "", fmt.Errorf("unexpected SCAN response length: %d", len(resp.Array))
	}
	nextCursor := string(resp.Array[0].Bulk)
	keys := make([]string, 0, len(resp.Array[1].Array))
	for _, item := range resp.Array[1].Array {
		keys = append(keys, string(item.Bulk))
	}
	if nextCursor == "0" {
		nextCursor = ""
	}
	return keys, nextCursor, nil
}

func (r *RedisStore) call(ctx context.Context, cmd string, args ...string) ([]byte, error) {
	value, err := r.callValue(ctx, cmd, args...)
	if err != nil {
		return nil, err
	}
	return value.Bytes(), nil
}

func (r *RedisStore) callValue(ctx context.Context, cmd string, args ...string) (respValue, error) {
	timeout := r.Timeout
	if timeout == 0 {
		timeout = 3 * time.Second
	}

	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", r.Addr)
	if err != nil {
		return respValue{}, err
	}
	defer conn.Close()

	_ = conn.SetDeadline(time.Now().Add(timeout))
	req := buildRESP(append([]string{cmd}, args...))
	if _, err := conn.Write(req); err != nil {
		return respValue{}, err
	}

	br := bufio.NewReader(conn)
	return parseRESPValue(br)
}

func buildRESP(parts []string) []byte {
	var b bytes.Buffer
	fmt.Fprintf(&b, "*%d\r\n", len(parts))
	for _, p := range parts {
		fmt.Fprintf(&b, "$%d\r\n%s\r\n", len(p), p)
	}
	return b.Bytes()
}

type respValue struct {
	kind  byte
	Bulk  []byte
	Array []respValue
}

func (v respValue) Bytes() []byte {
	switch v.kind {
	case '+', ':':
		return v.Bulk
	case '$':
		return v.Bulk
	default:
		return nil
	}
}

func parseRESPValue(r *bufio.Reader) (respValue, error) {
	t, err := r.ReadByte()
	if err != nil {
		return respValue{}, err
	}
	switch t {
	case '+':
		line, err := r.ReadString('\n')
		if err != nil {
			return respValue{}, err
		}
		return respValue{kind: '+', Bulk: []byte(trimCRLF(line))}, nil
	case '-':
		line, err := r.ReadString('\n')
		if err != nil {
			return respValue{}, err
		}
		return respValue{}, errors.New("redis error: " + strings.TrimSpace(line))
	case ':':
		line, err := r.ReadString('\n')
		if err != nil {
			return respValue{}, err
		}
		return respValue{kind: ':', Bulk: []byte(trimCRLF(line))}, nil
	case '$':
		line, err := r.ReadString('\n')
		if err != nil {
			return respValue{}, err
		}
		n, err := strconv.Atoi(trimCRLF(line))
		if err != nil {
			return respValue{}, err
		}
		if n == -1 {
			return respValue{kind: '$'}, nil
		}
		buf := make([]byte, n+2)
		if _, err := io.ReadFull(r, buf); err != nil {
			return respValue{}, err
		}
		return respValue{kind: '$', Bulk: buf[:n]}, nil
	case '*':
		line, err := r.ReadString('\n')
		if err != nil {
			return respValue{}, err
		}
		n, err := strconv.Atoi(trimCRLF(line))
		if err != nil {
			return respValue{}, err
		}
		if n < 0 {
			return respValue{kind: '*'}, nil
		}
		items := make([]respValue, 0, n)
		for i := 0; i < n; i++ {
			item, err := parseRESPValue(r)
			if err != nil {
				return respValue{}, err
			}
			items = append(items, item)
		}
		return respValue{kind: '*', Array: items}, nil
	default:
		return respValue{}, fmt.Errorf("unsupported redis response type: %q", t)
	}
}

func trimCRLF(s string) string {
	if len(s) >= 2 && s[len(s)-2:] == "\r\n" {
		return s[:len(s)-2]
	}
	return s
}
