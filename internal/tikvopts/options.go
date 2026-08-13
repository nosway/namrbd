package tikvopts

import (
	"errors"
	"fmt"
	"time"
)

type APIVersion string

const (
	APIVersionV1    APIVersion = "v1"
	APIVersionV1TTL APIVersion = "v1ttl"
	APIVersionV2    APIVersion = "v2"
)

type Options struct {
	PDEndpoints []string
	Timeout     time.Duration
	APIVersion  APIVersion
	Keyspace    string
	TLS         TLSSecurity
}

type TLSSecurity struct {
	Enabled  bool
	CAPath   string
	CertPath string
	KeyPath  string
}

func Validate(opts Options) error {
	if len(opts.PDEndpoints) == 0 {
		return errors.New("tikv pd endpoints are required")
	}
	switch opts.APIVersion {
	case "", APIVersionV1, APIVersionV1TTL:
	case APIVersionV2:
		if opts.TLS.Enabled && (opts.TLS.CAPath == "" || opts.TLS.CertPath == "" || opts.TLS.KeyPath == "") {
			return errors.New("tikv tls requires ca, cert, and key paths")
		}
	default:
		return fmt.Errorf("invalid tikv api version %q: must be v1, v1ttl, or v2", opts.APIVersion)
	}
	if opts.TLS.Enabled && (opts.TLS.CAPath == "" || opts.TLS.CertPath == "" || opts.TLS.KeyPath == "") {
		return errors.New("tikv tls requires ca, cert, and key paths")
	}
	return nil
}
