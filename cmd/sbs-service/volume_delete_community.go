//go:build !enterprise

package main

import "context"

func (s *server) validateEnterpriseVolumeDeletion(context.Context, string) error {
	return nil
}
