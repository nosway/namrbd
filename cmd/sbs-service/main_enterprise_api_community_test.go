//go:build !enterprise

package main

import (
	"context"
	"testing"

	adminv1 "github.com/nosway/namrbd/sbs/admin/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestCommunityEnterpriseLifecycleRPCsRemainUnimplemented(t *testing.T) {
	ctx := context.Background()
	srv := &server{}
	tests := []struct {
		name string
		call func() error
	}{
		{name: "backup", call: func() error {
			_, err := srv.CreateBackupTarget(ctx, &adminv1.CreateBackupTargetRequest{})
			return err
		}},
		{name: "disaster recovery", call: func() error {
			_, err := srv.CreateDRReplicationLink(ctx, &adminv1.CreateDRReplicationLinkRequest{})
			return err
		}},
		{name: "nvme export", call: func() error {
			_, err := srv.CreateNVMeExport(ctx, &adminv1.CreateNVMeExportRequest{})
			return err
		}},
		{name: "persistent reservation", call: func() error {
			_, err := srv.RegisterPersistentReservation(ctx, &adminv1.RegisterPersistentReservationRequest{})
			return err
		}},
		{name: "tiering", call: func() error {
			_, err := srv.CreateTieringTarget(ctx, &adminv1.CreateTieringTargetRequest{})
			return err
		}},
		{name: "compression", call: func() error {
			_, err := srv.CreateCompressionPolicy(ctx, &adminv1.CreateCompressionPolicyRequest{})
			return err
		}},
		{name: "WORM", call: func() error {
			_, err := srv.RegisterWORMSubject(ctx, &adminv1.RegisterWORMSubjectRequest{})
			return err
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := test.call(); status.Code(err) != codes.Unimplemented {
				t.Fatalf("status=%s err=%v want Unimplemented", status.Code(err), err)
			}
		})
	}
}
