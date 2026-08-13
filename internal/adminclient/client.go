package adminclient

import (
	"context"
	"fmt"
	"strings"

	adminv1 "github.com/nosway/namrbd/sbs/admin/v1"
	internalv1 "github.com/nosway/namrbd/sbs/internalapi/v1"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Client struct {
	conn       *grpc.ClientConn
	Admin      adminv1.AdminServiceClient
	Operations adminv1.OperationsServiceClient
	Placement  internalv1.PlacementResolverServiceClient
}

func Dial(ctx context.Context, endpoint string) (*Client, error) {
	endpoint = strings.TrimSpace(endpoint)
	if endpoint == "" {
		return nil, fmt.Errorf("admin endpoint is required")
	}
	target := grpcTarget(endpoint)
	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("dial admin endpoint %q: %w", endpoint, err)
	}
	return &Client{
		conn:       conn,
		Admin:      adminv1.NewAdminServiceClient(conn),
		Operations: adminv1.NewOperationsServiceClient(conn),
		Placement:  internalv1.NewPlacementResolverServiceClient(conn),
	}, nil
}

func grpcTarget(endpoint string) string {
	if strings.Contains(endpoint, "://") {
		return endpoint
	}
	return "passthrough:///" + endpoint
}

func (c *Client) Close() error {
	if c == nil || c.conn == nil {
		return nil
	}
	return c.conn.Close()
}
