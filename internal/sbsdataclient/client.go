package sbsdataclient

import (
	"context"
	"fmt"

	sbsv1 "github.com/nosway/namrbd/sbs/v1"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Client struct {
	conn   *grpc.ClientConn
	Volume sbsv1.VolumeServiceClient
}

func Dial(ctx context.Context, endpoint string) (*Client, error) {
	if endpoint == "" {
		return nil, fmt.Errorf("sbs-data endpoint is required")
	}
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("dial sbs-data endpoint %q: %w", endpoint, err)
	}
	return &Client{
		conn:   conn,
		Volume: sbsv1.NewVolumeServiceClient(conn),
	}, nil
}

func (c *Client) Close() error {
	if c == nil || c.conn == nil {
		return nil
	}
	return c.conn.Close()
}
