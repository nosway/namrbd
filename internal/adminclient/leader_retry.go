package adminclient

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"unicode"

	adminv1 "github.com/nosway/namrbd/sbs/admin/v1"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type EndpointSpec struct {
	NodeID   string
	Endpoint string
}

type AdminDialer func(context.Context, string) (adminv1.AdminServiceClient, func() error, error)

type LeaderAwareAdminConfig struct {
	PrimaryEndpoint string
	Endpoints       []EndpointSpec
	Dial            AdminDialer
}

type LeaderAwareAdminClient struct {
	mu              sync.Mutex
	primaryEndpoint string
	endpoints       []EndpointSpec
	clients         map[string]adminv1.AdminServiceClient
	closers         map[string]func() error
	dial            AdminDialer
}

func ParseEndpointSpecs(primary, raw string) []EndpointSpec {
	entries := splitEndpointEntries(raw)
	if strings.TrimSpace(primary) != "" {
		entries = append([]string{primary}, entries...)
	}

	specs := make([]EndpointSpec, 0, len(entries))
	seenEndpoints := map[string]struct{}{}
	for _, entry := range entries {
		nodeID, endpoint := splitEndpointSpec(entry)
		if endpoint == "" {
			continue
		}
		if _, ok := seenEndpoints[endpoint]; ok {
			continue
		}
		seenEndpoints[endpoint] = struct{}{}
		specs = append(specs, EndpointSpec{
			NodeID:   nodeID,
			Endpoint: endpoint,
		})
	}
	return specs
}

func NewLeaderAwareAdminClient(ctx context.Context, cfg LeaderAwareAdminConfig) (*LeaderAwareAdminClient, error) {
	specs := cfg.Endpoints
	if len(specs) == 0 {
		specs = ParseEndpointSpecs(cfg.PrimaryEndpoint, "")
	}
	primary := strings.TrimSpace(cfg.PrimaryEndpoint)
	if primary == "" && len(specs) > 0 {
		primary = strings.TrimSpace(specs[0].Endpoint)
	}
	if primary == "" {
		return nil, fmt.Errorf("admin endpoint is required")
	}
	dialer := cfg.Dial
	if dialer == nil {
		dialer = defaultAdminDialer
	}
	client := &LeaderAwareAdminClient{
		primaryEndpoint: primary,
		endpoints:       specs,
		clients:         map[string]adminv1.AdminServiceClient{},
		closers:         map[string]func() error{},
		dial:            dialer,
	}
	if _, err := client.clientForEndpoint(ctx, primary); err != nil {
		return nil, err
	}
	return client, nil
}

func (c *LeaderAwareAdminClient) Invoke(ctx context.Context, cluster *adminv1.ClusterRef, call func(adminv1.AdminServiceClient) error) error {
	if c == nil {
		return fmt.Errorf("leader-aware admin client is nil")
	}
	if call == nil {
		return fmt.Errorf("admin call is nil")
	}
	primary, err := c.clientForEndpoint(ctx, c.primaryEndpoint)
	if err != nil {
		return err
	}
	err = call(primary)
	leaderID, ok := LeaderHintFromError(err)
	if !ok {
		return err
	}

	leaderEndpoint := c.endpointForLeaderID(leaderID)
	if leaderEndpoint == "" {
		leaderEndpoint = c.discoverLeaderEndpoint(ctx, cluster, primary, leaderID)
	}
	if leaderEndpoint == "" || leaderEndpoint == c.primaryEndpoint {
		return err
	}
	leaderClient, dialErr := c.clientForEndpoint(ctx, leaderEndpoint)
	if dialErr != nil {
		return err
	}
	return call(leaderClient)
}

func (c *LeaderAwareAdminClient) Close() error {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	var firstErr error
	for endpoint, closer := range c.closers {
		if closer == nil {
			continue
		}
		if err := closer(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("close admin endpoint %q: %w", endpoint, err)
		}
	}
	return firstErr
}

func LeaderHintFromError(err error) (string, bool) {
	if err == nil || status.Code(err) != codes.Unavailable {
		return "", false
	}
	message := status.Convert(err).Message()
	match := currentLeaderPattern.FindStringSubmatch(message)
	if len(match) != 2 {
		return "", false
	}
	leaderID := strings.TrimSpace(strings.Trim(match[1], `"'.,;`))
	if leaderID == "" {
		return "", false
	}
	return leaderID, true
}

func defaultAdminDialer(ctx context.Context, endpoint string) (adminv1.AdminServiceClient, func() error, error) {
	client, err := Dial(ctx, endpoint)
	if err != nil {
		return nil, nil, err
	}
	return client.Admin, client.Close, nil
}

func (c *LeaderAwareAdminClient) clientForEndpoint(ctx context.Context, endpoint string) (adminv1.AdminServiceClient, error) {
	endpoint = strings.TrimSpace(endpoint)
	if endpoint == "" {
		return nil, fmt.Errorf("admin endpoint is required")
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	if client, ok := c.clients[endpoint]; ok {
		return client, nil
	}
	client, closer, err := c.dial(ctx, endpoint)
	if err != nil {
		return nil, err
	}
	c.clients[endpoint] = client
	if closer != nil {
		c.closers[endpoint] = closer
	}
	return client, nil
}

func (c *LeaderAwareAdminClient) endpointForLeaderID(leaderID string) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, spec := range c.endpoints {
		if sameLeaderID(spec.NodeID, leaderID) {
			return spec.Endpoint
		}
	}
	return ""
}

func (c *LeaderAwareAdminClient) discoverLeaderEndpoint(ctx context.Context, cluster *adminv1.ClusterRef, client adminv1.AdminServiceClient, leaderID string) string {
	resp, err := client.ListNodes(ctx, &adminv1.ListNodesRequest{Cluster: cluster})
	if err != nil {
		return ""
	}
	for _, node := range resp.GetNodes() {
		if !sameLeaderID(node.GetNodeId(), leaderID) {
			continue
		}
		endpoint := strings.TrimSpace(node.GetGrpcEndpoint())
		if endpoint == "" {
			return ""
		}
		c.rememberLeaderEndpoint(leaderID, endpoint)
		return endpoint
	}
	return ""
}

func (c *LeaderAwareAdminClient) rememberLeaderEndpoint(leaderID, endpoint string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, spec := range c.endpoints {
		if sameLeaderID(spec.NodeID, leaderID) && spec.Endpoint == endpoint {
			return
		}
	}
	c.endpoints = append(c.endpoints, EndpointSpec{NodeID: leaderID, Endpoint: endpoint})
}

func splitEndpointEntries(raw string) []string {
	return strings.FieldsFunc(raw, func(r rune) bool {
		return r == ',' || unicode.IsSpace(r)
	})
}

func splitEndpointSpec(raw string) (string, string) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", ""
	}
	if left, right, ok := strings.Cut(raw, "="); ok {
		left = strings.TrimSpace(left)
		right = strings.TrimSpace(right)
		if left != "" && right != "" && !strings.Contains(left, "://") {
			return left, right
		}
	}
	return "", raw
}

func sameLeaderID(a, b string) bool {
	a = strings.TrimSpace(a)
	b = strings.TrimSpace(b)
	if a == "" || b == "" {
		return false
	}
	if a == b {
		return true
	}
	return strings.TrimPrefix(a, "svc-") == strings.TrimPrefix(b, "svc-")
}

var currentLeaderPattern = regexp.MustCompile(`current[[:space:]]+leader=([^[:space:]]+)`)
