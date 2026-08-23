package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/nosway/namrbd/internal/serviceconfig"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/nosway/namrbd/internal/mcpops"
	namrbdversion "github.com/nosway/namrbd/version"
)

func main() {
	if len(os.Args) >= 2 && (os.Args[1] == "--version" || os.Args[1] == "version") {
		fmt.Println(namrbdversion.BuildSummary())
		return
	}
	cfg := mcpops.DefaultConfig()
	fs := flag.NewFlagSet("namrbd-mcp", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	configPath := fs.String("config", "", "service config file path")
	fs.StringVar(&cfg.OperationsEndpoint, "operations-endpoint", cfg.OperationsEndpoint, "sbs-service read-only operations endpoint")
	fs.StringVar(&cfg.Mode, "mode", cfg.Mode, "MCP posture: observe or operate")
	fs.StringVar(&cfg.ApprovalPolicy, "approval-policy", cfg.ApprovalPolicy, "approval policy: dry-run, external-token, or local-confirmation")
	fs.StringVar(&cfg.OperationOutputDir, "operation-output-dir", cfg.OperationOutputDir, "directory for future MCP operation records")
	fs.DurationVar(&cfg.HTTPTimeout, "http-timeout", cfg.HTTPTimeout, "HTTP collector timeout")

	if err := fs.Parse(os.Args[1:]); err != nil {
		os.Exit(2)
	}
	if fs.NArg() != 0 {
		fmt.Fprintf(os.Stderr, "namrbd-mcp: unexpected positional arguments: %v\n", fs.Args())
		os.Exit(2)
	}
	// Without --config the server behaves exactly as before.
	if strings.TrimSpace(*configPath) != "" {
		summary, err := applyMCPConfig(*configPath, &cfg, explicitlySetFlags(fs), serviceconfig.OSEnv)
		if blob, mErr := json.Marshal(summary); mErr == nil {
			fmt.Fprintf(os.Stderr, "service config summary: %s\n", blob)
		}
		if err != nil {
			fmt.Fprintf(os.Stderr, "namrbd-mcp: service config: %v\n", err)
			os.Exit(2)
		}
	}

	cfg = cfg.Normalized()
	if cfg.Mode != mcpops.ModeObserve && cfg.Mode != mcpops.ModeOperate {
		fmt.Fprintf(os.Stderr, "namrbd-mcp: unsupported mode %q\n", cfg.Mode)
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := mcpops.RunStdio(ctx, cfg, os.Stdin, os.Stdout); err != nil {
		fmt.Fprintf(os.Stderr, "namrbd-mcp: %v\n", err)
		os.Exit(1)
	}
}
