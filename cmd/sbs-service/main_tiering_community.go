//go:build !enterprise

package main

import "context"

func startEnterpriseTieringReconciler(_ context.Context, _ *server) {}
