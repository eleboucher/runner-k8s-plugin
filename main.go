package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"runtime/debug"
	"strings"
	"syscall"
	"time"

	pluginv1 "code.forgejo.org/forgejo/runner/v12/act/plugin/proto/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
)

func main() {
	configureLogging()

	listen := flag.String("listen", "unix:///var/run/forgejo-runner-k8s.sock", "gRPC listen address (unix:///path or :port)")
	flag.Parse()

	srv := grpc.NewServer(
		grpc.ChainUnaryInterceptor(recoveryUnary()),
		grpc.ChainStreamInterceptor(recoveryStream()),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             15 * time.Second,
			PermitWithoutStream: true,
		}),
	)

	k8sSrv := newK8sServer()
	pluginv1.RegisterBackendPluginServer(srv, k8sSrv)

	healthSrv := health.NewServer()
	grpc_health_v1.RegisterHealthServer(srv, healthSrv)

	lis, err := listenOn(*listen)
	if err != nil {
		slog.Error("failed to listen", "address", *listen, "error", err)
		os.Exit(1)
	}
	defer lis.Close()

	healthSrv.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
	slog.Info("plugin listening", "address", *listen)

	go func() {
		if err := srv.Serve(lis); err != nil {
			slog.Error("gRPC server failed", "error", err)
			os.Exit(1)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	slog.Info("shutting down")
	healthSrv.Shutdown()

	// Drain RPCs before tearing down environments.
	srv.GracefulStop()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer shutdownCancel()
	if err := k8sSrv.Shutdown(shutdownCtx); err != nil {
		slog.Error("shutdown cleanup failed", "error", err)
	}
}

func listenOn(addr string) (net.Listener, error) {
	if sockPath, ok := strings.CutPrefix(addr, "unix://"); ok {
		_ = os.Remove(sockPath)
		return net.Listen("unix", sockPath)
	}
	if !strings.Contains(addr, ":") {
		return nil, fmt.Errorf("invalid address %q: expected unix:///path or host:port", addr)
	}
	return net.Listen("tcp", addr)
}

// configureLogging sets the slog default handler. The runner spawns this
// plugin via hashicorp/go-plugin, which has no built-in mechanism to
// propagate its log level to the child process. To stay aligned with the
// runner's level, the runner is expected to inject FORGEJO_RUNNER_LOG_LEVEL
// (or HCLOG_LEVEL as a fallback) into the plugin's environment. When
// neither is set we default to Info, matching the runner's own default.
func configureLogging() {
	level := slog.LevelInfo
	for _, key := range []string{"FORGEJO_RUNNER_LOG_LEVEL", "HCLOG_LEVEL"} {
		if v := strings.TrimSpace(os.Getenv(key)); v != "" {
			if parsed, ok := parseLogLevel(v); ok {
				level = parsed
			}
			break
		}
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: level,
	})))
}

func parseLogLevel(s string) (slog.Level, bool) {
	switch strings.ToLower(s) {
	case "trace", "debug":
		return slog.LevelDebug, true
	case "info":
		return slog.LevelInfo, true
	case "warn", "warning":
		return slog.LevelWarn, true
	case "error", "err":
		return slog.LevelError, true
	}
	return slog.LevelInfo, false
}

func recoveryUnary() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
		defer func() {
			if r := recover(); r != nil {
				slog.Error("panic in RPC handler", "method", info.FullMethod, "panic", r, "stack", string(debug.Stack()))
				err = status.Errorf(codes.Internal, "internal error: %v", r)
			}
		}()
		return handler(ctx, req)
	}
}

func recoveryStream() grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) (err error) {
		defer func() {
			if r := recover(); r != nil {
				slog.Error("panic in streaming RPC handler", "method", info.FullMethod, "panic", r, "stack", string(debug.Stack()))
				err = status.Errorf(codes.Internal, "internal error: %v", r)
			}
		}()
		return handler(srv, ss)
	}
}
