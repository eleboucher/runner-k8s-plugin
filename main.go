package main

import (
	"context"
	"flag"
	"runtime/debug"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"

	pluginv1 "code.forgejo.org/forgejo/runner/v12/act/plugin/proto/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func main() {
	listen := flag.String("listen", "unix:///var/run/forgejo-runner-k8s.sock", "gRPC listen address (unix:///path or :port)")
	flag.Parse()

	srv := grpc.NewServer(
		grpc.ChainUnaryInterceptor(recoveryUnary()),
		grpc.ChainStreamInterceptor(recoveryStream()),
	)
	pluginv1.RegisterBackendPluginServer(srv, newK8sServer())

	lis, err := listenOn(*listen)
	if err != nil {
		slog.Error("failed to listen", "address", *listen, "error", err)
		os.Exit(1)
	}
	defer lis.Close()

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
	srv.GracefulStop()
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
