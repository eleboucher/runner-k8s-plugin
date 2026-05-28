package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"runtime"
	"strings"
	"sync"
	"time"

	"code.forgejo.org/forgejo/runner/v12/act/container"
	pluginv1 "code.forgejo.org/forgejo/runner/v12/act/plugin/proto/v1"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	k8sexec "k8s.io/client-go/util/exec"
)

// parseLabels parses "k=v,k=v" pairs. ${ENV_ID} in values expands to envID.
func parseLabels(raw, envID string) map[string]string {
	labels := make(map[string]string)
	for pair := range strings.SplitSeq(raw, ",") {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}
		k, v, ok := strings.Cut(pair, "=")
		k = strings.TrimSpace(k)
		if !ok || k == "" {
			slog.Warn("ignoring malformed label", "entry", pair)
			continue
		}
		labels[k] = strings.ReplaceAll(strings.TrimSpace(v), "${ENV_ID}", envID)
	}
	return labels
}

// runnerArch maps GOARCH to the values exposed via ${{ runner.arch }}.
func runnerArch() string {
	switch runtime.GOARCH {
	case "amd64":
		return "X64"
	case "386":
		return "X86"
	case "arm64":
		return "ARM64"
	case "arm":
		return "ARM"
	default:
		return runtime.GOARCH
	}
}

type k8sEnvironment struct {
	job    *K8sJob
	config *K8sJobConfig
}

type k8sServer struct {
	pluginv1.UnimplementedBackendPluginServer

	pluginInstanceID string

	mu   sync.Mutex
	envs map[string]*k8sEnvironment
}

func newK8sServer() *k8sServer {
	s := &k8sServer{
		pluginInstanceID: uuid.New().String(),
		envs:             make(map[string]*k8sEnvironment),
	}
	slog.Info("plugin instance initialized", "instance_id", s.pluginInstanceID)
	return s
}

func (s *k8sServer) getEnv(id string) (*k8sEnvironment, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	env, ok := s.envs[id]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "environment %q not found", id)
	}
	return env, nil
}

func (s *k8sServer) Capabilities(_ context.Context, _ *pluginv1.CapabilitiesRequest) (*pluginv1.CapabilitiesResponse, error) {
	return &pluginv1.CapabilitiesResponse{
		Name:                       "k8sjob",
		RootPath:                   k8sSharedMount,
		ActPath:                    k8sActPath,
		ToolCachePath:              k8sToolCache,
		PathVariableName:           "PATH",
		DefaultPathVariable:        k8sDefaultPath,
		PathSeparator:              ":",
		SupportsDockerActions:      false,
		ManagesOwnNetworking:       true,
		SupportsServiceContainers:  true,
		EnvironmentCaseInsensitive: false,
		SupportsLocalCopy:          true,
		RunnerContext: map[string]string{
			"os":         "Linux",
			"arch":       runnerArch(),
			"temp":       "/tmp",
			"tool_cache": k8sToolCache,
		},
	}, nil
}

func (s *k8sServer) Create(ctx context.Context, req *pluginv1.CreateRequest) (*pluginv1.CreateResponse, error) {
	opts := req.GetBackendOptions()

	namespace := opts["namespace"]
	if namespace == "" {
		namespace = "default"
	}

	var pollTimeout time.Duration
	if v := opts["poll_timeout"]; v != "" {
		d, err := time.ParseDuration(v)
		if err == nil {
			pollTimeout = d
		}
	}
	if pollTimeout == 0 {
		pollTimeout = 10 * time.Minute
	}

	var jobTimeout time.Duration
	if v := opts["job_timeout"]; v != "" {
		d, err := time.ParseDuration(v)
		if err == nil {
			jobTimeout = d
		}
	}
	if jobTimeout == 0 {
		jobTimeout = 3 * time.Hour
	}

	podSpec := opts["label_arg"]
	if podSpec == "" {
		podSpec = opts["podspec"]
	}

	k8sCfg := &K8sJobConfig{
		Namespace:   namespace,
		PodSpec:     podSpec,
		KubeConfig:  opts["kubeconfig"],
		PollTimeout: pollTimeout,
		JobTimeout:  jobTimeout,
	}

	logWriter := &grpcLogWriter{}

	job, err := NewK8sJob(&container.NewContainerInput{
		Image:      req.GetImage(),
		Name:       req.GetName(),
		Env:        req.GetEnv(),
		WorkingDir: req.GetWorkingDir(),
		Stdout:     logWriter,
		Stderr:     logWriter,
	}, k8sCfg)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "create k8s job: %v", err)
	}

	envID := uuid.New().String()
	// Set plugin-internal labels after user labels so they can't be shadowed.
	labels := parseLabels(opts["labels"], envID)
	labels["forgejo-runner/environment-id"] = envID
	labels["forgejo-runner/plugin-instance"] = s.pluginInstanceID
	job.extraLabels = labels

	// Add service containers
	for _, svc := range req.GetServices() {
		job.AddServiceContainerRaw(svc.GetName(), svc.GetImage(), svc.GetEnv(), svc.GetPorts())
	}

	// Execute Create (stores capAdd/capDrop)
	if err := job.Create(req.GetCapAdd(), req.GetCapDrop())(ctx); err != nil {
		return nil, status.Errorf(codes.Internal, "job create: %v", err)
	}

	s.mu.Lock()
	s.envs[envID] = &k8sEnvironment{job: job, config: k8sCfg}
	s.mu.Unlock()

	slog.Info("created environment", "id", envID, "image", req.GetImage(), "namespace", namespace)
	return &pluginv1.CreateResponse{EnvironmentId: envID}, nil
}

func (s *k8sServer) Start(ctx context.Context, req *pluginv1.StartRequest) (*pluginv1.StartResponse, error) {
	env, err := s.getEnv(req.GetEnvironmentId())
	if err != nil {
		return nil, err
	}

	if err := env.job.Start(false)(ctx); err != nil {
		return nil, status.Errorf(codes.Internal, "job start: %v", err)
	}

	imageEnv := s.readContainerEnv(ctx, env)

	return &pluginv1.StartResponse{ImageEnv: imageEnv}, nil
}

func (s *k8sServer) readContainerEnv(ctx context.Context, env *k8sEnvironment) map[string]string {
	var buf bytes.Buffer
	oldOut, oldErr := env.job.ReplaceLogWriter(&buf, io.Discard)
	defer env.job.ReplaceLogWriter(oldOut, oldErr)

	if err := env.job.Exec([]string{"env", "-0"}, nil, "", "")(ctx); err != nil {
		slog.Warn("failed to read container env", "error", err)
		return nil
	}

	result := make(map[string]string)
	for _, entry := range strings.Split(buf.String(), "\x00") {
		if k, v, ok := strings.Cut(entry, "="); ok && k != "" {
			result[k] = v
		}
	}
	return result
}

func (s *k8sServer) Exec(req *pluginv1.ExecRequest, stream grpc.ServerStreamingServer[pluginv1.ExecOutput]) error {
	env, err := s.getEnv(req.GetEnvironmentId())
	if err != nil {
		return err
	}

	var mu sync.Mutex
	stdoutW := &execStreamWriter{mu: &mu, stream: stream, streamType: pluginv1.ExecOutput_STDOUT}
	stderrW := &execStreamWriter{mu: &mu, stream: stream, streamType: pluginv1.ExecOutput_STDERR}

	oldOut, oldErr := env.job.ReplaceLogWriter(stdoutW, stderrW)
	defer env.job.ReplaceLogWriter(oldOut, oldErr)

	execErr := env.job.Exec(req.GetCommand(), req.GetEnv(), req.GetUser(), req.GetWorkdir())(stream.Context())

	exitCode := int32(0)
	errorMsg := ""
	if execErr != nil {
		var ce k8sexec.CodeExitError
		if errors.As(execErr, &ce) {
			exitCode = int32(ce.Code)
		} else {
			exitCode = 1
			errorMsg = execErr.Error()
		}
	}

	mu.Lock()
	_ = stream.Send(&pluginv1.ExecOutput{
		Done:         true,
		ExitCode:     exitCode,
		ErrorMessage: errorMsg,
	})
	mu.Unlock()

	return nil
}

func (s *k8sServer) CopyIn(stream grpc.ClientStreamingServer[pluginv1.CopyInChunk, pluginv1.CopyInResponse]) error {
	first, err := stream.Recv()
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "copyin recv first: %v", err)
	}

	env, err := s.getEnv(first.GetEnvironmentId())
	if err != nil {
		return err
	}

	destPath := first.GetDestPath()
	pr, pw := io.Pipe()

	go func() {
		defer pw.Close()
		if len(first.GetData()) > 0 {
			if _, err := pw.Write(first.GetData()); err != nil {
				pw.CloseWithError(err)
				return
			}
		}
		for {
			chunk, err := stream.Recv()
			if err == io.EOF {
				return
			}
			if err != nil {
				pw.CloseWithError(err)
				return
			}
			if _, err := pw.Write(chunk.GetData()); err != nil {
				pw.CloseWithError(err)
				return
			}
		}
	}()

	if err := env.job.CopyTarStream(stream.Context(), destPath, pr); err != nil {
		return status.Errorf(codes.Internal, "copyin: %v", err)
	}

	return stream.SendAndClose(&pluginv1.CopyInResponse{})
}

func (s *k8sServer) CopyLocal(ctx context.Context, req *pluginv1.CopyLocalRequest) (*pluginv1.CopyLocalResponse, error) {
	env, err := s.getEnv(req.GetEnvironmentId())
	if err != nil {
		return nil, err
	}

	if err := env.job.CopyDir(req.GetDestPath(), req.GetSrcPath(), false)(ctx); err != nil {
		return nil, status.Errorf(codes.Internal, "copylocal: %v", err)
	}

	return &pluginv1.CopyLocalResponse{}, nil
}

func (s *k8sServer) CopyOut(req *pluginv1.CopyOutRequest, stream grpc.ServerStreamingServer[pluginv1.CopyOutChunk]) error {
	env, err := s.getEnv(req.GetEnvironmentId())
	if err != nil {
		return err
	}

	rc, err := env.job.GetContainerArchive(stream.Context(), req.GetSrcPath())
	if err != nil {
		return status.Errorf(codes.Internal, "copyout: %v", err)
	}
	defer rc.Close()

	buf := make([]byte, 256*1024)
	for {
		n, readErr := rc.Read(buf)
		if n > 0 {
			if err := stream.Send(&pluginv1.CopyOutChunk{Data: buf[:n]}); err != nil {
				return err
			}
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return status.Errorf(codes.Internal, "copyout read: %v", readErr)
		}
	}
	return nil
}

func (s *k8sServer) UpdateEnv(ctx context.Context, req *pluginv1.UpdateEnvRequest) (*pluginv1.UpdateEnvResponse, error) {
	env, err := s.getEnv(req.GetEnvironmentId())
	if err != nil {
		return nil, err
	}

	current := req.GetCurrentEnv()
	if current == nil {
		current = make(map[string]string)
	}
	if err := env.job.UpdateFromEnv(req.GetSrcPath(), &current)(ctx); err != nil {
		return nil, status.Errorf(codes.Internal, "updateenv: %v", err)
	}

	return &pluginv1.UpdateEnvResponse{UpdatedEnv: current}, nil
}

func (s *k8sServer) IsHealthy(ctx context.Context, req *pluginv1.IsHealthyRequest) (*pluginv1.IsHealthyResponse, error) {
	env, err := s.getEnv(req.GetEnvironmentId())
	if err != nil {
		return nil, err
	}

	wait, err := env.job.IsHealthy(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "ishealthy: %v", err)
	}

	return &pluginv1.IsHealthyResponse{Wait: durationpb.New(wait)}, nil
}

func (s *k8sServer) Remove(ctx context.Context, req *pluginv1.RemoveRequest) (*pluginv1.RemoveResponse, error) {
	envID := req.GetEnvironmentId()
	env, err := s.getEnv(envID)
	if err != nil {
		return nil, err
	}

	if err := env.job.Remove()(ctx); err != nil {
		slog.Warn("failed to remove environment", "id", envID, "error", err)
	}
	_ = env.job.Close()(ctx)

	s.mu.Lock()
	delete(s.envs, envID)
	s.mu.Unlock()

	slog.Info("removed environment", "id", envID)
	return &pluginv1.RemoveResponse{}, nil
}

func (s *k8sServer) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	snapshot := make(map[string]*k8sEnvironment, len(s.envs))
	for id, env := range s.envs {
		snapshot[id] = env
	}
	clear(s.envs)
	s.mu.Unlock()

	if len(snapshot) == 0 {
		return nil
	}

	slog.Info("cleaning up active environments", "count", len(snapshot))

	type result struct {
		id  string
		err error
	}
	results := make(chan result, len(snapshot))

	for id, env := range snapshot {
		go func() {
			results <- result{id: id, err: env.job.Remove()(ctx)}
		}()
	}

	var errs int
	for range len(snapshot) {
		r := <-results
		if r.err != nil {
			slog.Warn("failed to remove environment during shutdown", "id", r.id, "error", r.err)
			errs++
		} else {
			slog.Info("removed environment during shutdown", "id", r.id)
		}
	}

	if errs > 0 {
		return fmt.Errorf("failed to remove %d/%d environments", errs, len(snapshot))
	}
	return nil
}

type execStreamWriter struct {
	mu         *sync.Mutex
	stream     grpc.ServerStreamingServer[pluginv1.ExecOutput]
	streamType pluginv1.ExecOutput_Stream
}

func (w *execStreamWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.stream.Send(&pluginv1.ExecOutput{
		Stream: w.streamType,
		Data:   p,
	}); err != nil {
		return 0, err
	}
	return len(p), nil
}

type grpcLogWriter struct{}

func (w *grpcLogWriter) Write(p []byte) (int, error) {
	if len(p) > 0 {
		slog.Debug("k8s output", "data", string(p))
	}
	return len(p), nil
}

var (
	_ pluginv1.BackendPluginServer = (*k8sServer)(nil)
	_ io.Writer                    = (*execStreamWriter)(nil)
	_ io.Writer                    = (*grpcLogWriter)(nil)
)
