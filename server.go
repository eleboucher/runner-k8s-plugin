package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"code.forgejo.org/forgejo/runner/v13/act/container"
	pluginv1alpha "code.forgejo.org/forgejo/runner/v13/act/plugin/proto/v1alpha"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	k8sexec "k8s.io/client-go/util/exec"
	"sigs.k8s.io/yaml"
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

// parsePullPolicy validates the image_pull_policy backend option.
func parsePullPolicy(raw string) (corev1.PullPolicy, error) {
	switch p := corev1.PullPolicy(raw); p {
	case "", corev1.PullAlways, corev1.PullIfNotPresent, corev1.PullNever:
		return p, nil
	default:
		return "", fmt.Errorf("invalid image_pull_policy %q (must be %s, %s or %s)",
			raw, corev1.PullAlways, corev1.PullIfNotPresent, corev1.PullNever)
	}
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
	mu     sync.Mutex
}

type k8sServer struct {
	pluginv1alpha.UnimplementedBackendPluginServer

	pluginInstanceID string

	// logJobOutput mirrors job stdout/stderr into the plugin's own logs in
	// addition to the runner stream. Off by default: job output is unmasked
	// and can contain secrets.
	logJobOutput bool

	mu   sync.Mutex
	envs map[string]*k8sEnvironment
}

func newK8sServer() *k8sServer {
	logJobOutput, _ := strconv.ParseBool(os.Getenv("FORGEJO_RUNNER_K8S_LOG_JOB_OUTPUT"))
	s := &k8sServer{
		pluginInstanceID: uuid.New().String(),
		envs:             make(map[string]*k8sEnvironment),
		logJobOutput:     logJobOutput,
	}
	slog.Info("plugin instance initialized", "instance_id", s.pluginInstanceID, "log_job_output", logJobOutput)
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

func (s *k8sServer) Capabilities(_ context.Context, _ *pluginv1alpha.CapabilitiesRequest) (*pluginv1alpha.CapabilitiesResponse, error) {
	return &pluginv1alpha.CapabilitiesResponse{
		Name: "k8sjob",
	}, nil
}

func (s *k8sServer) Create(ctx context.Context, req *pluginv1alpha.CreateRequest) (*pluginv1alpha.CreateResponse, error) {
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

	jobTimeout := req.GetEnvironmentTimeout().AsDuration()
	if jobTimeout <= 0 {
		// Keep the old option as a fallback for callers that do not yet send
		// environment_timeout.
		if v := opts["job_timeout"]; v != "" {
			d, err := time.ParseDuration(v)
			if err == nil {
				jobTimeout = d
			}
		}
	}
	if jobTimeout == 0 {
		jobTimeout = 3 * time.Hour
	}

	podSpec := req.GetLabelArg()
	if podSpec == "" {
		podSpec = opts["podspec"]
	}

	pullPolicy, err := parsePullPolicy(opts["image_pull_policy"])
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%v", err)
	}

	var resources *corev1.ResourceRequirements
	if v := opts["resources"]; v != "" {
		var rr corev1.ResourceRequirements
		if err := yaml.Unmarshal([]byte(v), &rr); err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "parse resources option: %v", err)
		}
		resources = &rr
	}

	k8sCfg := &K8sJobConfig{
		Namespace:       namespace,
		PodSpec:         podSpec,
		KubeConfig:      opts["kubeconfig"],
		PollTimeout:     pollTimeout,
		JobTimeout:      jobTimeout,
		ImagePullPolicy: pullPolicy,
		Resources:       resources,
	}

	logWriter := &grpcLogWriter{stream: "create"}

	job, err := NewK8sJob(&container.NewContainerInput{
		Image:  req.GetImage(),
		Name:   req.GetName(),
		Stdout: logWriter,
		Stderr: logWriter,
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
	return &pluginv1alpha.CreateResponse{
		EnvironmentId:              envID,
		RootPath:                   k8sSharedMount,
		ActPath:                    k8sActPath,
		ToolCachePath:              k8sToolCache,
		TempPath:                   "/tmp",
		PathVariableName:           stringPtr("PATH"),
		DefaultPathVariable:        stringPtr(k8sDefaultPath),
		PathSeparator:              stringPtr(":"),
		EnvironmentCaseInsensitive: false,
		Os:                         "Linux",
		Arch:                       runnerArch(),
	}, nil
}

func (s *k8sServer) Start(req *pluginv1alpha.StartRequest, stream grpc.ServerStreamingServer[pluginv1alpha.StartOutput]) error {
	env, err := s.getEnv(req.GetEnvironmentId())
	if err != nil {
		return err
	}
	env.mu.Lock()
	defer env.mu.Unlock()

	if err := env.job.Start(false)(stream.Context()); err != nil {
		return status.Errorf(codes.Internal, "job start: %v", err)
	}

	imageEnv := s.readContainerEnv(stream.Context(), env)
	return stream.Send(&pluginv1alpha.StartOutput{
		Output: &pluginv1alpha.StartOutput_StartComplete{
			StartComplete: &pluginv1alpha.StartComplete{ImageEnv: imageEnv},
		},
	})
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

func (s *k8sServer) Exec(req *pluginv1alpha.ExecRequest, stream grpc.ServerStreamingServer[pluginv1alpha.ExecOutput]) error {
	env, err := s.getEnv(req.GetEnvironmentId())
	if err != nil {
		return err
	}
	env.mu.Lock()
	defer env.mu.Unlock()

	var mu sync.Mutex
	stdoutW := &execStreamWriter{mu: &mu, stream: stream, streamType: pluginv1alpha.DataChunk_STDOUT}
	stderrW := &execStreamWriter{mu: &mu, stream: stream, streamType: pluginv1alpha.DataChunk_STDERR}

	var outW, errW io.Writer = stdoutW, stderrW
	if s.logJobOutput {
		// Tee output to the plugin's own logs as well as the runner stream.
		outW = io.MultiWriter(stdoutW, &grpcLogWriter{stream: "stdout"})
		errW = io.MultiWriter(stderrW, &grpcLogWriter{stream: "stderr"})
	}

	oldOut, oldErr := env.job.ReplaceLogWriter(outW, errW)
	defer env.job.ReplaceLogWriter(oldOut, oldErr)

	execErr := env.job.Exec(req.GetCommand(), req.GetEnv(), req.GetUser(), req.GetWorkdir())(stream.Context())

	if execErr != nil {
		var ce k8sexec.CodeExitError
		if errors.As(execErr, &ce) {
			mu.Lock()
			defer mu.Unlock()
			return stream.Send(&pluginv1alpha.ExecOutput{
				Output: &pluginv1alpha.ExecOutput_ExecComplete{
					ExecComplete: &pluginv1alpha.ExecComplete{ExitCode: int32(ce.Code)},
				},
			})
		}
		mu.Lock()
		defer mu.Unlock()
		return stream.Send(&pluginv1alpha.ExecOutput{
			Output: &pluginv1alpha.ExecOutput_ExecFailed{
				ExecFailed: &pluginv1alpha.ExecFailed{ErrorMessage: execErr.Error()},
			},
		})
	}

	mu.Lock()
	defer mu.Unlock()
	return stream.Send(&pluginv1alpha.ExecOutput{
		Output: &pluginv1alpha.ExecOutput_ExecComplete{
			ExecComplete: &pluginv1alpha.ExecComplete{},
		},
	})
}

func (s *k8sServer) CopyIn(stream grpc.ClientStreamingServer[pluginv1alpha.CopyInChunk, pluginv1alpha.CopyInResponse]) error {
	first, err := stream.Recv()
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "copyin recv first: %v", err)
	}
	if first.EnvironmentId == nil || first.DestPath == nil {
		return status.Error(codes.InvalidArgument, "copyin first chunk must set environment_id and dest_path")
	}

	env, err := s.getEnv(first.GetEnvironmentId())
	if err != nil {
		return err
	}
	env.mu.Lock()
	defer env.mu.Unlock()

	destPath := first.GetDestPath()
	pr, pw := io.Pipe()
	var recvErr error
	var recvErrMu sync.Mutex
	setRecvErr := func(err error) {
		recvErrMu.Lock()
		defer recvErrMu.Unlock()
		recvErr = err
	}
	getRecvErr := func() error {
		recvErrMu.Lock()
		defer recvErrMu.Unlock()
		return recvErr
	}

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
			if chunk.EnvironmentId != nil || chunk.DestPath != nil {
				err := errors.New("copyin only permits environment_id and dest_path in the first chunk")
				setRecvErr(err)
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
		if recvErr := getRecvErr(); recvErr != nil {
			return status.Errorf(codes.InvalidArgument, "%v", recvErr)
		}
		return status.Errorf(codes.Internal, "copyin: %v", err)
	}
	if recvErr := getRecvErr(); recvErr != nil {
		return status.Errorf(codes.InvalidArgument, "%v", recvErr)
	}

	return stream.SendAndClose(&pluginv1alpha.CopyInResponse{})
}

func (s *k8sServer) CopyOut(req *pluginv1alpha.CopyOutRequest, stream grpc.ServerStreamingServer[pluginv1alpha.CopyOutChunk]) error {
	env, err := s.getEnv(req.GetEnvironmentId())
	if err != nil {
		return err
	}
	env.mu.Lock()
	defer env.mu.Unlock()

	rc, err := env.job.GetContainerArchive(stream.Context(), req.GetSrcPath())
	if err != nil {
		return status.Errorf(codes.Internal, "copyout: %v", err)
	}
	defer rc.Close()

	buf := make([]byte, 256*1024)
	for {
		n, readErr := rc.Read(buf)
		if n > 0 {
			if err := stream.Send(&pluginv1alpha.CopyOutChunk{Data: buf[:n]}); err != nil {
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

func (s *k8sServer) Remove(ctx context.Context, req *pluginv1alpha.RemoveRequest) (*pluginv1alpha.RemoveResponse, error) {
	envID := req.GetEnvironmentId()
	env, err := s.getEnv(envID)
	if err != nil {
		return nil, err
	}
	env.mu.Lock()
	defer env.mu.Unlock()

	if err := env.job.Remove()(ctx); err != nil {
		slog.Warn("failed to remove environment", "id", envID, "error", err)
	}
	_ = env.job.Close()(ctx)

	s.mu.Lock()
	delete(s.envs, envID)
	s.mu.Unlock()

	slog.Info("removed environment", "id", envID)
	return &pluginv1alpha.RemoveResponse{}, nil
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
	stream     grpc.ServerStreamingServer[pluginv1alpha.ExecOutput]
	streamType pluginv1alpha.DataChunk_Stream
}

func (w *execStreamWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.stream.Send(&pluginv1alpha.ExecOutput{
		Output: &pluginv1alpha.ExecOutput_Data{
			Data: &pluginv1alpha.DataChunk{Stream: w.streamType, Data: p},
		},
	}); err != nil {
		return 0, err
	}
	return len(p), nil
}

type grpcLogWriter struct {
	stream string
}

func (w *grpcLogWriter) Write(p []byte) (int, error) {
	// Check the level before string(p): this is on the per-chunk Exec path.
	if len(p) > 0 && slog.Default().Enabled(context.Background(), slog.LevelDebug) {
		slog.Debug("k8s output", "stream", w.stream, "data", string(p))
	}
	return len(p), nil
}

func stringPtr(v string) *string {
	return &v
}

var (
	_ pluginv1alpha.BackendPluginServer = (*k8sServer)(nil)
	_ io.Writer                         = (*execStreamWriter)(nil)
	_ io.Writer                         = (*grpcLogWriter)(nil)
)
