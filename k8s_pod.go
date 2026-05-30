package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"code.forgejo.org/forgejo/runner/v12/act/common"
	"code.forgejo.org/forgejo/runner/v12/act/container"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	metav1watch "k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/remotecommand"
	"k8s.io/client-go/tools/watch"
	"k8s.io/kubectl/pkg/scheme"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/yaml"
)

// k8sPodSpecImageSentinel means "defer to the podspec's image".
const k8sPodSpecImageSentinel = "k8spod"

type K8sJobConfig struct {
	Namespace   string
	PodSpec     string // path to podspec YAML file
	KubeConfig  string
	PollTimeout time.Duration
	JobTimeout  time.Duration // total job timeout, used to set pod sleep duration
}

type serviceContainerSpec struct {
	name  string
	image string
	env   []corev1.EnvVar
	ports []corev1.ContainerPort
}

type K8sJob struct {
	client      kubernetes.Interface
	restCfg     *rest.Config
	job         *batchv1.Job
	podName     string // discovered pod name, valid after Start completes
	namespace   string
	input       container.NewContainerInput
	config      *K8sJobConfig
	services    []serviceContainerSpec
	capAdd      []string
	capDrop     []string
	extraLabels map[string]string

	mu     sync.Mutex
	stdout io.Writer
	stderr io.Writer
}

func (p *K8sJob) AddServiceContainerRaw(name, image string, env map[string]string, ports []string) {
	k8sEnv := make([]corev1.EnvVar, 0, len(env))
	for k, v := range env {
		k8sEnv = append(k8sEnv, corev1.EnvVar{Name: k, Value: v})
	}
	k8sPorts := make([]corev1.ContainerPort, 0, len(ports))
	for _, port := range ports {
		// Take the container port from "container", "host:container", or
		// "host:container/proto"; host mappings are irrelevant in k8s.
		spec := port
		if i := strings.LastIndex(spec, "/"); i >= 0 {
			spec = spec[:i]
		}
		parts := strings.Split(spec, ":")
		portStr := parts[len(parts)-1]
		portNum, err := strconv.Atoi(portStr)
		if err != nil || portNum < 1 || portNum > 65535 {
			slog.Warn("ignoring invalid service port", "service", name, "port", port, "error", err)
			continue
		}
		k8sPorts = append(k8sPorts, corev1.ContainerPort{ContainerPort: int32(portNum)})
	}
	p.mu.Lock()
	p.services = append(p.services, serviceContainerSpec{
		name:  name,
		image: image,
		env:   k8sEnv,
		ports: k8sPorts,
	})
	p.mu.Unlock()
}

func (p *K8sJob) Create(capAdd, capDrop []string) common.Executor {
	return func(_ context.Context) error {
		p.capAdd = capAdd
		p.capDrop = capDrop
		return nil
	}
}

func (p *K8sJob) Start(_ bool) common.Executor {
	return func(ctx context.Context) error {
		job, err := p.createJob(ctx)
		if err != nil {
			return fmt.Errorf("create job: %w", err)
		}
		slog.Info("created job", "namespace", job.Namespace, "name", job.Name)

		pollTimeout := p.config.PollTimeout
		if pollTimeout == 0 {
			pollTimeout = 10 * time.Minute
		}
		startCtx, cancel := context.WithDeadline(ctx, time.Now().Add(pollTimeout))
		defer cancel()

		if err := p.waitForJobRunning(startCtx, job); err != nil {
			if delErr := p.deleteJob(context.Background()); delErr != nil {
				slog.Warn("failed to clean up job after startup failure", "error", delErr)
			}
			return fmt.Errorf("wait for job: %w", err)
		}

		slog.Info("job is running", "name", job.Name)

		podName, err := p.waitForAllContainersReady(startCtx)
		if err != nil {
			if delErr := p.deleteJob(context.Background()); delErr != nil {
				slog.Warn("failed to clean up job after containers ready failure", "error", delErr)
			}
			return fmt.Errorf("wait for containers to be ready: %w", err)
		}
		slog.Info("all containers ready", "name", job.Name)

		if err := p.waitForExecReady(startCtx, podName); err != nil {
			if delErr := p.deleteJob(context.Background()); delErr != nil {
				slog.Warn("failed to clean up job after exec ready failure", "error", delErr)
			}
			return fmt.Errorf("wait for exec ready: %w", err)
		}
		slog.Debug("exec ready", "name", job.Name)

		p.podName = podName

		return nil
	}
}

func (p *K8sJob) Exec(command []string, env map[string]string, user, workdir string) common.Executor {
	return func(ctx context.Context) error {
		if user != "" {
			slog.Debug("ignoring user parameter", "user", user)
		}

		// Fall back to the configured workdir (equivalent to Docker's container
		// WorkingDir) so that callers like run-steps that pass an empty workdir
		// still execute inside the repository checkout directory.
		if workdir == "" && p.input.WorkingDir != "" {
			workdir = p.input.WorkingDir
		}

		if workdir != "" {
			if err := p.mkdir(ctx, workdir); err != nil {
				return fmt.Errorf("create workdir %q: %w", workdir, err)
			}
		}

		// env(1) handles variable names with dashes (e.g. INPUT_SHOW-PROGRESS)
		// that POSIX export rejects. env -C is not portable (missing in BusyBox),
		// so workdir uses sh+cd instead.
		envcmd := []string{"env"}
		for k, v := range env {
			envcmd = append(envcmd, k+"="+v)
		}
		var fullCmd []string
		if workdir != "" {
			fullCmd = slices.Concat(
				[]string{"sh", "-c", `cd "$0" && exec "$@"`, workdir},
				envcmd,
				command,
			)
		} else {
			fullCmd = slices.Concat(envcmd, command)
		}

		p.mu.Lock()
		stdout, stderr := p.stdout, p.stderr
		p.mu.Unlock()

		exec, err := p.newExecCommand(p.podName, &corev1.PodExecOptions{
			Container: k8sMainContainerName,
			Command:   fullCmd,
			Stdin:     false,
			Stdout:    stdout != nil,
			Stderr:    stderr != nil,
			TTY:       false,
		})
		if err != nil {
			return fmt.Errorf("setup exec: %w", err)
		}

		err = exec.StreamWithContext(ctx, remotecommand.StreamOptions{
			Stdout: stdout,
			Stderr: stderr,
		})
		if err != nil {
			return fmt.Errorf("exec: %w", err)
		}

		return nil
	}
}

func (p *K8sJob) Copy(destPath string, files ...*container.FileEntry) common.Executor {
	return func(ctx context.Context) error {
		if err := p.mkdir(ctx, destPath); err != nil {
			return fmt.Errorf("mkdir %q: %w", destPath, err)
		}
		if len(files) == 0 {
			return nil
		}

		pr, pw := io.Pipe()
		// Closing pr unblocks the producer if extract aborts mid-stream.
		defer pr.Close()
		go func() {
			defer pw.Close()
			gz := gzip.NewWriter(pw)
			defer gz.Close()
			tw := tar.NewWriter(gz)
			defer tw.Close()
			for _, f := range files {
				if err := tw.WriteHeader(&tar.Header{
					Name:     f.Name,
					Mode:     f.Mode,
					Size:     int64(len(f.Body)),
					Typeflag: tar.TypeReg,
				}); err != nil {
					pw.CloseWithError(err)
					return
				}
				if _, err := tw.Write([]byte(f.Body)); err != nil {
					pw.CloseWithError(err)
					return
				}
			}
		}()

		return p.execTarExtract(ctx, destPath, pr)
	}
}

func (p *K8sJob) CopyDir(destPath, srcPath string, _ bool) common.Executor {
	return func(ctx context.Context) error {
		if err := p.mkdir(ctx, destPath); err != nil {
			return fmt.Errorf("mkdir %q: %w", destPath, err)
		}

		pr, pw := io.Pipe()
		defer pr.Close()
		go func() {
			defer pw.Close()
			gz := gzip.NewWriter(pw)
			defer gz.Close()
			tw := tar.NewWriter(gz)
			defer tw.Close()
			if err := tw.AddFS(os.DirFS(srcPath)); err != nil {
				pw.CloseWithError(err)
				return
			}
		}()

		return p.execTarExtract(ctx, destPath, pr)
	}
}

func (p *K8sJob) CopyTarStream(ctx context.Context, destPath string, tarStream io.Reader) error {
	if err := p.mkdir(ctx, destPath); err != nil {
		return fmt.Errorf("mkdir %q: %w", destPath, err)
	}

	pr, pw := io.Pipe()
	defer pr.Close()
	go func() {
		defer pw.Close()
		gz := gzip.NewWriter(pw)
		defer gz.Close()
		if _, err := io.Copy(gz, tarStream); err != nil {
			pw.CloseWithError(err)
		}
	}()

	return p.execTarExtract(ctx, destPath, pr)
}

// archiveBufferLimit is the threshold at which GetContainerArchive switches
// from memory buffering to spilling to a temp file.
const archiveBufferLimit = 50 * 1024 * 1024 // 50MB

// tempFileReader wraps an open temp file for reading as an io.ReadCloser.
// The file is removed when Close is called.
type tempFileReader struct {
	tmp *os.File
}

func (r *tempFileReader) Read(p []byte) (int, error) {
	return r.tmp.Read(p)
}

func (r *tempFileReader) Close() error {
	if r.tmp == nil {
		return nil
	}
	name := r.tmp.Name()
	err := r.tmp.Close()
	removeErr := os.Remove(name)
	r.tmp = nil
	if err == nil {
		err = removeErr
	}
	return err
}

// spillWriter writes to a buffer initially, then spills to a temp file once
// the threshold is exceeded. This allows GetContainerArchive to handle both
// small and large archives without OOM for small ones while streaming large
// ones to disk instead of memory.
type spillWriter struct {
	buf       *bytes.Buffer
	tmp       *os.File
	threshold int64
	written   int64
}

func (s *spillWriter) Write(p []byte) (int, error) {
	if s.tmp != nil {
		n, err := s.tmp.Write(p)
		s.written += int64(n)
		return n, err
	}
	if s.written+int64(len(p)) > s.threshold {
		if err := s.spillToDisk(p); err != nil {
			return 0, err
		}
		return len(p), nil
	}
	n, err := s.buf.Write(p)
	s.written += int64(n)
	return n, err
}

func (s *spillWriter) spillToDisk(firstChunk []byte) error {
	tmp, err := os.CreateTemp("", "forgejo-archive-*.tar")
	if err != nil {
		return fmt.Errorf("create temp file for archive overflow: %w", err)
	}
	s.tmp = tmp
	if _, err := s.buf.WriteTo(tmp); err != nil {
		tmp.Close()
		os.Remove(tmp.Name())
		s.tmp = nil
		return fmt.Errorf("spill buffer to temp file: %w", err)
	}
	if len(firstChunk) > 0 {
		if _, err := tmp.Write(firstChunk); err != nil {
			tmp.Close()
			os.Remove(tmp.Name())
			s.tmp = nil
			return fmt.Errorf("write first chunk after spill: %w", err)
		}
		s.written += int64(len(firstChunk))
	}
	return nil
}

func (p *K8sJob) GetContainerArchive(ctx context.Context, srcPath string) (io.ReadCloser, error) {
	dir := filepath.Dir(srcPath)
	base := filepath.Base(srcPath)

	exec, err := p.newExecCommand(p.podName, &corev1.PodExecOptions{
		Container: k8sMainContainerName,
		Command:   []string{"tar", "cf", "-", "-C", dir, base},
		Stdout:    true,
		Stderr:    true,
	})
	if err != nil {
		return nil, fmt.Errorf("setup tar exec: %w", err)
	}

	sw := &spillWriter{
		buf:       new(bytes.Buffer),
		threshold: archiveBufferLimit,
	}
	var errBuf bytes.Buffer
	if err := exec.StreamWithContext(ctx, remotecommand.StreamOptions{
		Stdout: sw,
		Stderr: &errBuf,
	}); err != nil {
		if sw.tmp != nil {
			sw.tmp.Close()
			os.Remove(sw.tmp.Name())
		}
		if isClosedStreamError(err) {
			return nil, fmt.Errorf("tar exec: stream closed (container may have terminated)")
		}
		return nil, fmt.Errorf("tar exec: %w (stderr: %s)", err, errBuf.String())
	}

	if sw.tmp != nil {
		if _, err := sw.tmp.Seek(0, 0); err != nil {
			sw.tmp.Close()
			os.Remove(sw.tmp.Name())
			return nil, fmt.Errorf("seek temp file: %w", err)
		}
		return &tempFileReader{tmp: sw.tmp}, nil
	}
	return io.NopCloser(bytes.NewReader(sw.buf.Bytes())), nil
}

func (p *K8sJob) Pull(_ bool) common.Executor {
	return func(_ context.Context) error {
		return nil
	}
}

func (p *K8sJob) ConnectToNetwork(_ string) common.Executor {
	return func(_ context.Context) error {
		return nil
	}
}

func (p *K8sJob) UpdateFromEnv(srcPath string, env *map[string]string) common.Executor {
	return container.ParseEnvFile(p, srcPath, env).IfNot(common.Dryrun)
}

func (p *K8sJob) UpdateFromImageEnv(_ *map[string]string) common.Executor {
	return func(_ context.Context) error {
		return nil
	}
}

func (p *K8sJob) Remove() common.Executor {
	return func(ctx context.Context) error {
		return p.deleteJob(ctx)
	}
}

func (p *K8sJob) Close() common.Executor {
	return p.Remove()
}

func (p *K8sJob) ReplaceLogWriter(stdout, stderr io.Writer) (io.Writer, io.Writer) {
	p.mu.Lock()
	defer p.mu.Unlock()
	oldStdout := p.stdout
	oldStderr := p.stderr
	p.stdout = stdout
	p.stderr = stderr
	return oldStdout, oldStderr
}

func (p *K8sJob) IsHealthy(ctx context.Context) (time.Duration, error) {
	if p.job == nil {
		return 0, errors.New("job not started")
	}
	job, err := p.client.BatchV1().Jobs(p.namespace).Get(ctx, p.job.Name, metav1.GetOptions{})
	if err != nil {
		return 0, fmt.Errorf("get job status: %w", err)
	}
	for _, cond := range job.Status.Conditions {
		if cond.Type == batchv1.JobFailed && cond.Status == corev1.ConditionTrue {
			return 0, fmt.Errorf("job failed: reason=%s message=%s", cond.Reason, cond.Message)
		}
	}
	if job.Status.Active > 0 {
		return 0, nil
	}
	if job.Status.Succeeded > 0 {
		return 0, errors.New("job completed unexpectedly")
	}
	if job.Status.Failed > 0 {
		return 0, errors.New("job failed")
	}
	return 0, nil
}

func (*K8sJob) BackendID() string {
	return "k8sjob"
}

func (*K8sJob) SupportsDockerContainerActions() bool {
	return false
}

func (*K8sJob) ManagesOwnNetworking() bool {
	return true
}

func (*K8sJob) GetActPath() string {
	return k8sActPath
}

func (*K8sJob) GetRoot() string {
	return k8sSharedMount
}

func (*K8sJob) GetName() string {
	return "k8sjob"
}

func (*K8sJob) GetPathVariableName() string {
	return "PATH"
}

func (*K8sJob) DefaultPathVariable() string {
	return k8sDefaultPath
}

func (*K8sJob) JoinPathVariable(paths ...string) string {
	return strings.Join(paths, ":")
}

func (*K8sJob) ToContainerPath(path string) string {
	return path
}

func (*K8sJob) IsEnvironmentCaseInsensitive() bool {
	return false
}

func (*K8sJob) GetRunnerContext(_ context.Context) map[string]any {
	return map[string]any{
		"os":         "Linux",
		"arch":       runnerArch(),
		"temp":       "/tmp",
		"tool_cache": k8sToolCache,
	}
}

func (p *K8sJob) createJob(ctx context.Context) (*batchv1.Job, error) {
	timeout := p.config.JobTimeout
	if timeout <= 0 {
		timeout = 3 * time.Hour
	}

	labels := map[string]string{
		"app.kubernetes.io/managed-by": "forgejo-runner",
	}
	maps.Copy(labels, p.extraLabels)

	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: "forgejo-runner-task-",
			Namespace:    p.namespace,
			Labels:       labels,
		},
	}

	if p.config.PodSpec != "" {
		data, err := os.ReadFile(p.config.PodSpec)
		if err != nil {
			return nil, fmt.Errorf("read podspec %q: %w", p.config.PodSpec, err)
		}
		var podSpec corev1.PodSpec
		if err := yaml.Unmarshal(data, &podSpec); err != nil {
			return nil, fmt.Errorf("parse podspec %q: %w", p.config.PodSpec, err)
		}
		job.Spec.Template.Spec = podSpec
	}

	// Pods don't inherit job labels; copy them so selectors like
	// topologySpreadConstraints can match.
	if job.Spec.Template.Labels == nil {
		job.Spec.Template.Labels = make(map[string]string, len(labels))
	}
	maps.Copy(job.Spec.Template.Labels, labels)

	mainIdx := p.findMainContainer(&job.Spec.Template)
	if mainIdx < 0 {
		job.Spec.Template.Spec.Containers = append([]corev1.Container{{
			Name: k8sMainContainerName,
		}}, job.Spec.Template.Spec.Containers...)
		mainIdx = 0
	}

	main := job.Spec.Template.Spec.Containers[mainIdx]

	if p.input.Image != "" && p.input.Image != k8sPodSpecImageSentinel {
		main.Image = p.input.Image
	}
	if main.Image == "" {
		return nil, errors.New("no container image specified (set it in the podspec or workflow runs-on)")
	}

	main.Command = []string{"sh", "-c", fmt.Sprintf("mkdir -p %s && sleep %d", k8sWorkDir, int64(timeout.Seconds())+10)}

	for _, kv := range p.input.Env {
		parts := strings.SplitN(kv, "=", 2)
		if len(parts) == 2 {
			main.Env = append(main.Env, corev1.EnvVar{Name: parts[0], Value: parts[1]})
		}
	}

	p.ensureSharedVolume(&job.Spec.Template, &main)

	if len(p.capAdd) > 0 || len(p.capDrop) > 0 {
		if main.SecurityContext == nil {
			main.SecurityContext = &corev1.SecurityContext{}
		}
		if main.SecurityContext.Capabilities == nil {
			main.SecurityContext.Capabilities = &corev1.Capabilities{}
		}
		for _, c := range p.capAdd {
			main.SecurityContext.Capabilities.Add = append(main.SecurityContext.Capabilities.Add, corev1.Capability(c))
		}
		for _, c := range p.capDrop {
			main.SecurityContext.Capabilities.Drop = append(main.SecurityContext.Capabilities.Drop, corev1.Capability(c))
		}
	}

	job.Spec.Template.Spec.Containers[mainIdx] = main

	p.mu.Lock()
	services := slices.Clone(p.services)
	p.mu.Unlock()

	var hostAliases []string
	for _, svc := range services {
		svcContainer := corev1.Container{
			Name:  svc.name,
			Image: svc.image,
			Env:   svc.env,
			Ports: svc.ports,
			VolumeMounts: []corev1.VolumeMount{{
				Name:      "shared",
				MountPath: k8sSharedMount,
			}},
		}
		job.Spec.Template.Spec.Containers = append(job.Spec.Template.Spec.Containers, svcContainer)
		hostAliases = append(hostAliases, svc.name)
	}

	if len(hostAliases) > 0 {
		job.Spec.Template.Spec.HostAliases = append(job.Spec.Template.Spec.HostAliases, corev1.HostAlias{
			IP:        "127.0.0.1",
			Hostnames: hostAliases,
		})
	}

	job.Spec.Template.Spec.RestartPolicy = corev1.RestartPolicyNever
	job.Spec.BackoffLimit = new(int32)
	*job.Spec.BackoffLimit = 0
	job.Spec.Completions = new(int32)
	*job.Spec.Completions = 1
	job.Spec.Parallelism = new(int32)
	*job.Spec.Parallelism = 1
	job.Spec.ActiveDeadlineSeconds = new(int64)
	*job.Spec.ActiveDeadlineSeconds = int64(timeout.Seconds())

	created, err := p.client.BatchV1().Jobs(p.namespace).Create(ctx, job, metav1.CreateOptions{})
	if err != nil {
		return nil, fmt.Errorf("k8s create job: %w", err)
	}
	p.job = created
	return created, nil
}

func (p *K8sJob) findMainContainer(podTemplate *corev1.PodTemplateSpec) int {
	for i, c := range podTemplate.Spec.Containers {
		if c.Name == k8sMainContainerName {
			return i
		}
	}
	return -1
}

func (p *K8sJob) ensureSharedVolume(podTemplate *corev1.PodTemplateSpec, main *corev1.Container) {
	hasVolume := false
	for _, v := range podTemplate.Spec.Volumes {
		if v.Name == "shared" {
			hasVolume = true
			break
		}
	}
	if !hasVolume {
		sizeLimit := resource.MustParse("10Gi")
		podTemplate.Spec.Volumes = append(podTemplate.Spec.Volumes, corev1.Volume{
			Name: "shared",
			VolumeSource: corev1.VolumeSource{
				EmptyDir: &corev1.EmptyDirVolumeSource{
					SizeLimit: &sizeLimit,
				},
			},
		})
	}

	hasMount := false
	for _, m := range main.VolumeMounts {
		if m.Name == "shared" {
			hasMount = true
			break
		}
	}
	if !hasMount {
		main.VolumeMounts = append(main.VolumeMounts, corev1.VolumeMount{
			Name:      "shared",
			MountPath: k8sSharedMount,
		})
	}
}

func (p *K8sJob) waitForJobRunning(ctx context.Context, job *batchv1.Job) error {
	// Use deadline from context if set, otherwise use PollTimeout
	if _, ok := ctx.Deadline(); !ok {
		pollTimeout := p.config.PollTimeout
		if pollTimeout == 0 {
			pollTimeout = 10 * time.Minute
		}
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, pollTimeout)
		defer cancel()
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		current, err := p.client.BatchV1().Jobs(p.namespace).Get(ctx, job.Name, metav1.GetOptions{})
		if err != nil {
			if apierrors.IsNotFound(err) {
				return errors.New("job was deleted while waiting for it to start")
			}
			return fmt.Errorf("get job status: %w", err)
		}
		if done, err := jobStartedOrTerminal(current); done {
			return err
		}

		watcher, err := p.client.BatchV1().Jobs(p.namespace).Watch(ctx, metav1.ListOptions{
			FieldSelector:   "metadata.name=" + job.Name,
			ResourceVersion: current.ResourceVersion,
		})
		if err != nil {
			return fmt.Errorf("watch job: %w", err)
		}
		defer watcher.Stop()

		_, err = watch.UntilWithoutRetry(ctx, watcher, func(event metav1watch.Event) (bool, error) {
			if event.Type == metav1watch.Deleted {
				return true, errors.New("job was deleted while waiting for it to start")
			}
			watchedJob, ok := event.Object.(*batchv1.Job)
			if !ok {
				return false, nil
			}
			return jobStartedOrTerminal(watchedJob)
		})
		if err != nil {
			if errors.Is(err, watch.ErrWatchClosed) {
				if ctx.Err() != nil {
					return errors.New("timeout waiting for job to become ready")
				}
				slog.Debug("watch channel closed, retrying", "job", job.Name)
				continue
			}
			if ctx.Err() != nil {
				return errors.New("timeout waiting for job to become ready")
			}
			return err
		}
		return nil
	}
}

func jobStartedOrTerminal(job *batchv1.Job) (bool, error) {
	for _, cond := range job.Status.Conditions {
		if cond.Type == batchv1.JobFailed && cond.Status == corev1.ConditionTrue {
			return true, fmt.Errorf("job failed: reason=%s message=%s", cond.Reason, cond.Message)
		}
	}
	if job.Status.Active > 0 || job.Status.Succeeded > 0 {
		return true, nil
	}
	if job.Status.Failed > 0 {
		return true, errors.New("job failed")
	}
	return false, nil
}

func (p *K8sJob) waitForAllContainersReady(ctx context.Context) (string, error) {
	if _, ok := ctx.Deadline(); !ok {
		pollTimeout := p.config.PollTimeout
		if pollTimeout == 0 {
			pollTimeout = 10 * time.Minute
		}
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, pollTimeout)
		defer cancel()
	}

	lw := &cache.ListWatch{
		ListFunc: func(opts metav1.ListOptions) (runtime.Object, error) {
			opts.LabelSelector = "batch.kubernetes.io/job-name=" + p.job.Name
			return p.client.CoreV1().Pods(p.namespace).List(ctx, opts)
		},
		WatchFunc: func(opts metav1.ListOptions) (metav1watch.Interface, error) {
			opts.LabelSelector = "batch.kubernetes.io/job-name=" + p.job.Name
			return p.client.CoreV1().Pods(p.namespace).Watch(ctx, opts)
		},
	}
	ev, err := watch.UntilWithSync(ctx, lw, &corev1.Pod{}, nil, func(event metav1watch.Event) (bool, error) {
		if event.Type == metav1watch.Deleted {
			return true, errors.New("pod was deleted while waiting for containers to be ready")
		}
		pod, ok := event.Object.(*corev1.Pod)
		if !ok {
			return false, nil
		}
		return podCondition(pod)
	})
	if wait.Interrupted(err) {
		return "", errors.New("timeout waiting for all containers to become ready")
	}
	if err != nil {
		return "", err
	}
	if ev == nil {
		return "", errors.New("watch ended without finding ready containers")
	}
	return ev.Object.(*corev1.Pod).Name, nil
}

func (p *K8sJob) waitForExecReady(ctx context.Context, podName string) error {
	if p.job == nil {
		return errors.New("job not started")
	}

	slog.Info("waiting for exec to be ready", "job", p.job.Name, "pod", podName)

	start := time.Now()
	err := wait.PollUntilContextCancel(ctx, 100*time.Millisecond, true, func(ctx context.Context) (done bool, err error) {
		exec, err := p.newExecCommand(podName, &corev1.PodExecOptions{
			Container: k8sMainContainerName,
			Command:   []string{"echo", "ready"},
			Stdin:     false,
			Stdout:    true,
			Stderr:    false,
		})
		if err != nil {
			slog.Debug("exec setup failed, retrying", "error", err)
			return false, nil
		}
		err = exec.StreamWithContext(ctx, remotecommand.StreamOptions{
			Stdout: io.Discard, Stderr: io.Discard,
		})
		if err != nil {
			slog.Debug("exec probe failed, retrying", "error", err)
			return false, nil
		}
		slog.Info("exec probe succeeded", "job", p.job.Name, "pod", podName)
		return true, nil
	})
	if errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("timeout waiting for container to accept exec after %v", time.Since(start))
	}
	return err
}

func podCondition(pod *corev1.Pod) (bool, error) {
	// Check for container failures
	for _, cs := range pod.Status.ContainerStatuses {
		if w := cs.State.Waiting; w != nil {
			switch w.Reason {
			case "ErrImagePull", "ImagePullBackOff", "CreateContainerError",
				"CreateContainerConfigError", "InvalidImageName":
				return false, fmt.Errorf("container %s: %s: %s", cs.Name, w.Reason, w.Message)
			}
		}
	}
	for _, cs := range pod.Status.InitContainerStatuses {
		if cs.Started != nil && !*cs.Started {
			// Init container started but failed
			if w := cs.State.Waiting; w != nil {
				switch w.Reason {
				case "ErrImagePull", "ImagePullBackOff", "CreateContainerError",
					"CreateContainerConfigError", "InvalidImageName":
					return false, fmt.Errorf("init container %s: %s: %s", cs.Name, w.Reason, w.Message)
				}
			}
		}
		if w := cs.State.Waiting; w != nil {
			switch w.Reason {
			case "ErrImagePull", "ImagePullBackOff", "CreateContainerError",
				"CreateContainerConfigError", "InvalidImageName":
				return false, fmt.Errorf("init container %s: %s: %s", cs.Name, w.Reason, w.Message)
			}
		}
	}

	// Check if pod has failed
	if pod.Status.Phase == corev1.PodFailed {
		return false, fmt.Errorf("pod failed: %s", pod.Status.Message)
	}

	// Check all containers are ready
	for _, cs := range pod.Status.ContainerStatuses {
		if !cs.Ready {
			return false, nil
		}
	}
	for _, cs := range pod.Status.InitContainerStatuses {
		if cs.Started == nil || !cs.Ready {
			return false, nil
		}
	}
	return true, nil
}

func (p *K8sJob) deleteJob(ctx context.Context) error {
	if p.job == nil {
		return nil
	}
	propagation := metav1.DeletePropagationForeground
	err := p.client.BatchV1().Jobs(p.namespace).Delete(ctx, p.job.Name, metav1.DeleteOptions{
		PropagationPolicy:  &propagation,
		GracePeriodSeconds: ptr.To[int64](30),
	})
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete job %q: %w", p.job.Name, err)
	}
	p.job = nil
	return nil
}

// isClosedStreamError reports whether err is a network error caused by the
// container or its network being torn down mid-stream.
func isClosedStreamError(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	return strings.Contains(s, "use of closed network connection") ||
		strings.Contains(s, "read/write on closed pipe")
}

// teeStderr always captures into errBuf so error messages can include stderr,
// optionally also forwarding to extra.
func teeStderr(errBuf *bytes.Buffer, extra io.Writer) io.Writer {
	if extra == nil {
		return errBuf
	}
	return io.MultiWriter(errBuf, extra)
}

func (p *K8sJob) mkdir(ctx context.Context, path string) error {
	exec, err := p.newExecCommand(p.podName, &corev1.PodExecOptions{
		Container: k8sMainContainerName,
		Command:   []string{"mkdir", "-p", path},
		Stderr:    true,
	})
	if err != nil {
		return fmt.Errorf("setup mkdir exec: %w", err)
	}

	p.mu.Lock()
	stderr := p.stderr
	p.mu.Unlock()

	var errBuf bytes.Buffer
	if err := exec.StreamWithContext(ctx, remotecommand.StreamOptions{Stderr: teeStderr(&errBuf, stderr)}); err != nil {
		return fmt.Errorf("mkdir %q: %w (stderr: %s)", path, err, strings.TrimSpace(errBuf.String()))
	}
	return nil
}

func (p *K8sJob) execTarExtract(ctx context.Context, destPath string, input io.Reader) error {
	exec, err := p.newExecCommand(p.podName, &corev1.PodExecOptions{
		Container: k8sMainContainerName,
		Command:   []string{"tar", "xzf", "-", "-C", destPath},
		Stdin:     true,
		Stderr:    true,
	})
	if err != nil {
		return fmt.Errorf("setup tar extract exec: %w", err)
	}

	p.mu.Lock()
	stderr := p.stderr
	p.mu.Unlock()

	var errBuf bytes.Buffer
	if err := exec.StreamWithContext(ctx, remotecommand.StreamOptions{
		Stdin:  input,
		Stderr: teeStderr(&errBuf, stderr),
	}); err != nil {
		if isClosedStreamError(err) {
			return fmt.Errorf("tar extract to %q: stream closed (container may have terminated)", destPath)
		}
		return fmt.Errorf("tar extract to %q: %w (stderr: %s)", destPath, err, strings.TrimSpace(errBuf.String()))
	}
	return nil
}

func (p *K8sJob) newExecCommand(podName string, opts *corev1.PodExecOptions) (remotecommand.Executor, error) {
	if p.client == nil {
		return nil, errors.New("client not initialized")
	}
	slog.Debug("exec: pod info", "podName", podName, "namespace", p.namespace, "container", opts.Container)

	req := p.client.CoreV1().RESTClient().Post().
		Resource("pods").
		Name(podName).
		Namespace(p.namespace).
		SubResource("exec")
	req.VersionedParams(opts, scheme.ParameterCodec)
	url := req.URL()
	slog.Debug("exec: URL", "url", url.String())

	wsExec, err := remotecommand.NewWebSocketExecutor(p.restCfg, "GET", url.String())
	if err != nil {
		return nil, fmt.Errorf("create WebSocket executor: %w", err)
	}
	spdyExec, err := remotecommand.NewSPDYExecutor(p.restCfg, "POST", url)
	if err != nil {
		return nil, fmt.Errorf("create SPDY executor: %w", err)
	}
	exec, err := remotecommand.NewFallbackExecutor(wsExec, spdyExec, func(err error) bool {
		return true
	})
	if err != nil {
		return nil, fmt.Errorf("create fallback executor: %w", err)
	}
	return exec, nil
}
