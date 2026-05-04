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
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
	"k8s.io/kubectl/pkg/scheme"
	"k8s.io/streaming/pkg/httpstream"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/yaml"
)

// k8sPodSpecImageSentinel means "defer to the podspec's image".
const k8sPodSpecImageSentinel = "k8spod"

type K8sPodConfig struct {
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

type K8sPod struct {
	container.LinuxContainerEnvironmentExtensions

	client      kubernetes.Interface
	restCfg     *rest.Config
	pod         *corev1.Pod
	namespace   string
	input       container.NewContainerInput
	config      *K8sPodConfig
	services    []serviceContainerSpec
	capAdd      []string
	capDrop     []string
	extraLabels map[string]string

	mu     sync.Mutex
	stdout io.Writer
	stderr io.Writer
}

func (p *K8sPod) AddServiceContainerRaw(name, image string, env map[string]string, ports []string) {
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

func (p *K8sPod) Create(capAdd, capDrop []string) common.Executor {
	return func(_ context.Context) error {
		p.capAdd = capAdd
		p.capDrop = capDrop
		return nil
	}
}

func (p *K8sPod) Start(_ bool) common.Executor {
	return func(ctx context.Context) error {
		pod, err := p.createPod(ctx)
		if err != nil {
			return fmt.Errorf("create pod: %w", err)
		}
		slog.Info("created pod", "namespace", pod.Namespace, "name", pod.Name)

		if err := p.waitForPodRunning(ctx, pod); err != nil {
			if delErr := p.deletePod(context.Background()); delErr != nil {
				slog.Warn("failed to clean up pod after startup failure", "error", delErr)
			}
			return fmt.Errorf("wait for pod: %w", err)
		}

		p.pod = pod
		slog.Info("pod is running", "name", pod.Name)

		if err := p.waitForAllContainersReady(ctx); err != nil {
			return fmt.Errorf("wait for containers to be ready: %w", err)
		}
		slog.Info("all containers ready", "name", pod.Name)

		return nil
	}
}

func (p *K8sPod) Exec(command []string, env map[string]string, user, workdir string) common.Executor {
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

		exec, err := p.newExecCommand(&corev1.PodExecOptions{
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

		if _, healthErr := p.IsHealthy(ctx); healthErr != nil {
			return fmt.Errorf("pod unhealthy after exec: %w", healthErr)
		}

		return nil
	}
}

func (p *K8sPod) Copy(destPath string, files ...*container.FileEntry) common.Executor {
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

func (p *K8sPod) CopyDir(destPath, srcPath string, _ bool) common.Executor {
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

func (p *K8sPod) CopyTarStream(ctx context.Context, destPath string, tarStream io.Reader) error {
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

func (p *K8sPod) GetContainerArchive(ctx context.Context, srcPath string) (io.ReadCloser, error) {
	dir := filepath.Dir(srcPath)
	base := filepath.Base(srcPath)

	exec, err := p.newExecCommand(&corev1.PodExecOptions{
		Container: k8sMainContainerName,
		Command:   []string{"tar", "cf", "-", "-C", dir, base},
		Stdout:    true,
		Stderr:    true,
	})
	if err != nil {
		return nil, fmt.Errorf("setup tar exec: %w", err)
	}

	pr, pw := io.Pipe()
	go func() {
		var errBuf bytes.Buffer
		err := exec.StreamWithContext(ctx, remotecommand.StreamOptions{
			Stdout: pw,
			Stderr: &errBuf,
		})
		if err != nil {
			pw.CloseWithError(fmt.Errorf("tar exec: %w (stderr: %s)", err, errBuf.String()))
		} else {
			pw.Close()
		}
	}()

	return pr, nil
}

func (p *K8sPod) Pull(_ bool) common.Executor {
	return func(_ context.Context) error {
		return nil
	}
}

func (p *K8sPod) ConnectToNetwork(_ string) common.Executor {
	return func(_ context.Context) error {
		return nil
	}
}

func (p *K8sPod) UpdateFromEnv(srcPath string, env *map[string]string) common.Executor {
	return container.ParseEnvFile(p, srcPath, env).IfNot(common.Dryrun)
}

func (p *K8sPod) UpdateFromImageEnv(_ *map[string]string) common.Executor {
	return func(_ context.Context) error {
		return nil
	}
}

func (p *K8sPod) Remove() common.Executor {
	return func(ctx context.Context) error {
		return p.deletePod(ctx)
	}
}

func (p *K8sPod) Close() common.Executor {
	return p.Remove()
}

func (p *K8sPod) ReplaceLogWriter(stdout, stderr io.Writer) (io.Writer, io.Writer) {
	p.mu.Lock()
	defer p.mu.Unlock()
	oldStdout := p.stdout
	oldStderr := p.stderr
	p.stdout = stdout
	p.stderr = stderr
	return oldStdout, oldStderr
}

func (p *K8sPod) IsHealthy(ctx context.Context) (time.Duration, error) {
	if p.pod == nil {
		return 0, errors.New("pod not started")
	}
	pod, err := p.client.CoreV1().Pods(p.namespace).Get(ctx, p.pod.Name, metav1.GetOptions{})
	if err != nil {
		return 0, fmt.Errorf("get pod status: %w", err)
	}
	if pod.Status.Phase != corev1.PodRunning {
		return 0, fmt.Errorf("pod is %s: reason=%s message=%s", pod.Status.Phase, pod.Status.Reason, pod.Status.Message)
	}
	// Phase==Running can mask a dead main container; inspect it directly.
	for _, cs := range pod.Status.ContainerStatuses {
		if cs.Name != k8sMainContainerName {
			continue
		}
		if cs.State.Terminated != nil {
			return 0, fmt.Errorf("main container terminated: exit=%d reason=%s message=%s",
				cs.State.Terminated.ExitCode, cs.State.Terminated.Reason, cs.State.Terminated.Message)
		}
		if cs.State.Waiting != nil && cs.State.Waiting.Reason == "CrashLoopBackOff" {
			return 0, fmt.Errorf("main container in CrashLoopBackOff: %s", cs.State.Waiting.Message)
		}
	}
	return 0, nil
}

func (*K8sPod) BackendName() string {
	return "k8spod"
}

func (*K8sPod) SupportsDockerActions() bool {
	return false
}

func (*K8sPod) ManagesOwnNetworking() bool {
	return true
}

func (*K8sPod) GetActPath() string {
	return k8sActPath
}

func (*K8sPod) GetRoot() string {
	return k8sSharedMount
}

func (*K8sPod) GetName() string {
	return "k8spod"
}

func (p *K8sPod) GetRunnerContext(ctx context.Context) map[string]any {
	return map[string]any{
		"os":         "Linux",
		"arch":       container.RunnerArch(ctx),
		"temp":       "/tmp",
		"tool_cache": k8sToolCache,
	}
}

func (p *K8sPod) createPod(ctx context.Context) (*corev1.Pod, error) {
	timeout := p.config.JobTimeout
	if timeout <= 0 {
		timeout = 3 * time.Hour
	}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: "forgejo-runner-task-",
			Namespace:    p.namespace,
			Labels: map[string]string{
				"app.kubernetes.io/managed-by": "forgejo-runner",
			},
		},
	}

	maps.Copy(pod.Labels, p.extraLabels)

	if p.config.PodSpec != "" {
		data, err := os.ReadFile(p.config.PodSpec)
		if err != nil {
			return nil, fmt.Errorf("read podspec %q: %w", p.config.PodSpec, err)
		}
		if err := yaml.Unmarshal(data, &pod.Spec); err != nil {
			return nil, fmt.Errorf("parse podspec %q: %w", p.config.PodSpec, err)
		}
	}

	mainIdx := p.findMainContainer(pod)
	if mainIdx < 0 {
		pod.Spec.Containers = append([]corev1.Container{{
			Name: k8sMainContainerName,
		}}, pod.Spec.Containers...)
		mainIdx = 0
	}

	// Work on a copy to avoid pointer invalidation when appending sidecars.
	main := pod.Spec.Containers[mainIdx]

	if p.input.Image != "" && p.input.Image != k8sPodSpecImageSentinel {
		main.Image = p.input.Image
	}
	if main.Image == "" {
		return nil, errors.New("no container image specified (set it in the podspec or workflow runs-on)")
	}

	// Replace the podspec command with a sleep — work runs via exec.
	// WorkingDir is intentionally not set here; per-step workdir is applied in Exec.
	main.Command = []string{"sh", "-c", fmt.Sprintf("mkdir -p %s && sleep %d", k8sWorkDir, int64(timeout.Seconds()))}

	for _, kv := range p.input.Env {
		parts := strings.SplitN(kv, "=", 2)
		if len(parts) == 2 {
			main.Env = append(main.Env, corev1.EnvVar{Name: parts[0], Value: parts[1]})
		}
	}

	p.ensureSharedVolume(pod, &main)

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

	pod.Spec.Containers[mainIdx] = main

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
		pod.Spec.Containers = append(pod.Spec.Containers, svcContainer)
		hostAliases = append(hostAliases, svc.name)
	}

	if len(hostAliases) > 0 {
		pod.Spec.HostAliases = append(pod.Spec.HostAliases, corev1.HostAlias{
			IP:        "127.0.0.1",
			Hostnames: hostAliases,
		})
	}

	pod.Spec.RestartPolicy = corev1.RestartPolicyNever

	created, err := p.client.CoreV1().Pods(p.namespace).Create(ctx, pod, metav1.CreateOptions{})
	if err != nil {
		return nil, fmt.Errorf("k8s create pod: %w", err)
	}
	p.pod = created
	return created, nil
}

func (p *K8sPod) findMainContainer(pod *corev1.Pod) int {
	for i, c := range pod.Spec.Containers {
		if c.Name == k8sMainContainerName {
			return i
		}
	}
	return -1
}

func (p *K8sPod) ensureSharedVolume(pod *corev1.Pod, main *corev1.Container) {
	hasVolume := false
	for _, v := range pod.Spec.Volumes {
		if v.Name == "shared" {
			hasVolume = true
			break
		}
	}
	if !hasVolume {
		// Default 10Gi limit; override by defining a "shared" volume in the podspec.
		sizeLimit := resource.MustParse("10Gi")
		pod.Spec.Volumes = append(pod.Spec.Volumes, corev1.Volume{
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

func (p *K8sPod) waitForPodRunning(ctx context.Context, pod *corev1.Pod) error {
	pollTimeout := p.config.PollTimeout
	if pollTimeout == 0 {
		pollTimeout = 10 * time.Minute
	}

	watchCtx, cancel := context.WithTimeout(ctx, pollTimeout)
	defer cancel()

	for {
		// Get-then-Watch with ResourceVersion: covers the case where the
		// transition happens between iterations and the watch would miss it.
		current, err := p.client.CoreV1().Pods(p.namespace).Get(watchCtx, pod.Name, metav1.GetOptions{})
		if err != nil {
			if apierrors.IsNotFound(err) {
				return errors.New("pod was deleted while waiting for it to start")
			}
			return fmt.Errorf("get pod status: %w", err)
		}
		if done, err := podPhaseTerminal(current); done {
			return err
		}

		watcher, err := p.client.CoreV1().Pods(p.namespace).Watch(watchCtx, metav1.ListOptions{
			FieldSelector:   "metadata.name=" + pod.Name,
			ResourceVersion: current.ResourceVersion,
		})
		if err != nil {
			return fmt.Errorf("watch pod: %w", err)
		}

		done, err := p.consumePodWatch(watcher)
		watcher.Stop()
		if done {
			return err
		}

		if watchCtx.Err() != nil {
			return errors.New("timeout waiting for pod to become ready")
		}
		slog.Debug("watch channel closed, retrying", "pod", pod.Name)
	}
}

// podPhaseTerminal reports whether the phase is decisive (Running/Failed/Succeeded).
func podPhaseTerminal(pod *corev1.Pod) (bool, error) {
	switch pod.Status.Phase {
	case corev1.PodRunning:
		return true, nil
	case corev1.PodFailed:
		return true, fmt.Errorf("pod failed: reason=%s message=%s", pod.Status.Reason, pod.Status.Message)
	case corev1.PodSucceeded:
		return true, errors.New("pod completed unexpectedly")
	default:
		return false, nil
	}
}

func (p *K8sPod) consumePodWatch(watcher watch.Interface) (bool, error) {
	for event := range watcher.ResultChan() {
		switch event.Type {
		case watch.Modified, watch.Added:
			if eventPod, ok := event.Object.(*corev1.Pod); ok {
				if done, err := podPhaseTerminal(eventPod); done {
					return true, err
				}
			}
		case watch.Deleted:
			return true, errors.New("pod was deleted while waiting for it to start")
		case watch.Error:
			if status, ok := event.Object.(*metav1.Status); ok {
				return true, fmt.Errorf("watch error: %s", status.Message)
			}
			return true, errors.New("unknown watch error")
		}
	}
	return false, nil
}

func (p *K8sPod) waitForAllContainersReady(ctx context.Context) error {
	pollTimeout := p.config.PollTimeout
	if pollTimeout == 0 {
		pollTimeout = 10 * time.Minute
	}

	watchCtx, cancel := context.WithTimeout(ctx, pollTimeout)
	defer cancel()

	for {
		watcher, err := p.client.CoreV1().Pods(p.namespace).Watch(watchCtx, metav1.ListOptions{
			FieldSelector: "metadata.name=" + p.pod.Name,
		})
		if err != nil {
			return fmt.Errorf("watch pod for container readiness: %w", err)
		}

		for event := range watcher.ResultChan() {
			if event.Type != watch.Modified && event.Type != watch.Added {
				continue
			}
			eventPod, ok := event.Object.(*corev1.Pod)
			if !ok {
				continue
			}
			if podAllContainersReady(eventPod) {
				watcher.Stop()
				return nil
			}
		}
		watcher.Stop()

		if watchCtx.Err() != nil {
			return fmt.Errorf("timeout waiting for all containers to become ready")
		}
		slog.Debug("watch channel closed, retrying", "pod", p.pod.Name)
	}
}

func podAllContainersReady(pod *corev1.Pod) bool {
	for _, cs := range pod.Status.ContainerStatuses {
		if !cs.Ready {
			return false
		}
	}
	for _, cs := range pod.Status.InitContainerStatuses {
		if cs.Started != nil && *cs.Started && !cs.Ready {
			return false
		}
	}
	return true
}

func (p *K8sPod) deletePod(ctx context.Context) error {
	if p.pod == nil {
		return nil
	}
	propagation := metav1.DeletePropagationForeground
	err := p.client.CoreV1().Pods(p.namespace).Delete(ctx, p.pod.Name, metav1.DeleteOptions{
		PropagationPolicy:  &propagation,
		GracePeriodSeconds: ptr.To[int64](30),
	})
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete pod %q: %w", p.pod.Name, err)
	}
	p.pod = nil
	return nil
}

// teeStderr always captures into errBuf so error messages can include stderr,
// optionally also forwarding to extra.
func teeStderr(errBuf *bytes.Buffer, extra io.Writer) io.Writer {
	if extra == nil {
		return errBuf
	}
	return io.MultiWriter(errBuf, extra)
}

func (p *K8sPod) mkdir(ctx context.Context, path string) error {
	exec, err := p.newExecCommand(&corev1.PodExecOptions{
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

func (p *K8sPod) execTarExtract(ctx context.Context, destPath string, input io.Reader) error {
	exec, err := p.newExecCommand(&corev1.PodExecOptions{
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
		return fmt.Errorf("tar extract to %q: %w (stderr: %s)", destPath, err, strings.TrimSpace(errBuf.String()))
	}
	return nil
}

func (p *K8sPod) newExecCommand(opts *corev1.PodExecOptions) (remotecommand.Executor, error) {
	if p.pod == nil {
		return nil, errors.New("pod not started")
	}

	req := p.client.CoreV1().RESTClient().Post().
		Resource("pods").
		Name(p.pod.Name).
		Namespace(p.namespace).
		SubResource("exec")
	req.VersionedParams(opts, scheme.ParameterCodec)
	url := req.URL()

	wsExec, err := remotecommand.NewWebSocketExecutor(p.restCfg, "GET", url.String())
	if err != nil {
		return nil, fmt.Errorf("create WebSocket executor: %w", err)
	}
	spdyExec, err := remotecommand.NewSPDYExecutor(p.restCfg, "POST", url)
	if err != nil {
		return nil, fmt.Errorf("create SPDY executor: %w", err)
	}
	exec, err := remotecommand.NewFallbackExecutor(wsExec, spdyExec, func(err error) bool {
		return httpstream.IsUpgradeFailure(err) || httpstream.IsHTTPSProxyError(err)
	})
	if err != nil {
		return nil, fmt.Errorf("create fallback executor: %w", err)
	}
	return exec, nil
}
