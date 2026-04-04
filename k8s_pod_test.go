package main

import (
	"context"
	"io"
	"os"
	"testing"
	"time"

	"code.forgejo.org/forgejo/runner/v12/act/container"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

var (
	_ container.ExecutionsEnvironment = &K8sPod{}
	_ container.ServiceAdder          = &K8sPod{}
)

func newTestK8sPod(t *testing.T, fakeClient *fake.Clientset) *K8sPod {
	t.Helper()
	p := &K8sPod{
		client:    fakeClient,
		namespace: "test-ns",
		input: container.NewContainerInput{
			Image:      "node:22-bookworm",
			Name:       "test-job",
			Env:        []string{"FOO=bar", "BAZ=qux"},
			WorkingDir: "/shared/workdir",
		},
		config: &K8sPodConfig{
			Namespace:   "test-ns",
			PollTimeout: 5 * time.Second,
			JobTimeout:  1 * time.Hour,
		},
		stdout: io.Discard,
		stderr: io.Discard,
	}
	p.SetToolCache(k8sToolCache)
	return p
}

func TestK8sPod_CreatePod_DefaultSpec(t *testing.T) {
	fakeClient := fake.NewSimpleClientset()
	p := newTestK8sPod(t, fakeClient)

	pod, err := p.createPod(t.Context())
	require.NoError(t, err)

	assert.Equal(t, "test-ns", pod.Namespace)
	assert.Equal(t, "forgejo-runner", pod.Labels["app.kubernetes.io/managed-by"])

	require.NotEmpty(t, pod.Spec.Containers)
	main := pod.Spec.Containers[0]
	assert.Equal(t, k8sMainContainerName, main.Name)
	assert.Equal(t, "node:22-bookworm", main.Image)
	assert.Equal(t, "/shared/workdir", main.WorkingDir)

	envNames := make(map[string]string)
	for _, e := range main.Env {
		envNames[e.Name] = e.Value
	}
	assert.Equal(t, "bar", envNames["FOO"])
	assert.Equal(t, "qux", envNames["BAZ"])

	foundVolume := false
	for _, v := range pod.Spec.Volumes {
		if v.Name == "shared" {
			foundVolume = true
			assert.NotNil(t, v.EmptyDir)
		}
	}
	assert.True(t, foundVolume)

	foundMount := false
	for _, m := range main.VolumeMounts {
		if m.Name == "shared" && m.MountPath == k8sSharedMount {
			foundMount = true
		}
	}
	assert.True(t, foundMount)
	assert.Equal(t, corev1.RestartPolicyNever, pod.Spec.RestartPolicy)
}

func TestK8sPod_CreatePod_WithCapabilities(t *testing.T) {
	fakeClient := fake.NewSimpleClientset()
	p := newTestK8sPod(t, fakeClient)
	p.capAdd = []string{"NET_ADMIN"}
	p.capDrop = []string{"ALL"}

	pod, err := p.createPod(t.Context())
	require.NoError(t, err)

	main := pod.Spec.Containers[0]
	require.NotNil(t, main.SecurityContext)
	require.NotNil(t, main.SecurityContext.Capabilities)
	assert.Contains(t, main.SecurityContext.Capabilities.Add, corev1.Capability("NET_ADMIN"))
	assert.Contains(t, main.SecurityContext.Capabilities.Drop, corev1.Capability("ALL"))
}

func TestK8sPod_CreatePod_WithServiceContainers(t *testing.T) {
	fakeClient := fake.NewSimpleClientset()
	p := newTestK8sPod(t, fakeClient)
	p.AddServiceContainerRaw("redis", "redis:7", map[string]string{"REDIS_PASS": "secret"}, []string{"6379"})
	p.AddServiceContainerRaw("postgres", "postgres:16", map[string]string{"POSTGRES_DB": "test"}, []string{"5432:5432"})

	pod, err := p.createPod(t.Context())
	require.NoError(t, err)

	assert.Len(t, pod.Spec.Containers, 3)

	redis := pod.Spec.Containers[1]
	assert.Equal(t, "redis", redis.Name)
	assert.Equal(t, "redis:7", redis.Image)
	assert.Len(t, redis.Ports, 1)
	assert.Equal(t, int32(6379), redis.Ports[0].ContainerPort)

	foundMount := false
	for _, m := range redis.VolumeMounts {
		if m.Name == "shared" && m.MountPath == k8sSharedMount {
			foundMount = true
		}
	}
	assert.True(t, foundMount)

	require.NotEmpty(t, pod.Spec.HostAliases)
	alias := pod.Spec.HostAliases[0]
	assert.Equal(t, "127.0.0.1", alias.IP)
	assert.Contains(t, alias.Hostnames, "redis")
	assert.Contains(t, alias.Hostnames, "postgres")
}

func TestK8sPod_CreatePod_NoImage(t *testing.T) {
	fakeClient := fake.NewSimpleClientset()
	p := newTestK8sPod(t, fakeClient)
	p.input.Image = "k8spod"

	_, err := p.createPod(t.Context())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no container image specified")
}

func TestK8sPod_CreatePod_WithPodSpec(t *testing.T) {
	tmpFile := t.TempDir() + "/podspec.yaml"
	require.NoError(t, os.WriteFile(tmpFile, []byte(`containers:
  - name: main
    image: custom-image:latest
    resources:
      requests:
        cpu: "500m"
        memory: "512Mi"
restartPolicy: Never
`), 0o644))

	fakeClient := fake.NewSimpleClientset()
	p := newTestK8sPod(t, fakeClient)
	p.input.Image = "k8spod"
	p.config.PodSpec = tmpFile

	pod, err := p.createPod(t.Context())
	require.NoError(t, err)

	main := pod.Spec.Containers[0]
	assert.Equal(t, "custom-image:latest", main.Image)
	assert.NotNil(t, main.Resources.Requests)
}

func TestK8sPod_CreatePod_PodSpecImageOverriddenByInput(t *testing.T) {
	tmpFile := t.TempDir() + "/podspec.yaml"
	require.NoError(t, os.WriteFile(tmpFile, []byte(`containers:
  - name: main
    image: podspec-image:v1
`), 0o644))

	fakeClient := fake.NewSimpleClientset()
	p := newTestK8sPod(t, fakeClient)
	p.input.Image = "workflow-image:v2"
	p.config.PodSpec = tmpFile

	pod, err := p.createPod(t.Context())
	require.NoError(t, err)
	assert.Equal(t, "workflow-image:v2", pod.Spec.Containers[0].Image)
}

func TestK8sPod_CreatePod_InvalidPodSpec(t *testing.T) {
	tmpFile := t.TempDir() + "/bad.yaml"
	require.NoError(t, os.WriteFile(tmpFile, []byte("not: [valid: yaml: {{"), 0o644))

	fakeClient := fake.NewSimpleClientset()
	p := newTestK8sPod(t, fakeClient)
	p.config.PodSpec = tmpFile

	_, err := p.createPod(t.Context())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parse podspec")
}

func TestK8sPod_CreatePod_MissingPodSpecFile(t *testing.T) {
	fakeClient := fake.NewSimpleClientset()
	p := newTestK8sPod(t, fakeClient)
	p.config.PodSpec = "/nonexistent/podspec.yaml"

	_, err := p.createPod(t.Context())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "read podspec")
}

func TestK8sPod_WaitForPodRunning(t *testing.T) {
	fakeClient := fake.NewSimpleClientset()
	watcher := watch.NewFake()
	fakeClient.PrependWatchReactor("pods", k8stesting.DefaultWatchReactor(watcher, nil))

	p := newTestK8sPod(t, fakeClient)
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "test-pod", Namespace: "test-ns"}}

	go func() {
		watcher.Modify(&corev1.Pod{
			ObjectMeta: pod.ObjectMeta,
			Status:     corev1.PodStatus{Phase: corev1.PodPending},
		})
		watcher.Modify(&corev1.Pod{
			ObjectMeta: pod.ObjectMeta,
			Status:     corev1.PodStatus{Phase: corev1.PodRunning},
		})
	}()

	require.NoError(t, p.waitForPodRunning(t.Context(), pod))
}

func TestK8sPod_WaitForPodRunning_PodFailed(t *testing.T) {
	fakeClient := fake.NewSimpleClientset()
	watcher := watch.NewFake()
	fakeClient.PrependWatchReactor("pods", k8stesting.DefaultWatchReactor(watcher, nil))

	p := newTestK8sPod(t, fakeClient)
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "test-pod", Namespace: "test-ns"}}

	go func() {
		watcher.Modify(&corev1.Pod{
			ObjectMeta: pod.ObjectMeta,
			Status:     corev1.PodStatus{Phase: corev1.PodFailed, Reason: "OOMKilled"},
		})
	}()

	err := p.waitForPodRunning(t.Context(), pod)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "pod failed")
	assert.Contains(t, err.Error(), "OOMKilled")
}

func TestK8sPod_WaitForPodRunning_PodDeleted(t *testing.T) {
	fakeClient := fake.NewSimpleClientset()
	watcher := watch.NewFake()
	fakeClient.PrependWatchReactor("pods", k8stesting.DefaultWatchReactor(watcher, nil))

	p := newTestK8sPod(t, fakeClient)
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "test-pod", Namespace: "test-ns"}}

	go func() {
		watcher.Delete(&corev1.Pod{ObjectMeta: pod.ObjectMeta})
	}()

	err := p.waitForPodRunning(t.Context(), pod)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "deleted")
}

func TestK8sPod_WaitForPodRunning_Timeout(t *testing.T) {
	fakeClient := fake.NewSimpleClientset()
	fakeWatcher := watch.NewFake()
	fakeClient.PrependWatchReactor("pods", k8stesting.DefaultWatchReactor(fakeWatcher, nil))

	p := newTestK8sPod(t, fakeClient)
	p.config.PollTimeout = 100 * time.Millisecond
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "test-pod", Namespace: "test-ns"}}

	go func() {
		time.Sleep(150 * time.Millisecond)
		fakeWatcher.Stop()
	}()

	err := p.waitForPodRunning(t.Context(), pod)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "timeout")
}

func TestK8sPod_DeletePod(t *testing.T) {
	t.Run("deletes existing pod", func(t *testing.T) {
		existingPod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "test-pod", Namespace: "test-ns"}}
		fakeClient := fake.NewSimpleClientset(existingPod)
		p := newTestK8sPod(t, fakeClient)
		p.pod = existingPod

		require.NoError(t, p.deletePod(t.Context()))
		assert.Nil(t, p.pod)

		_, err := fakeClient.CoreV1().Pods("test-ns").Get(t.Context(), "test-pod", metav1.GetOptions{})
		require.Error(t, err)
	})

	t.Run("nil pod is a no-op", func(t *testing.T) {
		fakeClient := fake.NewSimpleClientset()
		p := newTestK8sPod(t, fakeClient)
		p.pod = nil
		require.NoError(t, p.deletePod(t.Context()))
	})
}

func TestK8sPod_IsHealthy(t *testing.T) {
	t.Run("healthy running pod", func(t *testing.T) {
		runningPod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "test-pod", Namespace: "test-ns"},
			Status:     corev1.PodStatus{Phase: corev1.PodRunning},
		}
		fakeClient := fake.NewSimpleClientset(runningPod)
		p := newTestK8sPod(t, fakeClient)
		p.pod = runningPod

		_, err := p.IsHealthy(t.Context())
		require.NoError(t, err)
	})

	t.Run("failed pod", func(t *testing.T) {
		failedPod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "test-pod", Namespace: "test-ns"},
			Status:     corev1.PodStatus{Phase: corev1.PodFailed, Reason: "OOMKilled"},
		}
		fakeClient := fake.NewSimpleClientset(failedPod)
		p := newTestK8sPod(t, fakeClient)
		p.pod = failedPod

		_, err := p.IsHealthy(t.Context())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "OOMKilled")
	})

	t.Run("nil pod", func(t *testing.T) {
		fakeClient := fake.NewSimpleClientset()
		p := newTestK8sPod(t, fakeClient)

		_, err := p.IsHealthy(t.Context())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not started")
	})
}

func TestK8sPod_AddServiceContainerRaw(t *testing.T) {
	p := &K8sPod{}

	p.AddServiceContainerRaw("redis", "redis:7", map[string]string{"REDIS_PASS": "secret"}, []string{"6379"})
	p.AddServiceContainerRaw("postgres", "postgres:16", map[string]string{"POSTGRES_DB": "test"}, []string{"5432:5432"})

	require.Len(t, p.services, 2)
	assert.Equal(t, "redis", p.services[0].name)
	assert.Equal(t, "redis:7", p.services[0].image)
	assert.Len(t, p.services[0].ports, 1)
	assert.Equal(t, int32(6379), p.services[0].ports[0].ContainerPort)
	assert.Equal(t, "postgres", p.services[1].name)
	assert.Len(t, p.services[1].ports, 1)
	assert.Equal(t, int32(5432), p.services[1].ports[0].ContainerPort)
}

func TestK8sPod_ReplaceLogWriter(t *testing.T) {
	p := &K8sPod{stdout: io.Discard, stderr: io.Discard}

	var newOut, newErr testWriter
	oldOut, oldErr := p.ReplaceLogWriter(&newOut, &newErr)
	assert.Equal(t, io.Discard, oldOut)
	assert.Equal(t, io.Discard, oldErr)

	p.mu.Lock()
	assert.Equal(t, io.Writer(&newOut), p.stdout)
	assert.Equal(t, io.Writer(&newErr), p.stderr)
	p.mu.Unlock()
}

func TestK8sPod_InterfaceMethods(t *testing.T) {
	p := &K8sPod{}

	assert.Equal(t, "k8spod", p.BackendName())
	assert.False(t, p.SupportsDockerActions())
	assert.True(t, p.ManagesOwnNetworking())
	assert.Equal(t, k8sActPath, p.GetActPath())
	assert.Equal(t, k8sSharedMount, p.GetRoot())
	assert.Equal(t, "k8spod", p.GetName())
	assert.Equal(t, "PATH", p.GetPathVariableName())
	assert.False(t, p.IsEnvironmentCaseInsensitive())

	rc := p.GetRunnerContext(context.Background())
	assert.Equal(t, "Linux", rc["os"])
	assert.Equal(t, "/tmp", rc["temp"])
	assert.Equal(t, k8sToolCache, rc["tool_cache"])
}

func TestK8sPod_NoOps(t *testing.T) {
	ctx := t.Context()
	p := &K8sPod{}

	require.NoError(t, p.Pull(true)(ctx))
	require.NoError(t, p.Pull(false)(ctx))
	require.NoError(t, p.ConnectToNetwork("some-network")(ctx))

	env := map[string]string{"existing": "value"}
	require.NoError(t, p.UpdateFromImageEnv(&env)(ctx))
	assert.Equal(t, "value", env["existing"])
}

func TestK8sPod_Start_CleansUpOnWaitFailure(t *testing.T) {
	fakeClient := fake.NewSimpleClientset()
	fakeWatcher := watch.NewFake()
	fakeClient.PrependWatchReactor("pods", k8stesting.DefaultWatchReactor(fakeWatcher, nil))

	p := newTestK8sPod(t, fakeClient)
	p.config.PollTimeout = 100 * time.Millisecond

	go func() {
		time.Sleep(150 * time.Millisecond)
		fakeWatcher.Stop()
	}()

	err := p.Start(false)(t.Context())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "wait for pod")
	assert.Nil(t, p.pod)

	pods, listErr := fakeClient.CoreV1().Pods("test-ns").List(t.Context(), metav1.ListOptions{})
	require.NoError(t, listErr)
	assert.Empty(t, pods.Items)
}

func TestK8sPod_NewExecCommand_NilPod(t *testing.T) {
	p := &K8sPod{pod: nil}
	_, err := p.newExecCommand(&corev1.PodExecOptions{Command: []string{"echo"}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "pod not started")
}

func TestK8sPod_Create_StoresCapabilities(t *testing.T) {
	p := &K8sPod{}
	require.NoError(t, p.Create([]string{"NET_ADMIN", "SYS_PTRACE"}, []string{"ALL"})(t.Context()))
	assert.Equal(t, []string{"NET_ADMIN", "SYS_PTRACE"}, p.capAdd)
	assert.Equal(t, []string{"ALL"}, p.capDrop)
}

func TestK8sPod_Close_DelegatesToRemove(t *testing.T) {
	existingPod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "test-pod", Namespace: "test-ns"}}
	fakeClient := fake.NewSimpleClientset(existingPod)
	p := newTestK8sPod(t, fakeClient)
	p.pod = existingPod

	require.NoError(t, p.Close()(t.Context()))
	assert.Nil(t, p.pod)
}

func TestK8sPod_Remove_NilPod(t *testing.T) {
	fakeClient := fake.NewSimpleClientset()
	p := newTestK8sPod(t, fakeClient)
	p.pod = nil
	require.NoError(t, p.Remove()(t.Context()))
}

func TestK8sPod_EnsureSharedVolume_Idempotent(t *testing.T) {
	p := &K8sPod{}
	pod := &corev1.Pod{
		Spec: corev1.PodSpec{
			Volumes: []corev1.Volume{{
				Name:         "shared",
				VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
			}},
			Containers: []corev1.Container{{
				Name:         k8sMainContainerName,
				VolumeMounts: []corev1.VolumeMount{{Name: "shared", MountPath: k8sSharedMount}},
			}},
		},
	}

	p.ensureSharedVolume(pod, &pod.Spec.Containers[0])
	assert.Len(t, pod.Spec.Volumes, 1)
	assert.Len(t, pod.Spec.Containers[0].VolumeMounts, 1)
}

func TestK8sPod_EnsureSharedVolume_AddsWhenMissing(t *testing.T) {
	p := &K8sPod{}
	pod := &corev1.Pod{
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: k8sMainContainerName}},
		},
	}

	p.ensureSharedVolume(pod, &pod.Spec.Containers[0])
	require.Len(t, pod.Spec.Volumes, 1)
	assert.Equal(t, "shared", pod.Spec.Volumes[0].Name)
	require.Len(t, pod.Spec.Containers[0].VolumeMounts, 1)
	assert.Equal(t, k8sSharedMount, pod.Spec.Containers[0].VolumeMounts[0].MountPath)
}

func TestK8sPod_CreatePod_JobTimeoutPropagation(t *testing.T) {
	fakeClient := fake.NewSimpleClientset()
	p := newTestK8sPod(t, fakeClient)
	p.config.JobTimeout = 30 * time.Minute

	pod, err := p.createPod(t.Context())
	require.NoError(t, err)
	assert.Contains(t, pod.Spec.Containers[0].Command[2], "sleep 1800")
}

func TestK8sPod_CreatePod_DefaultTimeout(t *testing.T) {
	fakeClient := fake.NewSimpleClientset()
	p := newTestK8sPod(t, fakeClient)
	p.config.JobTimeout = 0

	pod, err := p.createPod(t.Context())
	require.NoError(t, err)
	assert.Contains(t, pod.Spec.Containers[0].Command[2], "sleep 10800")
}

func TestK8sPod_FindMainContainer(t *testing.T) {
	p := &K8sPod{}

	t.Run("found", func(t *testing.T) {
		pod := &corev1.Pod{Spec: corev1.PodSpec{Containers: []corev1.Container{
			{Name: "sidecar"},
			{Name: k8sMainContainerName},
		}}}
		assert.Equal(t, 1, p.findMainContainer(pod))
	})

	t.Run("not found", func(t *testing.T) {
		pod := &corev1.Pod{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "sidecar"}}}}
		assert.Equal(t, -1, p.findMainContainer(pod))
	})

	t.Run("empty", func(t *testing.T) {
		assert.Equal(t, -1, p.findMainContainer(&corev1.Pod{}))
	})
}

func TestK8sPod_CreatePod_EnvParsing(t *testing.T) {
	fakeClient := fake.NewSimpleClientset()
	p := newTestK8sPod(t, fakeClient)
	p.input.Env = []string{"SIMPLE=value", "WITH_EQUALS=a=b=c", "MALFORMED"}

	pod, err := p.createPod(t.Context())
	require.NoError(t, err)

	envMap := make(map[string]string)
	for _, e := range pod.Spec.Containers[0].Env {
		envMap[e.Name] = e.Value
	}
	assert.Equal(t, "value", envMap["SIMPLE"])
	assert.Equal(t, "a=b=c", envMap["WITH_EQUALS"])
	_, has := envMap["MALFORMED"]
	assert.False(t, has)
}

func TestNewK8sPod_NilConfig(t *testing.T) {
	_, err := NewK8sPod(&container.NewContainerInput{Image: "test"}, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "K8sPodConfig is required")
}

type testWriter struct{}

func (testWriter) Write(p []byte) (int, error) { return len(p), nil }
