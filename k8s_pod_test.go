package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"testing"
	"time"

	"code.forgejo.org/forgejo/runner/v12/act/container"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

var (
	_ container.ExecutionsEnvironment = &K8sJob{}
	_ container.ServiceAdder          = &K8sJob{}
)

func newTestK8sJob(t *testing.T, fakeClient *fake.Clientset) *K8sJob {
	t.Helper()
	p := &K8sJob{
		client:    fakeClient,
		namespace: "test-ns",
		input: container.NewContainerInput{
			Image:      "node:22-bookworm",
			Name:       "test-job",
			Env:        []string{"FOO=bar", "BAZ=qux"},
			WorkingDir: "/shared/workdir",
		},
		config: &K8sJobConfig{
			Namespace:   "test-ns",
			PollTimeout: 5 * time.Second,
			JobTimeout:  1 * time.Hour,
		},
		stdout: io.Discard,
		stderr: io.Discard,
	}
	return p
}

func TestK8sJob_CreateJob_DefaultSpec(t *testing.T) {
	fakeClient := fake.NewSimpleClientset()
	p := newTestK8sJob(t, fakeClient)

	job, err := p.createJob(t.Context())
	require.NoError(t, err)

	assert.Equal(t, "test-ns", job.Namespace)
	assert.Equal(t, "forgejo-runner", job.Labels["app.kubernetes.io/managed-by"])
	assert.NotContains(t, job.Labels, "forgejo-runner/environment-id")
	assert.NotContains(t, job.Labels, "forgejo-runner/plugin-instance")

	require.NotEmpty(t, job.Spec.Template.Spec.Containers)
	main := job.Spec.Template.Spec.Containers[0]
	assert.Equal(t, k8sMainContainerName, main.Name)
	assert.Equal(t, "node:22-bookworm", main.Image)
	assert.Empty(t, main.WorkingDir)

	envNames := make(map[string]string)
	for _, e := range main.Env {
		envNames[e.Name] = e.Value
	}
	assert.Equal(t, "bar", envNames["FOO"])
	assert.Equal(t, "qux", envNames["BAZ"])

	foundVolume := false
	for _, v := range job.Spec.Template.Spec.Volumes {
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
	assert.Equal(t, corev1.RestartPolicyNever, job.Spec.Template.Spec.RestartPolicy)
	assert.Equal(t, int32(0), ptrDeref(job.Spec.BackoffLimit))
	assert.Equal(t, int32(1), ptrDeref(job.Spec.Completions))
	assert.Equal(t, int32(1), ptrDeref(job.Spec.Parallelism))
	assert.Equal(t, int64(3600), ptrDeref(job.Spec.ActiveDeadlineSeconds))
}

func ptrDeref[T any](v *T) T {
	if v == nil {
		var zero T
		return zero
	}
	return *v
}

func TestK8sJob_CreateJob_WithCapabilities(t *testing.T) {
	fakeClient := fake.NewSimpleClientset()
	p := newTestK8sJob(t, fakeClient)
	p.capAdd = []string{"NET_ADMIN"}
	p.capDrop = []string{"ALL"}

	job, err := p.createJob(t.Context())
	require.NoError(t, err)

	main := job.Spec.Template.Spec.Containers[0]
	require.NotNil(t, main.SecurityContext)
	require.NotNil(t, main.SecurityContext.Capabilities)
	assert.Contains(t, main.SecurityContext.Capabilities.Add, corev1.Capability("NET_ADMIN"))
	assert.Contains(t, main.SecurityContext.Capabilities.Drop, corev1.Capability("ALL"))
}

func TestK8sJob_CreateJob_WithServiceContainers(t *testing.T) {
	fakeClient := fake.NewSimpleClientset()
	p := newTestK8sJob(t, fakeClient)
	p.AddServiceContainerRaw("redis", "redis:7", map[string]string{"REDIS_PASS": "secret"}, []string{"6379"})
	p.AddServiceContainerRaw("postgres", "postgres:16", map[string]string{"POSTGRES_DB": "test"}, []string{"5432:5432"})

	job, err := p.createJob(t.Context())
	require.NoError(t, err)

	assert.Len(t, job.Spec.Template.Spec.Containers, 3)

	redis := job.Spec.Template.Spec.Containers[1]
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

	require.NotEmpty(t, job.Spec.Template.Spec.HostAliases)
	alias := job.Spec.Template.Spec.HostAliases[0]
	assert.Equal(t, "127.0.0.1", alias.IP)
	assert.Contains(t, alias.Hostnames, "redis")
	assert.Contains(t, alias.Hostnames, "postgres")
}

func TestK8sJob_CreateJob_NoImage(t *testing.T) {
	fakeClient := fake.NewSimpleClientset()
	p := newTestK8sJob(t, fakeClient)
	p.input.Image = "k8spod"

	_, err := p.createJob(t.Context())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no container image specified")
}

func TestK8sJob_CreateJob_WithPodSpec(t *testing.T) {
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
	p := newTestK8sJob(t, fakeClient)
	p.input.Image = "k8spod"
	p.config.PodSpec = tmpFile

	job, err := p.createJob(t.Context())
	require.NoError(t, err)

	main := job.Spec.Template.Spec.Containers[0]
	assert.Equal(t, "custom-image:latest", main.Image)
	assert.NotNil(t, main.Resources.Requests)
}

func TestK8sJob_CreateJob_PodSpecImageOverriddenByInput(t *testing.T) {
	tmpFile := t.TempDir() + "/podspec.yaml"
	require.NoError(t, os.WriteFile(tmpFile, []byte(`containers:
  - name: main
    image: podspec-image:v1
`), 0o644))

	fakeClient := fake.NewSimpleClientset()
	p := newTestK8sJob(t, fakeClient)
	p.input.Image = "workflow-image:v2"
	p.config.PodSpec = tmpFile

	job, err := p.createJob(t.Context())
	require.NoError(t, err)
	assert.Equal(t, "workflow-image:v2", job.Spec.Template.Spec.Containers[0].Image)
}

func TestK8sJob_CreateJob_InvalidPodSpec(t *testing.T) {
	tmpFile := t.TempDir() + "/bad.yaml"
	require.NoError(t, os.WriteFile(tmpFile, []byte("not: [valid: yaml: {{"), 0o644))

	fakeClient := fake.NewSimpleClientset()
	p := newTestK8sJob(t, fakeClient)
	p.config.PodSpec = tmpFile

	_, err := p.createJob(t.Context())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parse podspec")
}

func TestK8sJob_CreateJob_MissingPodSpecFile(t *testing.T) {
	fakeClient := fake.NewSimpleClientset()
	p := newTestK8sJob(t, fakeClient)
	p.config.PodSpec = "/nonexistent/podspec.yaml"

	_, err := p.createJob(t.Context())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "read podspec")
}

func TestK8sJob_WaitForJobRunning(t *testing.T) {
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: "test-job", Namespace: "test-ns"},
		Status:     batchv1.JobStatus{},
	}
	fakeClient := fake.NewSimpleClientset(job)
	watcher := watch.NewFake()
	fakeClient.PrependWatchReactor("jobs", k8stesting.DefaultWatchReactor(watcher, nil))

	p := newTestK8sJob(t, fakeClient)

	go func() {
		watcher.Modify(&batchv1.Job{
			ObjectMeta: job.ObjectMeta,
			Status:     batchv1.JobStatus{Active: 1},
		})
	}()

	require.NoError(t, p.waitForJobRunning(t.Context(), job))
}

func TestK8sJob_WaitForJobRunning_JobFailed(t *testing.T) {
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: "test-job", Namespace: "test-ns"},
		Status:     batchv1.JobStatus{},
	}
	fakeClient := fake.NewSimpleClientset(job)
	watcher := watch.NewFake()
	fakeClient.PrependWatchReactor("jobs", k8stesting.DefaultWatchReactor(watcher, nil))

	p := newTestK8sJob(t, fakeClient)

	go func() {
		watcher.Modify(&batchv1.Job{
			ObjectMeta: job.ObjectMeta,
			Status: batchv1.JobStatus{
				Conditions: []batchv1.JobCondition{{
					Type:    batchv1.JobFailed,
					Status:  corev1.ConditionTrue,
					Reason:  "OOMKilled",
					Message: "Job failed due to OOM",
				}},
			},
		})
	}()

	err := p.waitForJobRunning(t.Context(), job)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "job failed")
	assert.Contains(t, err.Error(), "OOMKilled")
}

func TestK8sJob_WaitForJobRunning_JobDeleted(t *testing.T) {
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: "test-job", Namespace: "test-ns"},
		Status:     batchv1.JobStatus{},
	}
	fakeClient := fake.NewSimpleClientset(job)
	watcher := watch.NewFake()
	fakeClient.PrependWatchReactor("jobs", k8stesting.DefaultWatchReactor(watcher, nil))

	p := newTestK8sJob(t, fakeClient)

	go func() {
		watcher.Delete(&batchv1.Job{ObjectMeta: job.ObjectMeta})
	}()

	err := p.waitForJobRunning(t.Context(), job)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "deleted")
}

func TestK8sJob_WaitForJobRunning_Timeout(t *testing.T) {
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: "test-job", Namespace: "test-ns"},
		Status:     batchv1.JobStatus{},
	}
	fakeClient := fake.NewSimpleClientset(job)
	fakeWatcher := watch.NewFake()
	fakeClient.PrependWatchReactor("jobs", k8stesting.DefaultWatchReactor(fakeWatcher, nil))

	p := newTestK8sJob(t, fakeClient)
	p.config.PollTimeout = 100 * time.Millisecond

	go func() {
		time.Sleep(150 * time.Millisecond)
		fakeWatcher.Stop()
	}()

	err := p.waitForJobRunning(t.Context(), job)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "timeout")
}

func TestK8sJob_WaitForJobRunning_AlreadyRunningOnGet(t *testing.T) {
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: "test-job", Namespace: "test-ns"},
		Status:     batchv1.JobStatus{Active: 1},
	}
	fakeClient := fake.NewSimpleClientset(job)
	p := newTestK8sJob(t, fakeClient)
	require.NoError(t, p.waitForJobRunning(t.Context(), job))
}

func TestK8sJob_DeleteJob(t *testing.T) {
	t.Run("deletes existing job", func(t *testing.T) {
		existingJob := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: "test-job", Namespace: "test-ns"}}
		fakeClient := fake.NewSimpleClientset(existingJob)
		p := newTestK8sJob(t, fakeClient)
		p.job = existingJob

		require.NoError(t, p.deleteJob(t.Context()))
		assert.Nil(t, p.job)

		_, err := fakeClient.BatchV1().Jobs("test-ns").Get(t.Context(), "test-job", metav1.GetOptions{})
		require.Error(t, err)
	})

	t.Run("nil job is a no-op", func(t *testing.T) {
		fakeClient := fake.NewSimpleClientset()
		p := newTestK8sJob(t, fakeClient)
		p.job = nil
		require.NoError(t, p.deleteJob(t.Context()))
	})
}

func TestK8sJob_IsHealthy(t *testing.T) {
	t.Run("healthy running job", func(t *testing.T) {
		runningJob := &batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{Name: "test-job", Namespace: "test-ns"},
			Status:     batchv1.JobStatus{Active: 1},
		}
		fakeClient := fake.NewSimpleClientset(runningJob)
		p := newTestK8sJob(t, fakeClient)
		p.job = runningJob

		_, err := p.IsHealthy(t.Context())
		require.NoError(t, err)
	})

	t.Run("failed job", func(t *testing.T) {
		failedJob := &batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{Name: "test-job", Namespace: "test-ns"},
			Status: batchv1.JobStatus{
				Conditions: []batchv1.JobCondition{{
					Type:    batchv1.JobFailed,
					Status:  corev1.ConditionTrue,
					Reason:  "OOMKilled",
					Message: "Job failed due to OOM",
				}},
			},
		}
		fakeClient := fake.NewSimpleClientset(failedJob)
		p := newTestK8sJob(t, fakeClient)
		p.job = failedJob

		_, err := p.IsHealthy(t.Context())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "OOMKilled")
	})

	t.Run("nil job", func(t *testing.T) {
		fakeClient := fake.NewSimpleClientset()
		p := newTestK8sJob(t, fakeClient)

		_, err := p.IsHealthy(t.Context())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not started")
	})

	t.Run("completed unexpectedly", func(t *testing.T) {
		completedJob := &batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{Name: "test-job", Namespace: "test-ns"},
			Status:     batchv1.JobStatus{Succeeded: 1},
		}
		fakeClient := fake.NewSimpleClientset(completedJob)
		p := newTestK8sJob(t, fakeClient)
		p.job = completedJob

		_, err := p.IsHealthy(t.Context())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "completed unexpectedly")
	})
}

func TestK8sJob_AddServiceContainerRaw(t *testing.T) {
	p := &K8sJob{}

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

func TestK8sJob_AddServiceContainerRaw_PortVariants(t *testing.T) {
	cases := []struct {
		name     string
		input    []string
		expected []int32
	}{
		{"plain", []string{"8080"}, []int32{8080}},
		{"host_container", []string{"5432:5433"}, []int32{5433}},
		{"with_proto", []string{"5432:5433/tcp"}, []int32{5433}},
		{"ip_bound", []string{"127.0.0.1:5432:5433"}, []int32{5433}},
		{"trims_trailing_proto_only", []string{"6379/tcp"}, []int32{6379}},
		{"invalid_skipped", []string{"abc", "0", "65536", "8080"}, []int32{8080}},
		{"empty_skipped", []string{"", "9090"}, []int32{9090}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := &K8sJob{}
			p.AddServiceContainerRaw("svc", "img:1", nil, tc.input)
			require.Len(t, p.services, 1)
			got := make([]int32, 0, len(p.services[0].ports))
			for _, port := range p.services[0].ports {
				got = append(got, port.ContainerPort)
			}
			assert.Equal(t, tc.expected, got)
		})
	}
}

func TestJobTerminal(t *testing.T) {
	cases := []struct {
		name     string
		active   int32
		failed   int32
		succeeded int32
		done     bool
		errMatch string
	}{
		{"active", 1, 0, 0, true, ""},
		{"failed", 0, 1, 0, true, "job failed"},
		{"succeeded", 0, 0, 1, true, ""},
		{"pending", 0, 0, 0, false, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			job := &batchv1.Job{
				Status: batchv1.JobStatus{
					Active:    tc.active,
					Failed:    tc.failed,
					Succeeded: tc.succeeded,
				},
			}
			done, err := jobTerminal(job)
			assert.Equal(t, tc.done, done)
			if tc.errMatch == "" {
				assert.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.errMatch)
			}
		})
	}
}

func TestK8sJob_CreateJob_DropsPodSpecCommand(t *testing.T) {
	tmpFile := t.TempDir() + "/podspec.yaml"
	require.NoError(t, os.WriteFile(tmpFile, []byte(`containers:
  - name: main
    image: custom:1
    command: ["/bin/should-be-dropped"]
    workingDir: /from/podspec
`), 0o644))
	fakeClient := fake.NewSimpleClientset()
	p := newTestK8sJob(t, fakeClient)
	p.input.Image = k8sPodSpecImageSentinel
	p.input.WorkingDir = ""
	p.config.PodSpec = tmpFile

	job, err := p.createJob(t.Context())
	require.NoError(t, err)
	main := job.Spec.Template.Spec.Containers[0]
	assert.NotContains(t, main.Command, "/bin/should-be-dropped")
	assert.Contains(t, main.Command[2], "sleep")
	assert.Equal(t, "/from/podspec", main.WorkingDir)
}

func TestK8sJob_ReplaceLogWriter(t *testing.T) {
	p := &K8sJob{stdout: io.Discard, stderr: io.Discard}

	var newOut, newErr testWriter
	oldOut, oldErr := p.ReplaceLogWriter(&newOut, &newErr)
	assert.Equal(t, io.Discard, oldOut)
	assert.Equal(t, io.Discard, oldErr)

	p.mu.Lock()
	assert.Equal(t, io.Writer(&newOut), p.stdout)
	assert.Equal(t, io.Writer(&newErr), p.stderr)
	p.mu.Unlock()
}

func TestK8sJob_InterfaceMethods(t *testing.T) {
	p := &K8sJob{}

	assert.Equal(t, "k8sjob", p.BackendID())
	assert.False(t, p.SupportsDockerContainerActions())
	assert.True(t, p.ManagesOwnNetworking())
	assert.Equal(t, k8sActPath, p.GetActPath())
	assert.Equal(t, k8sSharedMount, p.GetRoot())
	assert.Equal(t, "k8sjob", p.GetName())
	assert.Equal(t, "PATH", p.GetPathVariableName())
	assert.False(t, p.IsEnvironmentCaseInsensitive())

	rc := p.GetRunnerContext(context.Background())
	assert.Equal(t, "Linux", rc["os"])
	assert.Equal(t, "/tmp", rc["temp"])
	assert.Equal(t, k8sToolCache, rc["tool_cache"])
}

func TestK8sJob_NoOps(t *testing.T) {
	ctx := t.Context()
	p := &K8sJob{}

	require.NoError(t, p.Pull(true)(ctx))
	require.NoError(t, p.Pull(false)(ctx))
	require.NoError(t, p.ConnectToNetwork("some-network")(ctx))

	env := map[string]string{"existing": "value"}
	require.NoError(t, p.UpdateFromImageEnv(&env)(ctx))
	assert.Equal(t, "value", env["existing"])
}

func TestK8sJob_Start_CleansUpOnWaitFailure(t *testing.T) {
	fakeClient := fake.NewSimpleClientset()
	fakeWatcher := watch.NewFake()
	fakeClient.PrependWatchReactor("jobs", k8stesting.DefaultWatchReactor(fakeWatcher, nil))

	p := newTestK8sJob(t, fakeClient)
	p.config.PollTimeout = 100 * time.Millisecond

	go func() {
		time.Sleep(150 * time.Millisecond)
		fakeWatcher.Stop()
	}()

	err := p.Start(false)(t.Context())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "wait for job")
	assert.Nil(t, p.job)

	jobs, listErr := fakeClient.BatchV1().Jobs("test-ns").List(t.Context(), metav1.ListOptions{})
	require.NoError(t, listErr)
	assert.Empty(t, jobs.Items)
}


func TestK8sJob_Create_StoresCapabilities(t *testing.T) {
	p := &K8sJob{}
	require.NoError(t, p.Create([]string{"NET_ADMIN", "SYS_PTRACE"}, []string{"ALL"})(t.Context()))
	assert.Equal(t, []string{"NET_ADMIN", "SYS_PTRACE"}, p.capAdd)
	assert.Equal(t, []string{"ALL"}, p.capDrop)
}

func TestK8sJob_Close_DelegatesToRemove(t *testing.T) {
	existingJob := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: "test-job", Namespace: "test-ns"}}
	fakeClient := fake.NewSimpleClientset(existingJob)
	p := newTestK8sJob(t, fakeClient)
	p.job = existingJob

	require.NoError(t, p.Close()(t.Context()))
	assert.Nil(t, p.job)
}

func TestK8sJob_Remove_NilJob(t *testing.T) {
	fakeClient := fake.NewSimpleClientset()
	p := newTestK8sJob(t, fakeClient)
	p.job = nil
	require.NoError(t, p.Remove()(t.Context()))
}

func TestK8sJob_WaitForExecReady_NilJob(t *testing.T) {
	fakeClient := fake.NewSimpleClientset()
	p := newTestK8sJob(t, fakeClient)
	p.job = nil
	p.config.PollTimeout = 100 * time.Millisecond

	err := p.waitForExecReady(context.Background(), "any-pod")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "job not started")
}

func TestK8sJob_EnsureSharedVolume_Idempotent(t *testing.T) {
	p := &K8sJob{}
	podTemplate := &corev1.PodTemplateSpec{
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

	p.ensureSharedVolume(podTemplate, &podTemplate.Spec.Containers[0])
	assert.Len(t, podTemplate.Spec.Volumes, 1)
	assert.Len(t, podTemplate.Spec.Containers[0].VolumeMounts, 1)
}

func TestK8sJob_EnsureSharedVolume_AddsWhenMissing(t *testing.T) {
	p := &K8sJob{}
	podTemplate := &corev1.PodTemplateSpec{
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: k8sMainContainerName}},
		},
	}

	p.ensureSharedVolume(podTemplate, &podTemplate.Spec.Containers[0])
	require.Len(t, podTemplate.Spec.Volumes, 1)
	assert.Equal(t, "shared", podTemplate.Spec.Volumes[0].Name)
	require.Len(t, podTemplate.Spec.Containers[0].VolumeMounts, 1)
	assert.Equal(t, k8sSharedMount, podTemplate.Spec.Containers[0].VolumeMounts[0].MountPath)
}

func TestK8sJob_CreateJob_JobTimeoutPropagation(t *testing.T) {
	fakeClient := fake.NewSimpleClientset()
	p := newTestK8sJob(t, fakeClient)
	p.config.JobTimeout = 30 * time.Minute

	job, err := p.createJob(t.Context())
	require.NoError(t, err)
	assert.Contains(t, job.Spec.Template.Spec.Containers[0].Command[2], "sleep 1810")
}

func TestK8sJob_CreateJob_DefaultTimeout(t *testing.T) {
	fakeClient := fake.NewSimpleClientset()
	p := newTestK8sJob(t, fakeClient)
	p.config.JobTimeout = 0

	job, err := p.createJob(t.Context())
	require.NoError(t, err)
	assert.Contains(t, job.Spec.Template.Spec.Containers[0].Command[2], "sleep 10810")
}

func TestK8sJob_FindMainContainer(t *testing.T) {
	p := &K8sJob{}

	t.Run("found", func(t *testing.T) {
		podTemplate := &corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{
			{Name: "sidecar"},
			{Name: k8sMainContainerName},
		}}}
		assert.Equal(t, 1, p.findMainContainer(podTemplate))
	})

	t.Run("not found", func(t *testing.T) {
		podTemplate := &corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "sidecar"}}}}
		assert.Equal(t, -1, p.findMainContainer(podTemplate))
	})

	t.Run("empty", func(t *testing.T) {
		assert.Equal(t, -1, p.findMainContainer(&corev1.PodTemplateSpec{}))
	})
}

func TestK8sJob_CreateJob_EnvParsing(t *testing.T) {
	fakeClient := fake.NewSimpleClientset()
	p := newTestK8sJob(t, fakeClient)
	p.input.Env = []string{"SIMPLE=value", "WITH_EQUALS=a=b=c", "MALFORMED"}

	job, err := p.createJob(t.Context())
	require.NoError(t, err)

	envMap := make(map[string]string)
	for _, e := range job.Spec.Template.Spec.Containers[0].Env {
		envMap[e.Name] = e.Value
	}
	assert.Equal(t, "value", envMap["SIMPLE"])
	assert.Equal(t, "a=b=c", envMap["WITH_EQUALS"])
	_, has := envMap["MALFORMED"]
	assert.False(t, has)
}

func TestNewK8sJob_NilConfig(t *testing.T) {
	_, err := NewK8sJob(&container.NewContainerInput{Image: "test"}, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "K8sJobConfig is required")
}

func TestK8sJob_Exec_FallsBackToInputWorkingDir(t *testing.T) {
	p := &K8sJob{input: container.NewContainerInput{WorkingDir: "/shared/workdir"}}

	err := p.Exec([]string{"echo"}, nil, "", "")(t.Context())
	require.Error(t, err)
	assert.Contains(t, err.Error(), `create workdir "/shared/workdir"`)
}

func TestK8sJob_Exec_ExplicitWorkdirTakesPrecedence(t *testing.T) {
	p := &K8sJob{input: container.NewContainerInput{WorkingDir: "/shared/workdir"}}

	err := p.Exec([]string{"echo"}, nil, "", "/explicit")(t.Context())
	require.Error(t, err)
	assert.Contains(t, err.Error(), `create workdir "/explicit"`)
}

func TestK8sJob_CreateJob_ExtraLabels(t *testing.T) {
	fakeClient := fake.NewSimpleClientset()
	p := newTestK8sJob(t, fakeClient)
	p.extraLabels = map[string]string{
		"forgejo-runner/environment-id":  "test-env-123",
		"forgejo-runner/plugin-instance": "test-instance-456",
	}

	job, err := p.createJob(t.Context())
	require.NoError(t, err)

	assert.Equal(t, "forgejo-runner", job.Labels["app.kubernetes.io/managed-by"])
	assert.Equal(t, "test-env-123", job.Labels["forgejo-runner/environment-id"])
	assert.Equal(t, "test-instance-456", job.Labels["forgejo-runner/plugin-instance"])
}

type testWriter struct{}

func (testWriter) Write(p []byte) (int, error) { return len(p), nil }

func TestSpillWriter_StaysInMemory(t *testing.T) {
	buf := new(bytes.Buffer)
	sw := &spillWriter{
		buf:       buf,
		threshold: 100,
	}

	// Write well under threshold
	n, err := sw.Write([]byte("hello world"))
	require.NoError(t, err)
	assert.Equal(t, 11, n)
	assert.Nil(t, sw.tmp)
	assert.Equal(t, int64(11), sw.written)
}

func TestSpillWriter_SpillsToTempFile(t *testing.T) {
	buf := new(bytes.Buffer)
	sw := &spillWriter{
		buf:       buf,
		threshold: 100,
	}

	// Fill past threshold
	data := make([]byte, 150)
	for i := range data {
		data[i] = byte(i % 256)
	}
	n, err := sw.Write(data)
	require.NoError(t, err)
	assert.Equal(t, 150, n)
	assert.NotNil(t, sw.tmp)

	// Verify data landed in temp file
	content, err := os.ReadFile(sw.tmp.Name())
	require.NoError(t, err)
	assert.Equal(t, 150, len(content))
}

func TestSpillWriter_PartialSpill(t *testing.T) {
	buf := new(bytes.Buffer)
	sw := &spillWriter{
		buf:       buf,
		threshold: 100,
	}

	// Write first chunk (under threshold)
	_, err := sw.Write([]byte("first chunk - "))
	require.NoError(t, err)
	assert.Nil(t, sw.tmp)

	// Write chunk that pushes past threshold
	n2, err := sw.Write(make([]byte, 90))
	require.NoError(t, err)
	assert.Equal(t, 90, n2)
	assert.NotNil(t, sw.tmp)

	// Verify buffered content is in temp file
	content, err := os.ReadFile(sw.tmp.Name())
	require.NoError(t, err)
	assert.Contains(t, string(content), "first chunk")
}

func TestTempFileReader_CloseRemovesFile(t *testing.T) {
	tmp, err := os.CreateTemp("", "test-archive-*.tar")
	require.NoError(t, err)
	path := tmp.Name()
	_, err = tmp.Write([]byte("test data"))
	require.NoError(t, err)
	require.NoError(t, tmp.Close())

	f, err := os.OpenFile(path, os.O_RDONLY, 0o644)
	require.NoError(t, err)
	reader := &tempFileReader{tmp: f}
	require.NoError(t, reader.Close())

	// File should be gone
	_, err = os.Stat(path)
	assert.True(t, os.IsNotExist(err))
}

func TestTempFileReader_CloseReturnsReadError(t *testing.T) {
	tmp, err := os.CreateTemp("", "test-archive-*.tar")
	require.NoError(t, err)
	_, err = tmp.Write([]byte("test data"))
	require.NoError(t, err)
	require.NoError(t, tmp.Close())
	os.Remove(tmp.Name())

	// Trying to create reader on deleted file — close should still succeed
	reader := &tempFileReader{tmp: nil}
	assert.NoError(t, reader.Close())
}

func TestIsClosedStreamError(t *testing.T) {
	cases := []struct {
		name     string
		err      error
		expected bool
	}{
		{"nil", nil, false},
		{"closed pipe", errors.New("io: read/write on closed pipe"), true},
		{"closed network", errors.New("read tcp: use of closed network connection"), true},
		{"next reader", errors.New("next reader: read tcp: use of closed network connection"), true},
		{"other error", errors.New("connection refused"), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := isClosedStreamError(tc.err)
			assert.Equal(t, tc.expected, got)
		})
	}
}
