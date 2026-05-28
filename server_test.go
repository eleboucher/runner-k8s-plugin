package main

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestK8sServer_Shutdown_CleansUpJobs(t *testing.T) {
	fakeClient := fake.NewSimpleClientset()
	s := &k8sServer{
		pluginInstanceID: "test-instance",
		envs:             make(map[string]*k8sEnvironment),
	}

	for _, name := range []string{"job-1", "job-2"} {
		job := &batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "test-ns"},
		}
		_, err := fakeClient.BatchV1().Jobs("test-ns").Create(t.Context(), job, metav1.CreateOptions{})
		require.NoError(t, err)

		s.envs[name] = &k8sEnvironment{
			job: &K8sJob{
				client:    fakeClient,
				namespace: "test-ns",
				job:       job,
				stdout:    io.Discard,
				stderr:    io.Discard,
				config:    &K8sJobConfig{Namespace: "test-ns"},
			},
		}
	}

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	err := s.Shutdown(ctx)
	require.NoError(t, err)

	assert.Empty(t, s.envs)

	jobs, err := fakeClient.BatchV1().Jobs("test-ns").List(t.Context(), metav1.ListOptions{})
	require.NoError(t, err)
	assert.Empty(t, jobs.Items)
}

func TestK8sServer_Shutdown_NoEnvironments(t *testing.T) {
	s := &k8sServer{
		pluginInstanceID: "test-instance",
		envs:             make(map[string]*k8sEnvironment),
	}

	err := s.Shutdown(t.Context())
	require.NoError(t, err)
}

func TestK8sServer_NewServer_HasInstanceID(t *testing.T) {
	s := newK8sServer()
	assert.NotEmpty(t, s.pluginInstanceID)
	assert.NotNil(t, s.envs)
}

func TestRunnerArch(t *testing.T) {
	got := runnerArch()
	assert.NotEmpty(t, got)
	assert.Contains(t, []string{"X64", "X86", "ARM64", "ARM"}, got)
}

func TestParseLabels(t *testing.T) {
	cases := []struct {
		name  string
		raw   string
		envID string
		want  map[string]string
	}{
		{"empty", "", "env-1", map[string]string{}},
		{
			"single",
			"app.kubernetes.io/name=forgejo-runner",
			"env-1",
			map[string]string{"app.kubernetes.io/name": "forgejo-runner"},
		},
		{
			"multiple_with_whitespace",
			" app.kubernetes.io/name = forgejo-runner , app.kubernetes.io/part-of = ci ",
			"env-1",
			map[string]string{
				"app.kubernetes.io/name":    "forgejo-runner",
				"app.kubernetes.io/part-of": "ci",
			},
		},
		{
			"env_id_substitution",
			"app.kubernetes.io/instance=runner-${ENV_ID}",
			"abc-123",
			map[string]string{"app.kubernetes.io/instance": "runner-abc-123"},
		},
		{
			"malformed_entries_skipped",
			"=novalue,nokey,valid=ok,",
			"env-1",
			map[string]string{"valid": "ok"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := parseLabels(tc.raw, tc.envID)
			assert.Equal(t, tc.want, got)
		})
	}
}
