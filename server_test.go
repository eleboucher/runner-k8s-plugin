package main

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestK8sServer_Shutdown_CleansUpPods(t *testing.T) {
	fakeClient := fake.NewSimpleClientset()
	s := &k8sServer{
		pluginInstanceID: "test-instance",
		envs:             make(map[string]*k8sEnvironment),
	}

	for _, name := range []string{"pod-1", "pod-2"} {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "test-ns"},
		}
		_, err := fakeClient.CoreV1().Pods("test-ns").Create(t.Context(), pod, metav1.CreateOptions{})
		require.NoError(t, err)

		s.envs[name] = &k8sEnvironment{
			pod: &K8sPod{
				client:    fakeClient,
				namespace: "test-ns",
				pod:       pod,
				stdout:    io.Discard,
				stderr:    io.Discard,
				config:    &K8sPodConfig{Namespace: "test-ns"},
			},
		}
	}

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	err := s.Shutdown(ctx)
	require.NoError(t, err)

	assert.Empty(t, s.envs)

	pods, err := fakeClient.CoreV1().Pods("test-ns").List(t.Context(), metav1.ListOptions{})
	require.NoError(t, err)
	assert.Empty(t, pods.Items)
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
