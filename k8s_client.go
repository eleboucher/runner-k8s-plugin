package main

import (
	"fmt"
	"sync"
	"time"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

type k8sClientEntry struct {
	client  kubernetes.Interface
	restCfg *rest.Config
	err     error
}

var k8sClients sync.Map

type k8sClientOnce struct {
	once  sync.Once
	entry k8sClientEntry
}

func GetK8sClient(kubeconfigPath string) (kubernetes.Interface, *rest.Config, error) {
	newEntry := &k8sClientOnce{}
	actual, _ := k8sClients.LoadOrStore(kubeconfigPath, newEntry)
	entry := actual.(*k8sClientOnce)
	entry.once.Do(func() {
		entry.entry = initK8sClient(kubeconfigPath)
	})

	if entry.entry.err != nil {
		k8sClients.Delete(kubeconfigPath)
		return nil, nil, fmt.Errorf("kubernetes client initialization failed: %w", entry.entry.err)
	}
	return entry.entry.client, entry.entry.restCfg, nil
}

func initK8sClient(kubeconfigPath string) k8sClientEntry {
	restCfg, err := buildK8sConfig(kubeconfigPath)
	if err != nil {
		return k8sClientEntry{err: err}
	}
	restCfg.QPS = 50
	restCfg.Burst = 100
	restCfg.Timeout = 30 * time.Second

	client, err := kubernetes.NewForConfig(restCfg)
	if err != nil {
		return k8sClientEntry{err: err}
	}
	return k8sClientEntry{client: client, restCfg: restCfg}
}

func buildK8sConfig(kubeconfigPath string) (*rest.Config, error) {
	cfg, err := rest.InClusterConfig()
	if err == nil {
		return cfg, nil
	}

	if kubeconfigPath != "" {
		cfg, err := clientcmd.BuildConfigFromFlags("", kubeconfigPath)
		if err != nil {
			return nil, fmt.Errorf("failed to build config from kubeconfig %q: %w", kubeconfigPath, err)
		}
		return cfg, nil
	}

	loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
	configOverrides := &clientcmd.ConfigOverrides{}
	cfg, err = clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loadingRules, configOverrides).ClientConfig()
	if err != nil {
		return nil, fmt.Errorf("no kubernetes credentials found (tried in-cluster and kubeconfig): %w", err)
	}
	return cfg, nil
}
