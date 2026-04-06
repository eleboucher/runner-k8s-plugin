package main

import (
	"fmt"

	"code.forgejo.org/forgejo/runner/v12/act/container"
)

const (
	k8sMainContainerName = "main"
	k8sSharedMount       = "/shared"
	k8sActPath           = k8sSharedMount + "/act"
	k8sToolCache         = k8sSharedMount + "/toolcache"
	k8sWorkDir           = k8sSharedMount + "/workdir"
	k8sDefaultPath       = "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
)

func NewK8sPod(input *container.NewContainerInput, config *K8sPodConfig) (*K8sPod, error) {
	if config == nil {
		return nil, fmt.Errorf("K8sPodConfig is required")
	}

	client, restCfg, err := GetK8sClient(config.KubeConfig)
	if err != nil {
		return nil, fmt.Errorf("init k8s client: %w", err)
	}

	ns := config.Namespace
	if ns == "" {
		ns = "default"
	}

	p := &K8sPod{
		client:    client,
		restCfg:   restCfg,
		namespace: ns,
		input:     *input,
		config:    config,
		stdout:    input.Stdout,
		stderr:    input.Stderr,
	}
	p.SetToolCache(k8sToolCache)
	return p, nil
}
