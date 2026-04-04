# Forgejo Runner Kubernetes Plugin

A gRPC backend plugin for [Forgejo Runner](https://code.forgejo.org/forgejo/runner) that executes CI/CD jobs as Kubernetes pods.

## Overview

This plugin implements the Forgejo Runner backend plugin protocol, allowing the runner to execute workflow jobs natively on Kubernetes without Docker-in-Docker.

Each job gets its own pod with:
- A main container running the workflow steps
- Optional service containers as sidecars (accessible via `localhost`)
- A shared `/shared` volume for file exchange between steps
- Custom PodSpec support for GPU, resource limits, node selectors, etc.

## Installation

```bash
go build -o forgejo-runner-k8s .
```

## Usage

### 1. Start the plugin server

```bash
# Listen on a Unix socket
./forgejo-runner-k8s --listen unix:///var/run/forgejo-runner-k8s.sock

# Listen on a TCP port
./forgejo-runner-k8s --listen :9090
```

### 2. Configure the runner

In the runner's `config.yaml`, add a `plugins` section and define labels that route to the plugin:

```yaml
plugins:
  k8s:
    address: "unix:///var/run/forgejo-runner-k8s.sock"
    options:
      namespace: "ci-jobs"
      kubeconfig: "/etc/k8s/config"  # optional, uses in-cluster config by default
      poll_timeout: "10m"            # optional, default 10m

# Register labels using the plugin scheme.
# Format: <label-name>:<plugin-name>://<optional-arg>
#
# The optional arg is passed to the plugin as "label_arg" in backend_options.
# For the K8s plugin, this is the path to a PodSpec YAML file.
```

Then register labels on the runner connections:

```yaml
server:
  connections:
    main:
      url: https://forgejo.example.com
      labels:
        - "ubuntu-k8s:k8s://config/podspec-default.yaml"
        - "gpu:k8s://config/podspec-gpu.yaml"
```

### 3. Use in workflows

```yaml
jobs:
  build:
    runs-on: ubuntu-k8s
    steps:
      - run: echo "Running in a Kubernetes pod!"
```

## PodSpec files

PodSpec files let you customize the pod template per label. The plugin merges your spec with its defaults.

Example `config/podspec-gpu.yaml`:

```yaml
containers:
  - name: main
    image: nvidia/cuda:12.0-base
    resources:
      limits:
        nvidia.com/gpu: "1"
      requests:
        memory: "4Gi"
        cpu: "2"
nodeSelector:
  gpu: "true"
tolerations:
  - key: "nvidia.com/gpu"
    operator: "Exists"
    effect: "NoSchedule"
```

The `main` container name is special - the plugin uses it as the primary container for step execution. If your PodSpec doesn't include a `main` container, one is prepended automatically.

## Backend options

Options can be set in the runner's `plugins.<name>.options` config:

| Option | Default | Description |
|--------|---------|-------------|
| `namespace` | `default` | Kubernetes namespace for pods |
| `kubeconfig` | (in-cluster) | Path to kubeconfig file |
| `poll_timeout` | `10m` | Timeout for waiting on pod status |
| `podspec` | (none) | Default PodSpec file path (overridden by label arg) |

The runner also injects these dynamically:

| Option | Description |
|--------|-------------|
| `label_arg` | The label's argument (e.g., PodSpec path), resolved per-job |
| `job_timeout` | Maximum job duration, from the runner's context deadline |

## Architecture

```
Forgejo Instance
      |
      | (gRPC: fetch tasks)
      v
Forgejo Runner  ----gRPC---->  K8s Plugin Server  ---->  Kubernetes API
  (client)                      (this binary)              (pods)
```

The runner communicates with the plugin over gRPC using the `BackendPlugin` service (defined in `act/plugin/proto/v1/plugin.proto` in the runner repo). The plugin manages pod lifecycle: creation, command execution, file transfers, health checks, and cleanup.

Multiple runners can connect to the same plugin server. The plugin handles concurrent jobs via per-environment isolation (each job gets a unique `environment_id`).

## Development

```bash
# Build
go build -o forgejo-runner-k8s .

# Run locally (requires kubeconfig)
./forgejo-runner-k8s --listen :9090

# Test with a runner
# In runner config.yaml:
# plugins:
#   k8s:
#     address: "localhost:9090"
#     options:
#       namespace: "default"
```

## Backward compatibility

The in-tree Kubernetes backend in Forgejo Runner continues to work via `k8spod` labels. This plugin is an alternative that can be deployed and managed independently.

To migrate from the in-tree backend:
1. Deploy this plugin server
2. Change labels from `mylabel:k8spod://podspec.yaml` to `mylabel:k8s://podspec.yaml`
3. Add the `plugins.k8s` section to the runner config
