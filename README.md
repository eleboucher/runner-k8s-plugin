# Forgejo Runner Kubernetes Plugin

A backend plugin for [Forgejo Runner](https://code.forgejo.org/forgejo/runner) that executes CI/CD jobs as Kubernetes pods. Each job runs in its own pod with optional service containers as sidecars, a shared volume, and custom PodSpec support.

## Building

- Install [Go](https://go.dev/doc/install)
- `go build -o forgejo-runner-k8s .`

## Configuration

The plugin supports two transport modes. In both cases, labels use the plugin scheme name (`k8s` below) and the runner routes jobs to the matching config.

### v1: standalone gRPC server

The plugin runs as a sidecar process. The runner connects over a Unix socket or TCP.

```bash
./forgejo-runner-k8s --listen unix:///var/run/forgejo-runner-k8s.sock
```

```yaml
plugins:
  k8s:
    address: "unix:///var/run/forgejo-runner-k8s.sock"
    options:
      namespace: ci-jobs
```

### v2: go-plugin (binary launch)

The runner launches the plugin binary as a subprocess via [go-plugin](https://github.com/hashicorp/go-plugin). No sidecar or socket needed. The binary auto-detects how it was launched.

```yaml
pluginsv2:
  k8s:
    path: /usr/local/bin/forgejo-runner-k8s
    options:
      namespace: ci-jobs
```

### Labels

```yaml
server:
  connections:
    main:
      url: https://forgejo.example.com
      labels:
        - "ubuntu-k8s:k8s://config/podspec-default.yaml"
        - "gpu:k8s://config/podspec-gpu.yaml"
```

### Backend options

Set in `plugins.<name>.options` or `pluginsv2.<name>.options`:

| Option | Default | Description |
|--------|---------|-------------|
| `namespace` | `default` | Kubernetes namespace for pods |
| `kubeconfig` | in-cluster | Path to kubeconfig file |
| `poll_timeout` | `10m` | Timeout waiting for pod readiness |
| `podspec` | — | Default PodSpec path (overridden by label arg) |
| `image_pull_policy` | k8s default | Main container `imagePullPolicy`: `Always`, `IfNotPresent` or `Never`. Kubernetes defaults `:latest`/untagged images to `Always`. |
| `labels` | — | Extra pod labels as `k=v,k=v`. `${ENV_ID}` expands to the per-pod environment ID. |

The runner also injects `label_arg` (per-job label argument, e.g. PodSpec path) and `job_timeout` automatically.

Pods always carry `app.kubernetes.io/managed-by=forgejo-runner`, `forgejo-runner/environment-id`, and `forgejo-runner/plugin-instance`. Use `labels` for anything else:

```yaml
# v1
plugins:
  k8s:
    address: "unix:///var/run/forgejo-runner-k8s.sock"
    options:
      labels: "app.kubernetes.io/name=forgejo-runner,app.kubernetes.io/instance=runner-${ENV_ID}"

# v2
pluginsv2:
  k8s:
    path: /usr/local/bin/forgejo-runner-k8s
    options:
      labels: "app.kubernetes.io/name=forgejo-runner,app.kubernetes.io/instance=runner-${ENV_ID}"
```

## PodSpec files

PodSpec files customize the pod template per label. A container named `main` is used for step execution. If absent, one is prepended automatically.

```yaml
containers:
  - name: main
    image: nvidia/cuda:12.0-base
    resources:
      limits:
        nvidia.com/gpu: "1"
nodeSelector:
  gpu: "true"
```

## Migrating from the in-tree backend

The in-tree `k8spod` backend continues to work. To switch to this plugin:

1. Deploy the plugin (sidecar for v1, or copy the binary for v2)
2. Change labels from `mylabel:k8spod://podspec.yaml` to `mylabel:k8s://podspec.yaml`
3. Add the plugin section to the runner config

## Testing

```bash
go test ./...
```

## License

Same as Forgejo Runner — [GPL version 3.0](https://code.forgejo.org/forgejo/runner/src/branch/main/LICENSE) or any later version.
