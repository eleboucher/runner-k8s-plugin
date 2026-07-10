# Forgejo Runner Kubernetes Plugin

A backend plugin for [Forgejo Runner](https://code.forgejo.org/forgejo/runner) that executes CI/CD jobs as Kubernetes pods. Each job runs in its own pod with optional service containers as sidecars, a shared volume, and custom PodSpec support.

## Deploying

The easiest way to run the full runner + plugin on Kubernetes is the Helm chart in
[`charts/forgejo-runner`](charts/forgejo-runner), which runs the plugin as a
native sidecar and wires up RBAC, config, and graceful shutdown for you:

```sh
helm install forgejo-runner \
  oci://git.erwanleboucher.dev/eleboucher/charts/forgejo-runner \
  --namespace forgejo --create-namespace \
  -f my-values.yaml
```

See the [chart README](charts/forgejo-runner/README.md) and
[example values](charts/forgejo-runner/examples/values-example.yaml).

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
| `resources` | — | Inline YAML `ResourceRequirements` applied to any container (init or regular) that doesn't declare its own. Mainly covers service containers, which otherwise carry no resources. PodSpec resources are kept as-is. |

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

## Logging

The plugin logs to stderr. The level follows `FORGEJO_RUNNER_LOG_LEVEL` (or `HCLOG_LEVEL`), defaulting to `info`.

| Env var | Default | Description |
|---------|---------|-------------|
| `FORGEJO_RUNNER_K8S_LOG_JOB_OUTPUT` | `false` | Mirror job stdout/stderr into the plugin's own logs (at `debug` level), in addition to streaming it to the runner. Off by default: job output is unmasked and may contain secrets that would then land in the plugin's pod logs. |

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
