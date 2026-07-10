# forgejo-runner-k8s

Forgejo Runner with the Kubernetes backend plugin, running the plugin as a native sidecar.

![Version: 0.0.0](https://img.shields.io/badge/Version-0.0.0-informational?style=flat-square) ![Type: application](https://img.shields.io/badge/Type-application-informational?style=flat-square) ![AppVersion: 0.0.0](https://img.shields.io/badge/AppVersion-0.0.0-informational?style=flat-square)

## Installing

```sh
helm install forgejo-runner-k8s \
  oci://git.erwanleboucher.dev/eleboucher/charts/forgejo-runner-k8s \
  --namespace forgejo --create-namespace \
  -f my-values.yaml
```

The chart runs the Forgejo runner with the Kubernetes backend plugin as a
[native sidecar](https://kubernetes.io/docs/concepts/workloads/pods/sidecar-containers/)
(`initContainer` with `restartPolicy: Always`), so the plugin socket outlives job
draining during shutdown. Job pods are created in the release namespace, where the
chart also grants the required RBAC.

## Requirements

- Kubernetes **1.29+** (native sidecar support).
- A runner image that supports the backend plugin protocol. Upstream forgejo-runner
  does not yet; use the fork at `git.erwanleboucher.dev/eleboucher/runner`, matched to
  a compatible plugin version.
- An existing Secret with the runner registration `token` and `uuid`
  (`forgejo.existingSecret`).

## Configuration

No default `runner.yaml` or PodSpec is shipped — you must provide `runnerConfig`
and `podSpecs`. Copy [`examples/values-example.yaml`](./examples/values-example.yaml)
as a starting point. Your `runnerConfig` must point `plugins.k8s.address` at
`plugin.socketPath` and set `plugins.k8s.options.namespace` to the release namespace.

## Values

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| affinity | object | `{}` | Affinity. |
| command | list | `[]` | Override the runner container command. When empty, the chart builds a `forgejo-runner daemon` invocation from `forgejo.url` and the mounted secret. |
| deploymentAnnotations | object | `{"reloader.stakater.com/auto":"true"}` | Deployment annotations. The default rolls the deployment when the config ConfigMap changes (requires stakater/reloader; the annotation must sit on the workload, not the pod). |
| forgejo.existingSecret | string | `""` | Name of an existing Secret holding the runner registration `token` and `uuid` (REQUIRED). Manage it however you like (SOPS, External Secrets, a plain Secret); the chart only reads it. |
| forgejo.secretKeys.token | string | `"token"` | Key within the Secret holding the registration token. |
| forgejo.secretKeys.uuid | string | `"uuid"` | Key within the Secret holding the runner UUID. |
| forgejo.url | string | `""` | Forgejo instance URL the runner registers against (REQUIRED), e.g. `http://forgejo-http:3000`. |
| fullnameOverride | string | `""` | Override the full release name. |
| image.digest | string | `""` | Pin the runner image by digest (`sha256:…`); when set, overrides the tag. |
| image.pullPolicy | string | `"IfNotPresent"` | Runner image pull policy. |
| image.repository | string | `"git.erwanleboucher.dev/eleboucher/runner"` | Runner image repository. |
| image.tag | string | `""` | Runner image tag (REQUIRED). |
| imagePullSecrets | list | `[]` | Image pull secrets for private registries. |
| nameOverride | string | `""` | Override the chart name used in resource names. |
| nodeSelector | object | `{}` | Node selector. |
| plugin.image.digest | string | `""` | Pin the plugin image by digest (`sha256:…`); when set, overrides the tag. |
| plugin.image.pullPolicy | string | `"IfNotPresent"` | Plugin image pull policy. |
| plugin.image.repository | string | `"git.erwanleboucher.dev/eleboucher/runner-k8s-plugin"` | Plugin image repository. |
| plugin.image.tag | string | `""` | Plugin image tag; defaults to the chart appVersion when empty. Since a release sets appVersion == chart version, the plugin image uses the same tag as the chart by default. |
| plugin.resources | object | `{"limits":{"memory":"128Mi"},"requests":{"cpu":"5m","memory":"32Mi"}}` | Plugin sidecar resources. |
| plugin.socketPath | string | `"/plugin/forgejo-runner-k8s.sock"` | Unix socket the plugin listens on and the runner connects to. Your runnerConfig's `plugins.k8s.address` MUST be `unix://<this path>`. |
| podAnnotations | object | `{}` | Pod annotations. |
| podLabels | object | `{}` | Extra pod labels. |
| podSecurityContext | object | `{"fsGroup":1000,"fsGroupChangePolicy":"OnRootMismatch","runAsGroup":1000,"runAsNonRoot":true,"runAsUser":1000}` | Pod security context. |
| podSpecs | object | `{}` | PodSpec files referenced by your runner labels, keyed by filename -> content (REQUIRED). Mounted read-only into `/config` next to `runner.yaml`. |
| rbac.create | bool | `true` | Create the Role/RoleBinding granting the runner permission to manage job pods. |
| replicaCount | int | `1` | Number of runner replicas. |
| resources | object | `{"limits":{"memory":"1Gi"},"requests":{"cpu":"5m","memory":"256Mi"}}` | Runner container resources. |
| runnerConfig | string | `""` | Full contents of `runner.yaml` (REQUIRED — no default is shipped). See [examples/values-example.yaml](./examples/values-example.yaml) for a complete config. The plugin listens on `plugin.socketPath`, so set `plugins.k8s.address: "unix:///plugin/forgejo-runner-k8s.sock"` and `plugins.k8s.options.namespace` to this release's namespace (where job pods are created). |
| securityContext | object | `{"allowPrivilegeEscalation":false,"capabilities":{"drop":["ALL"]},"readOnlyRootFilesystem":true}` | Container security context (applied to both the runner and the plugin sidecar). |
| serviceAccount.annotations | object | `{}` | Annotations for the ServiceAccount. |
| serviceAccount.automount | bool | `true` | Automount the API token (required — the runner talks to the API server via the plugin). |
| serviceAccount.create | bool | `true` | Create a ServiceAccount. |
| serviceAccount.name | string | `""` | Name of the ServiceAccount; generated when empty. |
| terminationGracePeriodSeconds | int | `300` | Termination grace period. Must cover `runner.shutdown_timeout` plus the plugin's ~60s cleanup budget. |
| tolerations | list | `[]` | Tolerations. |

