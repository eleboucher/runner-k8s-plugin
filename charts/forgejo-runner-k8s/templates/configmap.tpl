{{- if not (trim .Values.runnerConfig) }}
{{- fail "forgejo-runner-k8s: .Values.runnerConfig is required. Provide the full runner.yaml — see examples/values-example.yaml." }}
{{- end }}
{{- if not .Values.podSpecs }}
{{- fail "forgejo-runner-k8s: .Values.podSpecs is required. Provide at least one PodSpec file referenced by your runner labels — see examples/values-example.yaml." }}
{{- end }}
apiVersion: v1
kind: ConfigMap
metadata:
  name: {{ include "forgejo-runner-k8s.fullname" . }}-config
  labels:
    {{- include "forgejo-runner-k8s.labels" . | nindent 4 }}
data:
  runner.yaml: |-
    {{- .Values.runnerConfig | nindent 4 }}
  {{- range $name, $content := .Values.podSpecs }}
  {{ $name }}: |-
    {{- $content | nindent 4 }}
  {{- end }}
