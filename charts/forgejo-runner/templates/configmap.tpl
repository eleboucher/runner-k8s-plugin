{{- if not (trim .Values.runnerConfig) }}
{{- fail "forgejo-runner: .Values.runnerConfig is required. Provide the full runner.yaml — see examples/values-example.yaml." }}
{{- end }}
{{- if not .Values.podSpecs }}
{{- fail "forgejo-runner: .Values.podSpecs is required. Provide at least one PodSpec file referenced by your runner labels — see examples/values-example.yaml." }}
{{- end }}
apiVersion: v1
kind: ConfigMap
metadata:
  name: {{ include "forgejo-runner.fullname" . }}-config
  labels:
    {{- include "forgejo-runner.labels" . | nindent 4 }}
data:
  runner.yaml: |-
    {{- .Values.runnerConfig | nindent 4 }}
  {{- range $name, $content := .Values.podSpecs }}
  {{ $name }}: |-
    {{- $content | nindent 4 }}
  {{- end }}
