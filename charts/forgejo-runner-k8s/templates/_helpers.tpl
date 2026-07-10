{{/* Chart name, overridable. */}}
{{- define "forgejo-runner-k8s.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/* Fully qualified app name. */}}
{{- define "forgejo-runner-k8s.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{- define "forgejo-runner-k8s.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "forgejo-runner-k8s.labels" -}}
helm.sh/chart: {{ include "forgejo-runner-k8s.chart" . }}
{{ include "forgejo-runner-k8s.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{- define "forgejo-runner-k8s.selectorLabels" -}}
app.kubernetes.io/name: {{ include "forgejo-runner-k8s.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{- define "forgejo-runner-k8s.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "forgejo-runner-k8s.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/* Runner image reference; digest wins over tag. */}}
{{- define "forgejo-runner-k8s.image" -}}
{{- if .Values.image.digest -}}
{{- printf "%s@%s" .Values.image.repository .Values.image.digest -}}
{{- else -}}
{{- printf "%s:%s" .Values.image.repository .Values.image.tag -}}
{{- end -}}
{{- end }}

{{/* Plugin image reference; digest wins over tag, tag defaults to the chart appVersion. */}}
{{- define "forgejo-runner-k8s.pluginImage" -}}
{{- if .Values.plugin.image.digest -}}
{{- printf "%s@%s" .Values.plugin.image.repository .Values.plugin.image.digest -}}
{{- else -}}
{{- printf "%s:%s" .Values.plugin.image.repository (default .Chart.AppVersion .Values.plugin.image.tag) -}}
{{- end -}}
{{- end }}
