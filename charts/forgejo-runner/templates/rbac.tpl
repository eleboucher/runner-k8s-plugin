{{- if .Values.rbac.create }}
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: {{ include "forgejo-runner.fullname" . }}
  labels:
    {{- include "forgejo-runner.labels" . | nindent 4 }}
rules:
  - apiGroups: ["batch"]
    resources: ["jobs"]
    verbs: ["create", "get", "list", "watch", "delete"]
  - apiGroups: [""]
    resources: ["pods"]
    verbs: ["get", "list", "watch"]
  - apiGroups: [""]
    resources: ["pods/exec"]
    verbs: ["create", "get"]
  - apiGroups: [""]
    resources: ["pods/log"]
    verbs: ["get", "list", "watch"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: {{ include "forgejo-runner.fullname" . }}
  labels:
    {{- include "forgejo-runner.labels" . | nindent 4 }}
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: {{ include "forgejo-runner.fullname" . }}
subjects:
  - kind: ServiceAccount
    name: {{ include "forgejo-runner.serviceAccountName" . }}
    namespace: {{ .Release.Namespace }}
{{- end }}
