{{- if not .Values.forgejo.url }}
{{- fail "forgejo-runner-k8s: .Values.forgejo.url is required." }}
{{- end }}
{{- if not .Values.forgejo.existingSecret }}
{{- fail "forgejo-runner-k8s: .Values.forgejo.existingSecret is required." }}
{{- end }}
{{- if and (not .Values.image.tag) (not .Values.image.digest) }}
{{- fail "forgejo-runner-k8s: set .Values.image.tag or .Values.image.digest (runner image)." }}
{{- end }}
apiVersion: apps/v1
kind: Deployment
metadata:
  name: {{ include "forgejo-runner-k8s.fullname" . }}
  labels:
    {{- include "forgejo-runner-k8s.labels" . | nindent 4 }}
spec:
  replicas: {{ .Values.replicaCount }}
  selector:
    matchLabels:
      {{- include "forgejo-runner-k8s.selectorLabels" . | nindent 6 }}
  template:
    metadata:
      annotations:
        {{- toYaml .Values.podAnnotations | nindent 8 }}
      labels:
        {{- include "forgejo-runner-k8s.selectorLabels" . | nindent 8 }}
        {{- with .Values.podLabels }}
        {{- toYaml . | nindent 8 }}
        {{- end }}
    spec:
      serviceAccountName: {{ include "forgejo-runner-k8s.serviceAccountName" . }}
      automountServiceAccountToken: {{ .Values.serviceAccount.automount }}
      terminationGracePeriodSeconds: {{ .Values.terminationGracePeriodSeconds }}
      {{- with .Values.imagePullSecrets }}
      imagePullSecrets:
        {{- toYaml . | nindent 8 }}
      {{- end }}
      {{- with .Values.podSecurityContext }}
      securityContext:
        {{- toYaml . | nindent 8 }}
      {{- end }}
      initContainers:
        # Native sidecar: started before, and terminated after, the runner container,
        # so the plugin socket outlives job draining during shutdown.
        - name: k8s-plugin
          image: {{ include "forgejo-runner-k8s.pluginImage" . }}
          imagePullPolicy: {{ .Values.plugin.image.pullPolicy }}
          restartPolicy: Always
          args:
            - --listen
            - unix://{{ .Values.plugin.socketPath }}
          {{- with .Values.securityContext }}
          securityContext:
            {{- toYaml . | nindent 12 }}
          {{- end }}
          {{- with .Values.plugin.resources }}
          resources:
            {{- toYaml . | nindent 12 }}
          {{- end }}
          volumeMounts:
            - name: plugin
              mountPath: {{ dir .Values.plugin.socketPath }}
      containers:
        - name: runner
          image: {{ include "forgejo-runner-k8s.image" . }}
          imagePullPolicy: {{ .Values.image.pullPolicy }}
          {{- if .Values.command }}
          command:
            {{- toYaml .Values.command | nindent 12 }}
          {{- else }}
          command:
            - sh
            - -ec
            - |
              exec /bin/forgejo-runner daemon \
                --config /config/runner.yaml \
                --url {{ .Values.forgejo.url | quote }} \
                --uuid "$(cat /etc/forgejo-runner-secret/{{ .Values.forgejo.secretKeys.uuid }})" \
                --token-url file:///etc/forgejo-runner-secret/{{ .Values.forgejo.secretKeys.token }}
          {{- end }}
          {{- with .Values.securityContext }}
          securityContext:
            {{- toYaml . | nindent 12 }}
          {{- end }}
          {{- with .Values.resources }}
          resources:
            {{- toYaml . | nindent 12 }}
          {{- end }}
          volumeMounts:
            - name: config
              mountPath: /config
            - name: runner-secret
              mountPath: /etc/forgejo-runner-secret
              readOnly: true
            - name: plugin
              mountPath: {{ dir .Values.plugin.socketPath }}
            - name: data
              mountPath: /data
            - name: tmp
              mountPath: /tmp
      {{- with .Values.nodeSelector }}
      nodeSelector:
        {{- toYaml . | nindent 8 }}
      {{- end }}
      {{- with .Values.affinity }}
      affinity:
        {{- toYaml . | nindent 8 }}
      {{- end }}
      {{- with .Values.tolerations }}
      tolerations:
        {{- toYaml . | nindent 8 }}
      {{- end }}
      volumes:
        - name: config
          configMap:
            name: {{ include "forgejo-runner-k8s.fullname" . }}-config
        - name: runner-secret
          secret:
            secretName: {{ .Values.forgejo.existingSecret }}
        - name: plugin
          emptyDir: {}
        - name: data
          emptyDir: {}
        - name: tmp
          emptyDir: {}
