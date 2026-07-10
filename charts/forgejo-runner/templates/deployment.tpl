{{- if not .Values.forgejo.url }}
{{- fail "forgejo-runner: .Values.forgejo.url is required." }}
{{- end }}
{{- if not .Values.forgejo.existingSecret }}
{{- fail "forgejo-runner: .Values.forgejo.existingSecret is required." }}
{{- end }}
{{- if and (not .Values.image.tag) (not .Values.image.digest) }}
{{- fail "forgejo-runner: set .Values.image.tag or .Values.image.digest (runner image)." }}
{{- end }}
apiVersion: apps/v1
kind: Deployment
metadata:
  name: {{ include "forgejo-runner.fullname" . }}
  labels:
    {{- include "forgejo-runner.labels" . | nindent 4 }}
  {{- with .Values.deploymentAnnotations }}
  annotations:
    {{- toYaml . | nindent 4 }}
  {{- end }}
spec:
  replicas: {{ .Values.replicaCount }}
  selector:
    matchLabels:
      {{- include "forgejo-runner.selectorLabels" . | nindent 6 }}
  template:
    metadata:
      {{- with .Values.podAnnotations }}
      annotations:
        {{- toYaml . | nindent 8 }}
      {{- end }}
      labels:
        {{- include "forgejo-runner.selectorLabels" . | nindent 8 }}
        {{- with .Values.podLabels }}
        {{- toYaml . | nindent 8 }}
        {{- end }}
    spec:
      serviceAccountName: {{ include "forgejo-runner.serviceAccountName" . }}
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
          image: {{ include "forgejo-runner.pluginImage" . }}
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
            # the plugin shares the runner's volumes: it reads podspecs from /config and,
            # for the CopyLocal fast path, reads the runner's action cache under /data;
            # /tmp is writable scratch for large-archive spill (readOnlyRootFilesystem).
            - name: config
              mountPath: /config
              readOnly: true
            - name: data
              mountPath: /data
              readOnly: true
            - name: tmp
              mountPath: /tmp
            - name: plugin
              mountPath: {{ dir .Values.plugin.socketPath }}
      containers:
        - name: runner
          image: {{ include "forgejo-runner.image" . }}
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
            name: {{ include "forgejo-runner.fullname" . }}-config
        - name: runner-secret
          secret:
            secretName: {{ .Values.forgejo.existingSecret }}
        - name: plugin
          emptyDir: {}
        - name: data
          emptyDir: {}
        - name: tmp
          emptyDir: {}
