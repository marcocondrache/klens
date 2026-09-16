apiVersion: v1
kind: Pod
metadata:
  name: {{ include "klens.fullname" . }}-test-connection
  namespace: {{ .Release.Namespace }}
  labels:
    {{- include "klens.labels" . | nindent 4 }}
  annotations:
    helm.sh/hook: test
    helm.sh/hook-delete-policy: before-hook-creation
spec:
  restartPolicy: Never
  securityContext:
    runAsNonRoot: true
    runAsUser: 65532
    runAsGroup: 65532
    seccompProfile:
      type: RuntimeDefault
  containers:
    - name: connection
      image: {{ include "klens.testImage" . | quote }}
      imagePullPolicy: {{ .Values.tests.image.pullPolicy }}
      securityContext:
        allowPrivilegeEscalation: false
        readOnlyRootFilesystem: true
        capabilities:
          drop:
            - ALL
      command:
        - curl
      args:
        - -fsS
        - http://{{ include "klens.fullname" . }}:{{ .Values.service.port }}/health
