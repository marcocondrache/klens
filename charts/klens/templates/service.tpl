apiVersion: v1
kind: Service
metadata:
  name: {{ include "klens.fullname" . }}
  namespace: {{ .Release.Namespace }}
  labels:
    {{- include "klens.labels" . | nindent 4 }}
  {{- with .Values.service.annotations }}
  annotations:
    {{- tpl (toYaml .) $ | nindent 4 }}
  {{- end }}
spec:
  type: {{ .Values.service.type }}
  ports:
    - name: http
      port: {{ .Values.service.port }}
      targetPort: http
      protocol: TCP
  selector:
    {{- include "klens.selectorLabels" . | nindent 4 }}
