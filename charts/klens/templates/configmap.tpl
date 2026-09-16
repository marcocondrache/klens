{{- include "klens.validateBind" . }}
{{- if not .Values.existingConfigMap -}}
apiVersion: v1
kind: ConfigMap
metadata:
  name: {{ include "klens.fullname" . }}
  namespace: {{ .Release.Namespace }}
  labels:
    {{- include "klens.labels" . | nindent 4 }}
data:
  config.yaml: |
    {{- tpl (toYaml .Values.config) $ | nindent 4 }}
{{- end }}
