{{- if and (not .Values.secret.existingSecret) (gt (len (.Values.secret.stringData | default dict)) 0) -}}
apiVersion: v1
kind: Secret
metadata:
  name: {{ include "klens.fullname" . }}
  namespace: {{ .Release.Namespace }}
  labels:
    {{- include "klens.labels" . | nindent 4 }}
type: Opaque
stringData:
  {{- range $key, $value := .Values.secret.stringData }}
  {{ $key }}: {{ tpl (toString $value) $ | quote }}
  {{- end }}
{{- end }}
