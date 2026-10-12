{{- if include "klens.mcpEnabled" . -}}
apiVersion: v1
kind: Service
metadata:
  name: {{ include "klens.mcpServiceName" . }}
  namespace: {{ .Release.Namespace }}
  labels:
    {{- include "klens.labels" . | nindent 4 }}
  {{- with .Values.mcp.service.annotations }}
  annotations:
    {{- tpl (toYaml .) $ | nindent 4 }}
  {{- end }}
spec:
  type: {{ .Values.mcp.service.type }}
  ports:
    - name: mcp
      port: {{ .Values.mcp.service.port }}
      targetPort: mcp
      protocol: TCP
  selector:
    {{- include "klens.selectorLabels" . | nindent 4 }}
{{- end }}
