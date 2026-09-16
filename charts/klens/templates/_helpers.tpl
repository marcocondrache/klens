{{- define "klens.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "klens.fullname" -}}
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

{{- define "klens.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "klens.labels" -}}
helm.sh/chart: {{ include "klens.chart" . }}
{{ include "klens.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{- define "klens.selectorLabels" -}}
app.kubernetes.io/name: {{ include "klens.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{- define "klens.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "klens.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Digest wins over tag. Tag defaults to appVersion.
*/}}
{{- define "klens.image" -}}
{{- if .Values.image.digest -}}
{{- printf "%s@%s" .Values.image.repository .Values.image.digest -}}
{{- else -}}
{{- printf "%s:%s" .Values.image.repository (.Values.image.tag | default .Chart.AppVersion) -}}
{{- end -}}
{{- end }}

{{- define "klens.testImage" -}}
{{- $img := .Values.tests.image -}}
{{- printf "%s:%s" $img.repository $img.tag -}}
{{- end }}

{{- define "klens.configMapName" -}}
{{- default (include "klens.fullname" .) .Values.existingConfigMap -}}
{{- end }}

{{- define "klens.secretName" -}}
{{- if .Values.secret.existingSecret -}}
{{- .Values.secret.existingSecret -}}
{{- else if gt (len (.Values.secret.stringData | default dict)) 0 -}}
{{- include "klens.fullname" . -}}
{{- end -}}
{{- end }}

{{/*
Loopback bind is a silent Service miss. A bind port that does not match
service.port is the same miss. Fail when the chart owns the file.
*/}}
{{- define "klens.validateBind" -}}
{{- if not .Values.existingConfigMap }}
{{- $bind := .Values.config.bind | default "" | toString }}
{{- $port := .Values.service.port | toString }}
{{- if not $bind }}
{{- fail "config.bind is required when the chart owns the ConfigMap. Use 0.0.0.0 and match service.port." }}
{{- end }}
{{- if or (hasPrefix "127.0.0.1:" $bind) (eq $bind "127.0.0.1") (hasPrefix "[::1]:" $bind) (eq $bind "::1") }}
{{- fail "config.bind must not be a loopback address. The Service cannot reach the process. Use 0.0.0.0." }}
{{- end }}
{{- if not (hasSuffix (printf ":%s" $port) $bind) }}
{{- fail "config.bind port must match service.port so the Service and probes can reach the process." }}
{{- end }}
{{- end }}
{{- end }}
