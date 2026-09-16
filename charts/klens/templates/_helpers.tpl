{{/*
Expand the name of the chart.
*/}}
{{- define "klens.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name (truncated to the 63-char DNS limit).
*/}}
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

{{/*
Chart name and version as used by the chart label.
*/}}
{{- define "klens.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "klens.labels" -}}
helm.sh/chart: {{ include "klens.chart" . }}
{{ include "klens.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "klens.selectorLabels" -}}
app.kubernetes.io/name: {{ include "klens.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Service account name to use.
*/}}
{{- define "klens.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "klens.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Container image reference. A digest pins immutably and wins when set.
Otherwise repository:tag, with tag defaulting to the chart appVersion.
*/}}
{{- define "klens.image" -}}
{{- if .Values.image.digest -}}
{{- printf "%s@%s" .Values.image.repository .Values.image.digest -}}
{{- else -}}
{{- printf "%s:%s" .Values.image.repository (.Values.image.tag | default .Chart.AppVersion) -}}
{{- end -}}
{{- end }}

{{/*
Image for the helm test connection pod.
*/}}
{{- define "klens.testImage" -}}
{{- $img := .Values.tests.image -}}
{{- printf "%s:%s" $img.repository $img.tag -}}
{{- end }}

{{/*
Name of the ConfigMap holding the klens config file.
*/}}
{{- define "klens.configMapName" -}}
{{- default (include "klens.fullname" .) .Values.existingConfigMap -}}
{{- end }}

{{/*
Name of the Secret holding interpolation env, or empty if none.
*/}}
{{- define "klens.secretName" -}}
{{- if .Values.secret.existingSecret -}}
{{- .Values.secret.existingSecret -}}
{{- else if gt (len (.Values.secret.stringData | default dict)) 0 -}}
{{- include "klens.fullname" . -}}
{{- end -}}
{{- end }}

{{/*
Reject a loopback bind when the chart owns the ConfigMap. The Service cannot
reach 127.0.0.1 inside the pod.
*/}}
{{- define "klens.validateBind" -}}
{{- if not .Values.existingConfigMap }}
{{- $bind := .Values.config.bind | default "" | toString }}
{{- if or (hasPrefix "127.0.0.1:" $bind) (eq $bind "127.0.0.1") (hasPrefix "[::1]:" $bind) (eq $bind "::1") }}
{{- fail "config.bind must not be a loopback address. The Service cannot reach the process. Use 0.0.0.0." }}
{{- end }}
{{- end }}
{{- end }}
