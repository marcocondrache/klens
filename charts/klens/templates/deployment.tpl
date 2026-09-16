{{- include "klens.validateBind" . }}
apiVersion: apps/v1
kind: Deployment
metadata:
  name: {{ include "klens.fullname" . }}
  namespace: {{ .Release.Namespace }}
  labels:
    {{- include "klens.labels" . | nindent 4 }}
  {{- with .Values.deploymentAnnotations }}
  annotations:
    {{- toYaml . | nindent 4 }}
  {{- end }}
spec:
  replicas: {{ .Values.replicaCount }}
  selector:
    matchLabels:
      {{- include "klens.selectorLabels" . | nindent 6 }}
  template:
    metadata:
      labels:
        {{- include "klens.labels" . | nindent 8 }}
        {{- with .Values.podLabels }}
        {{- tpl (toYaml .) $ | nindent 8 }}
        {{- end }}
      annotations:
        {{- if not .Values.existingConfigMap }}
        checksum/config: {{ include (print $.Template.BasePath "/configmap.tpl") . | sha256sum }}
        {{- end }}
        {{- if and (not .Values.secret.existingSecret) (gt (len (.Values.secret.stringData | default dict)) 0) }}
        checksum/secret: {{ include (print $.Template.BasePath "/secret.tpl") . | sha256sum }}
        {{- end }}
        {{- with .Values.podAnnotations }}
        {{- tpl (toYaml .) $ | nindent 8 }}
        {{- end }}
    spec:
      {{- with .Values.imagePullSecrets }}
      imagePullSecrets:
        {{- tpl (toYaml .) $ | nindent 8 }}
      {{- end }}
      serviceAccountName: {{ include "klens.serviceAccountName" . }}
      automountServiceAccountToken: {{ .Values.serviceAccount.automount }}
      enableServiceLinks: false
      {{- with .Values.priorityClassName }}
      priorityClassName: {{ tpl . $ | quote }}
      {{- end }}
      terminationGracePeriodSeconds: {{ .Values.terminationGracePeriodSeconds }}
      securityContext:
        {{- tpl (toYaml .Values.podSecurityContext) $ | nindent 8 }}
      containers:
        - name: klens
          image: {{ include "klens.image" . | quote }}
          imagePullPolicy: {{ .Values.image.pullPolicy }}
          securityContext:
            {{- tpl (toYaml .Values.securityContext) $ | nindent 12 }}
          env:
            - name: KLENS_CONFIG_PATH
              value: /config/config.yaml
            {{- range $key, $value := .Values.env }}
            - name: {{ $key }}
              value: {{ tpl (toString $value) $ | quote }}
            {{- end }}
            {{- with .Values.extraEnv }}
            {{- tpl (toYaml .) $ | nindent 12 }}
            {{- end }}
          {{- $secretName := include "klens.secretName" . }}
          {{- if or $secretName (gt (len (.Values.envFrom | default list)) 0) }}
          envFrom:
            {{- if $secretName }}
            - secretRef:
                name: {{ $secretName }}
            {{- end }}
            {{- with .Values.envFrom }}
            {{- tpl (toYaml .) $ | nindent 12 }}
            {{- end }}
          {{- end }}
          ports:
            - name: http
              containerPort: {{ .Values.service.port }}
              protocol: TCP
          startupProbe:
            {{- tpl (toYaml .Values.startupProbe) $ | nindent 12 }}
          livenessProbe:
            {{- tpl (toYaml .Values.livenessProbe) $ | nindent 12 }}
          readinessProbe:
            {{- tpl (toYaml .Values.readinessProbe) $ | nindent 12 }}
          {{- with .Values.resources }}
          resources:
            {{- tpl (toYaml .) $ | nindent 12 }}
          {{- end }}
          volumeMounts:
            - name: config
              mountPath: /config
              readOnly: true
            {{- with .Values.volumeMounts }}
            {{- tpl (toYaml .) $ | nindent 12 }}
            {{- end }}
      volumes:
        - name: config
          configMap:
            name: {{ include "klens.configMapName" . }}
        {{- with .Values.volumes }}
        {{- tpl (toYaml .) $ | nindent 8 }}
        {{- end }}
      {{- with .Values.nodeSelector }}
      nodeSelector:
        {{- tpl (toYaml .) $ | nindent 8 }}
      {{- end }}
      {{- with .Values.affinity }}
      affinity:
        {{- tpl (toYaml .) $ | nindent 8 }}
      {{- end }}
      {{- with .Values.tolerations }}
      tolerations:
        {{- tpl (toYaml .) $ | nindent 8 }}
      {{- end }}
      {{- with .Values.topologySpreadConstraints }}
      topologySpreadConstraints:
        {{- tpl (toYaml .) $ | nindent 8 }}
      {{- end }}
