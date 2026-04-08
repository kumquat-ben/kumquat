{{/* Copyright (c) 2026 Benjamin Levin. All Rights Reserved. */}}
{{/* Unauthorized use or distribution is strictly prohibited. */}}
{{- define "kumquat-backend.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "kumquat-backend.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s" (include "kumquat-backend.name" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "kumquat-backend.labels" -}}
app.kubernetes.io/name: {{ include "kumquat-backend.name" . }}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version | replace "+" "_" }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "kumquat-backend.selectorLabels" -}}
app.kubernetes.io/name: {{ include "kumquat-backend.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}
