{{/* Copyright (c) 2026 Benjamin Levin. All Rights Reserved. */}}
{{/* Unauthorized use or distribution is strictly prohibited. */}}
{{- define "kumquat-blockchain.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "kumquat-blockchain.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s" (include "kumquat-blockchain.name" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "kumquat-blockchain.labels" -}}
app.kubernetes.io/name: {{ include "kumquat-blockchain.name" . }}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version | replace "+" "_" }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "kumquat-blockchain.selectorLabels" -}}
app.kubernetes.io/name: {{ include "kumquat-blockchain.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}
