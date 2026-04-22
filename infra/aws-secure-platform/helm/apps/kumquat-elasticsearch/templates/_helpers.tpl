{{- define "kumquat-elasticsearch.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "kumquat-elasticsearch.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- include "kumquat-elasticsearch.name" . -}}
{{- end -}}
{{- end -}}

{{- define "kumquat-elasticsearch.labels" -}}
app.kubernetes.io/name: {{ include "kumquat-elasticsearch.name" . }}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version | replace "+" "_" }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "kumquat-elasticsearch.selectorLabels" -}}
app.kubernetes.io/name: {{ include "kumquat-elasticsearch.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}
