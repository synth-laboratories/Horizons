{{/*
Expand the name of the chart.
*/}}
{{- define "horizons.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
*/}}
{{- define "horizons.fullname" -}}
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
Create chart name and version as used by the chart label.
*/}}
{{- define "horizons.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "horizons.labels" -}}
helm.sh/chart: {{ include "horizons.chart" . }}
{{ include "horizons.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "horizons.selectorLabels" -}}
app.kubernetes.io/name: {{ include "horizons.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use.
*/}}
{{- define "horizons.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "horizons.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Resolve DATABASE_URL: external > in-cluster postgres.
*/}}
{{- define "horizons.databaseUrl" -}}
{{- if .Values.externalPostgres.url }}
{{- .Values.externalPostgres.url }}
{{- else if .Values.postgres.enabled }}
{{- printf "postgresql://%s:%s@%s-postgres:5432/%s" .Values.postgres.user .Values.postgres.password (include "horizons.fullname" .) .Values.postgres.database }}
{{- else }}
{{- "" }}
{{- end }}
{{- end }}

{{/*
Resolve REDIS_URL: external > in-cluster redis.
*/}}
{{- define "horizons.redisUrl" -}}
{{- if .Values.externalRedis.url }}
{{- .Values.externalRedis.url }}
{{- else if .Values.redis.enabled }}
{{- printf "redis://%s-redis:6379" (include "horizons.fullname" .) }}
{{- else }}
{{- "" }}
{{- end }}
{{- end }}

{{/*
Name for the Secret that holds sensitive values.
*/}}
{{- define "horizons.secretName" -}}
{{- if .Values.existingSecret }}
{{- .Values.existingSecret }}
{{- else }}
{{- include "horizons.fullname" . }}
{{- end }}
{{- end }}
