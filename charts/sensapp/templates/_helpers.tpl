{{- define "sensapp.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "sensapp.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "sensapp.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "sensapp.labels" -}}
helm.sh/chart: {{ include "sensapp.chart" . }}
app.kubernetes.io/name: {{ include "sensapp.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "sensapp.selectorLabels" -}}
app.kubernetes.io/name: {{ include "sensapp.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "sensapp.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "sensapp.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{/*
"true" when the chart makes the JWT secret itself: authentication is on, and no secret was given
(neither `auth.jwtSecret` nor `auth.existingSecret`). Empty otherwise.
*/}}
{{- define "sensapp.generateJwtSecret" -}}
{{- if and (not .Values.auth.disabled) (not .Values.auth.jwtSecret) (not .Values.auth.existingSecret) -}}
true
{{- end -}}
{{- end -}}

{{/*
The JWT secret the chart makes: the one of the Secret of the release when it exists, so that an
upgrade keeps it (and the tokens made with it), a new random one on the first install. `helm
template` and the tools that only render (Argo CD) cannot look the Secret up: they would make
a new secret at each render, so give `auth.jwtSecret` or `auth.existingSecret` there.
*/}}
{{- define "sensapp.jwtSecret" -}}
{{- $existing := lookup "v1" "Secret" .Release.Namespace (include "sensapp.fullname" .) -}}
{{- if and $existing $existing.data (hasKey $existing.data "SENSAPP_JWT_SECRET") -}}
{{- index $existing.data "SENSAPP_JWT_SECRET" | b64dec -}}
{{- else -}}
{{- randAlphaNum 48 -}}
{{- end -}}
{{- end -}}

{{/* "true" when the Secret of the release holds something */}}
{{- define "sensapp.hasSecret" -}}
{{- if or (not .Values.storage.existingSecret) .Values.auth.jwtSecret .Values.auth.previousSecrets .Values.secretEnv (include "sensapp.generateJwtSecret" .) -}}
true
{{- end -}}
{{- end -}}
