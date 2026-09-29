{{/* Chart name (overridable). */}}
{{- define "ome-bot.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/* Fully-qualified app name; avoids doubling when the release already contains it. */}}
{{- define "ome-bot.fullname" -}}
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

{{- define "ome-bot.selectorLabels" -}}
app.kubernetes.io/name: {{ include "ome-bot.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "ome-bot.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{ include "ome-bot.selectorLabels" . }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: ome
{{- end -}}

{{/* Full image ref, hub-prefixed like the other OME charts. */}}
{{- define "ome-bot.image" -}}
{{- $hub := .Values.global.hub -}}
{{- $repo := .Values.image.repository -}}
{{- $tag := .Values.image.tag | default .Chart.AppVersion -}}
{{- if and $hub (not (contains "/" $repo)) -}}
{{- printf "%s/%s:%s" $hub $repo $tag -}}
{{- else -}}
{{- printf "%s:%s" $repo $tag -}}
{{- end -}}
{{- end -}}

{{- define "ome-bot.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "ome-bot.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{/* Mount points of the config ConfigMap and the secrets volume. */}}
{{- define "ome-bot.configDir" -}}/etc/ome-bot/config{{- end -}}
{{- define "ome-bot.secretsDir" -}}/etc/ome-bot/secrets{{- end -}}

{{/* Path of the mounted HMAC key. */}}
{{- define "ome-bot.authSecretFile" -}}
{{- printf "%s/%s" (include "ome-bot.secretsDir" .) .Values.config.auth.secretKey -}}
{{- end -}}
