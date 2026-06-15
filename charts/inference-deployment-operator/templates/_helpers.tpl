{{- define "inference-deployment-operator.name" -}}
inference-deployment-operator
{{- end }}

{{- define "inference-deployment-operator.image" -}}
{{- $repo := .Values.image.repository }}
{{- $tag := default .Chart.AppVersion .Values.image.tag }}
{{- printf "%s:%s" $repo $tag -}}
{{- end }}
