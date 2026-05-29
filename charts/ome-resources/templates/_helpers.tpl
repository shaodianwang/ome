{{/*
Constructs a full image URL with optional global hub prefix.
If repository already contains '/', hub is ignored.
Parameters:
  - values: helm values (to import "Values.global.hub", required)
  - repository: the repository name (required)
  - tag: the image tag (required)
*/}}
{{- define "ome.imageWithHub" -}}
{{- $hub := .values.global.hub }}
{{- $repo := .repository }}
{{- $tag := .tag }}
{{- if and $hub (not (contains "/" $repo)) -}}
{{- printf "%s/%s:%s" $hub $repo $tag -}}
{{- else }}
{{- printf "%s:%s" $repo $tag -}}
{{- end }}
{{- end }}

{{/*
Selector labels for the bundled Prometheus (Deployment + Service
selector). Kept minimal so they stay stable across re-renders.
*/}}
{{- define "ome-prometheus.selectorLabels" -}}
app.kubernetes.io/name: ome-prometheus
app.kubernetes.io/component: ome-prometheus
{{- end }}

{{/*
Full label set stamped on bundled Prometheus resources. Mirrors the
selector labels and folds in operator-supplied
.Values.prometheus.additionalLabels.
*/}}
{{- define "ome-prometheus.labels" -}}
{{ include "ome-prometheus.selectorLabels" . }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: ome
{{- with .Values.prometheus.additionalLabels }}
{{ toYaml . }}
{{- end }}
{{- end }}
