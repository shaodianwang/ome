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
Chart name and version, suitable for use as a Kubernetes label value.
*/}}
{{- define "ome.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Common chart provenance labels. Keep these out of selectors so a chart or app
version change does not make an existing workload selector immutable.
*/}}
{{- define "ome.labels" -}}
helm.sh/chart: {{ include "ome.chart" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: ome
{{- end -}}

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

{{/*
Alfred selector labels. control-plane is the stable selector key and must
never change; the Service, ServiceMonitor and topology spread select on it.
*/}}
{{- define "ome-alfred.selectorLabels" -}}
control-plane: ome-alfred
{{- end }}

{{/*
Stable per-pod labels: the selector plus the component identity. Kept free of
chart/version provenance so a chart publish alone never rolls the pods.
*/}}
{{- define "ome-alfred.podLabels" -}}
{{ include "ome-alfred.selectorLabels" . }}
app.kubernetes.io/name: ome-alfred
app.kubernetes.io/component: ome-alfred
{{- end }}

{{/*
Full label set stamped on every non-pod alfred resource.
*/}}
{{- define "ome-alfred.labels" -}}
{{ include "ome.labels" . }}
{{ include "ome-alfred.podLabels" . }}
{{- end }}

{{/*
Alfred image reference: alfred.image.hub overrides global.hub; a repository
containing '/' is used as-is (same contract as ome.imageWithHub).
*/}}
{{- define "ome-alfred.image" -}}
{{- $a := .Values.alfred -}}
{{- include "ome.imageWithHub" (dict "values" (merge (dict "global" (dict "hub" (default .Values.global.hub $a.image.hub))) .Values) "repository" $a.image.repository "tag" ($a.image.tag | toString)) -}}
{{- end }}

{{/*
Name of the recommendations ConfigMap. The policy config is the single source
of truth, so the pre-created ConfigMap and the RBAC write grant can never
drift from what the reporter writes to.
*/}}
{{- define "ome-alfred.recommendationsName" -}}
{{- .Values.alfred.config.recommendationsConfigMapName | default "alfred-recommendations" -}}
{{- end }}
