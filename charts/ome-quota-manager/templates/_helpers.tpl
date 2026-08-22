{{/*
Constructs an image ref with an optional global hub prefix. If the repository
already contains '/', hub is ignored. Mirrors the ome-scheduler helper.
Parameters: values, repository, tag.
*/}}
{{- define "ome-quota-manager.image" -}}
{{- $hub := .values.global.hub -}}
{{- $repo := .repository -}}
{{- $tag := .tag -}}
{{- if and $hub (not (contains "/" $repo)) -}}
{{- printf "%s/%s:%s" $hub $repo $tag -}}
{{- else -}}
{{- printf "%s:%s" $repo $tag -}}
{{- end -}}
{{- end -}}

{{/*
Chart name and version, suitable for use as a Kubernetes label value.
*/}}
{{- define "ome-quota-manager.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Selector labels — minimal and stable across re-renders.
*/}}
{{- define "ome-quota-manager.selectorLabels" -}}
app.kubernetes.io/name: ome-quota-manager
app.kubernetes.io/component: quota-manager
{{- end -}}

{{/*
Full label set stamped on chart resources.
*/}}
{{- define "ome-quota-manager.labels" -}}
{{ include "ome-quota-manager.selectorLabels" . }}
helm.sh/chart: {{ include "ome-quota-manager.chart" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: ome
{{- end -}}

{{/*
Where the webhook serving cert lives inside the container. Not a values knob —
it is a container-local path, not an operator decision — but it is shared
between the volume mount and the flag, and the two silently disagreeing would
mean a webhook that never serves.
*/}}
{{- define "ome-quota-manager.certDir" -}}
/tmp/k8s-webhook-server/serving-certs
{{- end -}}

{{/*
Fails the render unless mode is one of the two the binary accepts. There is no
safe default: workload mode writes Kueue objects on this cluster and management
mode projects shares onto others, so an unset or mistyped value must stop the
install rather than produce a Deployment that crash-loops on a flag check.
*/}}
{{- define "ome-quota-manager.mode" -}}
{{- $mode := .Values.quotaManager.mode -}}
{{- if not (has $mode (list "workload" "management")) -}}
{{- fail (printf "quotaManager.mode must be \"workload\" or \"management\", got %q" $mode) -}}
{{- end -}}
{{- $mode -}}
{{- end -}}
