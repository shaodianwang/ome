{{/*
ome-isvc.name — chart name, optionally overridden by .Values.nameOverride.
*/}}
{{- define "ome-isvc.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
ome-isvc.releasePrefix — the prefix used to derive resource names per
deployment. Defaults to `.Release.Name`; overridden by
`fullnameOverride` (escape hatch) or composed with `nameOverride`.
*/}}
{{- define "ome-isvc.releasePrefix" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else if .Values.nameOverride -}}
{{- printf "%s-%s" .Release.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{/*
ome-isvc.chart — `helm.sh/chart` label value (name-version).
*/}}
{{- define "ome-isvc.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
ome-isvc.isvcName — InferenceService name for a deployment.
Args:  dict "root" $ "deployment" .
Returns:  <releasePrefix>-<deployment.name>
*/}}
{{- define "ome-isvc.isvcName" -}}
{{- $prefix := include "ome-isvc.releasePrefix" .root -}}
{{- printf "%s-%s" $prefix .deployment.name | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
ome-isvc.runtimeName — tenant child ServingRuntime / ClusterServingRuntime
name for a deployment.
Args:  dict "root" $ "deployment" .
Returns:  <releasePrefix>-<deployment.name>-csr
*/}}
{{- define "ome-isvc.runtimeName" -}}
{{- $prefix := include "ome-isvc.releasePrefix" .root -}}
{{- printf "%s-%s-csr" $prefix .deployment.name | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
ome-isvc.runtimeKind — ServingRuntime when scope=namespace,
ClusterServingRuntime when scope=cluster.
*/}}
{{- define "ome-isvc.runtimeKind" -}}
{{- if eq .Values.scope "cluster" -}}ClusterServingRuntime{{- else -}}ServingRuntime{{- end -}}
{{- end -}}

{{/*
ome-isvc.baseModelKind — BaseModel or ClusterBaseModel based on the
modelRef.scope (defaults to cluster when unset).
Args:  dict "root" $ "scope" .modelRef.scope
*/}}
{{- define "ome-isvc.baseModelKind" -}}
{{- if eq (default "cluster" .scope) "namespace" -}}BaseModel{{- else -}}ClusterBaseModel{{- end -}}
{{- end -}}

{{/*
ome-isvc.componentName — translate scientist-facing component names
into the OME Component identifier expected by RolloutCoordinationSpec.
  prefill -> engine
  decode  -> decoder
  engine  -> engine
  router  -> router
*/}}
{{- define "ome-isvc.componentName" -}}
{{- $n := . -}}
{{- if eq $n "prefill" -}}engine
{{- else if eq $n "decode" -}}decoder
{{- else if eq $n "engine" -}}engine
{{- else if eq $n "router" -}}router
{{- else -}}{{ fail (printf "ome-isvc: unknown component name %q (expected prefill/decode/engine/router)" $n) -}}
{{- end -}}
{{- end -}}

{{/*
ome-isvc.deploymentVendor — resolves the effective vendor profile for a
deployment: deployments[].vendor if set, else .Values.vendor (the
top-level default). Fails loudly if neither is set.
Args: dict "root" $ "deployment" .
*/}}
{{- define "ome-isvc.deploymentVendor" -}}
{{- $d := .deployment -}}
{{- $root := .root -}}
{{- $v := default $root.Values.vendor $d.vendor -}}
{{- if not $v -}}{{- fail (printf "ome-isvc: deployment %q has no vendor and .Values.vendor is unset" $d.name) -}}{{- end -}}
{{- $v -}}
{{- end -}}

{{/*
ome-isvc.referencedVendors — set of vendor profile ids referenced by
some entry in .Values.deployments (resolving inheritance from
.Values.vendor when a deployment omits its own vendor). Returned as a
JSON map literal so templates can use `hasKey` against it.
*/}}
{{- define "ome-isvc.referencedVendors" -}}
{{- $vendors := dict -}}
{{- $root := . -}}
{{- range $i, $d := .Values.deployments -}}
{{- $v := default $root.Values.vendor $d.vendor -}}
{{- if $v -}}{{- $_ := set $vendors $v true -}}{{- end -}}
{{- end -}}
{{- $vendors | toJson -}}
{{- end -}}

{{/*
ome-isvc.vendorIsActive — returns "true" when the named vendor profile
should be rendered as a ClusterServingRuntime by THIS release. A vendor
is active when either:
  - .Values.vendors.deployAll is true; or
  - some entry in .Values.deployments references it.
Args:  dict "root" $ "vendor" "<vendor-id>"
*/}}
{{- define "ome-isvc.vendorIsActive" -}}
{{- $root := .root -}}
{{- $vendor := .vendor -}}
{{- if and $root.Values.vendors $root.Values.vendors.deployAll -}}true
{{- else -}}
{{- $referenced := include "ome-isvc.referencedVendors" $root | fromJson -}}
{{- if hasKey $referenced $vendor -}}true{{- end -}}
{{- end -}}
{{- end -}}

{{/*
ome-isvc.labels — standard chart labels applied to every resource.
Merges in .Values.commonLabels. Scoped by release, not by deployment.
*/}}
{{- define "ome-isvc.labels" -}}
helm.sh/chart: {{ include "ome-isvc.chart" . }}
{{ include "ome-isvc.selectorLabels" . }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: ome
{{- with .Values.commonLabels }}
{{ toYaml . }}
{{- end }}
{{- end -}}

{{/*
ome-isvc.selectorLabels — minimum stable label set for selectors.
*/}}
{{- define "ome-isvc.selectorLabels" -}}
app.kubernetes.io/name: {{ include "ome-isvc.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{/*
ome-isvc.deploymentLabels — per-deployment labels: chart labels plus a
deployment-name dimension. Useful for filtering inside a multi-deployment
release.
Args:  dict "root" $ "deployment" .
*/}}
{{- define "ome-isvc.deploymentLabels" -}}
{{ include "ome-isvc.labels" .root }}
ome.io/isvc-deployment: {{ .deployment.name | quote }}
{{- end -}}

{{/*
ome-isvc.commonAnnotations — concatenation of .Values.commonAnnotations
for sprinkling on every resource.
*/}}
{{- define "ome-isvc.commonAnnotations" -}}
{{- with .Values.commonAnnotations }}
{{ toYaml . }}
{{- end }}
{{- end -}}

{{/*
ome-isvc.componentArgs — assemble the args list for a component block.
The vendor profile supplies the base `command` (binary + base flags);
this helper builds the `args` list that follows. Order:

  1. Synthesised flags from explicit knobs (only emitted when set):
       --context-length         <contextLength>
       --tensor-parallel-size   <tensorParallelSize>
       --mem-fraction-static    <gpuMemoryUtilization>
       --max-running-requests   <maxConcurrentRequests>
  2. User-supplied `args:` from the values block, appended last so the
     escape hatch wins on duplicate flags.

Each knob is emitted ONLY when the values block explicitly carries the
key (`hasKey`) — unset knobs leave the vendor default in place.

Args:  dict "block" .prefill | .decode | .engine
Returns: a list of strings (use with `toYaml` at the call site).
*/}}
{{- define "ome-isvc.componentArgs" -}}
{{- $b := .block -}}
{{- $args := list -}}
{{- if hasKey $b "contextLength" -}}
{{- $args = append $args "--context-length" -}}
{{- $args = append $args (printf "%d" (int $b.contextLength)) -}}
{{- end -}}
{{- if hasKey $b "tensorParallelSize" -}}
{{- $args = append $args "--tensor-parallel-size" -}}
{{- $args = append $args (printf "%d" (int $b.tensorParallelSize)) -}}
{{- end -}}
{{- if hasKey $b "gpuMemoryUtilization" -}}
{{- $args = append $args "--mem-fraction-static" -}}
{{- $args = append $args (printf "%v" $b.gpuMemoryUtilization) -}}
{{- end -}}
{{- if hasKey $b "maxConcurrentRequests" -}}
{{- $args = append $args "--max-running-requests" -}}
{{- $args = append $args (printf "%d" (int $b.maxConcurrentRequests)) -}}
{{- end -}}
{{- if $b.args -}}
{{- range $a := $b.args -}}{{- $args = append $args $a -}}{{- end -}}
{{- end -}}
{{- $args | toJson -}}
{{- end -}}

{{/*
ome-isvc.renderComponent — emit the InferenceService engine/decoder
spec for a scientist-facing component block. Renders replicas + a
runner override (image, command args if `image`/`args` are set) without
producing an empty runner subtree when both are unset.

Args:  dict
  "block"          .prefill | .decode | .engine
  "appendArgs"     true|false (always true for our use; signature reserved
                   for future per-component flag knobs)
*/}}
{{- define "ome-isvc.renderComponent" -}}
{{- $b := .block -}}
{{- if $b.replicas -}}
{{- if hasKey $b.replicas "min" }}
minReplicas: {{ $b.replicas.min }}
{{- end }}
{{- if hasKey $b.replicas "max" }}
maxReplicas: {{ $b.replicas.max }}
{{- end }}
{{- end }}
{{- if or $b.image (and $b.args (gt (len $b.args) 0)) }}
runner:
  name: ome-container
  {{- if $b.image }}
  image: {{ $b.image | quote }}
  {{- end }}
  {{- if and $b.args (gt (len $b.args) 0) }}
  args:
    {{- toYaml $b.args | nindent 4 }}
  {{- end }}
{{- end -}}
{{- end -}}

{{/*
ome-isvc.renderEngineComponent — like renderComponent but also handles
multinode worker.size when block.workerSize is set.
Args:  dict "block" .engine "servingMode" .servingMode
*/}}
{{- define "ome-isvc.renderEngineComponent" -}}
{{- $b := .block -}}
{{- $servingMode := .servingMode -}}
{{- if $b.replicas -}}
{{- if hasKey $b.replicas "min" }}
minReplicas: {{ $b.replicas.min }}
{{- end }}
{{- if hasKey $b.replicas "max" }}
maxReplicas: {{ $b.replicas.max }}
{{- end }}
{{- end }}
{{- if or $b.image (and $b.args (gt (len $b.args) 0)) }}
runner:
  name: ome-container
  {{- if $b.image }}
  image: {{ $b.image | quote }}
  {{- end }}
  {{- if and $b.args (gt (len $b.args) 0) }}
  args:
    {{- toYaml $b.args | nindent 4 }}
  {{- end }}
{{- end }}
{{- if and (eq $servingMode "multinode") $b.workerSize }}
worker:
  size: {{ $b.workerSize }}
{{- end -}}
{{- end -}}

{{/*
ome-isvc.renderRouter — emit the InferenceService router spec from a
scientist-facing router block. Adds router-specific extra args
(timeout, load-balancer) into the `runner.args` list because the OME
router runner is a single container that takes them as CLI flags. Same
image/args override mechanic as the prefill/decode/engine components.

Args:  dict "block" .router
*/}}
{{- define "ome-isvc.renderRouter" -}}
{{- $b := .block -}}
{{- if $b.replicas -}}
{{- if hasKey $b.replicas "min" }}
minReplicas: {{ $b.replicas.min }}
{{- end }}
{{- if hasKey $b.replicas "max" }}
maxReplicas: {{ $b.replicas.max }}
{{- end }}
{{- end }}
{{- $extraArgs := list -}}
{{- if hasKey $b "timeoutSeconds" -}}
{{- $extraArgs = append $extraArgs "--backend-timeout" -}}
{{- $extraArgs = append $extraArgs (printf "%d" (int $b.timeoutSeconds)) -}}
{{- end -}}
{{- /* streamingTimeoutSeconds: SSE / token deadline for long generations.
       Emitted as --streaming-timeout-secs; the exact sglang router flag
       name may differ — adjust here if the upstream binary expects
       something else (e.g. --stream-timeout-secs). */ -}}
{{- if hasKey $b "streamingTimeoutSeconds" -}}
{{- $extraArgs = append $extraArgs "--streaming-timeout-secs" -}}
{{- $extraArgs = append $extraArgs (printf "%d" (int $b.streamingTimeoutSeconds)) -}}
{{- end -}}
{{- if $b.loadBalancingAlgorithm -}}
{{- $extraArgs = append $extraArgs "--load-balancer" -}}
{{- $extraArgs = append $extraArgs $b.loadBalancingAlgorithm -}}
{{- end -}}
{{- if $b.args -}}
{{- range $a := $b.args -}}{{- $extraArgs = append $extraArgs $a -}}{{- end -}}
{{- end -}}
{{- if or $b.image (gt (len $extraArgs) 0) }}
runner:
  name: router
  {{- if $b.image }}
  image: {{ $b.image | quote }}
  {{- end }}
  {{- if gt (len $extraArgs) 0 }}
  args:
    {{- toYaml $extraArgs | nindent 4 }}
  {{- end }}
{{- end -}}
{{- end -}}

{{/*
ome-isvc.coordinationGroup — synthesize a single RolloutCoordinationGroup
for a deployment that has multiple Components. Emits nothing if the
deployment has only one Component (engine).

Args:  dict "deployment" . "components" (list of OME component names)
*/}}
{{- define "ome-isvc.coordinationGroup" -}}
{{- $coord := .deployment.coordination -}}
{{- $components := .components -}}
{{- if and $coord (gt (len $components) 1) }}
- components:
    {{- range $c := $components }}
    - {{ $c }}
    {{- end }}
  policy: {{ $coord.policy | default "Independent" }}
  {{- if $coord.order }}
  order:
    {{- range $o := $coord.order }}
    - {{ include "ome-isvc.componentName" $o }}
    {{- end }}
  {{- end }}
  {{- if $coord.soak }}
  soak: {{ $coord.soak | quote }}
  {{- end }}
  {{- if $coord.pacing }}
  pacing:
    {{- if $coord.pacing.type }}
    type: {{ $coord.pacing.type }}
    {{- end }}
    {{- if hasKey $coord.pacing "maxSurge" }}
    maxSurge: {{ $coord.pacing.maxSurge }}
    {{- end }}
    {{- if hasKey $coord.pacing "maxUnavailable" }}
    maxUnavailable: {{ $coord.pacing.maxUnavailable }}
    {{- end }}
    {{- if $coord.pacing.ratioTolerance }}
    ratioTolerance: {{ $coord.pacing.ratioTolerance }}
    {{- end }}
  {{- end }}
{{- end -}}
{{- end -}}
