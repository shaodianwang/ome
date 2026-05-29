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
ome-isvc.engineBaseArgs — chart-hardcoded runtime base config every
engine/prefill/decode tenant inherits. These are flags that are
universally true for the sglang server binary (host/port wiring and
proven-good defaults like prefix caching). The vendor profile carries
NO `command`/`args` (see templates/vendors/*.yaml); this list is the
first layer of the tenant CSR's args.

Layering order (see README "Args layering"):
  1. Runtime base       (THIS template — host/port/server defaults)
  2. Synthesised flags  (componentSynthesizedArgs — typed knobs)
  3. User args[]        (escape hatch — wins on duplicate --flag)

Returns: a YAML list of strings (alternating flag/value).
*/}}
{{- define "ome-isvc.engineBaseArgs" -}}
- --host
- 0.0.0.0
- --port
- "30000"
- --enable-prefix-cache
- --schedule-policy
- lpm
{{- end -}}

{{/*
ome-isvc.routerBaseArgs — chart-hardcoded runtime base config for the
router component. Same contract as ome-isvc.engineBaseArgs. Empty by
default; the router's behaviour is fully specified by the typed knobs
(timeoutSeconds, loadBalancingAlgorithm, ...) plus the scientist's
`args:`. Define entries here if a future router flag becomes
"universally true" for every tenant.
*/}}
{{- define "ome-isvc.routerBaseArgs" -}}
{{- end -}}

{{/*
ome-isvc.mergeArgs — merges two args lists with dedup-by-flag-name.
User entries WIN on duplicate `--flag` and entirely new user flags get
APPENDED at the end (after base entries).

Input dict:
  .base = list of strings (chart-hardcoded base args; alternating
          `--flag` then value, with bare boolean flags allowed).
  .user = list of strings (scientist's `args:` from values.yaml; same
          shape as .base).

Walking semantics:
  - Items starting with `--` are flags.
  - The NEXT item is the value IFF it does NOT start with `--`;
    otherwise the flag is a bare boolean (no value).
  - We build an ordered list of flag names (preserving base order,
    appending net-new user flags), plus a map flag -> value.
  - User entries OVERWRITE base entries on duplicate flag (so user
    `--port 31000` replaces base `--port 30000` rather than producing
    two `--port` entries in the final list).
  - Bare boolean values are represented by the empty string in the
    map and re-emitted as a single line.

Output: a YAML sequence ready to drop into `args:` (with `nindent`).
Each line is either `- --flag` (boolean) or `- --flag` followed by
`- "value"`.
*/}}
{{- define "ome-isvc.mergeArgs" -}}
{{- $base := default (list) .base -}}
{{- $user := default (list) .user -}}
{{- $merged := dict -}}
{{- $order := list -}}
{{- /* Walk base in pairs. Use index-based iteration so we can peek ahead. */ -}}
{{- $baseLen := len $base -}}
{{- range $i, $item := $base -}}
{{- if hasPrefix "--" $item -}}
{{- /* Flag. Peek next item. */ -}}
{{- $val := "" -}}
{{- $next := add $i 1 -}}
{{- if lt $next $baseLen -}}
{{- $peek := index $base $next -}}
{{- if not (hasPrefix "--" $peek) -}}
{{- $val = $peek -}}
{{- end -}}
{{- end -}}
{{- if not (hasKey $merged $item) -}}
{{- $order = append $order $item -}}
{{- end -}}
{{- $_ := set $merged $item $val -}}
{{- end -}}
{{- end -}}
{{- /* Walk user in pairs, same logic but user wins on duplicates. */ -}}
{{- $userLen := len $user -}}
{{- range $i, $item := $user -}}
{{- if hasPrefix "--" $item -}}
{{- $val := "" -}}
{{- $next := add $i 1 -}}
{{- if lt $next $userLen -}}
{{- $peek := index $user $next -}}
{{- if not (hasPrefix "--" $peek) -}}
{{- $val = $peek -}}
{{- end -}}
{{- end -}}
{{- if not (hasKey $merged $item) -}}
{{- $order = append $order $item -}}
{{- end -}}
{{- $_ := set $merged $item $val -}}
{{- end -}}
{{- end -}}
{{- /* Emit the merged list as a YAML sequence. */ -}}
{{- range $flag := $order }}
- {{ $flag }}
{{- $val := index $merged $flag }}
{{- if ne $val "" }}
- {{ $val | quote }}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
ome-isvc.componentSynthesizedArgs — emit the synthesised typed-flag
block for a component (engine/prefill/decode). One sglang flag per
explicitly set knob; unset knobs are omitted (so the chart base or
sglang default takes effect).

Args:  dict "block" .prefill | .decode | .engine
Returns: a YAML list of strings (alternating flag/value). Empty if no
knob is set.

Mapping (see README "Synthesised CLI flags"):
  contextLength                    -> --context-length
  tensorParallelSize               -> --tensor-parallel-size
  gpuMemoryUtilization             -> --mem-fraction-static
  maxConcurrentRequests            -> --max-running-requests
  dtype                            -> --dtype
  quantization                     -> --quantization
  kvCacheDtype                     -> --kv-cache-dtype
  moeRunnerBackend                 -> --moe-runner-backend
  speculative.algorithm            -> --speculative-algorithm        (gates the block)
  speculative.draftModelPath       -> --speculative-draft-model
  speculative.draftModelQuantization -> --speculative-draft-model-quantization
*/}}
{{- define "ome-isvc.componentSynthesizedArgs" -}}
{{- $b := .block -}}
{{- if hasKey $b "contextLength" }}
- --context-length
- "{{ printf "%d" (int $b.contextLength) }}"
{{- end -}}
{{- if hasKey $b "tensorParallelSize" }}
- --tensor-parallel-size
- "{{ printf "%d" (int $b.tensorParallelSize) }}"
{{- end -}}
{{- if hasKey $b "gpuMemoryUtilization" }}
- --mem-fraction-static
- "{{ printf "%v" $b.gpuMemoryUtilization }}"
{{- end -}}
{{- if hasKey $b "maxConcurrentRequests" }}
- --max-running-requests
- "{{ printf "%d" (int $b.maxConcurrentRequests) }}"
{{- end -}}
{{- /* Model artifact knobs — emit ONLY when set to a non-empty value. */ -}}
{{- if and (hasKey $b "dtype") $b.dtype }}
- --dtype
- {{ $b.dtype | quote }}
{{- end -}}
{{- if and (hasKey $b "quantization") $b.quantization }}
- --quantization
- {{ $b.quantization | quote }}
{{- end -}}
{{- if and (hasKey $b "kvCacheDtype") $b.kvCacheDtype }}
- --kv-cache-dtype
- {{ $b.kvCacheDtype | quote }}
{{- end -}}
{{- if and (hasKey $b "moeRunnerBackend") $b.moeRunnerBackend }}
- --moe-runner-backend
- {{ $b.moeRunnerBackend | quote }}
{{- end -}}
{{- /* Speculative decoding — algorithm GATES the whole block. */ -}}
{{- if and $b.speculative $b.speculative.algorithm }}
- --speculative-algorithm
- {{ $b.speculative.algorithm | quote }}
{{- if $b.speculative.draftModelPath }}
- --speculative-draft-model
- {{ $b.speculative.draftModelPath | quote }}
{{- end -}}
{{- if $b.speculative.draftModelQuantization }}
- --speculative-draft-model-quantization
- {{ $b.speculative.draftModelQuantization | quote }}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
ome-isvc.componentArgs — assemble the FULL args list for a component
block, layered as: runtime base + synthesised typed flags + user args,
with dedup-by-flag-name (user wins on duplicates). Renamed knobs are
emitted only when explicitly set — unset knobs leave the chart base /
sglang default in place.

Args:  dict "block" .prefill | .decode | .engine
Returns: JSON-encoded list of strings (use `fromJsonArray` at the call
site to materialise back to a Helm list, then `toYaml`).
*/}}
{{- define "ome-isvc.componentArgs" -}}
{{- $b := .block -}}
{{- $baseRaw := include "ome-isvc.engineBaseArgs" . | trim -}}
{{- $base := list -}}
{{- if $baseRaw -}}{{- $base = $baseRaw | fromYamlArray -}}{{- end -}}
{{- $synthRaw := include "ome-isvc.componentSynthesizedArgs" (dict "block" $b) | trim -}}
{{- $synth := list -}}
{{- if $synthRaw -}}{{- $synth = $synthRaw | fromYamlArray -}}{{- end -}}
{{- $baseAndSynth := concat $base $synth -}}
{{- $user := default (list) $b.args -}}
{{- $mergedRaw := include "ome-isvc.mergeArgs" (dict "base" $baseAndSynth "user" $user) | trim -}}
{{- $merged := list -}}
{{- if $mergedRaw -}}{{- $merged = $mergedRaw | fromYamlArray -}}{{- end -}}
{{- $merged | toJson -}}
{{- end -}}

{{/*
ome-isvc.renderLifecycle — emit the OMENative `lifecycle:` block (or
nothing) for a scientist-facing component. STRICT pass-through: only
emits a key when the user has set it, never defaults in the chart.
Each nested subtree (updateStrategy, rollingUpdate, inPlaceUpdateStrategy,
migrationPolicy) is emitted only when at least one of its sub-keys is
set, and the outer `lifecycle:` line is emitted only when at least one
leaf field is set. This avoids producing an empty `lifecycle: {}` /
`updateStrategy: {}` that would override the controller defaulter with
an empty map.

Mirrors pkg/apis/ome/v1beta1/lifecycle_types.go::LifecycleSpec.

Args:  dict "block" .prefill | .decode | .engine | .router
*/}}
{{- define "ome-isvc.renderLifecycle" -}}
{{- $b := .block -}}
{{- $lc := $b.lifecycle -}}
{{- if $lc -}}
{{- /* Pre-compute whether any sub-tree has a set value. We only emit the
       outer `lifecycle:` line when something would actually be written
       beneath it. */ -}}
{{- $us := $lc.updateStrategy -}}
{{- $ru := and $us $us.rollingUpdate -}}
{{- $ip := and $us $us.inPlaceUpdateStrategy -}}
{{- $mp := $lc.migrationPolicy -}}
{{- $hasRU := and $ru (or (hasKey $ru "partition") (hasKey $ru "maxUnavailable")) -}}
{{- $hasIP := and $ip (or (hasKey $ip "gracePeriodSeconds") (hasKey $ip "markNotReadyDuringLifecycle")) -}}
{{- $hasUS := and $us (or $us.type $hasRU $hasIP) -}}
{{- $hasMP := and $mp $mp.mode -}}
{{- $hasAny := or $lc.restartPolicy $lc.readyPolicy $lc.instanceReadyTimeout $hasUS $hasMP -}}
{{- if $hasAny }}
lifecycle:
  {{- if $lc.restartPolicy }}
  restartPolicy: {{ $lc.restartPolicy }}
  {{- end }}
  {{- if $hasUS }}
  updateStrategy:
    {{- if $us.type }}
    type: {{ $us.type }}
    {{- end }}
    {{- if $hasRU }}
    rollingUpdate:
      {{- if hasKey $ru "partition" }}
      partition: {{ $ru.partition }}
      {{- end }}
      {{- if hasKey $ru "maxUnavailable" }}
      maxUnavailable: {{ $ru.maxUnavailable }}
      {{- end }}
    {{- end }}
    {{- if $hasIP }}
    inPlaceUpdateStrategy:
      {{- if hasKey $ip "gracePeriodSeconds" }}
      gracePeriodSeconds: {{ $ip.gracePeriodSeconds }}
      {{- end }}
      {{- if hasKey $ip "markNotReadyDuringLifecycle" }}
      markNotReadyDuringLifecycle: {{ $ip.markNotReadyDuringLifecycle }}
      {{- end }}
    {{- end }}
  {{- end }}
  {{- if $lc.readyPolicy }}
  readyPolicy: {{ $lc.readyPolicy }}
  {{- end }}
  {{- if $lc.instanceReadyTimeout }}
  instanceReadyTimeout: {{ $lc.instanceReadyTimeout | quote }}
  {{- end }}
  {{- if $hasMP }}
  migrationPolicy:
    mode: {{ $mp.mode }}
  {{- end }}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
ome-isvc.renderComponent — emit the InferenceService engine/decoder
spec for a scientist-facing component block. Renders replicas + a
runner override (image, command args if `image`/`args` are set) without
producing an empty runner subtree when both are unset. Optionally
appends a `lifecycle:` block when the scientist sets any
`block.lifecycle.*` field.

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
{{- end }}
{{- include "ome-isvc.renderLifecycle" (dict "block" $b) -}}
{{- end -}}

{{/*
ome-isvc.renderEngineComponent — like renderComponent but also handles
multinode worker.size when block.workerSize is set. Also forwards any
scientist-set `block.lifecycle.*` via the shared renderLifecycle helper.
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
{{- end }}
{{- include "ome-isvc.renderLifecycle" (dict "block" $b) -}}
{{- end -}}

{{/*
ome-isvc.routerSynthesizedArgs — emit the synthesised typed-flag block
for the router component. One CLI flag per explicitly set router knob;
unset knobs are omitted.

Args:  dict "block" .router
Returns: a YAML list of strings (alternating flag/value). Empty if no
knob is set.
*/}}
{{- define "ome-isvc.routerSynthesizedArgs" -}}
{{- $b := .block -}}
{{- if hasKey $b "timeoutSeconds" }}
- --backend-timeout
- "{{ printf "%d" (int $b.timeoutSeconds) }}"
{{- end -}}
{{- /* streamingTimeoutSeconds: SSE / token deadline for long generations.
       Emitted as --streaming-timeout-secs; the exact sglang router flag
       name may differ — adjust here if the upstream binary expects
       something else (e.g. --stream-timeout-secs). */ -}}
{{- if hasKey $b "streamingTimeoutSeconds" }}
- --streaming-timeout-secs
- "{{ printf "%d" (int $b.streamingTimeoutSeconds) }}"
{{- end -}}
{{- if $b.loadBalancingAlgorithm }}
- --load-balancer
- {{ $b.loadBalancingAlgorithm | quote }}
{{- end -}}
{{- end -}}

{{/*
ome-isvc.routerArgs — assemble the FULL router args list using the
same 3-layer merge as the engine components: routerBaseArgs +
routerSynthesizedArgs + user args, deduped by --flag (user wins).

Args:  dict "block" .router
Returns: JSON-encoded list of strings.
*/}}
{{- define "ome-isvc.routerArgs" -}}
{{- $b := .block -}}
{{- $baseRaw := include "ome-isvc.routerBaseArgs" . | trim -}}
{{- $base := list -}}
{{- if $baseRaw -}}{{- $base = $baseRaw | fromYamlArray -}}{{- end -}}
{{- $synthRaw := include "ome-isvc.routerSynthesizedArgs" (dict "block" $b) | trim -}}
{{- $synth := list -}}
{{- if $synthRaw -}}{{- $synth = $synthRaw | fromYamlArray -}}{{- end -}}
{{- $baseAndSynth := concat $base $synth -}}
{{- $user := default (list) $b.args -}}
{{- $mergedRaw := include "ome-isvc.mergeArgs" (dict "base" $baseAndSynth "user" $user) | trim -}}
{{- $merged := list -}}
{{- if $mergedRaw -}}{{- $merged = $mergedRaw | fromYamlArray -}}{{- end -}}
{{- $merged | toJson -}}
{{- end -}}

{{/*
ome-isvc.renderRouter — emit the InferenceService router spec from a
scientist-facing router block. Adds the merged router args list onto
the runner because the OME router runner is a single container that
takes them as CLI flags. Same image/args override mechanic as the
prefill/decode/engine components, plus the same `lifecycle:` pass-through
via renderLifecycle.

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
{{- $merged := include "ome-isvc.routerArgs" (dict "block" $b) | fromJsonArray -}}
{{- if or $b.image (gt (len $merged) 0) }}
runner:
  name: router
  {{- if $b.image }}
  image: {{ $b.image | quote }}
  {{- end }}
  {{- if gt (len $merged) 0 }}
  args:
    {{- toYaml $merged | nindent 4 }}
  {{- end }}
{{- end }}
{{- include "ome-isvc.renderLifecycle" (dict "block" $b) -}}
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
