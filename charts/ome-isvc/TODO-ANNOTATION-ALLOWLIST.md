# TODO (v0.2): annotation allowlist for `isvc.annotations`

## Current behavior (v0.1)

`templates/inferenceservice.yaml` passes `.Values.isvc.annotations`
through verbatim onto the InferenceService. The chart does NOT enforce
an allowlist — we trust the OME validating webhook to reject
inappropriate annotations.

## Open question

Some annotations have InferenceService-level side effects (e.g.
`ome.io/deploymentMode: VirtualDeployment` bypasses scheduling
constraints; `ome.io/runtime-sync: <value>` triggers a coordinated
re-roll). A platform operator may want to restrict which annotations
a tenant can set.

## Proposed shape for v0.2 (NOT implemented yet)

1. Add `values.yaml` knob:
   ```yaml
   isvc:
     annotationAllowlist:
       enabled: false               # opt-in for backward compatibility
       allow:
         - ome.io/deploymentMode
         - ome.io/runtime-sync
   ```
2. In `templates/inferenceservice.yaml`, filter
   `.Values.isvc.annotations` against the allowlist with `hasKey` /
   `fail` so tenants get a clear render-time error.
3. Operators publish their own allowlist via Helm values overrides
   (e.g. a wrapper chart that pins `annotationAllowlist.enabled: true`
   and the list of approved keys).

The structure here is deliberately additive: the v0.1 default of
"allow all" still works when `annotationAllowlist.enabled: false`.
