# Member runtime resolution

`Resolver` reads the runtime and model inputs for an intended member service.
It follows the member's explicit-runtime precedence, automatic catalog ranking,
runtime inheritance, pin drift acknowledgements, and component merge rules.
Runtime configuration fills only components declared by the service.

The caller supplies a direct client bound to a verified member registration and
the member's operator namespace when reading configuration or runtime revisions.
For an existing derived service, the caller must verify ownership and supply its
current identity as `standing`. The resolver reads pin status from that live
object. A nil `standing` requires the intended service to be absent on the member.
Neither input objects nor API objects are written.

Pinning predicts the live spec for an initial pin or an acknowledged advance
without creating a ControllerRevision. Existing pins must have matching runtime
name, kind, and namespace metadata. Ambiguous scope, missing revisions, disabled
inputs, unacknowledged drift, and failed reads return no resolution.

Each resolution retains object UID/resource-version observations, missing-object
lookups, and the selected catalog's membership. `Runtime.Check` repeats those
reads with its caller's context. Replacement, deletion, mutation, newly shadowing
objects, partial catalogs, and API errors invalidate the observation. Concurrent
checks use separate read objects. This is conservative revalidation, not an
atomic snapshot across Kubernetes objects.

The returned components precede deployment defaults, accelerator selection, and
pod rendering. They are not final accelerator demand. Capacity consumers must
derive complete Engine/Decoder pod and gang shapes, resolve flavor attribution,
and verify member application independently. The runtime hash identifies the
selected runtime spec; it does not include service overrides, model data, or
the member's rendered workloads.

This package has no controller call site and changes no local service behavior.
