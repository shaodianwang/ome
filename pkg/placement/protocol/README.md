# Member execution authority

The source encodes an execution annotation as a versioned envelope containing
the allocation policy. Version `1` carries the plan ID/revision, source and
cluster identities, and the surge pause. Version `2` also requires a demand
contract: the resolved resource/flavor fingerprint and exactly one rendering
hash for each declared Engine/Decoder component. These versions identify fixed
wire formats; they are not operator configuration or behavior defaults.

Members accept these envelopes and existing flat policies without demand.
Demand is invalid in a flat policy or a version `1` envelope; version `2` is
invalid without it. Unknown versions,
unknown fields, incomplete authority, or an origin mismatch return an error.
Fields cannot be ignored safely when they may constrain replica growth. A reader
that only understands flat policies receives no valid allocation authority from
the envelope and fails policy validation.

The ISVC controller validates participating services before configuration reads,
finalizer changes, status updates, and component dispatch. This applies to every
deployment mode. Deletion can continue through the existing cleanup path.
Ordinary services without an origin marker ignore the execution annotation.

Engine and Decoder compare a contracted rendering with the actual rendered pod
specs immediately before workload projection. The hash includes component,
deployment mode, pod-set names and counts, complete pod specs, and live
RuntimeClass identity and configuration. RuntimeClass overhead, required node
selection, and tolerations are normalized on copies. Changed or unavailable
classes, conflicting authored values, and mismatched renderings stop projection.
The replica floor is excluded so allocation changes can reuse one unit contract.
The `primary` and `workers` pod-set names and SHA-256 encoding are part of this
comparison format, not scheduling defaults.

Install the member CRD and controller role (including RuntimeClass `get` access),
and upgrade member controllers before the source emits version `2`.
A schema that lacks demand prunes it from InferenceReplica storage;
an application observation must compare the complete stored policy with the
expected policy and must not credit that partial result. Version `1` readers
reject version `2`; flat-only readers cannot validate either envelope.

This check establishes agreement at component projection. It does not freeze a
RuntimeClass after verification, validate arbitrary admission mutations, or
establish deployment-backend support for shared surge accounting. Source-side
capacity planning still requires persisted demand evidence, member identity and
application fences, and admitted-resource observations. Rendering hashes are
available from the resolver; they do not activate capacity-based placement.
