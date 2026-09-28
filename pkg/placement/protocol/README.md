# Member execution authority

The source encodes an execution annotation as a versioned envelope containing
the allocation policy. Version `1` carries the plan ID/revision, source and
cluster identities, and the surge pause. The version is a wire-format
discriminator, not an operator-configurable behavior or default.

Members accept both this envelope and existing flat policies. Unknown versions,
unknown fields, incomplete authority, or an origin mismatch return an error.
Fields cannot be ignored safely when they may constrain replica growth. A reader
that only understands flat policies receives no valid allocation authority from
the envelope and fails policy validation.

The ISVC controller validates participating services before configuration reads,
finalizer changes, status updates, and component dispatch. This applies to every
deployment mode. Deletion can continue through the existing cleanup path.
Ordinary services without an origin marker ignore the execution annotation.

Upgrade member controllers before the source begins emitting envelopes. This
versioned transport does not itself establish deployment-backend support, demand
compatibility, or member application; those require their own observations and
execution guards.
