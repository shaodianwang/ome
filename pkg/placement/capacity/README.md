# Nominal replica capacity

`MeasureUnit` measures the configured accelerator resources in a rendered
Engine/Decoder replica unit. Each component supplies named sets of identical
pods and an explicit count. A leader and its workers are separate sets. Router
is absent from this input because it follows a per-home replica policy.

The caller must supply the complete member rendering, including runtime and
service overrides, accelerator selection, worker counts, and any admission
resources such as RuntimeClass overhead. `InputFingerprint` identifies those
resolved dependencies. This helper does not render a service, resolve admission
configuration, or verify that a member applied the templates.

Accounting uses Kubernetes' pod request calculation: regular containers run
together; restartable init containers overlap subsequent initialization and
the application; sequential init containers contribute their peak; pod overhead
is added after that peak. Omitted accelerator requests inherit their limits,
matching API defaulting. Explicit requests must equal limits. Each set's pod
request is multiplied by its count with checked whole-unit arithmetic.

Missing configuration, invalid quantities, arithmetic overflow, dynamic resource
claims, unsupported pod-level resources, and nonzero unconfigured extended
resources return no result. An unconfigured extended resource might be another
accelerator, so it cannot be silently discarded. CPU and memory do not contribute
to accelerator demand. A component with no accelerators is retained, but the
whole unit must have positive accelerator demand to be normalized.

`UnitDemand` retains independent pod sets and their scheduling constraints for
resource/flavor attribution. It does not combine sets just because they request
the same resource: an Engine and Decoder may require different flavors. Its
fingerprint includes rendering dependencies, configured resource names, pod
counts, and complete templates. Pod-set and configuration ordering are canonical;
init-container ordering is preserved because it affects demand. Inputs are not
mutated and returned templates do not alias them.

After unambiguous flavor attribution, `Reader` combines demand sharing a
resource/flavor pool and normalizes identified, fresh per-cluster allocatable
reports to whole replica units. It takes the minimum ratio across required
pools. It never subtracts usage or quota. Missing or incompatible evidence
rejects the whole matched-set observation; the controller must retain its last
accepted plan. Aggregate hardware counts do not prove gang topology, CPU/memory
fit, or quota admission.
