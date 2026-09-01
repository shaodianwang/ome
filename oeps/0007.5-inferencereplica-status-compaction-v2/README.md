# OEP-0007.5: OMENative InferenceReplica Status Compaction v2

## Summary

**Tl;dr:** An OMENative Instance is one independently managed replica group.
OEP-0007.4 removes three live Pod observations from each InferenceReplica (IR)
Instance row. This v2 proposal losslessly compacts the durable values that must
survive no-Pod windows by storing repeated values once against canonical index
ranges.

The decoded logical status—the ordered `InstanceStatus` rows consumed by the
workload state machine—does **not** change. It retains the same lifecycle,
coordination, revision, operation, and failure values. The raw IR JSON does
change; that operator-facing compatibility gate is explicit below.

```text
DenseV1                         ColumnarV2
------------------------------    ---------------------------------
one object per Instance row       one member index set
repeated keys on every row        grouped columns for repeated values
rare fields mixed into rows       sparse entries for operations/failures
```

The representation remains on the IR. This OEP adds no CRD below the IR, no
status sidecar, and no Pod labels or annotations. The hierarchy remains
`InferenceService -> InferenceReplica -> Pod`.

ColumnarV2 is a storage codec at the existing IR-status persistence boundary:

1. decode the stored representation into the existing logical rows;
2. run the existing decision and mutation logic on those rows; and
3. encode the result inside the same conflict-retried status write.

For a synthetic uniform steady-state fixture, the modeled per-Instance section
changes as follows. Each row has index, incarnation, Ready phase, one full
running revision, the three retained counts, and admission. These are
compact-JSON bytes, **not** a full IR request or a release gate:

| Instances | DenseV1 dense rows | ColumnarV2 columns | Modeled reduction |
| ---: | ---: | ---: | ---: |
| 2,000 | 322,891 B | 363 B | 322,528 B (99.89%) |
| 5,000 | 808,891 B | 363 B | 808,528 B (99.96%) |

Real statuses include metadata, top-level fields, mixed revisions, operations,
and failures. The implementation must measure complete status-update requests
for those shapes. While `ColumnarV2` is the configured target, the writer stores
DenseV1 whenever columns are not smaller. Compaction therefore does not rely
on uniformity: the selected client request cannot make an adversarial status
larger than V1. API-server-managed fields are measured separately below.

## Motivation

### Failure mode

OEP-0007.3 reduces the number of large writes. OEP-0007.4 removes three
per-Instance observations that Pods can supply at the correct observation
epoch. Neither removes the dense durable row itself.

At 2,000 or 5,000 replicas, healthy rows repeat the same keys and usually the
same values thousands of times: `phase: Ready`, one running revision,
incarnation 1, one set of counters, and `admitted: true`. That repetition is
stored and transmitted on every IR status update. A single large update can
still reach the API-server or etcd request budget even after write batching.

Simply deleting the remaining rows is unsafe. Create, Update, Restart,
Migrate, and Delete persist intent before changing Pods and must recover while
no Pod exists. V2 must compact those facts without deriving them from a newer
Pod snapshot.

### Goals

1. Reduce replica-count-driven IR status growth at 2,000 and 5,000 Instances.
2. Preserve the complete DenseV1 logical rows and every decision made from
   them, including sparse and transitional states.
3. Make encoding canonical, bounded, deterministic, and safe against malformed
   or adversarial ranges.
4. In ColumnarV2 target mode, never select a larger v2 candidate when DenseV1
   is smaller.
5. Keep activation and decode limits in `inferenceservice-config`; the binary
   contains no behavioral default.

### Non-Goals

1. Changing OEP-0007.4's Pod derivation or any lifecycle, rollout, placement,
   retry, batching, gang, or retention policy.
2. Moving state onto Pods or adding another CRD or ConfigMap shard.
3. Bounding failure messages or migration history, which grow independently of
   repeated rows.
4. Guaranteeing one fixed size for arbitrary diagnostics. Lossless storage
   still reflects the size of unique failure and operation records.
5. Adding a migration controller, status-repair API, or runtime transition
   coordinator. Activation and break-glass repair use checked-in operational
   tooling around the same codec.

## Terminology

- **Instance:** one independently managed OMENative replica group. A singleton
  has one Pod; a gang Instance may have several Pods.
- **Logical row:** the complete DenseV1 state for one Instance after decoding.
  Controller code consumes this form; it is independent of the stored format.
- **Durable value:** a value retained through a no-Pod window because a later
  lifecycle or coordination decision cannot reconstruct it from current Pods.
- **DenseV1:** OEP-0007.4's dense row representation. It omits
  `readyPodCount`, `scheduledPodCount`, and `nodesOccupied`, while retaining
  durable and prior-observation fields.
- **ColumnarV2:** the representation selected here. Equal values are grouped by
  canonical Instance index sets, and exceptional nested records remain keyed
  by Instance index.
- **Index set:** a canonical string such as `0-8,10-114,116` representing sorted
  nonnegative Instance indices without expanding them in the stored object.
- **Exceptional entry:** one sparse record containing fields that are normally
  absent or too Instance-specific to group profitably, such as an Operation or
  LastFailure.
- **Dense fallback:** DenseV1 chosen for a logical status whose complete
  encoded candidate is no larger than ColumnarV2.
- **Target encoding:** the operator-selected write policy, `DenseV1` or
  `ColumnarV2`. It controls the next status write; it does not reinterpret the
  representation already committed on an IR.
- **Logical mutation:** a change to decoded status. A **representation-only
  write** changes DenseV1 versus ColumnarV2 without changing those logical
  values.
- **Mutation family:** one existing workload path that changes IR status, such
  as lifecycle, publication, RetryBlock, or migration handling. A publication
  mutation is the family that records current observed aggregate counters;
  other families preserve counters from the prior successful publication.
- **OnCommit:** the existing callback invoked after a status update succeeds;
  it receives the logical rows that were committed.
- **Managed fleet:** every declared local or remote IR reader and writer whose
  visible API scopes participate in one format transition. Its inventory is an
  operator attestation as well as a discoverable Kubernetes resource list.
- **Physically disjoint fleets:** fleets whose credentials, RBAC, caches, and
  list/watch scopes cannot read or write one another's IRs. Only such fleets can
  select different target encodings safely.
- **DenseV1-only manager:** an OEP-0007.4-compatible manager that understands
  the current compact dense rows but not ColumnarV2. This OEP does not restore
  rollback to binaries older than OEP-0007.4; that rollback is already unsafe.

## Audit Boundary

OEP-0007.4 owns the field-by-field Pod-derivation audit. V2 starts from its
logical contract and asks a narrower question: how can the retained rows be
encoded without repeating their keys and common values?

### What v1 leaves

Every current per-Instance field is accounted for below.

| Logical field | ColumnarV2 storage | Rule |
| --- | --- | --- |
| `index` | `members` | Sparse indices are preserved exactly. |
| dense slice order | `rowOrder` | Omitted for ascending order; otherwise preserves the exact index sequence. |
| `phase` | `phases[]` | Every member has exactly one phase. |
| `incarnation` | `incarnations[]` | Nonzero values are grouped; absence means zero. |
| `runningRevision` | `runningRevisions[]` | Nonempty full revision names are grouped. |
| `targetRevision` | `targetRevisions[]` | Nonempty full revision names are grouped. |
| `podCount` | `podCounts[]` | Nonzero prior-published values are grouped. |
| `servingPodCount` | `servingPodCounts[]` | Nonzero prior-published values are grouped. |
| `availablePodCount` | `availablePodCounts[]` | Nonzero prior-published values are grouped. |
| `admitted` | `admitted` | The index set contains every true value. |
| `activeOrdinal` | `activeOrdinalOne` | The index set contains value 1; absence means 0. |
| `conditions` | `entries[].conditions` | Preserved on the matching exceptional entry. |
| `operation` | `entries[].operation` | Preserved as the complete nested record. |
| `lastFailure` | `entries[].lastFailure` | Preserved as the complete nested record. |
| `readyPodCount` | not stored | Already omitted by DenseV1. |
| `scheduledPodCount` | not stored | Already omitted by DenseV1. |
| `nodesOccupied` | not stored | Already omitted by DenseV1. |

All other top-level IR status fields remain unchanged, including aggregate
counts, revisions, collision count, selector, RetryBlocks, Migrations,
Conditions, Traffic, and CoordinationGroupRef. V2 does not move them into the
columnar payload because they are not repeated once per Instance.

### Why the retained values remain durable

ColumnarV2 compresses values; it does not reclassify them as live observations.

OEP-0007.4 proves why these values stay durable: operation, identity, revision,
and failure state survives zero-Pod windows; placement needs admitted row
identity; and several gates consume values from the prior successful
publication. V2 preserves that proof by encoding the values, not recomputing
them. `lastFailure` also remains complete whether it came from a Pod or was
synthesized by timeout or migration recovery.

The partial-gang case remains the clearest safety test:

```text
prior persisted podCount >= expected
current live Pod count      < expected
current live Pod count      > 0
```

Replacing the first value with the current count prevents repair. V2 stores
that prior value in a grouped column and decodes it before planning.

## Proposal

### Contract

1. ColumnarV2 MUST decode to the same logical rows in the same slice order as
   DenseV1.
2. Encoding and decoding MUST preserve every retained value, pointer, nested
   record, timestamp, revision name, and Condition with API-semantic equality.
3. The codec MUST be deterministic: equal logical rows produce the same
   canonical typed payload and semantic no-op result. Tests may also pin the
   local serializer bytes; Kubernetes API GET formatting is not the contract.
4. The decoder MUST validate the complete payload before any lifecycle effect.
   Invalid data fails closed; it never becomes an empty or partial row set.
5. The decoder MUST enforce a configured cardinality limit before allocating a
   logical row slice from ColumnarV2. DenseV1 retains its current API
   deserialization and has no new cardinality rejection.
6. The representation marker and its payload MUST change in one status update.
   A committed object contains exactly one Instance representation.
7. Every supported manager, repair, or fixture path that persists ColumnarV2
   MUST compare complete DenseV1 and ColumnarV2 status candidates and persist
   ColumnarV2 only when it is strictly smaller. A tie selects DenseV1. Schema
   validity alone does not authorize a ColumnarV2 write.
8. V2 MUST add no Pod or EndpointSlice LIST, watch, write, label, or annotation.
9. Status ownership, operation ordering, and external effects remain exactly
    as defined by OEP-0007 and OEP-0007.4.

### Wire shape

A representative steady state with a few reincarnations and failures is:

```yaml
status:
  instanceStatusEncoding: ColumnarV2
  instanceStatusColumns:
    members: "0-499"
    # rowOrder is omitted because the dense row order is already ascending.
    phases:
    - {value: Ready, indexes: "0-499"}
    runningRevisions:
    - {value: example-engine-2f32f6fe, indexes: "0-499"}
    incarnations:
    - {value: 1, indexes: "0-8,10-114,116-499"}
    - {value: 2, indexes: "9,115"}
    podCounts:
    - {value: 1, indexes: "0-499"}
    servingPodCounts:
    - {value: 1, indexes: "0-499"}
    availablePodCounts:
    - {value: 1, indexes: "0-499"}
    admitted: "0-499"
    activeOrdinalOne: "6,43,175"
    entries:
    - index: 9
      lastFailure:
        podName: example-engine-9
        containerName: ome-container
        reason: Error
        exitCode: 143
```

`targetRevisions` is omitted above because all values are empty. During a
rollout it uses the same `{value, indexes}` form as `runningRevisions`.
Operations and Conditions are copied unchanged into the unique entry for their
index. An entry with no exceptional field is invalid and is not emitted.

`rowOrder` is an atomic `[]int32`. It is omitted when the dense slice order is
the ascending `members` order; otherwise it lists every member exactly once in
the original order. Current code observes dense slice order in escalation and
coordination output, so v2 preserves it rather than treating list-map order as
irrelevant.

The API uses distinct typed group structs for strings, integers, counts, and
phases so OpenAPI preserves their value types. The apparent generic shape in
YAML is a presentation convenience, not an untyped map. Incarnation values use
the existing `int64` domain; indices and counts retain `int32`.

Every group slice, `entries`, and `rowOrder` uses atomic list semantics, which
keeps managed-field paths bounded. Canonical order is bytewise lexical for
phase and revision groups, signed numeric for incarnation and count groups, and
ascending index for entries. Nested ordered fields retain their API semantics;
for example, Conditions keep their list-map key and Operation target nodes keep
atomic order. Negative nonzero incarnations remain representable because V1
allows them; negative counts remain DenseV1-only. Present strings and slices
have minimum length one. Optional scalar index sets use pointers so the codec
can reject explicitly empty values while reused nested records retain their V1
zero/absence normalization.

`instanceStatusEncoding` is an optional pointer and has one accepted value:
`ColumnarV2`. A missing value means DenseV1 and requires
`instanceStatusColumns` to be absent. ColumnarV2 requires `instanceStatuses` to
be absent and a valid nonempty column payload. The codec rejects both payloads,
neither payload for a nonempty ColumnarV2 object, and every unknown marker.
Reverse conversion removes the marker and columns while restoring the dense
list, so canonical DenseV1 adds no bytes to the current wire format.

The structural schema and CEL rules enforce this representation union wherever
Kubernetes validation can express it: an unmarked object cannot contain
columns, and a ColumnarV2 object must contain columns and cannot contain dense
rows. Required fields, enums, minimum string lengths, minimum list lengths, and
atomic-list semantics are expressed in the CRD. Codec validation remains
defense in depth for canonical ordering, range relationships, cardinality, and
objects persisted through an older or incompatible schema.

### Index-set grammar

An index set is a comma-separated list of a decimal index or closed range:

```text
index-set = term *( "," term )
term      = uint32 | uint32 "-" uint32
```

The accepted subset is canonical:

- `members` and every present index-set field are nonempty; a zero-row status
  uses DenseV1;
- indices are nonnegative and fit the existing Instance `int32` domain;
- zero is `0`; other values have no leading zero;
- terms are strictly ascending and nonoverlapping;
- adjacent terms are merged (`1,2,3` encodes as `1-3`);
- ranges have `start < end`; a singleton uses one number;
- whitespace, signs, empty terms, and trailing commas are invalid.

The decoder scans every index set with overflow-checked 64-bit arithmetic
before enumerating any index. It compares `members` cardinality with the
configured row limit, rejects every other set whose cardinality exceeds
`len(members)`, and rejects each group-slice length, a column's cumulative
disjoint references, entry count, or row-order length above that same bound.
These checks scan O(wire bytes + group count) before any row-proportional
allocation or index enumeration. Subset checks operate on canonical ranges
rather than allocating a numeric-span bitmap. A payload with one member and
`indexes: "0-2147483647"` therefore fails on cardinality before iteration;
repeating valid members across different columns does not count the logical
rows again.

Typed JSON deserialization necessarily allocates the bounded wire payload
before the codec runs; Kubernetes request limits bound that input. The
`maxDecodedInstances` guarantee covers codec expansion and iteration—it does
not claim pre-validation allocation of zero bytes.

For zero rows, an absent marker with an absent or empty `instanceStatuses`
field is valid. ColumnarV2 is never emitted for zero rows;
an empty column payload would make corruption indistinguishable from an empty
workload.

Controller-produced indices are nonnegative. A legacy object containing a
negative or duplicate index is not eligible for ColumnarV2; the writer keeps it
DenseV1 so the anomaly remains inspectable.

### Column defaults and validation

`members` defines the complete logical row set. Every other reference must be
a subset of it.

| Column | Coverage rule | Value when absent |
| --- | --- | --- |
| `rowOrder` | Exact permutation of `members` when present | ascending member order |
| `phases` | Exact, disjoint coverage of `members` | Invalid; Phase is required. |
| `incarnations` | Disjoint nonzero groups | `0` |
| `runningRevisions` | Disjoint nonempty groups | empty string |
| `targetRevisions` | Disjoint nonempty groups | empty string |
| three retained count columns | Disjoint positive groups | `0` |
| `admitted` | One index set | `false` |
| `activeOrdinalOne` | One index set | `0` |
| `entries` | At most one entry per member | nil exceptional fields |

The decoder rejects duplicate column values, overlapping groups, references
outside `members`, negative counts, unsupported phases, active ordinals other
than the encoded 0/1 domain, nonpositive values in sparse count groups, and
noncanonical array order. It also rejects simultaneous dense and columnar
payloads, a marker/payload mismatch, or an unknown marker. It does **not** add
cross-field lifecycle validation such as `servingPodCount <= podCount`; V2 must
not reject a DenseV1 logical state that the current controller can preserve.

These outcomes differ by direction. Any readable DenseV1 row set that cannot
be encoded canonically—including a negative count, unsupported phase, invalid
ordinal, duplicate index, or excessive cardinality—remains DenseV1 and retains
its current V1 handling, including any existing API-validation failure. V2 does
not add a new DenseV1 rejection. A malformed stored ColumnarV2 payload cannot
be trusted, so its reconcile fails before effects; corruption never triggers
dense fallback.

### Decode, mutate, encode

The codec sits at the API/workload boundary:

```text
fresh API object plus its stored encoding
        |
        v
decode durable rows into the existing DenseV1 logical shape
        |
        v
construct the existing epoch-scoped ComponentObservation
        |
        v
existing mutation family produces its V1 persistence candidate
  lifecycle/operation writers preserve prior-published counters
  publication writer materializes current counters on its owned copy
        |
        v
clear the three OEP-0007.4 Pod-derived fields on a write copy
        |
        v
validate -> select a canonical candidate
        |
        v
status update only when logical state changed or source != selected encoding
```

Decode happens after every IR GET whose consumer reads Instance status and
before that consumer plans or authorizes an effect. Spec- or metadata-only
paths—such as ownership checks, finalizer removal, and annotation updates—are
classified separately and preserve the raw status union without decoding it.
Read-only local and remote status consumers use the same logical view; none may
read `instanceStatuses` directly as a fallback.

A cycle-free leaf codec returns a freshly fetched IR in the existing DenseV1
logical shape plus the encoding observed before decode. The source encoding is
load-bearing: without it, a logical no-op cannot distinguish an already
converged object from DenseV1-to-ColumnarV2 activation or ColumnarV2-to-DenseV1
rollback. The writer compares source with the encoder-selected representation,
not merely the configured target, because a ColumnarV2 target may legitimately
select DenseV1 fallback.

The executable inventory covers reconciliation, aggregation, revision
retention, placement, and `irprojector` helpers while explicitly classifying
spec/metadata pass-through reads. Row-consuming coordination and placement
helpers use explicitly decoded accessors; raw helpers remain available only to
callers that do not inspect the per-Instance representation.

V2 does not collapse observation epochs. `ComponentObservation` continues to
decide which consumers read prior-published row values and which read current
Pod or EndpointSlice facts. The publication path may materialize all current
counters exactly as V1 does; the storage normalizer then clears only the three
fields OEP-0007.4 excludes before it builds either wire candidate.

The fetched object belongs to the codec reader; decode changes only that fresh
copy after recording its source encoding. The controller does not retain a
second 2,000- or 5,000-row raw status throughout reconciliation. On a conflict,
the writer discards the logical copy and source value and repeats the complete
flow from an uncached `APIReader` GET. Tests deliberately hold the informer
cache stale; fake-client retry success alone is insufficient evidence.

At the start of reconciliation, a transition gate performs this live read and
returns before lifecycle effects when the stored and selected representations
differ. It writes once, then immediately requeues. DenseV1 under a ColumnarV2
target must build both candidates because strict-smaller selection may retain
DenseV1; ColumnarV2 under a DenseV1 target always converts.

Already-converged objects take a fast path: DenseV1 under a DenseV1 target and
ColumnarV2 under a ColumnarV2 target do not rebuild and serialize both candidates
merely to prove another no-op. This relies on the sole-writer invariant: every
supported ColumnarV2 ingress, including repair and qualification fixtures, uses
the same current selector. A direct status-subresource write that bypasses the
adapter violates the ownership contract and is not a supported ingress.

The selected wire representation lives only on the write copy. After a
successful update, the adapter decodes the object returned by the API server and
uses that API-normalized logical state for mirrors and OnCommit callbacks. This
preserves current timestamp normalization, the three cleared V1 fields, row
ordering, and server metadata. An ambiguous-error postcondition confirmation
likewise decodes the authoritative live object before invoking OnCommit.
Returning only the pre-write logical candidate would change rollback
preconditions.

The writer builds a new payload rather than patching ranges in place. In-place
range surgery is difficult to make canonical and easy to get wrong under
conflict. Grouping and canonical sorting are O(N log N) in the worst case,
although common already-ordered rows are cheaper. Benchmarks decide whether a
bounded-domain linear implementation is justified.

### Dense fallback

Uniformity is common, not guaranteed. A status with a unique revision,
incarnation, or operation on every Instance can make grouped columns larger
than dense rows.

When configured for ColumnarV2, the writer serializes both complete status
candidates after the logical mutation:

- choose ColumnarV2 only when its serialized status is strictly smaller;
- choose DenseV1 on equality or when dense is smaller; and
- emit `instanceStatusEncoding: ColumnarV2` only for columns; DenseV1 clears the
  marker and column payload.

The comparison has no byte threshold or percentage baked into the binary.
Equal logical input always makes the same choice, so fallback cannot create a
write loop. A real logical change may legitimately switch the object between
DenseV1 and ColumnarV2; an unchanged reconcile may not. Measurement must
include the CPU and allocation cost of building both candidates.

### Status ownership

This OEP adds no status owner. The workload engine remains the logical owner of
per-Instance lifecycle transitions; the existing InferenceReplica adapter
remains the only Kubernetes persistence boundary for IR status. V2 introduces
one shared representation adapter, and **every** conflict-retried IR
`Status().Update` must route through it. A call-site test rejects direct writes
outside the adapter. The ISVC controller, placement controller, Pod watcher,
and codec do not independently write IR status.

That rule covers every running OME component. The operator-side repair helper
is the sole break-glass caller of an explicit repair-only entry point on the
same adapter. The entry point is not registered with a reconciler and accepts
only a complete independently validated replacement plus a live
resourceVersion; it cannot perform a normal lifecycle mutation. The executable
inventory classifies this path explicitly instead of hiding a second writer.

All existing status mutation families must pass through the codec before their
existing `Status().Update`: Instance batches, RetryBlocks, Migrations,
collision count, revision promotion, aggregate publication, Conditions,
terminal row removal, and teardown. Centralizing the codec prevents one
specialized writer from accidentally restoring dense fields or dropping a
columnar payload.

The executable inventory is type-aware rather than a search for one method
name. It rejects IR status writes through Update, Patch, server-side Apply,
generated `UpdateStatus`, dynamic clients, or `SubResource("status")` outside
the adapter. A matching read inventory rejects direct consumption of
`InstanceStatuses` or `InstanceStatusColumns` outside the codec and explicit
decoded-accessor boundary; spec/metadata-only reads receive named classifications.
Test-only fixture writes are explicitly classified rather than silently
exempting an entire directory. In particular, the multicluster performance
admitter and real-Kueue fixtures must use the logical codec/write boundary or
declare and enforce DenseV1-only operation.

### Observability

The status boundary uses only these label values:

| Metric labels | Fixed vocabulary |
| --- | --- |
| encoding, source, target | `dense_v1`, `columnar_v2` |
| conversion result | `committed`, `no_op`, `dense_fallback`, `rejected`, `error` |
| write cause | `logical_mutation`, `conversion`, `repair` |
| write result | `attempt`, `committed`, `confirmed`, `conflict`, `rejected`, `error` |
| codec-error reason | `unknown_encoding`, `representation_union`, `range_syntax`, `range_order`, `range_overflow`, `cardinality_limit`, `coverage`, `canonical_order`, `value_domain` |

The writer records selected serialized status bytes by encoding, conversions by
source/target/result, codec errors by reason, and writes by
cause/result/encoding. This makes the Stable no-amplification gate measurable
without object-identity labels. Metrics never include namespace, IR name, raw
range, revision, failure text, or payload content. They are operational
evidence, not authoritative downgrade proof; only a complete paginated API LIST
can prove that no ColumnarV2 object remains.

### Corruption recovery

A malformed ColumnarV2 object fails before lifecycle effects, but fail-closed is
not itself recovery. The manager emits a fixed-catalog error, Event reason, and
bounded metric reason that identify the IR without echoing ranges, revisions,
failure messages, or payload bytes. Error text comes only from the checked-in
catalog; tests enumerate that catalog, record its maximum message length, and
reject dynamic payload suffixes.

The checked-in operational runbook includes an operator-side, non-resident
repair helper. An operator supplies a complete known-good DenseV1 or
ColumnarV2 Instance representation obtained from a trusted backup or
investigation. Through the repair-only adapter entry point, the helper performs
an uncached live GET for resourceVersion and unrelated status, does not decode
the corrupt Instance payload, validates and decodes the replacement
independently, then runs those trusted logical rows through the configured
strict-smaller selector. It replaces only the Instance representation in one
status update with a resourceVersion precondition. It never clears rows, derives
missing durable history from Pods, or partially accepts a corrupt payload. Tests
cover stale resourceVersion, invalid replacement, both input encodings and the
selected output, preservation of unrelated status, and resumption of the same
logical decision after repair.

## Compatibility and Rollout

### Configuration

V2 adds a required target and a ColumnarV2 decode bound to
`inferenceservice-config`:

```yaml
omenativeStatus:
  instanceStatusEncoding: ColumnarV2
  maxDecodedInstances: <operator-set limit>
```

`instanceStatusEncoding` accepts `DenseV1` or `ColumnarV2`. `DenseV1` writes the
current list and is the downgrade target. `ColumnarV2` compares complete status
candidates and writes columns only when they are strictly smaller; otherwise it
writes DenseV1. This is a write target, not a third lifecycle policy.

The chart renders `DenseV1` by default; merging or installing the v2-capable
binary therefore cannot activate columns. The binary has no target-encoding
default. Missing or unknown target values abort setup before controllers or
webhooks register; unknown configuration fields, duplicate JSON fields, and
trailing data are also rejected. Readiness alone is not treated as a
reconciliation gate.

Every v2-capable manager performs an OpenAPI v3 schema preflight before creating
the controller manager or registering controllers and webhooks, even while its
target is DenseV1. Through the discovery endpoint, it verifies the IR status
subresource, retained dense field, exact marker enum, typed column properties,
minimums, atomic lists, and representation-union CEL. A missing, forbidden,
stale, minimal, or malformed schema aborts startup. Discovery avoids broad
permission to read CRD objects; deployments that do not inherit Kubernetes'
standard discovery access grant only GET on the OpenAPI non-resource URLs.
This makes the schema-first rollout an executable barrier rather than a runbook
assumption.

`maxDecodedInstances` has no binary or chart default. Any present value must be
positive in either target mode, and ColumnarV2 requires it to be present. It may
be absent while the target is DenseV1 and the authoritative pre-activation
audit proves that no ColumnarV2 object exists. If a DenseV1-target manager
unexpectedly observes columns without a bound, that IR fails closed before
effects. Helm rendering rejects every present zero or negative value and
ColumnarV2 without a value.

A v2-capable manager loads these values once at process startup. Silently
guessing either value would turn a missing rollout step into cluster-dependent
behavior. Target changes stop every OME manager and status writer under the
managed-fleet v2-reader barrier described below; approved v2-capable read-only
consumers may remain. Bound changes stop every reader and writer because the
bound controls whether an existing payload can be decoded. Ordinary rolling
changes to either value are forbidden. While every reader and writer is stopped,
the preflight may approve one new fleet-wide bound only after a non-expanding
scan proves every stored ColumnarV2 cardinality fits it. Lowering below any
object fails. Old- and new-bound processes never overlap.

The existing chart checksum identifies the rendered configuration and triggers
the DenseV1 binary rollout. Operators must change this block only through the
versioned deployment path; an in-place ConfigMap edit is not an activation
mechanism. The transition preflight checks the running image and configuration
digests before the global stop and again after restart. This is operational
evidence, not a new manager-side epoch or status field.

The limit is the deployment profile's supported ceiling for transient logical
rows in one IR, including surge, migration, replacement, and retained terminal
rows—not only desired replicas. The owner of each Helm or GitOps profile must
choose and test a finite ceiling for that profile. A profile without such a
ceiling cannot select ColumnarV2. The encoder checks the limit before writing:
an oversized DenseV1 candidate stays DenseV1, while a stored ColumnarV2 object
above the limit fails closed. Every local and remote reader must receive the
same bound before conversion. Operators must not lower or remove it until
the stopped-state preflight proves every stored payload fits the replacement
bound or authoritative verification finds zero ColumnarV2 objects.

If a mistaken configuration leaves a stored payload above the loaded bound,
the fleet remains fail-closed. Break-glass recovery first stops every reader and
writer, uses the non-expanding range validator to establish the required
cardinality, installs one reviewed higher bound on the entire v2 fleet, and only
then restarts decoding. The operator may instead use the repair helper with a
complete validated DenseV1 backup. Old- and new-bound readers never overlap.

### Transition preflight

Activation uses a checked-in runbook and operator-side, non-resident preflight
command, not a second controller or coordination API. Its operator-supplied
inventory names every managed cluster and scope, every local or remote IR
reader/writer, the approved v2 image and configuration digests, and every
restartable workload template or launcher—including rollback, autoscaling,
scheduled, GitOps, and external paths. The operator attests completeness;
Kubernetes discovery cannot find an unlisted external credential or script.

The preflight performs direct, uncached, paginated reads and emits an auditable
go/no-go report with inventory digest, scopes, image/configuration digests, page
and object counts, and observed representation counts. It fails on an
unreachable cluster, failed page, unapproved image, running manager or status
writer during the stop window, stale restart path, configuration mismatch, or
inventory drift. This proves the barrier only for the declared inventory.
Activation still depends on the operator's completeness attestation; an unknown
external client remains a residual risk that the command cannot disprove. A
separate checked-in source inventory rejects in-tree read/write bypasses. A
known-consumer inventory covers scripts, JSONPath queries, policies, exporters,
and full-status writers; every entry has an owner and readiness signoff.

### Upgrade and downgrade

OEP-0007.4 shipped no marker or compatibility bridge. A DenseV1-only manager
therefore reads a ColumnarV2 object as an empty `instanceStatuses` list and can
make a destructive decision before an API rejection protects the object. This
applies to remote readers such as placement as well as the local IR writer.

DenseV1 target mode is the pre-activation bridge. Existing and v2-capable
managers may overlap only while every writer emits the current dense format:

1. Install the additive CRD schema for `instanceStatusEncoding` and the fully
   typed `instanceStatusColumns` in every served cluster; keep the DenseV1
   schema.
2. Install explicit `DenseV1` and `maxDecodedInstances` configuration for every
   local and remote role that reads or writes IR status.
3. Roll every reader and writer—including workload-cluster managers and
   control-plane placement readers—to the approved v2 image digest. Old and new
   processes may overlap in this step because every writer remains DenseV1.
4. Freeze cluster/scope membership, reader/writer workload specifications,
   autoscaling, and rollback automation. Delete or disable retained pre-v2
   templates and update every launcher or rollback path to the approved v2
   image. Run the preflight to prove the declared-inventory reader barrier,
   compatible schemas, known-consumer signoff, and zero column payloads.
5. Scale every OME manager and every other IR status writer in the managed fleet
   to zero and stop every non-manager status-writer process. **All** OME
   controllers hosted by those managers stop; matching `failurePolicy: Fail`
   admission requests are rejected, and no OME healing or rollout proceeds.
   Unmatched API operations and existing serving traffic continue. Approved
   v2-capable read-only non-manager consumers may remain. The preflight must
   observe zero managers and status writers before configuration changes.
6. Keep OME manager Deployment replicas and any manager autoscalers pinned at
   zero while applying the ColumnarV2 configuration and restart templates for
   every stopped reader and writer. Serving ISVC/IR desired replicas and Pods
   remain untouched. Run the stopped-state preflight again; only after every
   restart path resolves to the approved v2 image, target, and bound may
   operators restore manager replicas. The runbook and drill must prove that
   manager Pods remain admissible after the fleet-wide OME control-plane outage.
7. Initial informer population enqueues every IR and converts each eligible
   object in one atomic status update. Run the inventory-wide preflight again,
   then release the freeze.

Any membership drift, unreachable cluster, failed page, remaining old process,
or failed restart before step 6 restores replicas aborts safely on the DenseV1
bridge. Starting the first ColumnarV2-target process is the activation commit
point because it may immediately persist columns. A failure after that point
keeps the freeze in place: stop every partially started writer, then either fix
and resume the approved v2 ColumnarV2 fleet or start the v2 fleet in DenseV1
target mode, reverse-convert every object, and prove global zero columns before
any DenseV1-only binary starts. Here and below, “global” means every scope in
the attested managed-fleet inventory. No binary can infer that an unaware
reader is absent merely by reading an IR; the global stop is the compatibility
barrier.
The marker and payload still change atomically, so a crash leaves one complete
representation.

Downgrade to an OEP-0007.4-compatible DenseV1-only binary reverses that process:

1. Freeze the same inventory, pin OME manager Deployment replicas and any
   manager autoscalers at zero, globally stop the v2 fleet, and require a
   stopped-state preflight. Serving ISVC/IR desired replicas and Pods remain
   untouched.
2. While the fleet remains stopped, apply DenseV1-target templates for the same
   v2 image. Re-run the stopped-state preflight, then restore replicas.
3. Let initial informer population rewrite every ColumnarV2 object, including
   logical no-ops. Direct paginated LISTs must then prove zero markers and column
   payloads in every managed scope.
4. Stop the fleet again and require another stopped-state preflight. Apply every
   DenseV1-only restart template while manager replicas remain zero, verify the
   image and all-dense object set, then restore manager replicas.
5. Reverify the all-dense fleet and release the freeze. Remove the additive CRD
   fields only in a later schema rollout.

Any unreachable cluster, failed page, residual column, reappearing ColumnarV2
writer, or failed rewrite blocks downgrade. DenseV1 re-expansion may receive the
same size rejection that V2 avoids. In that case, globally stop the failed
Dense-target fleet and restore the last approved ColumnarV2 configuration; stay
on a v2-capable binary. This proposal does not promise rollback when the old
representation no longer fits. Accordingly, `disable-supported: false` means
the OEP does not promise unconditional return to a DenseV1-only binary;
successful reverse conversion is the required evidence for a particular fleet.

Changing target encoding through an ordinary rolling update is forbidden:
overlapping DenseV1-target and ColumnarV2-target writers can reverse and forward
convert the same object indefinitely. Operators that cannot accept the complete
OME control-plane pause remain on DenseV1.

### Raw API contract

ColumnarV2 intentionally changes raw IR YAML: `instanceStatuses` is absent and
`instanceStatusColumns` is present. A client that indexes the dense list is not
compatible with ColumnarV2.

This is a new, explicit compatibility decision. The current API describes
`instanceStatuses` as per-Instance state; opt-in ColumnarV2 breaks direct
readers of that field. Before activation, an executable repository inventory
and a documented known-consumer audit must move every OME reader to the logical
codec boundary. API comments are updated to describe the representation union.

V2 provides no reconstructed status copy or new public diagnostic endpoint.
Either would duplicate the O(N) rows this OEP removes. Any incompatible client
in a manager fleet's visible IR scope blocks ColumnarV2 activation for that
whole scope. Only physically disjoint reader/writer fleets with independent
configuration may choose different targets. Otherwise the client must implement
the ColumnarV2 wire contract. Generated clients preserve the fields but do not
decode them. Release notes treat this as an opt-in raw status API break, not as
an invisible implementation detail.

The generated full CRD and its chart copy carry the typed representation union;
generated Go deepcopy/OpenAPI and checked-in Rust client types carry both
optional fields. Client serialization round-trips their wire shape without
reconstructing dense rows. The generated minimal CRD intentionally preserves an
untyped status and therefore fails the v2 schema preflight; it cannot host a
v2-capable manager. The OME codec rejects an unknown marker and never guesses
from whichever payload happens to be nonempty. Release notes and the activation
runbook call out this raw API change explicitly.

The checked-in Rust IR type already omits the existing `retryBlocks` and
`migrations` status fields. Because V2 must regenerate that type, the
API artifact PR reports this pre-existing generated correction separately
from ColumnarV2 fields, pins the Rust generator version, and adds generation
drift plus shared-wire-vector serde tests. It must not hide those missing fields
or present their generated churn as V2 production logic.

Malformed stored payloads, unknown markers, and decode-limit violations fail
before effects. Conflicts restart the complete cycle from a fresh object. An
API rejection leaves the previously committed status unchanged and returns the
write error. Error messages are bounded, and metrics never contain payloads or
IR identity labels.

## Test Plan

The test volume is intentionally larger than the production codec. Tests are
organized by invariant so reviewers can distinguish mechanics from lifecycle
coverage.

### Codec tests

| Invariant | Required cases |
| --- | --- |
| Lossless round trip | Every retained Instance field; nil/empty API normalization; all phases and operations; Conditions, timestamps, pointers, failures, full revision names, sparse indices. Arbitrary values in the three OEP-0007.4-omitted fields are cleared identically before both candidates. |
| Canonical encoding | Equal logical rows in the same row order encode identically across randomized map traversal; values and entries sort by the defined order; adjacent ranges merge; repeated encode/decode is stable. |
| Canonical wire rejection | Shuffled or duplicate group values/entries; unsorted or adjacent index terms; explicit empty representation-owned pointer/slice fields; redundant ascending `rowOrder`; zero incarnation groups; empty revision values; nonpositive count groups; and entries whose only exceptional slice is empty all fail rather than gaining a second accepted spelling. Typed decoding and real-apiserver fixtures distinguish absent from explicit empty; reused nested records retain V1 normalization. |
| Row order | Ascending rows omit `rowOrder`; every nonascending permutation round-trips exactly and preserves escalation/Event ordering. |
| Range validation | Valid singleton terms; rejected empty, whitespace, leading-zero, signed, descending, and degenerate (`n-n`) ranges; overlap, adjacency, duplicates, trailing commas, int32 index boundary, arithmetic overflow, and out-of-member references. |
| Typed values | Int32 count boundaries, int64 incarnation boundaries, phase enum values, full revision strings, and ActiveOrdinal 0/1. |
| Coverage | Missing or duplicate Phase, missing member, extra group member, duplicate entry, empty entry, and conflicting dense/columnar payload. |
| Defaults | Zero incarnation/counts/ordinal, false admission, empty revisions, and nil exceptional fields decode exactly as DenseV1. |
| Bounded decode | Huge compact ranges, configured-limit boundary, limit-minus-one/plus-one, and proof that no row-proportional or expanded allocation or index enumeration occurs before cardinality validation; typed wire-object deserialization is measured separately. |
| Determinism | Property tests and fuzzing over valid logical rows; fuzzing malformed bytes must return an error or a bounded decoded value, never panic or hang. |

Golden tests cover the human-readable YAML example and the exact JSON used for
size attribution. Property tests compare API-semantic logical equality rather
than Go pointer identity. Presubmit generates at least 50,000 deterministic
documents containing at least 1,000,000 aggregate logical rows. A
reflection/schema coverage test fails whenever a future per-Instance JSON field
lacks an explicit codec disposition. Range parsing, column decoding, and full
round trips each receive at least 60 seconds of ordinary PR fuzzing. The
implementation qualification runs each target for ten minutes and retains its
artifacts; every discovered regression input joins the checked-in corpus. The
repository also checks in the ten-minute scheduled entry point. Activating the
external nightly schedule and recording its first clean run is Beta evidence,
because a repository commit cannot prove scheduler state.
The deterministic floors exercise hundreds of 5,000-row-equivalent volumes
while varying sparse and grouped fields; they are regression breadth, not a
statistical safety claim. Sixty-second fuzz targets keep PR latency bounded,
while the longer nightly budget searches deeper without delaying reviews.

### Behavior-parity tests

Run OEP-0007.4's complete decision-parity matrix against DenseV1 and
ColumnarV2 using the same logical rows and object snapshots. Plans, effects,
aggregates, Conditions, Events, coordination, placement, revision retention,
and scale-subresource output must match. Cover zero through 5,000 Instances,
single-Pod and gang shapes, sparse and transitional rows, every operation, and
EndpointSlice lead/lag.

Coordination coverage explicitly exercises RatioBalanced and Sequential through
their production decoded-accessor call sites; testing the codec alone does not
cover those readers.

The OEP-0007.4 prior-epoch cases remain mandatory. In particular, publish a
complete gang, remove one member before Ready, restart the manager, and assert
that all formats choose the same Restart. Repeat with the first Restart status
write failing before deferred publication. Also remove an admitted row while
publication fails and prove placement still observes the same admitted count.

### Persistence tests

Every IR status mutation family receives table-driven tests in all readable
formats:

- single and batched Instance mutation;
- RetryBlock, migration accept/import/prune/phase mutation, and
  automatic-relocation outcome;
- collision-count and current-revision promotion;
- aggregate and Condition publication;
- terminal row removal and teardown.

Each test asserts fresh decode on conflict retry, one committed representation,
no field loss, OnCommit state equal to the decoded API-confirmed response, and
zero write when the logical status is unchanged. Dedicated tests force
columnar-smaller, dense-smaller, and equal-size
selection. Repeated reconciles must not alternate formats or generate a status
write storm. The sole unchanged-logical-status exception is an explicitly
requested conversion at the start-of-reconcile transition gate. The executable
inventory defined under Status ownership runs with this matrix.

The conflict test uses a real API server and a deliberately stale informer
cache: the first update receives 409, and only an uncached live GET supplies the
resourceVersion and status used by the second attempt.

At 2,000 and 5,000 rows, a real-apiserver fixture first creates DenseV1
list-map ownership, then forces a 409 during conversion. The live retry must
strip managed fields from its outgoing candidate, remain within the measured
request size, commit successfully, and preserve the API server's ownership
semantics on the following GET.

The write-count contract is exact: initial conversion commits once; one logical
mutation commits once; an injected conflict makes two attempts and one commit;
100 unchanged reconciles after conversion make zero attempts; and 1,000
encode/decode cycles keep identical canonical bytes. The repeated no-op and
round-trip counts are sentinels for accumulated write amplification or
canonical drift; one mismatch fails, so the numbers are not probabilistic
thresholds.

Fault injection covers 409 conflict, 413 rejection, 429 throttling, 5xx,
context cancellation, client timeout, and a crash immediately before and after
the status commit. No failure may authorize an unintended Pod/PodGroup create,
patch, drain, delete, placement win, or revision sweep.

### Compatibility tests

Use a real API server where structural-schema pruning matters. Each rollout
invariant has an injected failure and required evidence:

| Area | Required evidence |
| --- | --- |
| Schema and config | Unmarked DenseV1 and valid ColumnarV2 decode; the full-schema OpenAPI preflight succeeds in both target modes; binary-first, forbidden discovery, old/minimal/incompatible schema, invalid union, missing target, and invalid bound fail closed; the chart renders DenseV1. |
| Reader barrier | DenseV1-only and v2-capable managers may overlap only on DenseV1; any declared incompatible reader or stale restart template blocks activation, including a retained template that can resurrect the old binary. |
| Target stop | A target change remains blocked until every declared manager and writer is stopped and every restart path has the approved image and configuration digests. |
| Bound change | Rolling, missing, lowered, and undersized bounds fail; a stopped fleet accepts a reviewed bound only when every stored cardinality fits, with validated-backup repair as the alternative. |
| Frozen apply | Membership or workload drift invalidates evidence; manager replicas remain zero through configuration, serving ISVC/IR Pods remain untouched, the admission outage is observed, and manager restart succeeds. |
| Partial activation | Failure after the first column commit keeps old binaries stopped and proves both v2 resume and v2-driven reverse conversion; crashes before and after either conversion leave one complete representation. |
| Downgrade | Direct paginated reads prove zero columns; 413, an unreachable cluster, a failed page, a residual object, or a reappearing ColumnarV2 writer blocks old-binary startup without changing the prior authoritative status. |

The generated full CRD, chart copy, Go deepcopy/OpenAPI, and Rust client must
match the artifact matrix above, while the regenerated minimal CRD must remain
incompatible with and rejected by the schema preflight. Generation drift must
be clean. Rust serde tests share the Go codec's golden wire fixtures. A
previous-version fixture proves why mixed activation is forbidden: the old
binary sees no dense rows. The operational test passes only when no V2-unaware
process runs after ColumnarV2 configuration is installed; an API rejection
after an effect is not protection.

### Size and performance tests

Controller-runtime's status update serializes the whole IR, including
`spec.runners`; therefore measure complete serialized status-update requests—not
only the column payload—at 0, 1, 100, 1,000, 2,000, and 5,000 Instances for:

- uniform steady singletons and eight-Pod gangs, each with both the minimal
  measurement envelope and a checked-in neutral production-shaped IR spec with
  containers, resources, environment, volumes, affinity, and security context;
- mixed revisions, incarnations, counters, admission, and sparse indices;
- create, in-place update, surge, migration, delete, and gated states;
- 0, 13, and all-Instance LastFailure records;
- few and all-Instance Operations; and
- best-case, representative, and deliberately adversarial grouping.

Record bytes by field family and selected representation alongside CPU,
allocations, peak resident-memory delta, and status-write latency. The
measurement artifacts have distinct purposes:

| Artifact | Required contents |
| --- | --- |
| Local candidates | Complete DenseV1 and ColumnarV2 requests for every fixture, chosen format, and the constant spec/metadata envelope reported separately. |
| Raw API result | Same-server unnormalized GET including managedFields, or the rejected request size plus proof that the prior status remained unchanged. No rejected value gets a fabricated GET. |
| `status_size_v2.csv` | Drift-checked deterministic request, acceptance result, and normalized GET when one exists. Only variable UID, resourceVersion, and timestamp values become fixed-length placeholders; all fields and managed paths remain. |

Raw GET bytes and the absolute 256-KiB assertion stay in the CI artifact because
they are not stable golden data. The CSV is generated by
`make test-ir-status-size-v2` from the deterministic V1 fixture builder.

At 2,000 and 5,000 rows, also seed the accepted DenseV1 fixture and let the real
manager perform the normal informer-driven conversion. Measure that actual
status request and persisted result; direct fixture writes alone do not prove
the production adapter can convert the object.

The release gates are deliberately stricter than “the API accepted it”:

- the steady singleton and gang-8 Instance representation at 2,000 and 5,000
  Instances is at most 10% of the DenseV1 Instance representation;
- the representative mixed-revision Instance representation with 13 sparse
  failures or operations is at most 25% of DenseV1 at 5,000 Instances;
- for both minimal and production-shaped specs, the complete selected requests
  and persisted GETs for those steady and representative fixtures are at most
  50% of the equivalent accepted DenseV1 object and at most 256 KiB; when the
  DenseV1 object is rejected, only the absolute gate applies and the rejection
  is recorded;
- every adversarial selection equals the smaller complete candidate, with a
  tie selecting DenseV1; and
- no selected representation is larger than the equivalent DenseV1 request.

The 50% complete-object gate defines “material” despite irreducible spec and
metadata bytes; the 10% and 25% gates isolate the repeated status this OEP owns.
The 256-KiB threshold is release-test policy—25% of a conservative 1-MiB
qualification envelope—not a controller constant or API-limit claim. A lossless
format cannot bound 5,000 unique multi-kilobyte failure messages; the
all-Instance failure and operation fixtures therefore enforce no-regression
fallback rather than the 256-KiB steady-state gate.
Persisted ratios use unnormalized DenseV1 and ColumnarV2 objects from the same
API-server run. If DenseV1 is rejected, the CSV records that result and uses the
local Instance-representation denominator plus the absolute persisted-ColumnarV2
gate. Sparse fixtures use stable reviewed golden inputs; changing them requires
explicit size-review approval.

Benchmarks must prove bounded behavior at `maxDecodedInstances` and on a
one-range decompression-bomb input. The DenseV1 encode baseline starts with the
same logical persistence candidate, runs the common V1 storage normalizer,
constructs one complete dense status-update request, and serializes it. The
ColumnarV2 encode measurement starts from that same candidate, constructs both
complete requests, selects one, and serializes the selected request. The
DenseV1 decode baseline deserializes a complete dense API object and records its
source encoding; the ColumnarV2 measurement deserializes a complete columnar
object, validates it, and expands rows on that freshly fetched object.

The chart-default DenseV1 path has a hard-fail before/after qualification step;
the existing soft-fail small-workload performance lane is not evidence for this
gate. A new runner builds the recorded main merge-base and capability-head
manager images, then drives identical accepted 2,000- and 5,000-row DenseV1
inputs through fresh managers on the same stable worker. The artifact records
commit and image digests. Ten interleaved samples compare median process CPU and
allocation deltas, status-write latency, and peak resident-memory delta;
attempted and committed write counts must match exactly. For each sample, an
external runner creates one fresh isolated cgroup v2 container, remains outside
that cgroup, and runs exactly one manager or benchmark process with no sibling
work. After the workload reaches quiescence, the child waits at a completion
barrier while the runner reads `memory.peak` once; only then may it terminate the
child and remove the cgroup. A matching idle-control sample uses the identical
cgroup and process lifecycle. This adds no scrape or probe load to the measured
process and cannot inherit a sibling or prior sample's peak. The same mechanism
is mandatory for merge-base, head, DenseV1, and ColumnarV2; a worker without
cgroup v2 cannot run this gate. The artifact retains idle, workload, and
subtracted values; a nonpositive workload delta invalidates the sample instead
of becoming zero.

The head DenseV1 medians may increase by at most 25%. This allowance covers the
codec reader and source-provenance check without accepting a default-mode
regression hidden by Columnar savings. Changing the protocol or ceiling requires
OEP review, not a softer test.

At 2,000 and 5,000 rows, column build, candidate selection, and serialization
use no more than 3x the DenseV1 median time or bytes allocated; decode uses no
more than 2x; peak resident-memory delta is no more than 3x. The encode and
memory ceilings allow the normalized logical value and two candidate
representations to coexist; the decode ceiling allows the wire value and
expanded logical rows to coexist during validation. Codec memory uses the same
external baseline-subtracted process measurement around repeated operations.
From 2,000 to 5,000 rows, each measure grows no more than 3.25x: the row count
grows 2.5x and the remaining allowance covers sorting and allocator variance
without accepting superlinear growth. Ten interleaved samples compare medians
so machine speed does not become the gate. The qualification PR records the raw
benchmark results; changing a ceiling requires OEP review rather than weakening
a test to admit the implementation.

### KIND tests

Run every inventory-discovered KIND spec twice: once with a DenseV1 target and
once with a ColumnarV2 target. Target mode alone is not coverage because strict
fallback may still store DenseV1 for a small workload. Each case records every
persisted representation it reads and writes and declares its expected
representation coverage. `N/A` is allowed only when the case creates no IR,
retains zero rows, or proves a strict smaller-candidate DenseV1 fallback. The
suite contains named actual-ColumnarV2 paths for every lifecycle and
status-mutation family: seed enough rows or a validated column payload, restart
the manager, and prove the subsequent decision and write. A named actual-column
path with no column read is a failure. Representation events are attributed to
one case ID and reset and validated within that case. Every required
actual-column case itself records both a ColumnarV2 write and a post-restart
read, so its result proves coverage without depending on a cross-job summary.

Keep 2,000- and 5,000-Instance synthetic tests in unit or envtest; KIND is for
kubelet and EndpointSlice timing, not thousands of real Pods per presubmit.

Steady reconciliation adds no object observation. Instrumented tests assert no
increase in Pod or EndpointSlice LIST/watch counts relative to DenseV1. The
authoritative IR LIST used by the transition preflight is an explicit
administrative operation, not part of reconciliation.

Run every YAML/Python OMENative harness case—343 at proposal time—once per target
mode from the same commit and images. Assignment, unique case IDs, and executed
case IDs must match the repository inventory exactly. The ColumnarV2-target run
records actual representation reads/writes and includes a named subset that
persists columns, restarts the manager, and reconciles from them. No case may
produce FAIL, INFRA-FAIL, SKIP, a crash, or a missing result. The parked ledger
is exact: three cases are declared `infeasible`, and one is declared
`notimpl` for a known timing nondeterminism in its scenario. Each parked case
may PASS or be softened from its expected failure to XFAIL under the existing
harness rule, but its status assertions must still execute and any XFAIL must
retain the declared reason. Every other case must PASS in both modes, and no new
parked case or XFAIL is allowed.

This requires two harness changes before the capability PR may merge.
First, the Python status reader must decode DenseV1 and ColumnarV2 from shared
golden vectors and must fail a case when an expected IR-status assertion was
not evaluated; an
empty dense list may never turn a status check into a vacuous PASS. Second, PR
CI must package exact SHA/digest-pinned branch images without overwriting stable
tags and run the full harness rather than relying on the current main-only image
path.

Every shard requires exactly one terminal result for each assigned case ID and
rejects missing, duplicate, or extra results; SKIP, FAIL, INFRA-FAIL, unexpected
XFAIL or XPASS, crashes, and cancellations; and unmet parked-case or
representation expectations. Before upload, the generator validates the
concrete emitted manifest against repository inventory and rejects missing or
duplicate, or extra `(target, case-ID)` assignments. Generation tests
independently pin the same invariant. Every emitted shard is an unconditional,
required, non-soft-fail command step, so the build cannot pass unless every
shard exits zero. The shard-local verifier runs before artifact upload, and
upload handling cannot mask its nonzero status. These checks compose into exact
matrix coverage; a cross-job artifact collector is not required for
correctness. Per-shard result files remain useful diagnostics, but artifact
upload is not a prerequisite for the pass/fail result.

At proposal time, one target uses 26 KIND jobs and 62 harness jobs; naively
doubling both creates 176 clusters requesting up to 880 CPUs and roughly 2.4 TiB
of memory. The qualification pipeline therefore shards by measured duration and
may run target modes sequentially on a reused clean cluster, but it may not drop
or duplicate a `(target, case-ID)` pair. The full dual-target matrix is mandatory
before the capability PR may merge and for the three Beta main runs.
Ordinary unrelated PRs may path-gate it; permanent cheap codec, inventory, size,
and focused actual-column gates remain presubmit, with full dual coverage
scheduled.

## Implementation Sequence

Implementation is delivered as a small dependency-ordered stack. Each PR owns
one reviewable invariant and leaves the cluster in a valid state:

1. Add only the API wire fields and generated artifacts. Schema availability is
   not activation: until the reader barrier is complete, no shipped binary,
   repair command, non-qualification fixture, or documented operator path may
   persist ColumnarV2.
2. Add the bounded index-set primitive and codec as a library with no production
   writer call site.
3. Route every local and remote production IR reader through the codec; add the
   shared `maxDecodedInstances` configuration and schema/startup preflight; and
   prove the complete consumer inventory. DenseV1 remains the only supported
   production write target.
4. Route the existing status writer through the new boundary while it remains
   DenseV1-only. Add the read-only transition preflight and operational
   documentation. Any repair capability in this slice is restricted to a
   validated DenseV1 replacement.
5. Add or extend qualification independently: real-apiserver fault tests, size
   and performance gates, dual-target KIND, and the complete dual-target
   YAML/Python harness.
6. The final capability PR enables config-controlled ColumnarV2 candidate
   selection at the existing writer boundary, adds the ColumnarV2 configuration
   value, and adds the complete repair entry point. CI may select ColumnarV2
   only in isolated qualification clusters. The exact head commit and image
   digest must pass every Alpha gate before merge. The shipped chart remains
   DenseV1; production activation occurs only through the stopped-fleet
   transition.

A slice may be divided further when generated code, test infrastructure, or
operational tooling would obscure its production change. No intermediate PR
may write ColumnarV2 before all production readers can decode it, and no test
infrastructure PR may silently change controller behavior. Every PR description
reports hand-written production, generated, operational-tooling, fixture, and
test lines separately. Hand-written production remains limited to API types,
codec/configuration, metrics, and the existing IR read/write boundary; a new
controller, resource, or persistence path requires another OEP amendment.

## Graduation Criteria

**Alpha** requires every implementation-qualification obligation in the Test
Plan: codec and field parity, ten-minute fuzz runs, actual-column accounting,
dual-target KIND and harness runs, 2,000/5,000 size and performance gates,
real-apiserver faults, transition and repair tooling, consumer signoff, and both
successful and 413-blocked downgrade drills. Passing Alpha permits a controlled
canary activation; merge or install alone remains DenseV1.

**Beta** requires three consecutive authoritative main runs with no behavioral
test retry, no codec errors, no representation oscillation, no added object
observation, and exact harness inventory and representation accounting. Three
fresh runs expose environment-to-environment flakiness before wider rollout;
they are not a substitute for the canary. The controlled canary must also
complete the verified activation procedure, and the external ten-minute nightly
fuzz schedule must have a recorded clean run. Here, oscillation means an
encoding change without a logical status mutation or target change;
deterministic fallback may change encoding as part of a real mutation.

**Stable** requires a canary soak lasting at least seven consecutive days, long
enough to cross daily automation and repeated manager restarts under normal
traffic. It includes an IR with at least 2,000 Instances and exercises scale,
rollout, restart, migration, delete, manager restart, and one verified
successful downgrade. Every scripted operation must reach the same externally
asserted end state as its DenseV1 qualification case. During the soak,
codec-error and noncanonical-input counters remain zero. Outside the explicitly
bounded activation and downgrade windows, representation-only write attempts
remain zero; every encoding switch is coupled to a logical mutation and chooses
the deterministically smaller request; each successful logical mutation records
no more than one committed status write. A separate oversized fixture must
prove that a 413 blocks downgrade without changing the prior status or
authorizing effects. The binary still receives no target-encoding or
decode-limit default.

## Drawbacks

1. A codec is more complex than a dense list and becomes part of crash
   recovery. Canonical validation and property testing are mandatory.
2. Raw IR YAML is less direct. Operators that require dense per-Instance JSON
   must remain on DenseV1 or decode the columnar representation.
3. Every write performs a bounded O(N log N) worst-case encode, and v2 mode may
   serialize two candidates. Measurements must show that saved API/etcd bytes
   justify the CPU cost.
4. Pathological unique values fall back to O(N) DenseV1. V2 removes common
   repetition; it does not claim constant size for arbitrary state.
5. Unbounded diagnostic strings or migration records can still exhaust the
   object budget independently.

## Alternatives

1. **Stop at DenseV1.** Simpler, but 5,000 healthy rows still repeat roughly
   0.8 MiB in the modeled per-Instance section.
2. **Store a compressed opaque blob.** Rejected: it defeats structural schema,
   field visibility, and ordinary Kubernetes tooling, while making every read
   depend on binary decompression.
3. **Use maps keyed by decimal index.** Better than repeated row keys, but it
   still repeats thousands of index strings and common values; ranges compress
   the steady shape more effectively.
4. **Always write columns.** Rejected: adversarial or highly unique state can be
   larger than DenseV1. Deterministic dense fallback is safer.
5. **Duplicate a reconstructed dense diagnostic view.** Rejected: another CRD,
   status field, or endpoint would either restore O(N) persisted rows or create
   a second API and ownership contract solely to hide the opt-in raw API change.
6. **Add a zero-control-plane-downtime `Preserve` mode.** Rejected for this
   proposal: DenseV1 target mode safely bridges the binary rollout, and a
   non-overlapping maintenance window prevents format oscillation with less
   state and fewer rollback paths. Deployments that cannot accept the documented
   OME control-plane outage remain on DenseV1.

## Next Action

After OEP approval, the implementation stack must satisfy every Alpha and
presubmit gate before a controlled canary selects ColumnarV2. Beta gates
precede wider rollout; Stable follows the seven-day canary and downgrade drill.
