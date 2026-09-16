# OMENative InferenceReplica status representation

An OMENative InferenceReplica (IR) stores one status row per Instance. The
manager can store those rows in two representations on the wire:

| Representation | Wire shape | Marker |
| --- | --- | --- |
| `DenseV1` | `status.instanceStatuses`: one object per Instance | `status.instanceStatusEncoding` absent |
| `ColumnarV2` | `status.instanceStatusColumns`: grouped columns over canonical index sets | `status.instanceStatusEncoding: ColumnarV2` |

Both decode to the same logical rows; every controller reads and mutates the
decoded rows and only the single status writer touches the wire form. A
committed object carries exactly one representation. This page covers what an
operator owns: the configuration block, the rollout order, the downgrade rule,
the metrics and events the writer emits, and the read-only preflight command.

## The configuration block

The manager reads the `omenativeStatus` block of the `inferenceservice-config`
ConfigMap once at startup. The block is required; a manager with a missing,
unknown, or malformed value refuses to start rather than guessing.

```yaml
omenativeStatus:
  instanceStatusEncoding: ColumnarV2   # write target: ColumnarV2 or DenseV1
  maxDecodedInstances: 20000           # ColumnarV2 decode bound; see below
```

| Key | Meaning | Rules |
| --- | --- | --- |
| `instanceStatusEncoding` | The representation the status writer targets. The writer converts each IR it reconciles to the target once and then keeps it there; it never reinterprets what is already stored. | Required. `ColumnarV2`, the shipped default, writes columns only when they are strictly smaller than the dense list and writes `DenseV1` otherwise. `DenseV1` always writes the dense list; it is retained as the rollback target. |
| `maxDecodedInstances` | The largest number of rows a stored `ColumnarV2` payload may expand to. It is the deployment profile's supported ceiling of transient rows per IR, including surge, migration, replacement, and retained terminal rows. | Positive integer. Required under `ColumnarV2`. May be omitted under `DenseV1` only when no `ColumnarV2` object exists; a `DenseV1` manager that meets columns without a bound fails that IR closed. Never lower it below the largest stored `ColumnarV2` row count; the preflight reports that number. |

The Helm chart renders the block from `ome.controller.omenativeStatus` and
ships `ColumnarV2` with an explicit bound (`20000` in the chart's values: four
times the largest fixture this repository qualifies the codec against, which
covers a full surge of that fixture with room for retained rows); the kustomize
ConfigMap ships the same values. Change the block only through the versioned
deployment path, never by editing the ConfigMap in place: the manager loads it
once, so an in-place edit takes effect at an unplanned restart. Flipping the
target is the rollback procedure below; raise the bound before any IR can
reach it, and never lower it while a stored payload exceeds the new value.

## Rollout order: schema first

Every representation-aware manager verifies the served InferenceReplica
schema through OpenAPI discovery before it registers any controller or
webhook, even under a `DenseV1` target. A cluster whose CRD lacks the typed
`instanceStatusColumns` schema and the representation-union validation rules
cannot host such a manager, and the manager exits at startup saying so.

Roll out in this order:

1. Install the CRD that carries `instanceStatusEncoding` and the typed
   `instanceStatusColumns` (the `ome-crd` chart) in every served cluster. A
   manager that starts before the schema is served exits at startup, so this
   order is enforced, not assumed.
2. Roll the manager with the shipped block, `ColumnarV2` with the chart's
   bound. The chart runs the manager with leader election, so one manager
   reconciles at a time; as the new manager visits each IR it rewrites the
   object as `ColumnarV2` when the columns are strictly smaller (recording
   `InstanceStatusConverted`) and leaves it dense otherwise. From that moment
   the build that predates the representation union must never run against
   these objects again: roll back only through the rollback rule below.

Two managers with different targets must never run against the same IRs; they
would convert the same object back and forth indefinitely. A fleet with
readers of the raw `instanceStatuses` field outside the manager, such as a
control-plane placement reader on an older build, scripts, or exporters, should
still take the stopped-fleet path: install the block as `DenseV1` first, move
every reader to a representation-aware build or to the decoded view, prove it
with the preflight, stop every manager and status writer, flip the target,
re-run the preflight, and only then restore managers. The preflight is the
operator's evidence either way.

## Rollback rule

Set the target to `DenseV1`, roll the same manager image, and wait for every
object to convert **before** rolling the manager image back to a build that
predates the representation union.

With a `DenseV1` target, a representation-aware manager converts every IR it
finds stored as `ColumnarV2` at the start of that IR's reconcile: it rewrites
the object as `DenseV1` once, with its logical rows unchanged and no lifecycle
effect on that pass, records `InstanceStatusConverted`, and requeues. Objects
already stored as `DenseV1` are untouched. Completion is visible twice: the
`ome_omenative_ir_status_conversions_total{from="columnar_v2",to="dense_v1"}`
counter stops rising once initial informer population has visited every IR,
and the preflight reports zero `columnarV2` objects in every cluster. The
preflight, not the counter, is the proof: only a complete paginated list can
show that no `ColumnarV2` object remains.

`DenseV1` stays in the binary as this rollback target. Removing it is a later
milestone that first requires every consumer of the raw field, in this
repository and in every fleet's inventory, to move to the decoded view; until
then a `DenseV1` target must remain selectable.

Why the order matters: a manager that predates the representation union knows
nothing about `instanceStatusColumns`. It reads a `ColumnarV2` object as an IR
with an **empty** `instanceStatuses` list, that is, as an IR with no Instances
at all. It will then plan from that view: create Pods for Instances that
already exist, treat admitted rows as absent, and drop lifecycle state that
the columns still hold. The API server only protects the object once that
manager tries to write its dense rows back next to the columns, and by then
Pods may already have been created or deleted. The same applies to any
remote reader of IR status, such as placement. The global stop is the
compatibility barrier; no binary can infer that an unaware reader is absent
by looking at an IR.

Two more cautions:

- A reverse conversion re-expands the object. An IR whose dense form no
  longer fits the API request limit is rejected with `StatusSizeExceeded`
  (below), and that IR cannot be converted back. Stay on a
  representation-aware build in that case.
- A `ColumnarV2` object above the configured `maxDecodedInstances` fails
  closed on every manager. Never lower the bound below the largest stored
  cardinality; the preflight reports that number.

## Reading the metrics and events

The single status writer emits four metrics for both representations alike.
Label values are a fixed vocabulary: encodings are `dense_v1` and
`columnar_v2`.

| Metric | Type | Labels | How to read it |
| --- | --- | --- | --- |
| `ome_omenative_ir_status_bytes` | gauge | `namespace`, `name`, `component`, `encoding` | Serialized status bytes of the most recent write attempt for one IR. Alert on it approaching the API request limit; an IR near the limit rejects every growing write. The series is removed when the IR is torn down. |
| `ome_omenative_ir_status_writes_total` | counter | `encoding`, `result` | Every write records `attempt` and then one of `committed`, `confirmed` (the update returned an error but a live read proved it committed), `conflict`, `rejected` (too large), or `error`. Outside a transition, `attempt` and `committed` grow together. |
| `ome_omenative_ir_status_codec_errors_total` | counter | `reason` | Stored representations the codec refused, by fixed reason (`unknown_encoding`, `representation_union`, `range_syntax`, `range_order`, `range_overflow`, `cardinality_limit`, `coverage`, `canonical_order`, `value_domain`). Any nonzero value names an IR whose reconciliation is suspended. |
| `ome_omenative_ir_status_conversions_total` | counter | `from`, `to` | Committed representation-only writes. It rises while a target change converts objects, after activation until every IR has been visited and during a rollback, and stays flat otherwise; a rising counter outside those windows means two managers with different targets are fighting over the same objects. |

Metrics never carry payload content. The write-boundary metrics (all but the
bytes gauge) carry no object identity either; use events to find the IR.

| Event | Type | On | Meaning |
| --- | --- | --- | --- |
| `StatusSizeExceeded` | Warning | the parent InferenceService (the IR when no parent resolves) | The API server rejected a status write as too large. The message names the IR and the encoded byte count. No lifecycle transition for that IR can be persisted until the status shrinks; the manager does not drop or compact anything on its own. |
| `InstanceStatusConverted` | Normal | the IR | The transition gate rewrote the stored representation. The message names the source and target representation and the row count. |
| `InstanceStatusDecodeFailed` | Warning | the IR | The stored representation cannot be decoded; the message carries the fixed reason. Reconciliation of that IR is suspended until the status is repaired. |

## The preflight command

`ome-status-preflight` is the read-only check to run before and after a
transition. Its root command never writes; the `repair` subcommand (below) is
the one explicit break-glass writer.

```sh
make ome-status-preflight              # builds bin/ome-status-preflight
bin/ome-status-preflight --inventory fleet.yaml
bin/ome-status-preflight --inventory fleet.yaml --json > preflight.json
```

The inventory is your attestation of the managed fleet. Every field is
explicit; the command supplies no defaults, and it never uses the ambient
kubeconfig or its current context.

```yaml
# fleet.yaml
managerImage: registry.example.com/ome/manager@sha256:<digest>   # exact reference every manager must run
omenativeStatus:                      # the block every cluster must have loaded
  instanceStatusEncoding: ColumnarV2
  maxDecodedInstances: 20000
manager:                              # where the manager and its configuration live
  namespace: ome
  deployment: ome-controller-manager
  container: manager
  configMap: inferenceservice-config
pageSize: 500                         # InferenceReplica list page size
clusters:
- name: prod-east
  kubeconfig: /etc/ome/prod-east.kubeconfig
  context: prod-east
- name: prod-west
  kubeconfig: /etc/ome/prod-west.kubeconfig
  context: prod-west
```

For each cluster the command reads, directly and uncached:

- the manager Deployment: the named container's image against `managerImage`,
  plus its replica counts, so the stop window can be confirmed by eye;
- the `omenativeStatus` block of the named ConfigMap, parsed with the
  manager's own strict parser and compared field by field;
- the served schema, through the same OpenAPI discovery check the manager
  runs at startup;
- every InferenceReplica, in pages of `pageSize`, classifying each stored
  representation with the manager's codec and decoding `ColumnarV2` payloads
  under the inventory's bound.

The report carries the SHA-256 digest of the inventory file, the expected
values, and per cluster: reachability, the manager image and replica counts,
the observed configuration, the schema result, the census (`pages`, `objects`,
`denseV1`, `columnarV2`, `columnarV2AboveBound`, `undecodable`,
`largestColumnarV2Rows`), and the no-go reasons. Reason kinds:

| Kind | Meaning |
| --- | --- |
| `unreachable` | The cluster, or one of its endpoints, could not be read at the transport level. |
| `failed_page` | One list page failed; the census is incomplete. |
| `unexpected_image` | The Deployment or container is missing, or runs an image other than `managerImage`. |
| `configuration_mismatch` | The block is missing, invalid, or differs from the expected values. |
| `stale_schema` | The served schema fails the startup schema check. |
| `columnar_v2_present` | `ColumnarV2` objects remain while the target is `DenseV1`; the detail lists them. |
| `columnar_v2_above_bound` | A `ColumnarV2` object has more rows than the expected bound allows. |
| `columnar_v2_undecodable` | A stored representation fails the codec for another reason. |

Exit status is 0 for go, 1 for no-go (the report is still written), and 2 for
a usage or inventory error. Use `--json` for the audit trail; the text form is
for reading at the terminal.

A `GO` proves the barrier only for the clusters and roles the inventory
declares. A reader that is not in the inventory, such as an external script
with its own credentials, cannot be found by the command; completeness of the
inventory remains the operator's attestation.

## Codec size and cost gates

The codec's unit tests pin the encoded-size gates on every run: the complete
selected status of the steady and representative fixtures stays under 256 KiB
and under half of the DenseV1 status, no selected representation is larger
than DenseV1, the mass-failure status stays below the etcd request limit, and
the selected status grows by no more than 3.25x from 2,000 to 5,000 rows. The
benchmark-backed encode, decode, and growth cost ceilings are enforced only
when the test process runs with `OME_CODEC_COST_GATES=strict`, as the
qualification lane does; any other run measures them once and reports the
ratios without gating, because a timing ratio on a shared worker is not
evidence either way.

## The repair command

`ome-status-preflight repair` is the break-glass writer for one IR whose
stored per-Instance representation must be replaced: a payload the codec
refuses (`InstanceStatusDecodeFailed` on the IR, a nonzero
`ome_omenative_ir_status_codec_errors_total`), or rows that must be restored
from a trusted backup. It is the only writer of IR status outside the manager,
it is dry-run by default, and it never runs the lifecycle engine.

```sh
bin/ome-status-preflight repair --inventory fleet.yaml --cluster prod-east \
  --namespace serving --name example-engine --replacement rows.yaml          # report only
bin/ome-status-preflight repair --inventory fleet.yaml --cluster prod-east \
  --namespace serving --name example-engine --replacement rows.yaml --apply  # one write
```

The replacement file holds only the rows, as a `DenseV1` list, and nothing
else:

```yaml
# rows.yaml
instanceStatuses:
- index: 0
  incarnation: 1
  phase: Ready
  runningRevision: example-engine-2f32f6fe
  podCount: 1
  servingPodCount: 1
  availablePodCount: 1
  admitted: true
```

What the command does, in order:

1. Loads the inventory, resolves the named cluster's kubeconfig context, and
   reads the cluster's `omenativeStatus` block with the manager's own parser.
   The block must equal the inventory's; the repair selects the representation
   with the target and bound the running manager uses and never guesses them.
2. Parses the replacement strictly (unknown or duplicate keys are errors) and
   validates it with the codec: a nonempty `instanceStatuses` list, no
   `instanceStatusEncoding`, no `instanceStatusColumns`, no other status
   field, and no more rows than `maxDecodedInstances`. A file carrying
   columns or a marker is refused; decode a `ColumnarV2` backup to rows first.
3. Reads the IR directly and does **not** decode the payload it is about to
   replace; only the `resourceVersion` and the status fields outside the
   per-Instance representation are used, and those fields are preserved as is.
4. Selects the representation the configured target selects for the
   replacement (`ColumnarV2` only when strictly smaller) and prints the stored
   encoding or its codec reason, the row counts, and the status size before and
   after.
5. With `--apply`, performs exactly one status write with the live
   `resourceVersion` as precondition. If anything, including the manager,
   wrote the object in between, the write is refused as a conflict and nothing
   changes; re-run from a fresh read.

Exit status is 0 on success (including a dry run), 1 when the repair is
refused (an invalid replacement, a configuration mismatch, a stale
`resourceVersion`), and 2 for a usage, inventory, or transport error. The
manager does not need to be stopped: its next reconcile of the repaired IR
decodes the new rows like any other object. The repair never clears rows,
never derives state from Pods, and never accepts part of a payload.
