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
  instanceStatusEncoding: DenseV1   # write target: DenseV1 or ColumnarV2
  maxDecodedInstances: 5000         # ColumnarV2 decode bound; see below
```

| Key | Meaning | Rules |
| --- | --- | --- |
| `instanceStatusEncoding` | The representation the status writer targets on its next write. It never reinterprets what is already stored. | Required. `DenseV1` writes the dense list. `ColumnarV2` writes columns only when they are strictly smaller than the dense list; otherwise it still writes `DenseV1`. A manager built without ColumnarV2 selection refuses to start under a `ColumnarV2` target. |
| `maxDecodedInstances` | The largest number of rows a stored `ColumnarV2` payload may expand to. It is the deployment profile's supported ceiling of transient rows per IR, including surge, migration, replacement, and retained terminal rows. | Positive integer. Required under `ColumnarV2`. May be omitted under `DenseV1` only when no `ColumnarV2` object exists; a `DenseV1` manager that meets columns without a bound fails that IR closed. |

The Helm chart renders the block from `ome.controller.omenativeStatus` and
defaults it to `DenseV1`; installing or upgrading the chart cannot activate
columns. Change the block only through the versioned deployment path, never by
editing the ConfigMap in place: the manager loads it once, so an in-place edit
takes effect at an unplanned restart. Changing either key is a stopped-fleet
operation (below), never a rolling one.

## Rollout order: schema first

Every representation-aware manager verifies the served InferenceReplica
schema through OpenAPI discovery before it registers any controller or
webhook, even under a `DenseV1` target. A cluster whose CRD lacks the typed
`instanceStatusColumns` schema and the representation-union validation rules
cannot host such a manager, and the manager exits at startup saying so.

Roll out in this order:

1. Install the CRD that carries `instanceStatusEncoding` and the typed
   `instanceStatusColumns` (the `ome-crd` chart) in every served cluster.
2. Install the explicit `omenativeStatus` block, `DenseV1` with the chosen
   bound, for every role that reads or writes IR status.
3. Roll the manager image. Old and new managers may overlap in this step
   because every writer still emits `DenseV1`.

Selecting `ColumnarV2` is a separate, stopped-fleet activation: every manager
and every other IR status writer in the fleet is scaled to zero, the block is
changed, the preflight is re-run, and only then are managers restored. Two
managers with different targets must never run against the same IRs; they
would convert the same object back and forth indefinitely.

## Downgrade rule

Set the target to `DenseV1` and wait for every object to convert **before**
rolling the manager image back to a build that predates the representation
union.

With a `DenseV1` target, a representation-aware manager converts every IR it
finds stored as `ColumnarV2` at the start of that IR's reconcile: it rewrites
the object as `DenseV1` once, with its logical rows unchanged and no lifecycle
effect on that pass, records `InstanceStatusConverted`, and requeues. Objects
already stored as `DenseV1` are untouched. Once initial informer population
has visited every IR, the fleet is all-dense; prove it with the preflight
(zero `columnarV2` objects in every cluster) rather than with the metrics.

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
| `ome_omenative_ir_status_conversions_total` | counter | `from`, `to` | Committed representation-only writes. It should rise only during a transition window and stay flat otherwise; a rising counter outside a window means two managers with different targets are fighting over the same objects. |

Metrics never carry payload content. The write-boundary metrics (all but the
bytes gauge) carry no object identity either; use events to find the IR.

| Event | Type | On | Meaning |
| --- | --- | --- | --- |
| `StatusSizeExceeded` | Warning | the parent InferenceService (the IR when no parent resolves) | The API server rejected a status write as too large. The message names the IR and the encoded byte count. No lifecycle transition for that IR can be persisted until the status shrinks; the manager does not drop or compact anything on its own. |
| `InstanceStatusConverted` | Normal | the IR | The transition gate rewrote the stored representation. The message names the source and target representation and the row count. |
| `InstanceStatusDecodeFailed` | Warning | the IR | The stored representation cannot be decoded; the message carries the fixed reason. Reconciliation of that IR is suspended until the status is repaired. |

## The preflight command

`ome-status-preflight` is the read-only check to run before and after a
transition. It never writes.

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
  instanceStatusEncoding: DenseV1
  maxDecodedInstances: 5000
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
