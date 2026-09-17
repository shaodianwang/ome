package types

import "errors"

// Gang scheduling holds — what one Instance's PodGroup says about the
// gang, and the InstanceOperation.Waiting tokens that report it.
//
// A gang is admitted as a unit, so the PodGroup carries facts no single
// member does: the scheduler's terminal verdict on the group, and
// whether the deterministic name is usable by this owner at all. Whether
// the gang can be PLACED is not among them — that reaches the Instance
// on its member pods' PodScheduled conditions, which the pod-level
// scheduler hold reads.

// WaitingReasonPodGroupTerminating is the Waiting token recorded while
// the deterministic PodGroup name is still occupied by an object whose
// deletion has begun. The gang cannot be announced until that object is
// collected, so the clock parks and the next pass re-ensures the group;
// the wait resolves itself and never escalates.
const WaitingReasonPodGroupTerminating = "PodGroupTerminating"

// PodGroupOwnershipConflictReason is the LastFailure reason for the one
// PodGroup state nothing this controller does can resolve: a
// deterministic name held by a controller that is not this owner.
const PodGroupOwnershipConflictReason = "PodGroupOwnershipConflict"

// ErrGangNameUnusable wraps the reasons an Instance's deterministic
// PodGroup name cannot be written this pass. It is the workload-level
// spelling of the podgroup package's sentinels, so an op can recognize
// the condition — and defer to the escalation pass instead of failing
// the reconcile — without importing the optional gang-scheduling API.
var ErrGangNameUnusable = errors.New("PodGroup name is not usable by this owner")

// GangState is what one Instance's PodGroup reports, reduced to the
// readings the escalation pass acts on. The empty value means the group
// is absent (the ensure pass recreates it) or scheduling normally —
// either way the row has nothing to wait on.
type GangState string

const (
	GangStateNone              GangState = ""
	GangStateTerminating       GangState = "Terminating"
	GangStateOwnershipConflict GangState = "OwnershipConflict"
	GangStateFailed            GangState = "Failed"
)

// GangRowOwnedElsewhere reports whether another state machine owns this
// row's outcome, so nothing read off its PodGroup may hold it, flip its
// phase, or rebuild its group. A teardown belongs to DeleteBatch, which
// paces its own removal past any wait and has nothing left to announce
// for a gang on its way out. A migration belongs to its record, the
// timeout authority for both pair rows.
func GangRowOwnedElsewhere(op *InstanceOperation) bool {
	if op == nil {
		return false
	}
	switch op.Type {
	case InstanceOperationDelete, InstanceOperationMigrate:
		return true
	default:
		return false
	}
}

// GangObservation is one Instance's PodGroup as the escalation pass
// reads it: the deterministic name, the classified state, and the
// operator-facing explanation that lands on LastFailure.
type GangObservation struct {
	Name    string
	State   GangState
	Message string
}

// GangObservations collects the per-Instance PodGroup classification for
// one Component reconcile. The PodGroup pass records into it before the
// dispatcher runs, and the pod-create choke point and escalation pass
// read it back.
//
// Every method is nil-safe, so an adapter or test that wires no PodGroup
// observation simply sees a gang that reports nothing.
type GangObservations struct {
	byIndex map[int32]GangObservation
}

// NewGangObservations returns an empty collector for one reconcile.
func NewGangObservations() *GangObservations {
	return &GangObservations{byIndex: make(map[int32]GangObservation)}
}

// Record stores the classification for one Instance index.
func (g *GangObservations) Record(idx int32, obs GangObservation) {
	if g == nil || g.byIndex == nil {
		return
	}
	g.byIndex[idx] = obs
}

// For returns the classification recorded for idx, or the zero
// observation when the pass recorded none.
func (g *GangObservations) For(idx int32) GangObservation {
	if g == nil {
		return GangObservation{}
	}
	return g.byIndex[idx]
}

// BlocksPods reports whether idx's deterministic PodGroup name is
// unusable this pass, so no member may be created against it: the name
// belongs to another controller, the object at it is on its way out, or
// the group has been reset and its replacement does not exist yet. What
// happens to the row itself — a parked wait or a terminal failure —
// belongs to the escalation pass.
func (g *GangObservations) BlocksPods(idx int32) bool {
	switch g.For(idx).State {
	case GangStateTerminating, GangStateOwnershipConflict, GangStateFailed:
		return true
	default:
		return false
	}
}

// OperationGangHeld reports whether an operation is parked on a
// gang-scheduling wait: the group's deterministic name is still held by
// an object being collected, so the gang cannot be announced yet.
func OperationGangHeld(op *InstanceOperation) bool {
	return op != nil && op.Waiting == WaitingReasonPodGroupTerminating
}
