package rolloutspec

// Machine is one state machine table before expansion: declared states,
// events, transitions, and the authored cells that cover them.
type Machine struct {
	Machine string `json:"machine"`
	// Scope is a one-line human description of what one row of this
	// machine represents.
	Scope string `json:"scope"`
	// Frozen machines may contain no DECIDE cell; validation enforces it.
	Frozen bool `json:"frozen"`
	// Invariants are the machine-wide properties cells cite by id. A
	// machine that declares none leaves cell citations unchecked.
	Invariants  []Invariant  `json:"invariants,omitempty"`
	States      []State      `json:"states"`
	Events      []Event      `json:"events"`
	Transitions []Transition `json:"transitions"`
	Cells       []Cell       `json:"cells"`
}

// Invariant is one property that holds across the machine. Cells cite it by
// id in Cell.Invariants rather than restating it.
type Invariant struct {
	ID        string `json:"id"`
	Statement string `json:"statement"`
}

// OpRef names the persisted operation type and step that refine a phase.
type OpRef struct {
	Type string `json:"type"`
	Step string `json:"step"`
}

// Applies narrows a state to the strategies and pod shapes it can occur in.
type Applies struct {
	Strategies []string `json:"strategies,omitempty"`
	Shape      []string `json:"shape,omitempty"`
}

// State is one row of the grid. NotApplicable maps an event id, or
// "event[variant]", to the reason that event cannot occur in this state; an
// unqualified key covers every variant of that event, an "event[variant]"
// key covers only that variant. Grid expansion synthesizes an n/a cell
// covering every window the key applies to.
type State struct {
	ID    string `json:"id"`
	Phase string `json:"phase"`
	// Op is nil when the state has no operation in flight. A present but
	// empty op ("op: {}", blank type) is invalid.
	Op *OpRef `json:"op,omitempty"`
	// Applies is nil when the state applies everywhere. A present but
	// empty applies ("applies: {}", both lists empty) is invalid.
	Applies       *Applies          `json:"applies,omitempty"`
	DecidedIn     string            `json:"decided_in"`
	NotApplicable map[string]string `json:"not_applicable,omitempty"`
	Note          string            `json:"note,omitempty"`
}

// Event is one column family of the grid. Variants split the column
// (e.g. source vs target pod); an event without variants has one implicit
// empty variant.
type Event struct {
	ID          string   `json:"id"`
	Family      string   `json:"family"`
	Variants    []string `json:"variants,omitempty"`
	Description string   `json:"description,omitempty"`
}

// Transition is one guarded edge. Boundary and after windows are keyed per
// outgoing transition of the source state.
type Transition struct {
	ID   string `json:"id"`
	From string `json:"from"`
	// Event is the event id this transition fires on. The field (and its
	// json key) cannot be named "on": YAML 1.1 resolves a bare on key to
	// the boolean true, which would fail strict unmarshal on every
	// fragment with the misleading error "unknown field \"true\"".
	Event string `json:"event"`
	// Variant is blank when the transition matches any variant of a
	// variant-bearing event.
	Variant   string   `json:"variant,omitempty"`
	Guard     string   `json:"guard"`
	To        string   `json:"to"`
	Effects   []string `json:"effects,omitempty"`
	DecidedIn string   `json:"decided_in"`
}

// Coverage names the checks that prove a cell. Cells with placement sim
// carry no coverage by construction, so a report counts them as an open gap.
type Coverage struct {
	Perm []string `json:"perm,omitempty"`
	Unit []string `json:"unit,omitempty"`
	Kind []string `json:"kind,omitempty"`
}

// Cell is one authored decision. Empty Variants means every variant of the
// event, empty Windows means every window, and empty Transition means every
// outgoing transition of State.
type Cell struct {
	State      string   `json:"state"`
	Event      string   `json:"event"`
	Variants   []string `json:"variants,omitempty"`
	Windows    []string `json:"windows,omitempty"`
	Transition string   `json:"transition,omitempty"`
	Outcome    string   `json:"outcome"`
	To         string   `json:"to,omitempty"`
	Reason     string   `json:"reason,omitempty"`
	Note       string   `json:"note,omitempty"`
	// Guard is the per-cell guard text: a note on why this cell's outcome
	// holds, distinct from Transition.Guard, which is the condition that
	// fires the transition itself.
	Guard      string   `json:"guard,omitempty"`
	Invariants []string `json:"invariants,omitempty"`
	Placement  string   `json:"placement,omitempty"`
	// Coverage is nil when no coverage has been recorded for this cell.
	Coverage *Coverage `json:"coverage,omitempty"`
	Status   string    `json:"status"`
}

// Outcome values.
const (
	OutcomeTransition = "transition"
	OutcomeIgnore     = "ignore"
	OutcomeDefer      = "defer"
	OutcomeReject     = "reject"
	OutcomeEscalate   = "escalate"
	OutcomeNA         = "n/a"
	OutcomeDecide     = "DECIDE"
)

// Window values, in grid order.
const (
	WindowBefore   = "before"
	WindowBoundary = "boundary"
	WindowAfter    = "after"
)

// Status values.
const (
	StatusCurrent = "current"
	StatusReview  = "review"
	StatusDecided = "decided"
)

// Placement values.
const (
	PlacementUnit = "unit"
	PlacementPerm = "perm"
	PlacementKind = "kind"
	PlacementSim  = "sim"
)

// Windows is the ordered window list used for expansion. Read-only; callers
// must not mutate.
var Windows = []string{WindowBefore, WindowBoundary, WindowAfter}

// outcomes, statuses, and placements are the closed vocabularies Validate
// enforces. Read-only; callers must not mutate.
var (
	outcomes   = set(OutcomeTransition, OutcomeIgnore, OutcomeDefer, OutcomeReject, OutcomeEscalate, OutcomeNA, OutcomeDecide)
	statuses   = set(StatusCurrent, StatusReview, StatusDecided)
	placements = set(PlacementUnit, PlacementPerm, PlacementKind, PlacementSim)
)

func set(values ...string) map[string]struct{} {
	s := make(map[string]struct{}, len(values))
	for _, v := range values {
		s[v] = struct{}{}
	}
	return s
}
