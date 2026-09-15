// Package rolloutspec holds the behavior tables for the OMENative rollout
// state machines and the tooling that keeps them exhaustive.
//
// A Machine declares states, events, and transitions. A Cell states what one
// machine must do when one event (optionally one variant of it) arrives in
// one state during one window: before a transition's guard is true, at the
// boundary where the guard is true but the controller's write has not
// landed, or immediately after the next state is persisted. Authored cells
// may cover many keys at once; Expand materializes the full grid so a
// missing or contradictory cell is a build error rather than an oversight.
//
// Fragment YAML is parsed under YAML 1.1 rules, so bare yes, no, on, off, y,
// and n values resolve to booleans; authors must quote them to keep a string.
//
// The canonical id of one grid cell is "<machine>/" plus GridCell.Key(), i.e.
// <machine>/<state>/<event>[<variant>]/<window>@<transition>, with the
// variant and transition segments omitted when empty. Reports and error
// messages that name a cell use this id.
package rolloutspec
