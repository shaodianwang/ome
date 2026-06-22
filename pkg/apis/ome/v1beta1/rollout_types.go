// Package v1beta1 — rollout types.
//
// spec.rollout is the single home for InferenceService rollout configuration.
// OEP-0017 (unified rollout v2) reshapes it from two mutually-exclusive engines
// (coordination + canary) into ONE model: an ordered list of rollout groups,
// each polymorphic in its progression (canary | blueGreen | rollingUpdate).
package v1beta1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

// RolloutSpec is the single rollout surface for an InferenceService: an ordered
// list of rollout groups. There is no coordination-vs-canary fork — sequencing
// is the list order, and canary is one progression a group may choose.
type RolloutSpec struct {
	// Groups is the ORDERED list of rollout groups. Sequence is the list
	// position: group N reaches completion before group N+1 begins. Components
	// listed together in one group roll together (a coupled unit); Components not
	// listed in any group roll independently (the default). To roll Components
	// one-at-a-time, put each in its own group, in order — a group needs only its
	// components; the progression may be omitted and defaults to blueGreen.
	//
	// Cross-group sequencing is currently enforced only for a run of
	// single-Component blueGreen groups (the classic one-at-a-time shape); mixed
	// progressions and multi-Component ordered groups roll concurrently on their
	// disjoint Components today — the general group sequencer is a follow-on.
	//
	// Only meaningful for OMENative-managed Components.
	// +optional
	// +listType=atomic
	// +kubebuilder:validation:MaxItems=3
	Groups []RolloutGroup `json:"groups,omitempty"`
}

// RolloutGroup is a set of Components that roll together, advancing through at
// most one progression. The progression is a presence-based one-of: set Canary,
// BlueGreen, or RollingUpdate — or none, which defaults to BlueGreen. Omitting it
// is the ergonomic spelling for "just roll this group" and for a single-Component
// group in a sequence (`components: [decoder]` then `components: [engine]`),
// where the empty `blueGreen: {}` selector would be pure ceremony.
//
// Failure mode if mis-set: two progressions is ambiguous (which one drives
// traffic?) and is rejected by the rule below.
// +kubebuilder:validation:XValidation:rule="(has(self.canary)?1:0) + (has(self.blueGreen)?1:0) + (has(self.rollingUpdate)?1:0) <= 1",message="at most one of canary, blueGreen, or rollingUpdate may be set on a rollout group"
// +kubebuilder:validation:XValidation:rule="!has(self.order) || self.order.all(c, c in self.components)",message="order must reference only Components listed in this group"
type RolloutGroup struct {
	// Components lists the Component names in this group (they roll together).
	// Valid values: "router", "engine", "decoder".
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=3
	// +listType=atomic
	Components []ComponentType `json:"components"`

	// Order optionally pins the sequence in which this group's Components roll.
	// It is honored today only when the group resolves to the internal Sequential
	// path (a run of single-Component groups); on a multi-Component blueGreen,
	// rollingUpdate, or canary group the Components currently advance together and
	// Order is not yet applied as a surge sequence. Ignored on single-Component
	// groups. Must reference only Components in this group.
	// +optional
	// +listType=atomic
	// +kubebuilder:validation:MaxItems=3
	Order []ComponentType `json:"order,omitempty"`

	// Canary advances the group through explicit capacity + service-level-traffic
	// steps with optional gates. One-of progression.
	// +optional
	Canary *GroupCanary `json:"canary,omitempty"`

	// BlueGreen surges the full new set, then flips traffic atomically. One-of
	// progression.
	// +optional
	BlueGreen *GroupBlueGreen `json:"blueGreen,omitempty"`

	// RollingUpdate replaces pods gradually within the surge/unavailable budget,
	// tolerating mixed revisions during the window. One-of progression.
	// +optional
	RollingUpdate *GroupRollingUpdate `json:"rollingUpdate,omitempty"`

	// Soak is the wait AFTER this group completes, before the next group begins.
	// It is honored only when the rollout is a sequence of single-Component
	// blueGreen groups (the run that collapses to the internal Sequential state
	// machine); ignored on the last group. Admission rejects a soak the engine
	// would silently drop — on a canary, multi-Component, or rollingUpdate group,
	// or on a rollout that does not collapse to that sequence.
	// +optional
	Soak *metav1.Duration `json:"soak,omitempty"`

	// MaintainRatio guards the cross-Component replica ratio while this group
	// rolls. Meaningful only on multi-Component blueGreen/rollingUpdate groups
	// (e.g. a PD engine+decoder pair); ignored on single-Component groups and
	// rejected by admission on canary groups (the canary engine does not enforce
	// it).
	// +optional
	MaintainRatio *MaintainRatio `json:"maintainRatio,omitempty"`
}

// GroupCanary is the stepped progression: bring up new-revision capacity and
// shift service-level traffic through an ordered list of steps, gated by
// PromotionPolicy.
type GroupCanary struct {
	// Steps is the ordered progression of capacity + service-level traffic. The
	// final step must reach 100% traffic for the rollout to complete.
	// +optional
	// +listType=atomic
	// +kubebuilder:validation:MaxItems=20
	Steps []RolloutGroupStep `json:"steps,omitempty"`

	// Promotion controls how the rollout advances between paused steps.
	//   - Manual: operator advances via the ome.io/rollout-promote annotation.
	//   - Auto: the controller advances on each step's Pause.Duration.
	// +kubebuilder:validation:Enum=Manual;Auto
	// +optional
	Promotion *PromotionPolicy `json:"promotion,omitempty"`

	// ScaleDownDelaySeconds is the wait between shifting traffic off the old
	// revision and scaling its pods down, to drain in-flight requests.
	// +optional
	ScaleDownDelaySeconds *int32 `json:"scaleDownDelaySeconds,omitempty"`
}

// RolloutGroupStep is one stage of a canary progression. Capacity and traffic
// are INDEPENDENT: capacity is the per-Component new-revision pod count; traffic
// is the service-level percentage of requests on the new revision. The
// independence allows "capacity ahead of traffic" (run 50% pods, send 10%
// traffic) for warm-up.
type RolloutGroupStep struct {
	// Capacity is the fraction of desired replicas to run on the new revision at
	// this step. Percentage ("25%") or absolute ("3"). Per-Component.
	// +kubebuilder:validation:XIntOrString
	Capacity intstr.IntOrString `json:"capacity"`

	// Traffic is the service-level percentage of requests routed to the new
	// revision at this step (0-100). Not per-Component.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=100
	Traffic int32 `json:"traffic"`

	// Pause holds the rollout at this step. Absent advances immediately.
	// +optional
	Pause *RolloutPause `json:"pause,omitempty"`
}

// GroupBlueGreen surges the full new set alongside the old, waits for Ready, then
// flips traffic atomically. It is also the default progression a group falls back
// to when none of canary/blueGreen/rollingUpdate is set. It carries no traffic
// steps (the flip is all-or-nothing); surge sequencing across Components is the
// group-level Order field, not a blue-green-specific knob. Empty today, reserved
// for future blue-green-specific options (e.g. a pre-flip verification window).
type GroupBlueGreen struct{}

// GroupRollingUpdate replaces pods gradually, paced by the surge/unavailable
// budget. Mixed revisions are tolerated during the window.
type GroupRollingUpdate struct {
	// MaxSurge is the maximum pods (fraction or absolute) above desired during
	// the roll. Default 25%.
	// +optional
	// +kubebuilder:validation:XIntOrString
	MaxSurge *intstr.IntOrString `json:"maxSurge,omitempty"`

	// MaxUnavailable is the maximum pods (fraction or absolute) that may be
	// not-Ready at once during the roll. Default 25% (matches the Kubernetes
	// Deployment convention; a default of 0 would deadlock single-replica rolls).
	// +optional
	// +kubebuilder:validation:XIntOrString
	MaxUnavailable *intstr.IntOrString `json:"maxUnavailable,omitempty"`
}

// MaintainRatio bounds how far a multi-Component group may drift from the
// cross-Component replica ratio observed at rollout start (e.g. prefill:decode).
type MaintainRatio struct {
	// Tolerance is the maximum percentage drift from the starting ratio. A roll
	// step that would exceed it is paused until the pools rebalance. Default 5.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=100
	Tolerance int32 `json:"tolerance"`
}

// PromotionPolicy selects how a canary advances between steps that carry a Pause.
type PromotionPolicy string

const (
	// PromotionPolicyManual requires explicit operator advancement via the
	// ome.io/rollout-promote annotation.
	PromotionPolicyManual PromotionPolicy = "Manual"
	// PromotionPolicyAuto advances based on each step's Pause.Duration. A step
	// with no Pause (or Pause.Duration unset) advances immediately.
	PromotionPolicyAuto PromotionPolicy = "Auto"
)

// RolloutPause holds a rollout at a step boundary.
type RolloutPause struct {
	// Duration to pause before auto-advancing.
	//   - If unset and PromotionPolicy is Manual, the controller waits
	//     indefinitely for the ome.io/rollout-promote annotation.
	//   - If PromotionPolicy is Auto and Duration is unset, the controller
	//     advances immediately (the Pause is a no-op).
	//   - If both are set, the controller advances after Duration.
	// +optional
	Duration *metav1.Duration `json:"duration,omitempty"`
}
