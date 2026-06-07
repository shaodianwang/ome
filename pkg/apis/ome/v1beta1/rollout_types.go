// Package v1beta1 — rollout types.
//
// spec.rollout is the single home for InferenceService rollout configuration:
// cross-Component coordination today, plus a reserved progressive-traffic
// (canary) plan that is admission-rejected until a step executor exists.
package v1beta1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

// RolloutSpec is the single rollout surface for an InferenceService.
type RolloutSpec struct {
	// Coordination declares cross-Component rollout coupling for
	// OMENative-managed Components. Each group lists the Components that
	// roll together and the policy that governs their coordination.
	// Components not listed in any group roll independently (the default).
	//
	// Only meaningful when at least one referenced Component uses
	// ome.io/deploymentMode=OMENative. The webhook rejects groups that
	// reference non-OMENative Components.
	// +optional
	Coordination *RolloutCoordinationSpec `json:"coordination,omitempty"`

	// Canary is a step-based progressive-traffic-shift plan: a new revision is
	// brought up at some pod capacity and a fraction of traffic shifts to it,
	// advancing through steps. Executed by the canary controller; mutually
	// exclusive with Coordination.
	// +optional
	Canary *CanarySpec `json:"canary,omitempty"`
}

// CanarySpec is a multi-step progressive rollout plan: a new revision is brought
// up at some pod capacity while a fraction of traffic shifts to it, advancing
// through steps.
//
// Step semantics:
//   - Each step specifies new-revision pod capacity (NewCapacity) independently
//     of traffic weight (TrafficWeight), so the new revision can run at higher
//     capacity than its traffic share before traffic shifts.
//   - The final step's TrafficWeight is 100 so the rollout completes.
//   - Each step's optional Pause holds the rollout (Manual: advance via
//     ome.io/rollout-promote; Auto: advance on Pause.Duration).
type CanarySpec struct {
	// Steps is the ordered list of rollout stages.
	// +optional
	// +listType=atomic
	Steps []RolloutStep `json:"steps,omitempty"`

	// PromotionPolicy controls how the rollout advances between paused steps.
	//   - Manual: operator advances via the ome.io/rollout-promote annotation.
	//   - Auto: controller advances based on each step's Pause.Duration.
	// +optional
	PromotionPolicy *PromotionPolicy `json:"promotionPolicy,omitempty"`

	// ScaleDownDelaySeconds is the wait between shifting traffic away from the
	// old revision and scaling its pods down, to drain in-flight requests.
	// +optional
	ScaleDownDelaySeconds *int32 `json:"scaleDownDelaySeconds,omitempty"`
}

// PromotionPolicy selects how a multi-step canary advances between steps that
// carry a Pause block.
type PromotionPolicy string

const (
	// PromotionPolicyManual requires explicit operator advancement via the
	// ome.io/rollout-promote annotation.
	PromotionPolicyManual PromotionPolicy = "Manual"
	// PromotionPolicyAuto advances based on each step's Pause.Duration. A step
	// with no Pause (or Pause.Duration unset) advances immediately.
	PromotionPolicyAuto PromotionPolicy = "Auto"
)

// RolloutStep specifies one stage of a progressive (canary) rollout.
//
// NewCapacity and TrafficWeight are independent dimensions. Common patterns:
//   - Symmetric canary: NewCapacity=10%, TrafficWeight=10
//   - Capacity ahead of traffic: NewCapacity=50%, TrafficWeight=10
//     (run more new pods than the traffic share, e.g. to warm up)
//   - Final cutover: NewCapacity=100%, TrafficWeight=100
type RolloutStep struct {
	// NewCapacity is the fraction of desired replicas to run for the new
	// revision at this step. Percentage ("25%") or absolute ("3").
	// +kubebuilder:validation:XIntOrString
	NewCapacity intstr.IntOrString `json:"newCapacity"`

	// OldCapacity is reserved. In v1 the old revision always runs the complement
	// of NewCapacity (100% - NewCapacity) via the StatefulSet-style partition, so
	// the total pod count never exceeds desired replicas. An explicit OldCapacity
	// must equal that complement; an independent old capacity or a surge above
	// desired (e.g. NewCapacity=100%, OldCapacity=100%) is admission-rejected —
	// capacity surge is not supported in v1.
	// +optional
	// +kubebuilder:validation:XIntOrString
	OldCapacity *intstr.IntOrString `json:"oldCapacity,omitempty"`

	// TrafficWeight is the percentage of traffic routed to the new revision at
	// this step. The old revision receives (100 - TrafficWeight).
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=100
	TrafficWeight int32 `json:"trafficWeight"`

	// Pause holds the rollout at this step. If absent, the controller advances
	// immediately after the step's capacity, traffic, and drain operations.
	// +optional
	Pause *RolloutPause `json:"pause,omitempty"`
}

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
