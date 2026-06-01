// Package v1beta1 — rollout strategy types.
//
// This file defines the multi-step progressive rollout shape.
// Single-step rollouts (the today-or-canary case) continue to use the
// existing per-Component ComponentExtensionSpec.CanaryTrafficPercent
// field. The two forms are mutually exclusive at admission.
package v1beta1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

// RolloutSpec defines a multi-step progressive rollout plan. When set
// on TrafficSpec.Rollout, the controller advances through the steps
// under the configured PromotionPolicy and the simple per-Component
// ComponentExtensionSpec.CanaryTrafficPercent MUST NOT be set on the
// same Component (admission rejects).
//
// Step semantics:
//   - Each step specifies pod capacity (NewCapacity, OldCapacity)
//     independently of traffic weight (TrafficWeight). This lets
//     operators run the new revision at full capacity with 0% traffic
//     for validation, then atomically shift traffic.
//   - The final step's TrafficWeight must be 100 (admission enforces)
//     so the rollout always completes.
//   - Each step's optional Pause holds the rollout. Under Manual
//     promotion, operators advance via the ome.io/rollout-promote
//     annotation; under Auto, the controller advances based on
//     Pause.Duration.
type RolloutSpec struct {
	// Steps is the ordered list of rollout stages.
	// +kubebuilder:validation:MinItems=1
	Steps []RolloutStep `json:"steps"`

	// PromotionPolicy controls how the rollout advances between steps.
	//   - Manual (default): operator advances via the
	//     ome.io/rollout-promote annotation.
	//   - Auto: controller advances based on each step's Pause.Duration.
	// +kubebuilder:validation:Enum=Manual;Auto
	// +optional
	PromotionPolicy *PromotionPolicy `json:"promotionPolicy,omitempty"`

	// ScaleDownDelaySeconds is the wait between shifting traffic away
	// from the old revision and scaling its pods down, to drain
	// in-flight requests. Default: 30.
	// +optional
	// +kubebuilder:default=30
	// +kubebuilder:validation:Minimum=0
	ScaleDownDelaySeconds *int32 `json:"scaleDownDelaySeconds,omitempty"`
}

// PromotionPolicy selects how a multi-step rollout advances between
// steps that carry a Pause block.
type PromotionPolicy string

const (
	// PromotionPolicyManual requires explicit operator advancement via
	// the ome.io/rollout-promote annotation.
	PromotionPolicyManual PromotionPolicy = "Manual"
	// PromotionPolicyAuto advances based on each step's Pause.Duration.
	// A step with no Pause (or Pause.Duration unset) advances
	// immediately.
	PromotionPolicyAuto PromotionPolicy = "Auto"
)

// RolloutStep specifies one stage of a progressive rollout.
//
// NewCapacity and TrafficWeight are independent dimensions. Common
// patterns:
//   - Symmetric canary: NewCapacity=10%, OldCapacity=90%, TrafficWeight=10
//   - Capacity surge:   NewCapacity=10%, OldCapacity=100%, TrafficWeight=10
//   - Blue-green:       NewCapacity=100%, OldCapacity=100%, TrafficWeight=0
//     (validate at full scale; later step sets TrafficWeight=100)
type RolloutStep struct {
	// NewCapacity is the fraction of desired replicas to run for the
	// new revision at this step. Percentage ("25%") or absolute ("3").
	// +kubebuilder:validation:XIntOrString
	NewCapacity intstr.IntOrString `json:"newCapacity"`

	// OldCapacity is the fraction of desired replicas to keep for the
	// old revision at this step. Defaults to (100% - NewCapacity).
	// Setting both >50% allows a temporary capacity surge — useful for
	// keeping full old capacity while the new revision validates at
	// the same scale.
	// +optional
	// +kubebuilder:validation:XIntOrString
	OldCapacity *intstr.IntOrString `json:"oldCapacity,omitempty"`

	// TrafficWeight is the percentage of traffic routed to the new
	// revision at this step. The old revision receives
	// (100 - TrafficWeight).
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=100
	TrafficWeight int32 `json:"trafficWeight"`

	// Pause holds the rollout at this step. If absent, the controller
	// advances to the next step immediately after the step's capacity,
	// traffic, and drain operations complete.
	// +optional
	Pause *RolloutPause `json:"pause,omitempty"`
}

// RolloutPause holds a rollout at a step boundary.
type RolloutPause struct {
	// Duration to pause before auto-advancing.
	//   - If unset and PromotionPolicy is Manual, the controller waits
	//     indefinitely for the ome.io/rollout-promote annotation.
	//   - If PromotionPolicy is Auto and Duration is unset, the
	//     controller advances immediately (i.e., the Pause is a no-op).
	//   - If both are set, the controller advances after Duration
	//     regardless of policy.
	// +optional
	Duration *metav1.Duration `json:"duration,omitempty"`
}
