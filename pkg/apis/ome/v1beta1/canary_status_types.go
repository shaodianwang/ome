package v1beta1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// CanaryStatus tracks progress of a spec.rollout.canary rollout. It is the
// executor's persistent state machine: which step is active, when it was
// entered (Auto promotion measures Pause.Duration from here), and which
// revision is the canary. Absent when no canary is in progress.
type CanaryStatus struct {
	// CanaryRevisionHash is the revision hash being rolled out.
	// +optional
	CanaryRevisionHash string `json:"canaryRevisionHash,omitempty"`

	// CurrentStep is the zero-based index into spec.rollout.canary.Steps.
	CurrentStep int32 `json:"currentStep"`

	// StepEnteredTime is when CurrentStep was entered. Auto promotion measures
	// Pause.Duration from this timestamp.
	// +optional
	StepEnteredTime *metav1.Time `json:"stepEnteredTime,omitempty"`

	// ObservedTrafficWeight is the external traffic weight currently programmed
	// for the canary revision (mirrors the active step's TrafficWeight once
	// applied).
	ObservedTrafficWeight int32 `json:"observedTrafficWeight"`

	// RolledBackRevisionHash is set when a rollback (ome.io/rollout-rollback)
	// abandons a canary: it records the rejected revision hash. While set, the
	// component is held on the stable revision and the rejected revision is NOT
	// retried — even after the annotation is cleared. The rollout re-arms only
	// when a different target revision appears (a fresh spec change / fix).
	// +optional
	RolledBackRevisionHash string `json:"rolledBackRevisionHash,omitempty"`
}
