package coordination

import (
	"testing"

	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	workloadtypes "github.com/ome-projects/ome/pkg/controller/v1beta1/workload/types"
)

// Regression for the OMENative playground p226 / p227 verdict
// "decoder peak 5 > ceiling 4".
//
// Shape (both cases): a PD [engine, decoder] coordination group, engine
// maxSurge=50% (per-Component) and decoder SurgeThenDrain maxSurge=1, with
// the group's pacing.maxSurge left unset → defaulted to "25%". Both
// Components are bumped (the harness perturbs every group member), so each
// rolls off its OWN revision and surges per its OWN budget — the documented
// per-Component surge model (see the p62 header: "each component paces on its
// OWN budget, no cross-ratio", and the ~40 sibling rollout cases p27/p30/p32/
// p33/p41/p48/p62/p80/p243/p244 that all expect a bumped SurgeThenDrain
// maxSurge=1 decoder to surge to N+1=5).
//
// The correct decoder peak is therefore N(4) + maxSurge(1) = 5. These tests
// lock that in at the gate level: the decoder is permitted EXACTLY ONE surge
// (one extra pod alive → peak 5) and is gated from a SECOND (peak never
// reaches 6). A future change that wrongly clamped the decoder to peak 4
// (surge 0) — the mis-authored p226/p227 ceil_d=4 expectation — would make
// the "allowed" assertions fail; a change that let it surge twice would make
// the "denied" assertions fail.

// mkPDGroupSurge builds the p226/p227 group shape for the DECODER's surge
// gate: a 2-member [engine, decoder] group with the given pacing, the group's
// pacing.MaxSurge defaulted to "25%" (= budget 1 at N=4), and a 4:4
// ObservedRatio anchor (needed by RatioBalanced; harmless for PerComponent).
// decoderSurgesInFlight controls how many decoder Instances are already on a
// surge step (Surge/SurgeDrain) from a prior wake-up.
func mkPDGroupSurge(pacingType v1beta1.CoordinationPacingType, decoderSurgesInFlight int) *v1beta1.InferenceService {
	groupSurge := intstr.FromString("25%") // the webhook default when pacing.maxSurge is unset
	pacing := &v1beta1.CoordinationPacing{
		Type:     pacingType,
		MaxSurge: &groupSurge,
	}
	if pacingType == v1beta1.CoordinationPacingRatioBalanced {
		pacing.RatioTolerancePercent = int32Ptr(25)
	}

	decoderInstances := make([]v1beta1.OMENativeInstanceStatus, 0, decoderSurgesInFlight)
	for i := 0; i < decoderSurgesInFlight; i++ {
		decoderInstances = append(decoderInstances, v1beta1.OMENativeInstanceStatus{
			Index:     int32(i),
			Phase:     v1beta1.OMENativeInstanceUpdating,
			Operation: &v1beta1.InstanceOperation{Type: v1beta1.InstanceOperationUpdate, Step: "Surge"},
		})
	}

	return &v1beta1.InferenceService{
		Spec: v1beta1.InferenceServiceSpec{
			Rollout: &v1beta1.RolloutSpec{Coordination: &v1beta1.RolloutCoordinationSpec{
				Groups: []v1beta1.RolloutCoordinationGroup{{
					Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
					Policy:     v1beta1.CoordinationPolicyRollingUpdate,
					Pacing:     pacing,
				}},
			}},
		},
		Status: v1beta1.InferenceServiceStatus{
			RolloutCoordination: &v1beta1.RolloutCoordinationStatus{
				Groups: []v1beta1.RolloutCoordinationGroupStatus{{
					Name:       "0",
					Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
					Policy:     v1beta1.CoordinationPolicyRollingUpdate,
					ObservedRatio: &v1beta1.RolloutCoordinationRatio{
						Original: map[v1beta1.ComponentType]int32{
							v1beta1.EngineComponent:  4,
							v1beta1.DecoderComponent: 4,
						},
					},
				}},
			},
			Components: map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
				v1beta1.EngineComponent: {Lifecycle: &v1beta1.LifecycleStatus{Replicas: 4, ServingReplicas: 4}},
				v1beta1.DecoderComponent: {Lifecycle: &v1beta1.LifecycleStatus{
					Replicas:         4,
					ServingReplicas:  4,
					InstanceStatuses: decoderInstances,
				}},
			},
		},
	}
}

// TestDecoderSurgeBudget_P226_RatioBalanced pins the p226 shape: a bumped
// SurgeThenDrain maxSurge=1 decoder under RatioBalanced pacing surges EXACTLY
// once (peak N+1 = 5) and no more.
func TestDecoderSurgeBudget_P226_RatioBalanced(t *testing.T) {
	// First surge: no decoder surge in flight → the gate stack (CheckRatio
	// +1 projects 5:4 = 1.25, in band [0.75,1.25]; CheckSurge 0+0+1 <=
	// budget 1) must ALLOW. This is the surge that takes the decoder to
	// peak 5.
	isvc := mkPDGroupSurge(v1beta1.CoordinationPacingRatioBalanced, 0)
	if allowed, reason := EvaluateUpdateGate(isvc, v1beta1.DecoderComponent, nil,
		workloadtypes.UpdateStrategySurgeThenDrain, 0, 0); !allowed {
		t.Fatalf("p226: decoder's FIRST surge (→ peak N+1=5) must be allowed; got denied: %s", reason)
	}

	// Second surge: one decoder Instance already surging → CheckSurge
	// projects 1+0+1 = 2 > budget 1 → must DENY. This is what keeps the
	// decoder peak at 5 and not 6. (If the decoder were wrongly clamped to
	// peak 4, the first-surge assertion above would already have failed.)
	isvc = mkPDGroupSurge(v1beta1.CoordinationPacingRatioBalanced, 1)
	if allowed, reason := EvaluateUpdateGate(isvc, v1beta1.DecoderComponent, nil,
		workloadtypes.UpdateStrategySurgeThenDrain, 0, 0); allowed {
		t.Fatalf("p226: decoder's SECOND surge (would be peak 6 > maxSurge=1 budget) must be denied; got allowed (reason=%s)", reason)
	}
}

// TestDecoderSurgeBudget_P227_PerComponent pins the p227 shape: identical to
// p226 but PerComponent pacing (no cross-ratio gate). The decoder still
// surges exactly once to peak 5 — governed solely by the group MaxSurge
// budget (defaulted "25%" = 1 at N=4).
func TestDecoderSurgeBudget_P227_PerComponent(t *testing.T) {
	isvc := mkPDGroupSurge(v1beta1.CoordinationPacingPerComponent, 0)
	if allowed, reason := EvaluateUpdateGate(isvc, v1beta1.DecoderComponent, nil,
		workloadtypes.UpdateStrategySurgeThenDrain, 0, 0); !allowed {
		t.Fatalf("p227: decoder's FIRST surge (→ peak N+1=5) must be allowed; got denied: %s", reason)
	}

	isvc = mkPDGroupSurge(v1beta1.CoordinationPacingPerComponent, 1)
	if allowed, reason := EvaluateUpdateGate(isvc, v1beta1.DecoderComponent, nil,
		workloadtypes.UpdateStrategySurgeThenDrain, 0, 0); allowed {
		t.Fatalf("p227: decoder's SECOND surge (would be peak 6 > maxSurge=1 budget) must be denied; got allowed (reason=%s)", reason)
	}
}

// TestDecoderSurgeBudget_InWakeUpDeltaGatesSecondSurge guards the
// within-wake-up tally path: even before status reflects the first surge, the
// dispatcher threads inFlightSurge into the gate, so a second surge in the
// SAME wake-up (inFlightSurge=1) is denied. This is the other half of
// "decoder peak never exceeds 5" — the stale-snapshot hole that would
// otherwise let the dispatcher fire two decoder surges in one pass.
func TestDecoderSurgeBudget_InWakeUpDeltaGatesSecondSurge(t *testing.T) {
	for _, pt := range []v1beta1.CoordinationPacingType{
		v1beta1.CoordinationPacingRatioBalanced,
		v1beta1.CoordinationPacingPerComponent,
	} {
		isvc := mkPDGroupSurge(pt, 0)
		// inFlightSurge=1 → CheckSurge projects 0+1+1 = 2 > budget 1.
		if allowed, reason := EvaluateUpdateGate(isvc, v1beta1.DecoderComponent, nil,
			workloadtypes.UpdateStrategySurgeThenDrain, 1, 0); allowed {
			t.Errorf("pacing=%s: a second decoder surge in one wake-up (inFlightSurge=1, budget=1) must be denied; got allowed (reason=%s)", pt, reason)
		}
	}
}
