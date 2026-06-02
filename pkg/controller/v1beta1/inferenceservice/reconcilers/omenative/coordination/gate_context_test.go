package coordination

import (
	"testing"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

// ResolveGateContext is the once-per-tick prelude helper backing all
// Check* gates. These tests pin the four short-circuit paths and the
// resolved-group success path; the per-gate behavior is already covered
// by the existing ratio_test.go / sequential_gate_test.go suites that
// continue to exercise the public CheckRatioGate / CheckSurgeGate /
// CheckUnavailabilityGate / CheckSequentialGate wrappers.

func TestResolveGateContext_NilISVC(t *testing.T) {
	ctx := ResolveGateContext(nil, v1beta1.EngineComponent)
	if !ctx.ShortCircuit {
		t.Fatalf("nil ISVC: want ShortCircuit=true, got %+v", ctx)
	}
	if ctx.ShortReason != "nil ISVC" {
		t.Errorf("nil ISVC: want ShortReason=nil ISVC, got %q", ctx.ShortReason)
	}
}

func TestResolveGateContext_NoCoordBlock(t *testing.T) {
	isvc := &v1beta1.InferenceService{}
	ctx := ResolveGateContext(isvc, v1beta1.EngineComponent)
	if !ctx.ShortCircuit {
		t.Fatalf("no coord block: want ShortCircuit=true, got %+v", ctx)
	}
	if ctx.ShortReason != "no coordination block" {
		t.Errorf("no coord block: want ShortReason=no coordination block, got %q", ctx.ShortReason)
	}
}

func TestResolveGateContext_ComponentNotInAnyGroup(t *testing.T) {
	isvc := &v1beta1.InferenceService{
		Spec: v1beta1.InferenceServiceSpec{
			RolloutCoordination: &v1beta1.RolloutCoordinationSpec{
				Groups: []v1beta1.RolloutCoordinationGroup{
					{
						Components: []v1beta1.ComponentType{v1beta1.DecoderComponent},
						Policy:     v1beta1.CoordinationPolicyIndependent,
					},
				},
			},
		},
	}
	ctx := ResolveGateContext(isvc, v1beta1.EngineComponent)
	if !ctx.ShortCircuit {
		t.Fatalf("not in any group: want ShortCircuit=true, got %+v", ctx)
	}
	if ctx.ShortReason != "component not in any coord group" {
		t.Errorf("not in any group: want ShortReason=component not in any coord group, got %q", ctx.ShortReason)
	}
}

func TestResolveGateContext_ResolvesGroup(t *testing.T) {
	isvc := &v1beta1.InferenceService{
		Spec: v1beta1.InferenceServiceSpec{
			RolloutCoordination: &v1beta1.RolloutCoordinationSpec{
				Groups: []v1beta1.RolloutCoordinationGroup{
					{
						Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
						Policy:     v1beta1.CoordinationPolicyBlueGreen,
					},
				},
			},
		},
	}
	ctx := ResolveGateContext(isvc, v1beta1.EngineComponent)
	if ctx.ShortCircuit {
		t.Fatalf("resolved group: want ShortCircuit=false, got %+v", ctx)
	}
	if ctx.Group.Policy != v1beta1.CoordinationPolicyBlueGreen {
		t.Errorf("resolved group: want Policy=BlueGreen, got %q", ctx.Group.Policy)
	}
	if len(ctx.Group.Components) != 2 {
		t.Errorf("resolved group: want 2 Components, got %d", len(ctx.Group.Components))
	}
	if ctx.ISVC != isvc {
		t.Errorf("resolved group: ISVC pointer not threaded through")
	}
	if ctx.Component != v1beta1.EngineComponent {
		t.Errorf("resolved group: Component not threaded through, got %q", ctx.Component)
	}
}

// TestGateContextCheckRatio_ShortCircuitsAllowed pins the
// short-circuit path on the GateContext-aware variant — the dispatcher
// reads the (allowed=true, reason) tuple even when the ISVC has no
// coord block. Behavior parity with CheckRatioGate's "nil ISVC" /
// "no coordination block" / "component not in any coord group"
// branches is what makes the wrapper safe to keep as a 1-liner.
func TestGateContextCheckRatio_ShortCircuitsAllowed(t *testing.T) {
	ctx := GateContext{
		ShortCircuit: true,
		ShortReason:  "no coordination block",
	}
	if allowed, reason := ctx.CheckRatio(0, -1); !allowed || reason != "no coordination block" {
		t.Errorf("short-circuit: got (allowed=%v, reason=%q), want (true, no coordination block)", allowed, reason)
	}
	if allowed, reason := ctx.CheckSurge(0); !allowed || reason != "no coordination block" {
		t.Errorf("short-circuit surge: got (allowed=%v, reason=%q)", allowed, reason)
	}
	if allowed, reason := ctx.CheckUnavailability(0); !allowed || reason != "no coordination block" {
		t.Errorf("short-circuit unavail: got (allowed=%v, reason=%q)", allowed, reason)
	}
	if allowed, reason := ctx.CheckSequential(); !allowed || reason != "no coordination block" {
		t.Errorf("short-circuit sequential: got (allowed=%v, reason=%q)", allowed, reason)
	}
}
