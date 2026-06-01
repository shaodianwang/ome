package workload_test

import (
	"testing"

	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload"
)

// TestPerComponentMaxSurgeBudget_Integer covers the int form across
// the documented edge cases. nil → BudgetNoLimit so the dispatcher
// defers to the coordination-group layer; 0 → 0 (RecreatePod-like
// behavior for this layer); positive integers pass through verbatim.
func TestPerComponentMaxSurgeBudget_Integer(t *testing.T) {
	for _, tc := range []struct {
		name     string
		input    *workload.RollingUpdate
		replicas int32
		want     int32
	}{
		{
			name:     "nil RollingUpdate returns BudgetNoLimit",
			input:    nil,
			replicas: 4,
			want:     workload.BudgetNoLimit,
		},
		{
			name:     "nil MaxSurge returns BudgetNoLimit",
			input:    &workload.RollingUpdate{},
			replicas: 4,
			want:     workload.BudgetNoLimit,
		},
		{
			name:     "MaxSurge=1 returns 1 regardless of replicas",
			input:    &workload.RollingUpdate{MaxSurge: intOrStringInt(1)},
			replicas: 5,
			want:     1,
		},
		{
			name:     "MaxSurge=0 returns 0 (no surge allowed by this layer)",
			input:    &workload.RollingUpdate{MaxSurge: intOrStringInt(0)},
			replicas: 5,
			want:     0,
		},
		{
			name:     "negative MaxSurge clamps to 0 (defensive)",
			input:    &workload.RollingUpdate{MaxSurge: intOrStringInt(-3)},
			replicas: 5,
			want:     0,
		},
		{
			name:     "MaxSurge=N permitted larger than replicas",
			input:    &workload.RollingUpdate{MaxSurge: intOrStringInt(10)},
			replicas: 3,
			want:     10,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := workload.PerComponentMaxSurgeBudget(tc.input, tc.replicas)
			if got != tc.want {
				t.Errorf("got %d, want %d", got, tc.want)
			}
		})
	}
}

// TestPerComponentMaxSurgeBudget_Percent covers the percent form with
// ceil rounding so a 25% expression on 4 replicas yields 1 (not 0). The
// task spec calls out this specific case as the expected behavior.
func TestPerComponentMaxSurgeBudget_Percent(t *testing.T) {
	for _, tc := range []struct {
		name     string
		pct      string
		replicas int32
		want     int32
	}{
		{name: "25% on 4 replicas → 1 (ceil)", pct: "25%", replicas: 4, want: 1},
		{name: "25% on 8 replicas → 2", pct: "25%", replicas: 8, want: 2},
		{name: "50% on 10 replicas → 5", pct: "50%", replicas: 10, want: 5},
		{name: "100% on 4 replicas → 4 (capped at total)", pct: "100%", replicas: 4, want: 4},
		{name: "0% on N replicas → 0", pct: "0%", replicas: 10, want: 0},
		{name: "33% on 6 replicas → 2 (ceil of 1.98)", pct: "33%", replicas: 6, want: 2},
		{name: "1% on 1 replica → 1 (ceil of 0.01)", pct: "1%", replicas: 1, want: 1},
		{name: "25% on 0 replicas → 0 (zero base)", pct: "25%", replicas: 0, want: 0},
		{name: "150% clamps to 100% → equals replicas", pct: "150%", replicas: 5, want: 5},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ru := &workload.RollingUpdate{MaxSurge: intOrStringStr(tc.pct)}
			got := workload.PerComponentMaxSurgeBudget(ru, tc.replicas)
			if got != tc.want {
				t.Errorf("got %d, want %d", got, tc.want)
			}
		})
	}
}

// TestPerComponentMaxUnavailableBudget_Integer mirrors the surge
// integer case for the unavailability budget.
func TestPerComponentMaxUnavailableBudget_Integer(t *testing.T) {
	for _, tc := range []struct {
		name     string
		input    *workload.RollingUpdate
		replicas int32
		want     int32
	}{
		{
			name:     "nil RollingUpdate returns BudgetNoLimit",
			input:    nil,
			replicas: 4,
			want:     workload.BudgetNoLimit,
		},
		{
			name:     "nil MaxUnavailable returns BudgetNoLimit",
			input:    &workload.RollingUpdate{MaxSurge: intOrStringInt(1)},
			replicas: 4,
			want:     workload.BudgetNoLimit,
		},
		{
			name:     "MaxUnavailable=2 returns 2",
			input:    &workload.RollingUpdate{MaxUnavailable: intOrStringInt(2)},
			replicas: 5,
			want:     2,
		},
		{
			name:     "MaxUnavailable=0 returns 0",
			input:    &workload.RollingUpdate{MaxUnavailable: intOrStringInt(0)},
			replicas: 5,
			want:     0,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := workload.PerComponentMaxUnavailableBudget(tc.input, tc.replicas)
			if got != tc.want {
				t.Errorf("got %d, want %d", got, tc.want)
			}
		})
	}
}

// TestPerComponentMaxUnavailableBudget_Percent mirrors the surge
// percent case for the unavailability budget.
func TestPerComponentMaxUnavailableBudget_Percent(t *testing.T) {
	ru := &workload.RollingUpdate{MaxUnavailable: intOrStringStr("25%")}
	got := workload.PerComponentMaxUnavailableBudget(ru, 4)
	if got != 1 {
		t.Errorf("25%% on 4 replicas: got %d, want 1", got)
	}
}

// TestCurrentSurgeInFlight verifies the per-Instance step-set counter
// the dispatcher feeds into the budget projection. Only Surge /
// SurgeDrain steps contribute; other ops (Drain, InPlace) don't add
// pods.
func TestCurrentSurgeInFlight(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   []workload.InstanceStatus
		want int32
	}{
		{name: "empty", in: nil, want: 0},
		{name: "no operation", in: []workload.InstanceStatus{
			{Index: 0, Phase: workload.InstancePhaseReady},
		}, want: 0},
		{name: "one Surge step", in: []workload.InstanceStatus{
			{Index: 0, Operation: &workload.InstanceOperation{Step: "Surge"}},
		}, want: 1},
		{name: "one SurgeDrain step", in: []workload.InstanceStatus{
			{Index: 0, Operation: &workload.InstanceOperation{Step: "SurgeDrain"}},
		}, want: 1},
		{name: "two surge variants and one in-place skipped", in: []workload.InstanceStatus{
			{Index: 0, Operation: &workload.InstanceOperation{Step: "Surge"}},
			{Index: 1, Operation: &workload.InstanceOperation{Step: "SurgeDrain"}},
			{Index: 2, Operation: &workload.InstanceOperation{Step: "InPlace"}}, // skipped
			{Index: 3, Operation: &workload.InstanceOperation{Step: "Drain"}},   // skipped
		}, want: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := workload.CurrentSurgeInFlight(tc.in)
			if got != tc.want {
				t.Errorf("got %d, want %d", got, tc.want)
			}
		})
	}
}

// TestCurrentUnavailableInFlight verifies the dual counter — Updating
// minus the Surge / SurgeDrain steps (which don't take a pod offline).
func TestCurrentUnavailableInFlight(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   []workload.InstanceStatus
		want int32
	}{
		{name: "empty", in: nil, want: 0},
		{name: "Ready not counted", in: []workload.InstanceStatus{
			{Index: 0, Phase: workload.InstancePhaseReady},
		}, want: 0},
		{name: "Updating + InPlace counted", in: []workload.InstanceStatus{
			{Index: 0, Phase: workload.InstancePhaseUpdating, Operation: &workload.InstanceOperation{Step: "InPlace"}},
		}, want: 1},
		{name: "Updating + Drain counted", in: []workload.InstanceStatus{
			{Index: 0, Phase: workload.InstancePhaseUpdating, Operation: &workload.InstanceOperation{Step: "Drain"}},
		}, want: 1},
		{name: "Updating + Surge skipped (no offline pod)", in: []workload.InstanceStatus{
			{Index: 0, Phase: workload.InstancePhaseUpdating, Operation: &workload.InstanceOperation{Step: "Surge"}},
		}, want: 0},
		{name: "Updating + SurgeDrain skipped", in: []workload.InstanceStatus{
			{Index: 0, Phase: workload.InstancePhaseUpdating, Operation: &workload.InstanceOperation{Step: "SurgeDrain"}},
		}, want: 0},
		{name: "Updating without Operation counted (defensive)", in: []workload.InstanceStatus{
			{Index: 0, Phase: workload.InstancePhaseUpdating},
		}, want: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := workload.CurrentUnavailableInFlight(tc.in)
			if got != tc.want {
				t.Errorf("got %d, want %d", got, tc.want)
			}
		})
	}
}

// Helpers for IntOrString construction in tests.

func intOrStringInt(v int) *intstr.IntOrString {
	x := intstr.FromInt(v)
	return &x
}

func intOrStringStr(s string) *intstr.IntOrString {
	x := intstr.FromString(s)
	return &x
}
