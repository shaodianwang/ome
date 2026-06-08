package workload

import "testing"

// TestSkipAsHeld guards the canary surge-stranding fix: a held Instance that is
// already mid-surge must NOT be skipped (it has to finish, else its surge pod
// strands — never promoted to serving — and permanently consumes the maxSurge
// budget, deadlocking the roll). Every other held Instance is skipped.
func TestSkipAsHeld(t *testing.T) {
	p := func(v int32) *int32 { return &v }
	held := &RollingUpdate{Partition: p(2)} // holds Instances with index 0,1
	cases := []struct {
		name string
		ru   *RollingUpdate
		idx  int32
		obs  *InstanceStatus
		want bool
	}{
		{"nil rollingUpdate → never held", nil, 0, nil, false},
		{"index >= partition → not held, rolls normally", held, 2, nil, false},
		{"held + no observed status → skip", held, 0, nil, true},
		{"held + not updating → skip", held, 1, &InstanceStatus{}, true},
		{"held + mid-surge → must finish, do NOT skip", held, 0, &InstanceStatus{Phase: InstancePhaseUpdating}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := skipAsHeld(tc.ru, tc.idx, tc.obs); got != tc.want {
				t.Errorf("skipAsHeld = %v, want %v", got, tc.want)
			}
		})
	}
}
