package routing

import (
	"testing"

	"k8s.io/apimachinery/pkg/api/resource"
)

// factor is a small helper to build a *resource.Quantity from a decimal string.
func factor(s string) *resource.Quantity {
	q := resource.MustParse(s)
	return &q
}

func TestWeights(t *testing.T) {
	tests := []struct {
		name  string
		homes []Home
		want  []int32
	}{
		{
			name:  "empty input yields empty output",
			homes: nil,
			want:  []int32{},
		},
		{
			name:  "single healthy home always weight 1",
			homes: []Home{{Cluster: "a", Allocated: 7, Ready: 3}},
			want:  []int32{1},
		},
		{
			name: "equal allocation and factor splits evenly",
			homes: []Home{
				{Cluster: "a", Allocated: 4, Ready: 4},
				{Cluster: "b", Allocated: 4, Ready: 4},
			},
			want: []int32{1, 1},
		},
		{
			name: "proportional to allocation reduced by gcd",
			homes: []Home{
				{Cluster: "a", Allocated: 5, Ready: 5},
				{Cluster: "b", Allocated: 2, Ready: 2},
			},
			want: []int32{5, 2},
		},
		{
			name: "allocation reduced to smallest whole ratio",
			homes: []Home{
				{Cluster: "a", Allocated: 50, Ready: 10},
				{Cluster: "b", Allocated: 30, Ready: 10},
			},
			want: []int32{5, 3},
		},
		{
			name: "heterogeneous hardware weighted by capacity factor",
			homes: []Home{
				{Cluster: "gb300", Allocated: 100, Ready: 100, Factor: factor("2")},
				{Cluster: "tpu", Allocated: 100, Ready: 100, Factor: factor("1")},
			},
			want: []int32{2, 1},
		},
		{
			name: "fractional factor kept exact via milli scale",
			homes: []Home{
				{Cluster: "gb300", Allocated: 100, Ready: 100, Factor: factor("1")},
				{Cluster: "tpu", Allocated: 100, Ready: 100, Factor: factor("0.5")},
			},
			want: []int32{2, 1},
		},
		{
			name: "unhealthy home is gated to zero, others keep ratio",
			homes: []Home{
				{Cluster: "a", Allocated: 6, Ready: 6},
				{Cluster: "b", Allocated: 4, Ready: 0},
				{Cluster: "c", Allocated: 2, Ready: 2},
			},
			want: []int32{3, 0, 1},
		},
		{
			name: "all homes unhealthy falls back to equal weight",
			homes: []Home{
				{Cluster: "a", Allocated: 5, Ready: 0},
				{Cluster: "b", Allocated: 3, Ready: 0},
			},
			want: []int32{1, 1},
		},
		{
			name: "nothing allocated falls back to equal weight",
			homes: []Home{
				{Cluster: "a", Allocated: 0, Ready: 4},
				{Cluster: "b", Allocated: 0, Ready: 4},
			},
			want: []int32{1, 1},
		},
		{
			name: "nil factor treated as identity",
			homes: []Home{
				{Cluster: "a", Allocated: 3, Ready: 3, Factor: nil},
				{Cluster: "b", Allocated: 1, Ready: 1, Factor: factor("1")},
			},
			want: []int32{3, 1},
		},
		{
			name: "non-positive factor treated as identity",
			homes: []Home{
				{Cluster: "a", Allocated: 2, Ready: 2, Factor: factor("0")},
				{Cluster: "b", Allocated: 2, Ready: 2, Factor: factor("-3")},
			},
			want: []int32{1, 1},
		},
		{
			name: "heterogeneous with differing allocation and factor",
			homes: []Home{
				// 4 replicas * 3.0 = 12 ; 6 replicas * 1.0 = 6 -> 2:1
				{Cluster: "fast", Allocated: 4, Ready: 4, Factor: factor("3")},
				{Cluster: "slow", Allocated: 6, Ready: 6, Factor: factor("1")},
			},
			want: []int32{2, 1},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Weights(tt.homes)
			if len(got) != len(tt.want) {
				t.Fatalf("len(Weights) = %d, want %d (%v)", len(got), len(tt.want), got)
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Errorf("Weights[%d] = %d, want %d (full: got %v want %v)", i, got[i], tt.want[i], got, tt.want)
				}
			}
		})
	}
}

func TestGCD(t *testing.T) {
	tests := []struct {
		a, b, want int64
	}{
		{0, 0, 0},
		{0, 5, 5},
		{5, 0, 5},
		{12, 8, 4},
		{100000, 50000, 50000},
		{7, 3, 1},
	}
	for _, tt := range tests {
		if got := gcd(tt.a, tt.b); got != tt.want {
			t.Errorf("gcd(%d, %d) = %d, want %d", tt.a, tt.b, got, tt.want)
		}
	}
}
