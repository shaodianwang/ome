package topology

import (
	"reflect"
	"testing"
)

// TestCountFree pins the accounting that feeds BestFit: count free whole nodes
// per domain. Occupied nodes don't add to the count but their domain still
// appears (with its real free count, possibly 0) so callers know the full
// domain set — a domain that exists but is full is different from one that
// doesn't exist. Nodes with no domain label are not part of any domain and are
// ignored.
func TestCountFree(t *testing.T) {
	cases := []struct {
		name  string
		nodes []Node
		want  FreeByDomain
	}{
		{"empty", nil, FreeByDomain{}},
		{
			"mixed free/occupied across domains",
			[]Node{
				{Domain: "a", Free: true},
				{Domain: "a", Free: true},
				{Domain: "a", Free: false}, // occupied — doesn't count
				{Domain: "b", Free: true},
				{Domain: "c", Free: false}, // domain exists but full -> c:0
				{Domain: "", Free: true},   // no topology label -> ignored
			},
			FreeByDomain{"a": 2, "b": 1, "c": 0},
		},
		{
			"all free single domain",
			[]Node{{Domain: "d", Free: true}, {Domain: "d", Free: true}},
			FreeByDomain{"d": 2},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := CountFree(tc.nodes)
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("CountFree() = %v, want %v", got, tc.want)
			}
		})
	}
}
