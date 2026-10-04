package kindutil

import (
	"context"
	"strings"
	"testing"

	"github.com/ome-projects/ome/pkg/constants"
)

// The model-Ready node label a fixture stamps must be the exact key the
// controller selects pods on. A ClusterBaseModel name past the label's
// budget is bounded with a hash on the pod side, so a fixture that spells
// the key out literally leaves every pod of such a model unschedulable.
func TestLabelClusterBaseModelReadyStampsTheKeyPodsSelectOn(t *testing.T) {
	for _, name := range []string{
		"llama",
		strings.Repeat("m", 32),
		strings.Repeat("m", 33),
		"crash-after-ready-gang-surgethendrain-cbm",
	} {
		t.Run(name, func(t *testing.T) {
			c := clientFor(fixtureNode())
			if err := LabelClusterBaseModelReady(context.Background(), c, name); err != nil {
				t.Fatal(err)
			}
			want := constants.GetClusterBaseModelLabel(name)
			if got := getNode(t, c).Labels[want]; got != "Ready" {
				t.Fatalf("node labels %v carry no %s=Ready: a pod selecting on that key cannot schedule", getNode(t, c).Labels, want)
			}
		})
	}
}

func TestLabelBaseModelReadyStampsTheKeyPodsSelectOn(t *testing.T) {
	for _, tc := range []struct{ namespace, name string }{
		{"team-a", "llama"},
		{"team-a", strings.Repeat("m", 40)},
		{strings.Repeat("n", 30), strings.Repeat("m", 30)},
	} {
		t.Run(tc.namespace+"/"+tc.name, func(t *testing.T) {
			c := clientFor(fixtureNode())
			if err := LabelBaseModelReady(context.Background(), c, tc.namespace, tc.name); err != nil {
				t.Fatal(err)
			}
			want := constants.GetBaseModelLabel(tc.namespace, tc.name)
			if got := getNode(t, c).Labels[want]; got != "Ready" {
				t.Fatalf("node labels %v carry no %s=Ready: a pod selecting on that key cannot schedule", getNode(t, c).Labels, want)
			}
		})
	}
}
