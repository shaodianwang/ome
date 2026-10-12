package replay_test

import (
	"context"
	"fmt"
	"os"
	"sort"
	"testing"

	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/replay"
)

// readingFinding is one disagreement with the scenario and tick it was
// made in.
type readingFinding struct {
	scenario string
	tick     int
	replay.ReadingFinding
}

// knownReadingClasses are the disagreement classes the corpus shows today,
// each with one line saying what the two readers say differently. This is
// the advisory phase: a class in the list is reported and counted, a class
// outside it fails the test. A class leaves the list when the reader it
// names moves onto the reading.
var knownReadingClasses = map[string]string{
	// query.PodSetFullyServing reads the promotion bar, the reading the
	// rotation bar.
	"fullyServing/query=true/reading=false": "the set serves on its containers and the gate, but the kubelet has not folded the gate into PodReady yet; the replay folds PodReady only on a pod.ready event, so a parked set stays in this shape for the rest of its scenario",

	// coordination.PodReadyAndServing counts PodReady alone and reads a
	// missing gate as serving; podreadiness.ReadyAndServing and the
	// reading need the gate.
	"podReadyAndServing/coordination=true/podreadiness=false": "a pod the scenario reported Ready before the engine wrote its gate; a real kubelet holds PodReady False until the declared gate is True, so the replay's pod.ready event is what makes this shape",
	"podReadyAndServing/coordination=false/podreadiness=true": "a pod being deleted, or one reported Ready while still Pending, whose conditions still read healthy: coordination excludes it on its deletion timestamp or phase, podreadiness counts its conditions; the reading excludes a deleting pod and ignores the phase",

	// The persisted serving count is the publication's count of the pod
	// set the pass started from, read one pass later; the reading counts
	// the pods as they are now.
	"persistedServingCount/row<reading": "the pods entered rotation after the pass's starting pod set was counted: the gate was written or PodReady folded this pass; the published count trails by one pass",
	"persistedServingCount/row>reading": "the pods left rotation after the pass's starting pod set was counted: a drain took the gate off, a delete or a crash took the pod; the published count trails by one pass",

	// types.RecreateOfDarkRow reads the persisted count on a Failed row;
	// the reading reads this pass's routable members (OutOfRotation).
	"recreateOfDarkRow/row=true/reading=false": "a Failed row whose pods are all being deleted, or whose pod came back into rotation after the count: the counter reads a set with nothing serving as dark; the reading needs a live member before it calls a set out of rotation, and a pod being deleted is not one",
	"recreateOfDarkRow/row=false/reading=true": "a gang whose leader stopped serving while a worker still carries PodReady and the gate, or a pod that left rotation after the count: the counter counts every pod of the set, the reading counts the routable members alone, and no Service publishes a worker",
}

// TestReadingBesideTodaysReadersOverTheCorpus replays every scenario with
// the reading comparison on. Two things are proven: the trace is
// byte-identical to the golden, so the observer changes nothing the engine
// writes; and every disagreement between a reader the passes use today and
// the one reading falls in a known class.
func TestReadingBesideTodaysReadersOverTheCorpus(t *testing.T) {
	var (
		ticks    int
		findings []readingFinding
	)
	for _, tc := range loadScenarios(t) {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			observe := func(view replay.TickView) {
				ticks++
				for _, finding := range replay.CompareReadings(view) {
					findings = append(findings, readingFinding{scenario: tc.name, tick: view.Tick, ReadingFinding: finding})
				}
			}
			trace, err := replay.Run(context.Background(), tc.scenario, replay.Options{Observer: observe})
			if err != nil {
				t.Fatalf("run with the observer on: %v\npartial trace:\n%s", err, trace)
			}
			want, err := os.ReadFile(tc.golden)
			if err != nil {
				t.Fatalf("read golden: %v", err)
			}
			if trace != string(want) {
				t.Fatalf("the observer changed the trace of %s\n--- want ---\n%s--- got ---\n%s", tc.name, want, trace)
			}
		})
	}
	byClass := map[string][]readingFinding{}
	for _, finding := range findings {
		byClass[finding.Class] = append(byClass[finding.Class], finding)
	}
	t.Logf("the reading beside today's readers over the replay corpus: %d ticks compared, %d disagreements in %d classes",
		ticks, len(findings), len(byClass))
	classes := make([]string, 0, len(byClass))
	for class := range byClass {
		classes = append(classes, class)
	}
	sort.Strings(classes)
	unknown := 0
	for _, class := range classes {
		list := byClass[class]
		ticksSeen := map[string]struct{}{}
		scenarios := map[string]struct{}{}
		for _, finding := range list {
			ticksSeen[fmt.Sprintf("%s@%d", finding.scenario, finding.tick)] = struct{}{}
			scenarios[finding.scenario] = struct{}{}
		}
		note, known := knownReadingClasses[class]
		if !known {
			note = "UNKNOWN"
			unknown++
		}
		example := list[0]
		t.Logf("class %s (%d findings, %d ticks, %d scenarios): %s\n  reader: %s\n  e.g. %s@%d %s",
			class, len(list), len(ticksSeen), len(scenarios), note, example.Reader, example.scenario, example.tick, example.Detail)
	}
	if unknown > 0 {
		t.Fatalf("%d disagreement class(es) are not accounted for; see the log", unknown)
	}
}
