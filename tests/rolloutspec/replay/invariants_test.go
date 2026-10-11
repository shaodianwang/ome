package replay_test

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"

	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/replay"
)

// invariantFinding is one finding with the scenario and tick it was made in.
type invariantFinding struct {
	scenario string
	tick     int
	replay.Finding
}

// knownInvariantClasses are the finding classes the corpus shows today,
// each with one line saying what the class means. This is the advisory
// phase: a class in the list is reported and counted, a class outside it
// fails the test. A later change empties the list.
var knownInvariantClasses = map[string]string{
	// I1: the pods missing from the floor were lost to a crash, a deletion
	// or a first materialization, never to a drain the engine chose.
	"I1/below-floor[Create/CreatePods]":                        "Instances created for the first time, or rebuilt from nothing after a loss, serve nothing until their pods come up",
	"I1/below-floor[Failed,Update/GangSurgeTarget]":            "a gang surge whose replacement leader was lost after the source had drained behind it; the source is stamped Failed and the replacement is rebuilt",
	"I1/below-floor[Failed]":                                   "an Instance whose promoted pod crashed on a held revision stays Failed until the ladder or the operator moves it",
	"I1/below-floor[Ready,Update/GangSurgeTarget]":             "a Ready Instance whose pod crashed out of rotation beside a replacement gang still coming up",
	"I1/below-floor[Ready]":                                    "a promoted pod crashed under load and left rotation on its own; the Instance reads Ready until the restart trigger or the ladder acts",
	"I1/below-floor[Restart/Drain,Update/GangSurgeTarget]":     "a restart drains a pod that had already crashed while a replacement gang is still coming up",
	"I1/below-floor[Restart/Drain]":                            "a restart drains a pod that had already crashed out of rotation; the shortfall predates the drain",
	"I1/below-floor[Update/GangSurgeTarget,Update/SurgeDrain]": "a gang surge whose replacement leader was deleted or exited after the source drained behind it; the member is recycled and rebuilt while the source stays drained",
	"I1/below-floor[Update/Surge]":                             "a crashed Instance re-surges toward the fix; its slot was already dark when the surge opened",

	// I3: the escalation exempts an attempt whose Instance still serves.
	"I3/overdue-unexplained[Update/SurgeDrain]": "a surge past its deadline whose replacement already serves beside the source: the escalation exempts a serving set and the attempt runs on with no wait token and no re-armed deadline",
	"I3/overdue-unexplained[Update/Surge]":      "a surge past its deadline whose replacement is deleting while the source alone keeps the set serving: the escalation exempts it and nothing names the wait",

	// I4: Failed records a spent attempt, not the pods still serving.
	"I4/failed-row-serving[Create/CreatePods]":      "a gang under a PodGroup another controller owns: its leader serves on a gate another writer wrote, and no pass adopts or promotes members under a foreign group",
	"I4/failed-row-serving[Migrate/CreateSurge]":    "a migration surge row disposed Failed whose pod later reports Ready and serving; no pass re-drives a Failed row on those observations",
	"I4/failed-row-serving[Restart/Drain]":          "a parked repair resumed: the pod reports Ready and the resume writes the serving gate, but waits out the availability window or a status conflict before it promotes",
	"I4/failed-row-serving[Update/Drain]":           "an attempt ended by the gang ownership verdict: its members come up and serve, but no pass re-drives a row the verdict ended",
	"I4/failed-row-serving[Update/GangSurgeTarget]": "a replacement gang stamped Failed by the deadline or a verdict while its members serve or return to serving; the pair is driven on from the source row and the replacement row stays Failed until the promote",
	"I4/failed-row-serving[Update/Surge]":           "a gang surge source stamped Failed when its replacement missed the deadline, with its own members still serving",
	"I4/failed-row-serving[no-operation]":           "an attempt disposed Failed with its operation cleared while the source pod keeps serving: a drain patch refused, a replacement that never served, a relocation directive recorded",

	// I4: a Ready row keeps its phase until something demotes it.
	"I4/ready-row-dark[no-pods]":           "a Ready row whose pods are all gone within one pass: the update was refused and the pass ended on the error, or an abandoned surge returned the row to Ready after its source was force-deleted, a pass before the fresh surge",
	"I4/ready-row-dark[pod-ready-pending]": "the engine promotes on ContainersReady plus its own serving mark; the kubelet folds the gate into PodReady a pass later",
	"I4/ready-row-dark[pods-not-serving]":  "a Ready row whose pod lost readiness, went Unknown, came back under a new UID or left rotation; a Ready row is demoted only when the restart policy or the stuck-pod grace says so",

	// I4: the published pod count is the pass's starting pod set, read
	// before the pass's own creates and deletes land, so it trails the live
	// pods by one pass whenever the pass changed the row's pods.
	"I4/podcount-differs[Create/CreatePods]":             "the create pass made or recycled the row's pods this pass; the published count trails by one pass",
	"I4/podcount-differs[Delete/Drain]":                  "the delete pass removed the row's pods this pass; the published count trails by one pass",
	"I4/podcount-differs[Failed]":                        "a member was rebuilt or a stuck pod reaped on a Failed row this pass; the published count trails by one pass",
	"I4/podcount-differs[Migrate/SurgeTarget]":           "the migration created its surge pod this pass; the published count trails by one pass",
	"I4/podcount-differs[Pending]":                       "the sweep removed a stuck pod of a demoted row this pass; the published count trails by one pass",
	"I4/podcount-differs[Restart/Drain]":                 "the restart deleted or recreated the row's pods this pass; the published count trails by one pass",
	"I4/podcount-differs[Update/Drain]":                  "the recreate deleted or recreated the row's pods this pass; the published count trails by one pass",
	"I4/podcount-differs[Update/GangSurgeTargetCleanup]": "the cleanup retired a replacement member this pass; the published count trails by one pass",
	"I4/podcount-differs[Update/GangSurgeTarget]":        "the gang surge created or recycled replacement members this pass; the published count trails by one pass",
	"I4/podcount-differs[Update/InPlace]":                "the in-place update recycled a terminal pod this pass; the published count trails by one pass",
	"I4/podcount-differs[Update/Parked]":                 "a pod of a parked attempt was force-deleted this pass; the published count trails by one pass",
	"I4/podcount-differs[Update/SurgeDrain]":             "the drain deleted the source pods this pass; the published count trails by one pass",
	"I4/podcount-differs[Update/Surge]":                  "the surge created its replacement pod this pass; the published count trails by one pass",

	// I5: a pass stamps first and the apiserver answers in the same pass,
	// or one owner takes two steps in one pass.
	"I5/identity-writes[Empty>Create/CreatePods>Failed]":                               "the create pass stamps the attempt and the apiserver refuses the pod create in the same pass, so the disposition records Failed behind the stamp",
	"I5/identity-writes[Failed>Create/CreatePods>Failed]":                              "a retried create stamps the attempt and the apiserver refuses the pod create in the same pass, so the disposition records Failed behind the stamp",
	"I5/identity-writes[Failed>Update/Surge>Update/SurgeDrain]":                        "a surge re-opened from Failed finds its replacement already runtime-ready and advances to the drain step in the same pass",
	"I5/identity-writes[Pending>Create/CreatePods>Failed]":                             "a recovery stamps the attempt and the apiserver refuses the pod create in the same pass, so the disposition records Failed behind the stamp",
	"I5/identity-writes[Ready>Restart/Drain>Failed]":                                   "a repair opens and the apiserver refuses its pod create in the same pass, so the disposition records Failed behind the stamp",
	"I5/identity-writes[Update/GangSurgeTarget>Update/GangSurgeTargetCleanup>Failed]":  "a replacement marker is claimed as a cleanup and its first delete is refused, or its deadline lands, in the same pass, so it is disposed Failed behind the claim",
	"I5/identity-writes[Update/GangSurgeTarget>Update/GangSurgeTargetCleanup>removed]": "an abandoned gang surge claims its marker as a cleanup and, with nothing left to retire, removes it in the same pass",
	"I5/identity-writes[Update/SurgeDrain>Failed>Failed]":                              "the ladder's verdict stamps the source Failed with the attempt kept, then the pair is abandoned and the attempt cleared in the same pass",
}

// TestOperatorInvariantsOverTheCorpus replays every scenario with the
// invariant observer on. Two things are proven: the trace is byte-identical
// to the golden, so the observer changes nothing the engine writes; and
// every finding falls in a known class.
func TestOperatorInvariantsOverTheCorpus(t *testing.T) {
	var (
		ticks    int
		findings []invariantFinding
	)
	for _, tc := range loadScenarios(t) {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			observe := func(view replay.TickView) {
				ticks++
				for _, finding := range replay.CheckInvariants(view) {
					findings = append(findings, invariantFinding{scenario: tc.name, tick: view.Tick, Finding: finding})
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
	byClass := map[string][]invariantFinding{}
	for _, finding := range findings {
		byClass[finding.Class] = append(byClass[finding.Class], finding)
	}
	t.Logf("operator invariants over the replay corpus: %d ticks checked, %d findings in %d classes",
		ticks, len(findings), len(byClass))
	classes := make([]string, 0, len(byClass))
	for class := range byClass {
		classes = append(classes, class)
	}
	sort.Strings(classes)
	unknown := 0
	for _, class := range classes {
		list := byClass[class]
		where := map[string]int{}
		for _, finding := range list {
			where[fmt.Sprintf("%s@%d", finding.scenario, finding.tick)]++
		}
		places := make([]string, 0, len(where))
		for place := range where {
			places = append(places, place)
		}
		sort.Strings(places)
		note, known := knownInvariantClasses[class]
		if !known {
			note = "UNKNOWN"
			unknown++
		}
		t.Logf("class %s (%d): %s\n  where: %s\n  e.g. %s",
			class, len(list), note, strings.Join(places, " "), list[0].Detail)
	}
	if unknown > 0 {
		t.Fatalf("%d finding class(es) are not accounted for; see the log", unknown)
	}
}
