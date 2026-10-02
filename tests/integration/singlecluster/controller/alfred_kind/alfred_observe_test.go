package alfred_kind

import (
	"bytes"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

// ---- metrics ---------------------------------------------------------------

type metricSample struct {
	Labels map[string]string
	Value  float64
}

// metricSet is one scrape: sample name -> samples. A histogram appears under
// its family name (the count) and under its _count and _sum wire names.
type metricSet map[string][]metricSample

func alfredPods(g gomega.Gomega) []corev1.Pod {
	pods := &corev1.PodList{}
	g.ExpectWithOffset(1, k8sClient.List(suiteCtx, pods, client.InNamespace(alfredNamespace),
		client.MatchingLabels{alfredPodLabelKey: alfredPodLabelValue})).To(gomega.Succeed())
	var live []corev1.Pod
	for _, p := range pods.Items {
		if p.DeletionTimestamp == nil && p.Status.Phase == corev1.PodRunning {
			live = append(live, p)
		}
	}
	return live
}

// alfredPodState is what a crash or a replacement of an alfred pod changes:
// its identity and its container restart count.
type alfredPodState struct {
	uid      types.UID
	restarts int32
}

// alfredPodStates keys the running alfred pods' states by pod name.
func alfredPodStates(g gomega.Gomega) map[string]alfredPodState {
	out := map[string]alfredPodState{}
	for _, p := range alfredPods(g) {
		out[p.Name] = alfredPodState{uid: p.UID, restarts: restartCount(&p)}
	}
	return out
}

// scrapePod reads /metrics of one alfred pod through the API server proxy.
func scrapePod(g gomega.Gomega, pod string) metricSet {
	raw, err := clientset.CoreV1().RESTClient().Get().
		Namespace(alfredNamespace).Resource("pods").
		Name(fmt.Sprintf("%s:%d", pod, alfredMetricsPort)).
		SubResource("proxy").Suffix("metrics").DoRaw(suiteCtx)
	g.ExpectWithOffset(1, err).NotTo(gomega.HaveOccurred(), "scrape %s", pod)
	parser := expfmt.NewTextParser(model.UTF8Validation)
	families, err := parser.TextToMetricFamilies(bytes.NewReader(raw))
	g.ExpectWithOffset(1, err).NotTo(gomega.HaveOccurred(), "parse metrics of %s", pod)
	out := metricSet{}
	for name, fam := range families {
		for _, m := range fam.GetMetric() {
			labels := map[string]string{}
			for _, lp := range m.GetLabel() {
				labels[lp.GetName()] = lp.GetValue()
			}
			var v float64
			switch fam.GetType() {
			case dto.MetricType_COUNTER:
				v = m.GetCounter().GetValue()
			case dto.MetricType_GAUGE:
				v = m.GetGauge().GetValue()
			case dto.MetricType_UNTYPED:
				v = m.GetUntyped().GetValue()
			case dto.MetricType_HISTOGRAM:
				h := m.GetHistogram()
				v = float64(h.GetSampleCount())
				out[name+"_count"] = append(out[name+"_count"], metricSample{Labels: labels, Value: v})
				out[name+"_sum"] = append(out[name+"_sum"], metricSample{Labels: labels, Value: h.GetSampleSum()})
			default:
				continue
			}
			out[name] = append(out[name], metricSample{Labels: labels, Value: v})
		}
	}
	return out
}

// value returns the first sample of name whose labels include match.
func (m metricSet) value(name string, match map[string]string) (float64, bool) {
	for _, s := range m[name] {
		if labelsMatch(s.Labels, match) {
			return s.Value, true
		}
	}
	return 0, false
}

// sum adds every sample of name whose labels include match.
func (m metricSet) sum(name string, match map[string]string) float64 {
	var total float64
	for _, s := range m[name] {
		if labelsMatch(s.Labels, match) {
			total += s.Value
		}
	}
	return total
}

func labelsMatch(labels, match map[string]string) bool {
	for k, v := range match {
		if labels[k] != v {
			return false
		}
	}
	return true
}

// leaderPod returns the alfred pod whose alfred_leader_status is 1.
func leaderPod(g gomega.Gomega) corev1.Pod {
	pods := alfredPods(g)
	g.ExpectWithOffset(1, pods).NotTo(gomega.BeEmpty(), "no running alfred pods")
	for _, p := range pods {
		if v, ok := scrapePod(g, p.Name).value("alfred_leader_status", nil); ok && v == 1 {
			return p
		}
	}
	g.ExpectWithOffset(1, false).To(gomega.BeTrue(), "no alfred pod reports leadership")
	return corev1.Pod{}
}

// leaderCount is the number of running alfred pods whose alfred_leader_status
// reads 1. The gauge is set once, when a replica wins the Lease, so only a
// pod that is gone stops counting.
func leaderCount(g gomega.Gomega) int {
	n := 0
	for _, p := range alfredPods(g) {
		if v, ok := scrapePod(g, p.Name).value("alfred_leader_status", nil); ok && v == 1 {
			n++
		}
	}
	return n
}

func scrapeLeader(g gomega.Gomega) metricSet { return scrapePod(g, leaderPod(g).Name) }

// reloadCount is the leader's policy reload count for one outcome; a series
// that has never been incremented reads as zero.
func reloadCount(g gomega.Gomega, outcome string) float64 {
	v, _ := scrapeLeader(g).value("alfred_policy_reload_total", map[string]string{"outcome": outcome})
	return v
}

// reloadSuccesses is the leader's successful policy reload count.
func reloadSuccesses(g gomega.Gomega) float64 { return reloadCount(g, "success") }

// reloadFailures is the leader's rejected policy reload count.
func reloadFailures(g gomega.Gomega) float64 { return reloadCount(g, "failure") }

// reloadCountEventually reads reloadCount with retries, so one pod-proxy
// hiccup does not fail the spec.
func reloadCountEventually(outcome string) float64 {
	var v float64
	gomega.EventuallyWithOffset(1, func(g gomega.Gomega) {
		v = reloadCount(g, outcome)
	}).WithTimeout(kindShortTimeout).WithPolling(kindPollInterval).Should(gomega.Succeed())
	return v
}

// ---- recommendations ConfigMap ---------------------------------------------

type schedulingDiag struct {
	Status string `json:"status"`
	Reason string `json:"reason"`
}

type recommendation struct {
	Workload       string          `json:"workload"`
	Component      string          `json:"component"`
	Instance       int32           `json:"instance"`
	Policy         string          `json:"policy"`
	Reason         string          `json:"reason"`
	Outcome        string          `json:"outcome"`
	AdvisoryReason string          `json:"advisoryReason,omitempty"`
	RejectReason   string          `json:"rejectReason,omitempty"`
	FromNode       string          `json:"fromNode,omitempty"`
	Target         string          `json:"target,omitempty"`
	HintTargets    []string        `json:"hintTargets,omitempty"`
	Score          float64         `json:"score"`
	Emergency      bool            `json:"emergency,omitempty"`
	CooldownOver   bool            `json:"cooldownOverridden,omitempty"`
	DispatchStatus string          `json:"dispatchStatus,omitempty"`
	RequestUUID    string          `json:"requestUUID,omitempty"`
	DispatchReason string          `json:"dispatchReason,omitempty"`
	Scheduling     *schedulingDiag `json:"scheduling,omitempty"`
}

type lastCycle struct {
	Timestamp       time.Time        `json:"timestamp"`
	Mode            string           `json:"mode"`
	Recommendations []recommendation `json:"recommendations"`
}

func recsConfigMap(g gomega.Gomega) *corev1.ConfigMap {
	cm := &corev1.ConfigMap{}
	g.ExpectWithOffset(1, k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: alfredNamespace, Name: alfredRecsConfigMap}, cm)).To(gomega.Succeed())
	return cm
}

// recommendations parses the last decision cycle; empty before the first one.
func recommendations(g gomega.Gomega) lastCycle {
	raw, ok := recsConfigMap(g).Data[alfredRecsKey]
	if !ok || raw == "" {
		return lastCycle{}
	}
	var cycle lastCycle
	g.ExpectWithOffset(1, json.Unmarshal([]byte(raw), &cycle)).To(gomega.Succeed(), "parse %s", alfredRecsKey)
	return cycle
}

func recsFor(cycle lastCycle, workload string) []recommendation {
	var out []recommendation
	for _, r := range cycle.Recommendations {
		if r.Workload == workload {
			out = append(out, r)
		}
	}
	return out
}

// instanceRec returns the last cycle's row for one Component's Instance; a
// component-wide advisory carries index -1 and never matches a member index.
func instanceRec(recs []recommendation, component v1beta1.ComponentType, instance int32) (recommendation, bool) {
	for _, r := range recs {
		if r.Component == string(component) && r.Instance == instance {
			return r, true
		}
	}
	return recommendation{}, false
}

// rejectedWith matches a recommendation the arbiter rejected for reason.
func rejectedWith(reason string) gomega.OmegaMatcher {
	return gomega.SatisfyAll(gomega.HaveField("Outcome", "rejected"), gomega.HaveField("RejectReason", reason))
}

// heldBehindRequest matches the record of a finding held back by a request of
// the same workload: rejected WorkloadBusy in the pass that admits the request
// and, from the next pass until the migration completes, advisory
// OMENativeStateIneligible, since the workload has an active migration once
// the request annotation is on the ISVC. The admission-pass record lives one
// tick, so a read after the dispatch accepts either form; the durable verdict
// is the RecommendationRejected event.
func heldBehindRequest() gomega.OmegaMatcher {
	return gomega.SatisfyAny(
		rejectedWith(rejectWorkloadBusy),
		gomega.SatisfyAll(gomega.HaveField("Outcome", advisoryOutcome), gomega.HaveField("AdvisoryReason", ineligibleAdvisory)),
	)
}

type nodeMaintenanceRecord struct {
	Requested bool     `json:"requested"`
	Triggers  []string `json:"triggers"`
}

type nodeRecord struct {
	NodeUID                        types.UID             `json:"nodeUID"`
	State                          string                `json:"state"`
	SuspectUntil                   *time.Time            `json:"suspectUntil,omitempty"`
	Workloads                      []string              `json:"workloads"`
	OMEAcceleratorOccupantsPresent bool                  `json:"omeAcceleratorOccupantsPresent"`
	SignaledAt                     *time.Time            `json:"signaledAt,omitempty"`
	DrainedAt                      *time.Time            `json:"drainedAt,omitempty"`
	MaintenanceRequestedAt         *time.Time            `json:"maintenanceRequestedAt,omitempty"`
	MaintenanceDrainedAt           *time.Time            `json:"maintenanceDrainedAt,omitempty"`
	Maintenance                    nodeMaintenanceRecord `json:"maintenance"`
}

// nodeRecordFor finds the reporter's health/maintenance record for node. The
// record key is "node.<name>"; matching by suffix keeps the prefix alfred's
// business. A record that does not decode is a schema drift and fails g.
func nodeRecordFor(g gomega.Gomega, node string) (nodeRecord, bool) {
	for key, raw := range recsConfigMap(g).Data {
		if key == alfredRecsKey || !strings.HasSuffix(key, "."+node) {
			continue
		}
		var rec nodeRecord
		g.ExpectWithOffset(1, json.Unmarshal([]byte(raw), &rec)).To(gomega.Succeed(), "parse node record %s: %s", key, raw)
		return rec, true
	}
	return nodeRecord{}, false
}

// waitNodeRecordGone blocks until the reporter holds no record for node. The
// repair events (NodeRepairNeeded, NodeDrainedForRepair) and the maintenance
// events (NodeMaintenanceRequested, NodeDrainedForMaintenance) are emitted
// once per node record, and a record is dropped only once no signal matches,
// so a spec raises its signal only after this to keep its events out of an
// earlier record's dedup.
func waitNodeRecordGone(node string) {
	ginkgo.By("the earlier unhealthy episode's node record is gone")
	gomega.EventuallyWithOffset(1, func(g gomega.Gomega) {
		_, ok := nodeRecordFor(g, node)
		g.Expect(ok).To(gomega.BeFalse(), "node record for %s still present", node)
	}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(gomega.Succeed())
}

// ---- dispatch journal ------------------------------------------------------

type journalWorkload struct {
	Namespace string `json:"Namespace"`
	Name      string `json:"Name"`
}

type journalEntry struct {
	UUID           string          `json:"uuid"`
	Workload       journalWorkload `json:"workload"`
	Component      string          `json:"component"`
	Instance       int32           `json:"instance"`
	FromNode       string          `json:"fromNode"`
	Targets        []string        `json:"targets,omitempty"`
	Payload        string          `json:"payload"`
	CreatedAt      time.Time       `json:"createdAt"`
	LastAttempt    *time.Time      `json:"lastAttempt,omitempty"`
	AcknowledgedAt *time.Time      `json:"acknowledgedAt,omitempty"`
	CompletedAt    *time.Time      `json:"completedAt,omitempty"`
	Phase          string          `json:"phase"`
	Reason         string          `json:"reason,omitempty"`
}

type journalState struct {
	Version      string         `json:"version"`
	Entries      []journalEntry `json:"entries"`
	BackoffUntil *time.Time     `json:"backoffUntil,omitempty"`
}

func journalConfigMap(g gomega.Gomega) *corev1.ConfigMap {
	cm := &corev1.ConfigMap{}
	g.ExpectWithOffset(1, k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: alfredNamespace, Name: alfredJournalName}, cm)).To(gomega.Succeed())
	return cm
}

// journalRaw is the journal document as stored, "" while the key is absent.
func journalRaw(g gomega.Gomega) string {
	return journalConfigMap(g).Data[alfredJournalKey]
}

// journal decodes the dispatch journal. A document without a version is a
// schema drift: a renamed field would otherwise read as zero entries.
func journal(g gomega.Gomega) journalState {
	raw := journalRaw(g)
	var js journalState
	g.ExpectWithOffset(1, json.Unmarshal([]byte(raw), &js)).To(gomega.Succeed(), "parse journal")
	if raw != "" {
		g.ExpectWithOffset(1, js.Version).NotTo(gomega.BeEmpty(), "journal %s has no version", alfredJournalKey)
	}
	return js
}

func journalEntriesFor(js journalState, ns, name string) []journalEntry {
	var out []journalEntry
	for _, e := range js.Entries {
		if e.Workload.Namespace == ns && e.Workload.Name == name {
			out = append(out, e)
		}
	}
	return out
}

func inFlight(entries []journalEntry) []journalEntry {
	var out []journalEntry
	for _, e := range entries {
		if e.Phase == "prepared" || e.Phase == "submitted" || e.Phase == "acknowledged" {
			out = append(out, e)
		}
	}
	return out
}

// journalTerminal reports whether a journal entry has reached a terminal
// phase. Unlike inFlight, which excludes stalled entries, a stalled entry is
// deliberately not terminal here: it still holds the dispatcher's serial
// gate, which is what waitJournalSettled waits out.
func journalTerminal(e journalEntry) bool {
	return e.Phase == "completed" || e.Phase == "failed"
}

// waitJournalSettled blocks until every journal entry is terminal. The
// dispatcher submits nothing while any entry is unresolved and the arbiter's
// in-flight cap counts every workload in the cluster, so a leftover from an
// earlier spec would hold or turn away this spec's requests.
func waitJournalSettled() {
	ginkgo.By("no earlier request is left unresolved")
	gomega.EventuallyWithOffset(1, func(g gomega.Gomega) {
		var open []journalEntry
		for _, e := range journal(g).Entries {
			if !journalTerminal(e) {
				open = append(open, e)
			}
		}
		g.Expect(open).To(gomega.BeEmpty(), func() string { return "unresolved: " + describeJournal(open) })
	}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(gomega.Succeed())
}

// expectSerialJournal ends the enclosing wait at once when the journal breaks
// the dispatcher's serial contract: two unresolved entries at once anywhere in
// it (a stalled entry still holds the gate), or an entry of the workload that
// has failed or stalled outside the awaited phases. A verdict is not something
// to wait out.
func expectSerialJournal(js journalState, ns, name string, awaited ...string) {
	var open []journalEntry
	for _, e := range js.Entries {
		if !journalTerminal(e) {
			open = append(open, e)
		}
	}
	if len(open) > 1 {
		gomega.StopTrying("two requests unresolved at once: " + describeJournal(open)).Now()
	}
	for _, e := range journalEntriesFor(js, ns, name) {
		if (e.Phase == "failed" || e.Phase == "stalled") && !slices.Contains(awaited, e.Phase) {
			gomega.StopTrying(fmt.Sprintf("journal %s for %s/%s %s instance %d is %s: %s",
				e.UUID, ns, name, e.Component, e.Instance, e.Phase, e.Reason)).Now()
		}
	}
}

// journalEntriesWithin counts entries created in the trailing window, which is
// how the dispatcher's hourly budget counts the journal.
func journalEntriesWithin(js journalState, d time.Duration) int {
	cutoff := time.Now().Add(-d)
	n := 0
	for _, e := range js.Entries {
		if e.CreatedAt.After(cutoff) {
			n++
		}
	}
	return n
}

// writeJournal replaces state.json in one optimistic update. A write that
// races alfred's own save fails g, so callers retry it from an Eventually.
func writeJournal(g gomega.Gomega, raw string) {
	cm := journalConfigMap(g)
	if cm.Data == nil {
		cm.Data = map[string]string{}
	}
	cm.Data[alfredJournalKey] = raw
	g.ExpectWithOffset(1, k8sClient.Update(suiteCtx, cm)).To(gomega.Succeed(), "write %s", alfredJournalKey)
}

// setJournal writes raw over state.json, retrying a write that races alfred's
// own save.
func setJournal(raw string) {
	gomega.EventuallyWithOffset(1, func(g gomega.Gomega) {
		writeJournal(g, raw)
	}).WithTimeout(kindShortTimeout).WithPolling(kindPollInterval).Should(gomega.Succeed())
}

// seedJournal overwrites state.json and returns a restore function; the
// previous bytes are restored at spec end if the spec never calls it.
func seedJournal(g gomega.Gomega, raw string) (restore func()) {
	prev := journalRaw(g)
	setJournal(raw)
	restored := false
	restore = func() {
		if !restored {
			restored = true
			setJournal(prev)
		}
	}
	ginkgo.DeferCleanup(restore)
	return restore
}

// waitJournalEntry blocks until the workload has an entry in one of phases and
// returns it; the wait ends early on the verdicts waitJournalEntryAfter names.
func waitJournalEntry(ns, name string, timeout time.Duration, phases ...string) journalEntry {
	return journalEntryWait(2, ns, name, nil, nil, timeout, phases...)
}

// waitJournalEntryAfter blocks until the workload has an entry outside seen
// (nil: any entry) in one of phases and returns it. verdicts, when given, logs
// the workload's findings as they change across the wait. A verdict is not
// something to wait out, so the wait ends at once on the contract breaks
// expectSerialJournal names: an entry of the workload that has failed or
// stalled, unless that phase is awaited, or two unresolved entries at once.
func waitJournalEntryAfter(ns, name string, seen map[string]struct{}, verdicts *verdictLog, timeout time.Duration, phases ...string) journalEntry {
	return journalEntryWait(2, ns, name, seen, verdicts, timeout, phases...)
}

// journalEntryWait is the poll behind waitJournalEntry and
// waitJournalEntryAfter; offset attributes a timeout to their caller.
func journalEntryWait(offset int, ns, name string, seen map[string]struct{}, verdicts *verdictLog, timeout time.Duration, phases ...string) journalEntry {
	var found journalEntry
	gomega.EventuallyWithOffset(offset, func(g gomega.Gomega) {
		if verdicts != nil {
			verdicts.observe(g)
		}
		js := journal(g)
		expectSerialJournal(js, ns, name, phases...)
		entries := journalEntriesFor(js, ns, name)
		for _, e := range entries {
			if _, old := seen[e.UUID]; old {
				continue
			}
			if slices.Contains(phases, e.Phase) {
				found = e
				return
			}
		}
		g.Expect(false).To(gomega.BeTrue(), func() string {
			return fmt.Sprintf("no journal entry for %s/%s in phases %v; journal: %s", ns, name, phases, describeJournal(entries))
		})
	}).WithTimeout(timeout).WithPolling(kindPollInterval).Should(gomega.Succeed())
	return found
}

// waitJournalPhase blocks until entry uuid reaches phase. An entry that
// reaches a different terminal phase ends the wait at once: it can never
// reach the awaited one.
func waitJournalPhase(uuid, phase string, timeout time.Duration) journalEntry {
	var found journalEntry
	gomega.EventuallyWithOffset(1, func(g gomega.Gomega) {
		for _, e := range journal(g).Entries {
			if e.UUID == uuid {
				found = e
				if journalTerminal(e) && e.Phase != phase {
					gomega.StopTrying(fmt.Sprintf("journal %s is %s (reason %q), not %s", uuid, e.Phase, e.Reason, phase)).Now()
				}
				g.Expect(e.Phase).To(gomega.Equal(phase), "journal %s phase (reason %q)", uuid, e.Reason)
				return
			}
		}
		g.Expect(false).To(gomega.BeTrue(), "journal entry %s vanished", uuid)
	}).WithTimeout(timeout).WithPolling(kindPollInterval).Should(gomega.Succeed())
	return found
}

// assertNoDispatch asserts alfred has not moved the fixture's workload off
// node: no journal entry, no migration-request annotation and no migration on
// the IR (OMENative fixtures). The journal check leads because the dispatcher
// persists a prepared journal entry before it patches the migration-request
// annotation, so an empty per-workload journal already implies no submission;
// the remaining checks pin the rest of the contract. It returns the workload's
// live pods on node for the caller to hold to the shape it expects: one pod,
// or a gang's leader and worker.
func assertNoDispatch(g gomega.Gomega, f *alfredFixture, node string) []corev1.Pod {
	ns, name := f.ISVC.Namespace, f.ISVC.Name
	g.ExpectWithOffset(1, journalEntriesFor(journal(g), ns, name)).To(gomega.BeEmpty(), "unexpected dispatch for %s/%s", ns, name)
	g.ExpectWithOffset(1, migrationRequestAnnotations(g, isvcKey(f))).To(gomega.BeEmpty(), "pending migration request on %s/%s", ns, name)
	if !f.raw { // a RawDeployment fixture has no InferenceReplica to record a migration on
		g.ExpectWithOffset(1, migrationEntries(g, ns, name, v1beta1.EngineComponent)).To(gomega.BeEmpty(), "migration recorded for %s/%s", ns, name)
	}
	return livePodsOnNode(omenativePods(g, ns, name), node)
}

// expectIneligibleFinding pins the fields every ineligible node-health
// advisory carries: the policy, the Component, the health reason, the source
// node, the advisory outcome and the eligibility reason.
func expectIneligibleFinding(g gomega.Gomega, r recommendation, component v1beta1.ComponentType, src string) {
	g.ExpectWithOffset(1, r.Policy).To(gomega.Equal(nodeHealthPolicy))
	g.ExpectWithOffset(1, r.Component).To(gomega.Equal(string(component)))
	g.ExpectWithOffset(1, r.Reason).To(gomega.Equal(nodeUnhealthyReason))
	g.ExpectWithOffset(1, r.FromNode).To(gomega.Equal(src))
	g.ExpectWithOffset(1, r.Outcome).To(gomega.Equal(advisoryOutcome))
	g.ExpectWithOffset(1, r.AdvisoryReason).To(gomega.Equal(ineligibleAdvisory))
}

// expectSteadinessAdvisory pins the finding for a non-steady Instance: an
// ineligible advisory that was never handed to the dispatcher or the
// scheduling gate, so it carries no target, dispatch status, request or
// scheduling diagnostic.
func expectSteadinessAdvisory(g gomega.Gomega, r recommendation, component v1beta1.ComponentType, src string) {
	expectIneligibleFinding(g, r, component, src)
	g.ExpectWithOffset(1, r.HintTargets).To(gomega.BeEmpty(), "an ineligible finding names no target")
	g.ExpectWithOffset(1, r.DispatchStatus).To(gomega.BeEmpty(), "an advisory never reaches the dispatcher")
	g.ExpectWithOffset(1, r.DispatchReason).To(gomega.BeEmpty(), "an advisory never reaches the dispatcher")
	g.ExpectWithOffset(1, r.RequestUUID).To(gomega.BeEmpty(), "an advisory never becomes a request")
	g.ExpectWithOffset(1, r.Scheduling).To(gomega.BeNil(), "an advisory never reaches the scheduling gate")
}

// expectIneligibleAdvisory asserts the node-health finding for a single-pod
// OMENative workload that fails the eligibility baseline: it is produced as
// an ineligible advisory, never reaches arbitration, and nothing is
// dispatched off node. The settle step also checks that the InferenceReplica
// is steady, so the reason is attributable to the gate under test rather
// than to a destabilized Instance. The quiet window only requires that every
// finding for the workload stays advisory, since a transient observation gap
// can add a component-wide advisory.
func expectIneligibleAdvisory(f *alfredFixture, node string) {
	ns, name := f.ISVC.Namespace, f.ISVC.Name

	ginkgo.By("the finding surfaces as an ineligible advisory")
	waitEvent(ns, name, "EvacuationRecommendationProduced", kindMediumTimeout)
	gomega.EventuallyWithOffset(1, func(g gomega.Gomega) {
		recs := recsFor(recommendations(g), workloadID(f))
		g.Expect(recs).NotTo(gomega.BeEmpty())
		expectIneligibleFinding(g, recs[0], v1beta1.EngineComponent, node)
		ir := freshIR(g, ns, name, v1beta1.EngineComponent)
		g.Expect(ir.Status.ObservedGeneration).To(gomega.Equal(ir.Generation), "%s/%s status is stale", ns, name)
		g.Expect(ir.Status.ReadyReplicas).To(gomega.Equal(int32(1)), "%s/%s is not steady", ns, name)
		g.Expect(ir.Status.CurrentRevision).NotTo(gomega.BeEmpty())
		g.Expect(ir.Status.CurrentRevision).To(gomega.Equal(ir.Status.UpdateRevision), "%s/%s has a rollout in flight", ns, name)
	}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(gomega.Succeed())

	ginkgo.By("it stays advisory and nothing is dispatched")
	cycleBefore := recommendations(gomega.Default).Timestamp
	gomega.ConsistentlyWithOffset(1, func(g gomega.Gomega) {
		recs := recsFor(recommendations(g), workloadID(f))
		g.Expect(recs).NotTo(gomega.BeEmpty())
		for _, r := range recs {
			g.Expect(r.Outcome).To(gomega.Equal(advisoryOutcome), "ineligible workload %s", workloadID(f))
		}
		g.Expect(assertNoDispatch(g, f, node)).To(gomega.HaveLen(1), "%s/%s pod left %s", ns, name, node)
	}).WithTimeout(quietPeriod).WithPolling(quietPollInterval).Should(gomega.Succeed())
	gomega.ExpectWithOffset(1, recommendations(gomega.Default).Timestamp).To(gomega.BeTemporally(">", cycleBefore), "no decision cycle during the quiet window")
}

// ---- state descriptions ----------------------------------------------------

// describeRecs renders one cycle's recommendations for a workload, so a
// failure names what alfred concluded (a component-wide observation advisory
// instead of a finding per Instance, for example) rather than just timing out.
func describeRecs(recs []recommendation) string {
	if len(recs) == 0 {
		return "none"
	}
	parts := make([]string, 0, len(recs))
	for _, r := range recs {
		parts = append(parts, fmt.Sprintf("instance=%d outcome=%s advisory=%q reject=%q dispatchStatus=%q dispatchReason=%q from=%s",
			r.Instance, r.Outcome, r.AdvisoryReason, r.RejectReason, r.DispatchStatus, r.DispatchReason, r.FromNode))
	}
	return strings.Join(parts, "; ")
}

// describeJournal renders a workload's journal entries, so a failure names
// the request state alfred recorded.
func describeJournal(entries []journalEntry) string {
	if len(entries) == 0 {
		return "none"
	}
	parts := make([]string, 0, len(entries))
	for _, e := range entries {
		completed := "-"
		if e.CompletedAt != nil {
			completed = e.CompletedAt.Format(time.RFC3339)
		}
		parts = append(parts, fmt.Sprintf("uuid=%s phase=%s reason=%q createdAt=%s completedAt=%s",
			e.UUID, e.Phase, e.Reason, e.CreatedAt.Format(time.RFC3339), completed))
	}
	return strings.Join(parts, "; ")
}

// evacuationState renders what alfred last reported and recorded for a
// workload: its findings in the latest cycle and its journal entries. It is
// built lazily, while another assertion is already failing, so a read error
// becomes text rather than a nested failure.
func evacuationState(ns, name string) string {
	var out string
	if err := quietly(func(g gomega.Gomega) {
		out = fmt.Sprintf("last cycle: %s; journal: %s",
			describeRecs(recsFor(recommendations(g), ns+"/"+name)),
			describeJournal(journalEntriesFor(journal(g), ns, name)))
	}); err != nil {
		return "state unavailable: " + err.Error()
	}
	return out
}

// quietly runs read on a Gomega whose first failure ends read and comes back
// as the error, so a diagnostic read never raises a nested failure.
func quietly(read func(g gomega.Gomega)) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%v", r)
		}
	}()
	read(gomega.NewGomega(func(message string, _ ...int) { panic(message) }))
	return nil
}

// verdictLog writes a workload's findings to the spec log each time they
// change, so a long wait leaves the sequence of alfred's verdicts (above all
// the dispatcher's withheld reasons) rather than only the last one.
type verdictLog struct {
	ns, name string
	last     string
}

func (v *verdictLog) observe(g gomega.Gomega) {
	cycle := recommendations(g)
	cur := describeRecs(recsFor(cycle, v.ns+"/"+v.name))
	if cur == v.last {
		return
	}
	v.last = cur
	fmt.Fprintf(ginkgo.GinkgoWriter, "cycle %s %s/%s: %s\n", cycle.Timestamp.Format(time.RFC3339), v.ns, v.name, cur)
}

// ---- migration-request annotations and payload -----------------------------

type migrationRequest struct {
	SchemaVersion   string   `json:"schemaVersion"`
	Component       string   `json:"component"`
	Instance        int32    `json:"instance"`
	FromNode        string   `json:"from_node"`
	HintTargetNodes []string `json:"hint_target_nodes,omitempty"`
	Reason          string   `json:"reason,omitempty"`
	RequestedAt     string   `json:"requested_at,omitempty"`
	RequestedBy     string   `json:"requested_by,omitempty"`
}

func parseMigrationRequest(g gomega.Gomega, payload string) migrationRequest {
	var req migrationRequest
	g.ExpectWithOffset(1, json.Unmarshal([]byte(payload), &req)).To(gomega.Succeed(), "parse migration request payload")
	return req
}

// migrationRequestAnnotations returns uuid -> payload for every pending
// request on the ISVC (the controller deletes them at accept).
func migrationRequestAnnotations(g gomega.Gomega, key client.ObjectKey) map[string]string {
	isvc := &v1beta1.InferenceService{}
	g.ExpectWithOffset(1, k8sClient.Get(suiteCtx, key, isvc)).To(gomega.Succeed())
	out := map[string]string{}
	for k, v := range isvc.Annotations {
		if strings.HasPrefix(k, migrationAnnoPrefix) {
			out[strings.TrimPrefix(k, migrationAnnoPrefix)] = v
		}
	}
	return out
}

// clearMigrationRequests removes every pending request from the ISVC's
// mailbox, whether or not the ISVC is Terminating; one that is already gone
// has nothing to clear.
func clearMigrationRequests(key client.ObjectKey) error {
	isvc := &v1beta1.InferenceService{}
	if err := k8sClient.Get(suiteCtx, key, isvc); err != nil {
		return client.IgnoreNotFound(err)
	}
	base := isvc.DeepCopy()
	pending := 0
	for k := range isvc.Annotations {
		if strings.HasPrefix(k, migrationAnnoPrefix) {
			delete(isvc.Annotations, k)
			pending++
		}
	}
	if pending == 0 {
		return nil
	}
	return client.IgnoreNotFound(k8sClient.Patch(suiteCtx, isvc, client.MergeFrom(base)))
}

// evacuateAndComplete raises GpuUnhealthy on node, waits for alfred to submit
// one request for the workload, verifies the payload contract, waits for the
// controller to complete it, and returns the closed journal entry.
func evacuateAndComplete(f *alfredFixture, node string, component v1beta1.ComponentType) journalEntry {
	markUnhealthy(node)
	entry := waitJournalEntry(f.ISVC.Namespace, f.ISVC.Name, dispatchTimeout, "submitted", "acknowledged", "completed")
	req := parseMigrationRequest(gomega.Default, entry.Payload)
	gomega.ExpectWithOffset(1, req.SchemaVersion).To(gomega.Equal("v1"))
	gomega.ExpectWithOffset(1, req.FromNode).To(gomega.Equal(node))
	gomega.ExpectWithOffset(1, req.RequestedBy).To(gomega.Equal("alfred"))
	waitMigrationCompleted(f.ISVC.Namespace, f.ISVC.Name, component, entry.UUID)
	return waitJournalPhase(entry.UUID, "completed", kindMediumTimeout)
}

// ---- events ----------------------------------------------------------------

// eventTimestamp is when an Event was last observed: lastTimestamp when the
// recorder maintains it, else eventTime, else the object's creation time.
func eventTimestamp(e *corev1.Event) time.Time {
	switch {
	case !e.LastTimestamp.IsZero():
		return e.LastTimestamp.Time
	case !e.EventTime.IsZero():
		return e.EventTime.Time
	default:
		return e.CreationTimestamp.Time
	}
}

// eventsNotBefore keeps the events observed at or after since, less
// eventSkew. The recorder folds a repeat into the existing Event object
// (count and lastTimestamp advance), so an old object counts again the
// moment it recurs.
func eventsNotBefore(events []corev1.Event, since time.Time) []corev1.Event {
	cutoff := since.Add(-eventSkew)
	var out []corev1.Event
	for i := range events {
		if !eventTimestamp(&events[i]).Before(cutoff) {
			out = append(out, events[i])
		}
	}
	return out
}

// eventsSince lists events in ns for involvedName with reason (server-side
// field selectors; k8sClient is a direct client) observed since the given
// time.
func eventsSince(g gomega.Gomega, ns, involvedName, reason string, since time.Time) []corev1.Event {
	list := &corev1.EventList{}
	g.ExpectWithOffset(1, k8sClient.List(suiteCtx, list, client.InNamespace(ns), client.MatchingFields{
		"involvedObject.name": involvedName, "reason": reason,
	})).To(gomega.Succeed())
	return eventsNotBefore(list.Items, since)
}

// nodeEventsSince lists events recorded against a Node (cluster-scoped
// objects' events land in the default namespace) observed since the given
// time.
func nodeEventsSince(g gomega.Gomega, node, reason string, since time.Time) []corev1.Event {
	list := &corev1.EventList{}
	g.ExpectWithOffset(1, k8sClient.List(suiteCtx, list, client.InNamespace("default"), client.MatchingFields{
		"involvedObject.name": node, "involvedObject.kind": "Node", "reason": reason,
	})).To(gomega.Succeed())
	return eventsNotBefore(list.Items, since)
}

// eventsFor and nodeEvents are bounded to the running spec: node events share
// the default namespace and every spec makes the same worker unhealthy, so an
// unbounded lookup would be satisfied by the previous spec's event.
func eventsFor(g gomega.Gomega, ns, involvedName, reason string) []corev1.Event {
	return eventsSince(g, ns, involvedName, reason, specStart)
}

func nodeEvents(g gomega.Gomega, node, reason string) []corev1.Event {
	return nodeEventsSince(g, node, reason, specStart)
}

func waitEvent(ns, involvedName, reason string, timeout time.Duration) {
	gomega.EventuallyWithOffset(1, func(g gomega.Gomega) {
		g.Expect(eventsFor(g, ns, involvedName, reason)).NotTo(gomega.BeEmpty(), "event %s on %s/%s", reason, ns, involvedName)
	}).WithTimeout(timeout).WithPolling(kindPollInterval).Should(gomega.Succeed())
}

func waitNodeEvent(node, reason string, timeout time.Duration) {
	gomega.EventuallyWithOffset(1, func(g gomega.Gomega) {
		g.Expect(nodeEvents(g, node, reason)).NotTo(gomega.BeEmpty(), "node event %s on %s", reason, node)
	}).WithTimeout(timeout).WithPolling(kindPollInterval).Should(gomega.Succeed())
}

// eventsFrom keeps the events of eventsFor that the named recorder component
// emitted: alfred and the controller record some of the same reasons against
// the same InferenceService, so the reason alone does not name the emitter.
func eventsFrom(g gomega.Gomega, ns, involvedName, reason, source string) []corev1.Event {
	var out []corev1.Event
	for _, e := range eventsFor(g, ns, involvedName, reason) {
		if e.Source.Component == source {
			out = append(out, e)
		}
	}
	return out
}

func alfredEventsFor(g gomega.Gomega, ns, involvedName, reason string) []corev1.Event {
	return eventsFrom(g, ns, involvedName, reason, alfredEventSource)
}

// waitEventFrom blocks until source has recorded reason against
// ns/involvedName within the running spec.
func waitEventFrom(ns, involvedName, reason, source string, timeout time.Duration) {
	gomega.EventuallyWithOffset(2, func(g gomega.Gomega) {
		g.Expect(eventsFrom(g, ns, involvedName, reason, source)).NotTo(gomega.BeEmpty(), "%s event %s on %s/%s", source, reason, ns, involvedName)
	}).WithTimeout(timeout).WithPolling(kindPollInterval).Should(gomega.Succeed())
}

func waitAlfredEvent(ns, involvedName, reason string, timeout time.Duration) {
	waitEventFrom(ns, involvedName, reason, alfredEventSource, timeout)
}

func waitControllerEvent(ns, involvedName, reason string, timeout time.Duration) {
	waitEventFrom(ns, involvedName, reason, controllerEventSource, timeout)
}

// eventsNaming keeps the events of eventsFrom whose message names the request
// uuid; "" keeps them all. Both sides stamp the uuid, so matching on it ties
// an event to one migration instead of counting event objects.
func eventsNaming(g gomega.Gomega, ns, isvc, reason, source, uuid string) []corev1.Event {
	var out []corev1.Event
	for _, e := range eventsFrom(g, ns, isvc, reason, source) {
		if uuid == "" || strings.Contains(e.Message, "uuid="+uuid) {
			out = append(out, e)
		}
	}
	return out
}

// waitEventUUID blocks until source has recorded reason against the ISVC with
// a message naming the request uuid.
func waitEventUUID(ns, isvc, reason, source, uuid string, timeout time.Duration) {
	gomega.EventuallyWithOffset(1, func(g gomega.Gomega) {
		g.Expect(eventsNaming(g, ns, isvc, reason, source, uuid)).NotTo(gomega.BeEmpty(), "%s event %s naming uuid=%s on %s/%s", source, reason, uuid, ns, isvc)
	}).WithTimeout(timeout).WithPolling(kindPollInterval).Should(gomega.Succeed())
}

// waitAlfredEventBestEffort waits for alfred to record reason against the
// ISVC, naming uuid when one is given, and logs rather than fails a wait that
// runs out. alfred records one Normal event per pass while a request is in
// flight, so a slow migration can spend the ISVC's event burst before the
// event awaited here; the journal's phase is the authoritative record.
func waitAlfredEventBestEffort(ns, isvc, reason, uuid string, timeout time.Duration) {
	quiet := gomega.NewGomega(func(string, ...int) {})
	recorded := quiet.Eventually(func(g gomega.Gomega) {
		g.Expect(eventsNaming(g, ns, isvc, reason, alfredEventSource, uuid)).NotTo(gomega.BeEmpty())
	}).WithTimeout(timeout).WithPolling(kindPollInterval).Should(gomega.Succeed())
	if !recorded {
		fmt.Fprintf(ginkgo.GinkgoWriter, "alfred event %s on %s/%s (uuid %q) not recorded within %s; the journal is authoritative\n",
			reason, ns, isvc, uuid, timeout)
	}
}

// ---- alfred's identity -----------------------------------------------------

// asAlfred returns clients that act as alfred's ServiceAccount, for admission
// and RBAC negatives. The impersonated username is exactly what the guard's
// matchCondition inspects.
func asAlfred() (client.Client, kubernetes.Interface) {
	cfg := rest.CopyConfig(restCfg)
	cfg.Impersonate = rest.ImpersonationConfig{
		UserName: "system:serviceaccount:" + alfredNamespace + ":" + alfredServiceAccount,
		Groups:   []string{"system:serviceaccounts", "system:serviceaccounts:" + alfredNamespace, "system:authenticated"},
	}
	c, err := client.New(cfg, client.Options{Scheme: k8sClient.Scheme()})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	cs, err := kubernetes.NewForConfig(cfg)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	return c, cs
}
