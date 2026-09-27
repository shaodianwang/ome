package alfred_kind

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/onsi/ginkgo/v2"
	ginkgotypes "github.com/onsi/ginkgo/v2/types"
	"github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/irstatus"
	"github.com/ome-projects/ome/tests/integration/util"
)

// Bounds on one diagnostics dump, so a failure never floods the job log.
const (
	diagLogTailLines        = int64(400)
	diagPrevLogTailLines    = int64(200)
	diagLogLimitBytes       = int64(256 << 10)
	diagRecsValueBytes      = 4 << 10
	diagJournalBytes        = 8 << 10
	diagConfigLines         = 60
	diagEventLines          = 80
	diagNodeEventLines      = 40
	diagFixturePodLines     = 60
	diagMetricLines         = 60
	diagRecommendationLines = 40
	diagWorkloadEventLines  = 120
	diagWorkloadNamespaces  = 8
	diagWorkloadISVCs       = 8
	diagWorkloadIRs         = 8
	diagInstanceRows        = 16
	diagMigrationLines      = 32
	diagMessageBytes        = 240
	// diagWindowSlack widens the spec-start watermark for logs and events.
	diagWindowSlack = 30 * time.Second
	// diagBudget caps the wall-clock one dump may spend against the API.
	diagBudget         = 2 * time.Minute
	diagRequestTimeout = 15 * time.Second
)

// processStart stands in for the spec watermark when no spec has run, as
// after a BeforeSuite failure.
var processStart = time.Now()

// specDumped records whether JustAfterEach wrote the bundle for the running
// spec; it is the only writer, so ReportAfterEach can cover the failures that
// cleanup itself raises without ever dumping a spec twice.
var specDumped bool

// Ginkgo runs JustAfterEach right after the It body and before every
// AfterEach and DeferCleanup node, and CurrentSpecReport already carries the
// It's outcome there, so the bundle captures the node conditions and alfred's
// cycle records as the spec left them, before its cleanup clears them. A
// failure or panic inside the node would be added to the spec, so every step
// tolerates errors and the dump recovers its own panics.
var _ = ginkgo.JustAfterEach(func() {
	report := ginkgo.CurrentSpecReport()
	specDumped = report.Failed()
	if specDumped {
		dumpSpecFailure(report, "before cleanup")
	}
})

// Ginkgo runs ReportAfterEach once the spec and its cleanup are done, and
// RunKindSuite tears the cluster and its clients down from t.Cleanup only
// once RunSpecs has returned, so alfred is still reachable here. The node
// prints the per-spec one-liner and dumps a failure only when cleanup raised
// it, since JustAfterEach saw a passing spec then.
var _ = ginkgo.ReportAfterEach(func(report ginkgo.SpecReport) {
	if report.State.Is(ginkgotypes.SpecStateSkipped | ginkgotypes.SpecStatePending) {
		return
	}
	d := newDiag(ginkgo.GinkgoWriter)
	defer d.close()
	d.printf("alfred pods: %s", d.summary())
	if report.Failed() && !specDumped {
		dumpSpecFailure(report, "after cleanup")
	}
})

// dumpSpecFailure writes the failure bundle for the running spec; phase says
// where in the spec's lifecycle it was taken.
func dumpSpecFailure(report ginkgo.SpecReport, phase string) {
	d := newDiag(ginkgo.GinkgoWriter)
	defer d.close()
	d.printf("==== alfred diagnostics (%s): %s [%s after %s] ====", phase, report.FullText(), report.State, report.RunTime.Round(time.Second))
	d.dump(specStart.Add(-diagWindowSlack))
	d.printf("==== end alfred diagnostics ====")
}

// A BeforeSuite failure runs no spec, so neither per-spec node fires; the
// suite report is the only place left to show what alfred looked like when
// the rollout wait gave up.
var _ = ginkgo.ReportAfterSuite("alfred diagnostics", func(report ginkgo.Report) {
	for i := range report.SpecReports {
		sr := &report.SpecReports[i]
		if sr.LeafNodeType.Is(ginkgotypes.NodeTypeBeforeSuite|ginkgotypes.NodeTypeSynchronizedBeforeSuite) && sr.Failed() {
			dumpAfterBeforeSuiteFailure()
		}
	}
})

func dumpAfterBeforeSuiteFailure() {
	d := newDiag(ginkgo.GinkgoWriter)
	defer d.close()
	d.printf("==== alfred diagnostics: BeforeSuite failed ====")
	d.dump(processStart)
	d.printf("==== end alfred diagnostics ====")
}

// diag writes one diagnostics dump. Every section reports an API error as a
// line and moves on; nothing here may fail or panic the reporting node.
type diag struct {
	ctx    context.Context
	cancel context.CancelFunc
	w      io.Writer
}

func newDiag(w io.Writer) *diag {
	parent := suiteCtx
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithTimeout(parent, diagBudget)
	return &diag{ctx: ctx, cancel: cancel, w: w}
}

// close recovers a panic raised while dumping and releases the context; the
// reporting nodes defer it.
func (d *diag) close() {
	if r := recover(); r != nil {
		d.printf("!! alfred diagnostics aborted: %v", r)
	}
	d.cancel()
}

func (d *diag) printf(format string, args ...any) {
	fmt.Fprintf(d.w, format, args...)
	_, _ = io.WriteString(d.w, "\n")
}

func (d *diag) section(name string) { d.printf("--- %s ---", name) }

func (d *diag) errorf(what string, err error) {
	d.printf("!! %s: %s", what, oneLine(err.Error(), diagMessageBytes))
}

// lines prints at most max lines, noting how many were dropped.
func (d *diag) lines(lines []string, max int, prefix string) {
	for i, l := range lines {
		if i == max {
			d.printf("%s... (%d more lines)", prefix, len(lines)-max)
			return
		}
		d.printf("%s%s", prefix, l)
	}
}

// quiet is a Gomega whose failures become dump lines, so the suite's
// ConfigMap readers can be reused without their assertions ending the report.
func (d *diag) quiet() gomega.Gomega {
	return gomega.NewGomega(func(message string, _ ...int) {
		d.printf("!! %s", oneLine(message, diagMessageBytes))
	})
}

// summary is the per-spec one-liner: every alfred pod with its node, restart
// count and readiness, then each lease holder and its last renewal.
func (d *diag) summary() string {
	var parts []string
	pods, err := d.alfredPodList()
	if err != nil {
		parts = append(parts, "list pods: "+oneLine(err.Error(), diagMessageBytes))
	} else if len(pods) == 0 {
		parts = append(parts, "none")
	}
	for i := range pods {
		p := &pods[i]
		s := fmt.Sprintf("%s@%s restarts=%d ready=%t", p.Name, p.Spec.NodeName, restartCount(p), podIsReady(p))
		if p.Status.Phase != corev1.PodRunning {
			s += " phase=" + string(p.Status.Phase)
		}
		if p.DeletionTimestamp != nil {
			s += " terminating"
		}
		parts = append(parts, s)
	}
	leases, err := d.leases()
	if err != nil {
		parts = append(parts, "list leases: "+oneLine(err.Error(), diagMessageBytes))
	} else if len(leases) == 0 {
		parts = append(parts, "leader=none (no lease)")
	}
	for i := range leases {
		l := &leases[i]
		parts = append(parts, fmt.Sprintf("leader=%s renewed=%s", strOf(l.Spec.HolderIdentity), renewal(l)))
	}
	return strings.Join(parts, "; ")
}

// dump writes the failure bundle; since bounds the log and event window. The
// long log sections come last so the state summary is read first.
func (d *diag) dump(since time.Time) {
	if k8sClient == nil || clientset == nil {
		d.printf("suite clients are not initialised; nothing to dump")
		return
	}
	d.printf("window since %s, now %s", stamp(since), stamp(time.Now()))
	pods := d.dumpAlfredPods()
	d.dumpLeases()
	d.dumpMetrics(pods)
	d.dumpConfigMaps()
	d.dumpEvents(since)
	d.dumpNodes()
	namespaces := d.dumpFixturePods()
	d.dumpWorkloadStatus(namespaces)
	d.dumpWorkloadEvents(namespaces, since)
	d.dumpAlfredLogs(pods, since)
}

func (d *diag) alfredPodList() ([]corev1.Pod, error) {
	list := &corev1.PodList{}
	if err := k8sClient.List(d.ctx, list, client.InNamespace(alfredNamespace),
		client.MatchingLabels{alfredPodLabelKey: alfredPodLabelValue}); err != nil {
		return nil, err
	}
	slices.SortFunc(list.Items, func(a, b corev1.Pod) int { return strings.Compare(a.Name, b.Name) })
	return list.Items, nil
}

// leases lists every Lease in alfred's namespace: the chart passes no lease
// name to the binary, so the holder is read from whatever is there.
func (d *diag) leases() ([]coordinationv1.Lease, error) {
	list, err := clientset.CoordinationV1().Leases(alfredNamespace).List(d.ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	return list.Items, nil
}

func (d *diag) dumpAlfredPods() []corev1.Pod {
	d.section("alfred deployment and pods")
	dep := &appsv1.Deployment{}
	if err := k8sClient.Get(d.ctx, client.ObjectKey{Namespace: alfredNamespace, Name: alfredDeployment}, dep); err != nil {
		d.errorf("get deployment", err)
	} else {
		d.printf("deployment %s generation=%d observed=%d replicas=%d updated=%d ready=%d available=%d unavailable=%d",
			dep.Name, dep.Generation, dep.Status.ObservedGeneration, dep.Status.Replicas, dep.Status.UpdatedReplicas,
			dep.Status.ReadyReplicas, dep.Status.AvailableReplicas, dep.Status.UnavailableReplicas)
	}
	pods, err := d.alfredPodList()
	if err != nil {
		d.errorf("list alfred pods", err)
		return nil
	}
	if len(pods) == 0 {
		d.printf("no pods match %s=%s", alfredPodLabelKey, alfredPodLabelValue)
	}
	for i := range pods {
		p := &pods[i]
		ready, readySince := podReadyCondition(p)
		d.printf("pod %s node=%s phase=%s ready=%s(since %s) restarts=%d started=%s deleting=%t",
			p.Name, p.Spec.NodeName, p.Status.Phase, ready, readySince, restartCount(p), metaTime(p.Status.StartTime), p.DeletionTimestamp != nil)
		for _, cs := range p.Status.ContainerStatuses {
			d.printf("  container %s ready=%t restarts=%d state=%s last=%s",
				cs.Name, cs.Ready, cs.RestartCount, containerState(cs.State), terminated(cs.LastTerminationState.Terminated))
		}
	}
	return pods
}

func (d *diag) dumpLeases() {
	d.section("leases in " + alfredNamespace)
	leases, err := d.leases()
	if err != nil {
		d.errorf("list leases", err)
		return
	}
	if len(leases) == 0 {
		d.printf("none")
	}
	for i := range leases {
		l := &leases[i]
		d.printf("lease %s holder=%s acquired=%s renewed=%s duration=%ds transitions=%d",
			l.Name, strOf(l.Spec.HolderIdentity), microTime(l.Spec.AcquireTime), renewal(l),
			int32Of(l.Spec.LeaseDurationSeconds), int32Of(l.Spec.LeaseTransitions))
	}
}

// dumpMetrics prints each running replica's alfred_* samples: leadership,
// loop timings and counters as the process itself reports them.
func (d *diag) dumpMetrics(pods []corev1.Pod) {
	d.section("alfred metrics (alfred_* samples per pod)")
	for i := range pods {
		p := &pods[i]
		if p.Status.Phase != corev1.PodRunning || p.DeletionTimestamp != nil {
			continue
		}
		raw, err := clientset.CoreV1().RESTClient().Get().
			Namespace(alfredNamespace).Resource("pods").
			Name(fmt.Sprintf("%s:%d", p.Name, alfredMetricsPort)).
			SubResource("proxy").Suffix("metrics").
			Timeout(diagRequestTimeout).DoRaw(d.ctx)
		if err != nil {
			d.errorf("scrape "+p.Name, err)
			continue
		}
		var lines []string
		for _, line := range strings.Split(string(raw), "\n") {
			if strings.HasPrefix(line, "alfred_") {
				lines = append(lines, line)
			}
		}
		d.printf("pod %s: %d alfred_* samples", p.Name, len(lines))
		d.lines(lines, diagMetricLines, "  ")
	}
}

func (d *diag) dumpConfigMaps() {
	g := d.quiet()

	d.section("configmap " + alfredRecsConfigMap)
	recs := recsConfigMap(g)
	if len(recs.Data) == 0 {
		d.printf("(no data)")
	}
	if raw, ok := recs.Data[alfredRecsKey]; ok {
		var cycle lastCycle
		if err := json.Unmarshal([]byte(raw), &cycle); err != nil {
			d.errorf("parse "+alfredRecsKey, err)
		} else {
			d.printf("last cycle at %s (%s ago) mode=%s recommendations=%d",
				stamp(cycle.Timestamp), ago(cycle.Timestamp), cycle.Mode, len(cycle.Recommendations))
			d.lines(recommendationLines(cycle.Recommendations), diagRecommendationLines, "  ")
		}
	}
	for _, k := range slices.Sorted(maps.Keys(recs.Data)) {
		d.printf("%s (%d bytes):", k, len(recs.Data[k]))
		d.printf("%s", clip(recs.Data[k], diagRecsValueBytes))
	}

	d.section("configmap " + alfredJournalName)
	journal := journalConfigMap(g)
	if raw, ok := journal.Data[alfredJournalKey]; ok {
		d.printf("%s resourceVersion=%s (%d bytes):", alfredJournalKey, journal.ResourceVersion, len(raw))
		d.printf("%s", clip(raw, diagJournalBytes))
	} else {
		d.printf("(no %s)", alfredJournalKey)
	}

	d.section("configmap " + alfredConfigMapName)
	cfg := alfredConfigMap(g)
	if raw, ok := cfg.Data[alfredConfigKey]; ok {
		d.printf("%s resourceVersion=%s (first %d lines):", alfredConfigKey, cfg.ResourceVersion, diagConfigLines)
		d.lines(strings.Split(raw, "\n"), diagConfigLines, "")
	} else {
		d.printf("(no %s)", alfredConfigKey)
	}
}

func (d *diag) dumpEvents(since time.Time) {
	d.section(fmt.Sprintf("events in %s since %s", alfredNamespace, stamp(since)))
	list := &corev1.EventList{}
	if err := k8sClient.List(d.ctx, list, client.InNamespace(alfredNamespace)); err != nil {
		d.errorf("list events", err)
	} else {
		d.eventLines(list.Items, since, diagEventLines)
	}

	// Node events land in the default namespace; the field selector is
	// server-side because k8sClient reads the API directly.
	d.section("node events since " + stamp(since))
	list = &corev1.EventList{}
	if err := k8sClient.List(d.ctx, list, client.InNamespace("default"),
		client.MatchingFields{"involvedObject.kind": "Node"}); err != nil {
		d.errorf("list node events", err)
	} else {
		d.eventLines(list.Items, since, diagNodeEventLines)
	}
}

// eventLines prints the events observed since the watermark, oldest first,
// keeping the newest when there are more than max. Each line names the
// recorder, since alfred and the controller record different reasons against
// the same objects, and the first-seen time of a repeated event. Objects are
// namespace-qualified when the events span more than one namespace.
func (d *diag) eventLines(events []corev1.Event, since time.Time, max int) {
	var kept []*corev1.Event
	namespaces := map[string]bool{}
	for i := range events {
		e := &events[i]
		namespaces[e.InvolvedObject.Namespace] = true
		if !eventTimestamp(e).Before(since) {
			kept = append(kept, e)
		}
	}
	slices.SortFunc(kept, func(a, b *corev1.Event) int { return eventTimestamp(a).Compare(eventTimestamp(b)) })
	if len(kept) == 0 {
		d.printf("none")
		return
	}
	if len(kept) > max {
		d.printf("... (%d older events omitted)", len(kept)-max)
		kept = kept[len(kept)-max:]
	}
	qualify := len(namespaces) > 1
	for _, e := range kept {
		d.printf("%s %s %s %s x%d%s from %s: %s", stamp(eventTimestamp(e)), e.Type, e.Reason, eventObject(e, qualify),
			e.Count, firstSeen(e), eventComponent(e), oneLine(e.Message, diagMessageBytes))
	}
}

// nodeLabelNoise hides the labels every node carries so the ones the specs
// raise (maintenance, preemptible, model readiness) stand out.
var nodeLabelNoise = []string{"kubernetes.io/", "beta.kubernetes.io/", "node-role.kubernetes.io/", "topology.kubernetes.io/"}

func (d *diag) dumpNodes() {
	d.section("nodes")
	list := &corev1.NodeList{}
	if err := k8sClient.List(d.ctx, list); err != nil {
		d.errorf("list nodes", err)
		return
	}
	slices.SortFunc(list.Items, func(a, b corev1.Node) int { return strings.Compare(a.Name, b.Name) })
	for i := range list.Items {
		n := &list.Items[i]
		var conds, taints, labels []string
		for _, c := range n.Status.Conditions {
			if c.Type == corev1.NodeReady || c.Status == corev1.ConditionTrue {
				conds = append(conds, fmt.Sprintf("%s=%s", c.Type, c.Status))
			}
		}
		for _, t := range n.Spec.Taints {
			taints = append(taints, fmt.Sprintf("%s=%s:%s", t.Key, t.Value, t.Effect))
		}
		for k, v := range n.Labels {
			if !slices.ContainsFunc(nodeLabelNoise, func(p string) bool { return strings.HasPrefix(k, p) }) {
				labels = append(labels, k+"="+v)
			}
		}
		slices.Sort(labels)
		d.printf("node %s unschedulable=%t conditions=[%s] taints=[%s] labels=[%s]", n.Name, n.Spec.Unschedulable,
			strings.Join(conds, " "), strings.Join(taints, " "), strings.Join(labels, " "))
	}
}

// dumpFixturePods lists every inference-service pod in the cluster, since the
// failed spec's namespace is not known here, and returns the namespaces that
// hold one, capped, for the workload sections that follow.
func (d *diag) dumpFixturePods() []string {
	d.section("inference-service pods (" + constants.InferenceServiceLabel + ")")
	list := &corev1.PodList{}
	if err := k8sClient.List(d.ctx, list, client.HasLabels{constants.InferenceServiceLabel}); err != nil {
		d.errorf("list pods", err)
		return nil
	}
	slices.SortFunc(list.Items, func(a, b corev1.Pod) int {
		return strings.Compare(a.Namespace+"/"+a.Name, b.Namespace+"/"+b.Name)
	})
	if len(list.Items) == 0 {
		d.printf("none")
		return nil
	}
	var lines []string
	namespaces := map[string]bool{}
	for i := range list.Items {
		p := &list.Items[i]
		namespaces[p.Namespace] = true
		lines = append(lines, fmt.Sprintf("%s/%s isvc=%s node=%s phase=%s ready=%t uid=%s deleting=%t",
			p.Namespace, p.Name, p.Labels[constants.InferenceServiceLabel], p.Spec.NodeName, p.Status.Phase,
			podIsReady(p), p.UID, p.DeletionTimestamp != nil))
	}
	d.lines(lines, diagFixturePodLines, "")
	sorted := slices.Sorted(maps.Keys(namespaces))
	if len(sorted) > diagWorkloadNamespaces {
		d.printf("... (%d namespaces beyond the first %d are not inspected)", len(sorted)-diagWorkloadNamespaces, diagWorkloadNamespaces)
		sorted = sorted[:diagWorkloadNamespaces]
	}
	d.printf("workload namespaces: %s", strings.Join(sorted, " "))
	return sorted
}

// dumpWorkloadEvents prints the events recorded in the namespaces that hold
// inference-service pods. Alfred records what it decided for each
// InferenceService there and the controller records the migration it ran;
// the events of every namespace share one cap.
func (d *diag) dumpWorkloadEvents(namespaces []string, since time.Time) {
	d.section("workload events since " + stamp(since))
	if len(namespaces) == 0 {
		d.printf("none (no inference-service pods)")
		return
	}
	var events []corev1.Event
	for _, ns := range namespaces {
		list := &corev1.EventList{}
		if err := k8sClient.List(d.ctx, list, client.InNamespace(ns)); err != nil {
			d.errorf("list events in "+ns, err)
			continue
		}
		events = append(events, list.Items...)
	}
	d.eventLines(events, since, diagWorkloadEventLines)
}

// dumpWorkloadStatus prints, for every InferenceService in the workload
// namespaces, the status alfred's dispatch preflight evaluates before it
// accepts a source: generations, revisions, counters and pause on the ISVC
// and on each InferenceReplica it owns, the representation the codec
// observes, the decoded rows and the migration records. Rows come through
// the shared decode helper; the stored representation is never read here.
func (d *diag) dumpWorkloadStatus(namespaces []string) {
	d.section("workload status (InferenceServices and their InferenceReplicas)")
	if len(namespaces) == 0 {
		d.printf("none (no inference-service pods)")
		return
	}
	var isvcs []*v1beta1.InferenceService
	irsByParent := map[string][]*v1beta1.InferenceReplica{}
	for _, ns := range namespaces {
		isvcList := &v1beta1.InferenceServiceList{}
		if err := k8sClient.List(d.ctx, isvcList, client.InNamespace(ns)); err != nil {
			d.errorf("list inferenceservices in "+ns, err)
		}
		for i := range isvcList.Items {
			isvcs = append(isvcs, &isvcList.Items[i])
		}
		irList := &v1beta1.InferenceReplicaList{}
		if err := k8sClient.List(d.ctx, irList, client.InNamespace(ns)); err != nil {
			d.errorf("list inferencereplicas in "+ns, err)
		}
		for i := range irList.Items {
			ir := &irList.Items[i]
			parent := ns + "/" + ir.Spec.ParentRef.Name
			irsByParent[parent] = append(irsByParent[parent], ir)
		}
	}
	slices.SortFunc(isvcs, func(a, b *v1beta1.InferenceService) int {
		return strings.Compare(a.Namespace+"/"+a.Name, b.Namespace+"/"+b.Name)
	})
	if len(isvcs) == 0 {
		d.printf("none")
		return
	}
	if len(isvcs) > diagWorkloadISVCs {
		d.printf("... (%d InferenceServices beyond the first %d omitted)", len(isvcs)-diagWorkloadISVCs, diagWorkloadISVCs)
		isvcs = isvcs[:diagWorkloadISVCs]
	}
	shown := 0
	for _, isvc := range isvcs {
		d.isvcStatus(isvc)
		irs := irsByParent[isvc.Namespace+"/"+isvc.Name]
		slices.SortFunc(irs, func(a, b *v1beta1.InferenceReplica) int { return strings.Compare(a.Name, b.Name) })
		for _, ir := range irs {
			if shown == diagWorkloadIRs {
				d.printf("  ... (InferenceReplicas beyond %d omitted)", diagWorkloadIRs)
				break
			}
			shown++
			d.irStatus(ir)
		}
	}
}

// isvcStatus prints the ISVC-level fields: the generation pair, deletion,
// the alfred opt-out, the migration-request annotations present (keys only)
// and each component's rollout pointers and lifecycle counters.
func (d *diag) isvcStatus(isvc *v1beta1.InferenceService) {
	var requests []string
	for k := range isvc.Annotations {
		if strings.HasPrefix(k, migrationAnnoPrefix) {
			requests = append(requests, k)
		}
	}
	slices.Sort(requests)
	d.printf("isvc %s/%s generation=%d observedGeneration=%d deleting=%t %s=%s migrationRequests=[%s]",
		isvc.Namespace, isvc.Name, isvc.Generation, isvc.Status.ObservedGeneration, isvc.DeletionTimestamp != nil,
		movableAnnotation, orDash(isvc.Annotations[movableAnnotation]), strings.Join(requests, " "))
	for _, c := range slices.Sorted(maps.Keys(isvc.Status.Components)) {
		cs := isvc.Status.Components[c]
		line := fmt.Sprintf("  component %s rolloutPhase=%s latestReadyRevision=%s latestRolledoutRevision=%s",
			c, orDash(string(cs.RolloutPhase)), orDash(cs.LatestReadyRevision), orDash(cs.LatestRolledoutRevision))
		if lc := cs.Lifecycle; lc != nil {
			line += fmt.Sprintf(" currentRevision=%s updateRevision=%s replicas=%d readyReplicas=%d availableReplicas=%d observedGeneration=%d",
				orDash(lc.CurrentRevision), orDash(lc.UpdateRevision), lc.Replicas, lc.ReadyReplicas, lc.AvailableReplicas, lc.ObservedGeneration)
		} else {
			line += " lifecycle=-"
		}
		d.printf("%s", line)
	}
}

// irStatus prints one InferenceReplica: its top-level status, the
// representation the codec observes, then the decoded rows and the migration
// records, each capped.
func (d *diag) irStatus(ir *v1beta1.InferenceReplica) {
	s := &ir.Status
	encoding, encErr := irstatus.ObservedEncoding(s)
	marker := string(encoding)
	if encErr != nil {
		marker = "!" + oneLine(encErr.Error(), diagMessageBytes)
	}
	rows, err := util.DecodedInstanceStatuses(ir)
	d.printf("  ir %s generation=%d observedGeneration=%d currentRevision=%s updateRevision=%s replicas=%d readyReplicas=%d availableReplicas=%d updatedReplicas=%d paused=%t pauseMode=%s encoding=%s rows=%d migrations=%d deleting=%t",
		ir.Name, ir.Generation, s.ObservedGeneration, orDash(s.CurrentRevision), orDash(s.UpdateRevision),
		s.Replicas, s.ReadyReplicas, s.AvailableReplicas, s.UpdatedReplicas, ir.Spec.Paused, orDash(string(ir.Spec.PauseMode)),
		marker, len(rows), len(s.Migrations), ir.DeletionTimestamp != nil)
	if err != nil {
		d.errorf("decode rows of "+ir.Name, err)
	}
	d.lines(instanceRowLines(rows), diagInstanceRows, "    ")
	d.lines(migrationLines(s.Migrations), diagMigrationLines, "    ")
}

// dumpAlfredLogs prints every container's log within the window, tail-capped,
// and the previous container's tail wherever a restart happened.
func (d *diag) dumpAlfredLogs(pods []corev1.Pod, since time.Time) {
	for i := range pods {
		p := &pods[i]
		restarts := map[string]int32{}
		for _, cs := range p.Status.ContainerStatuses {
			restarts[cs.Name] = cs.RestartCount
		}
		for _, c := range p.Spec.Containers {
			d.section(fmt.Sprintf("log %s/%s since %s (last %d lines)", p.Name, c.Name, stamp(since), diagLogTailLines))
			d.podLog(p.Name, &corev1.PodLogOptions{
				Container: c.Name, SinceTime: &metav1.Time{Time: since},
				TailLines: ptrInt64(diagLogTailLines), LimitBytes: ptrInt64(diagLogLimitBytes),
			})
			if restarts[c.Name] > 0 {
				d.section(fmt.Sprintf("previous log %s/%s (last %d lines before restart %d)", p.Name, c.Name, diagPrevLogTailLines, restarts[c.Name]))
				d.podLog(p.Name, &corev1.PodLogOptions{
					Container: c.Name, Previous: true,
					TailLines: ptrInt64(diagPrevLogTailLines), LimitBytes: ptrInt64(diagLogLimitBytes),
				})
			}
		}
	}
}

func (d *diag) podLog(pod string, opts *corev1.PodLogOptions) {
	ctx, cancel := context.WithTimeout(d.ctx, diagRequestTimeout)
	defer cancel()
	rc, err := clientset.CoreV1().Pods(alfredNamespace).GetLogs(pod, opts).Stream(ctx)
	if err != nil {
		d.errorf("stream log", err)
		return
	}
	defer func() { _ = rc.Close() }()
	raw, err := io.ReadAll(io.LimitReader(rc, diagLogLimitBytes))
	switch {
	case len(raw) == 0 && err == nil:
		d.printf("(empty)")
	case len(raw) > 0:
		_, _ = d.w.Write(raw)
		if raw[len(raw)-1] != '\n' {
			_, _ = io.WriteString(d.w, "\n")
		}
	}
	if err != nil {
		d.errorf("read log", err)
	}
}

// ---- formatting ------------------------------------------------------------

func restartCount(p *corev1.Pod) int32 {
	var n int32
	for _, cs := range p.Status.ContainerStatuses {
		n += cs.RestartCount
	}
	return n
}

func podReadyCondition(p *corev1.Pod) (status, since string) {
	for _, c := range p.Status.Conditions {
		if c.Type == corev1.PodReady {
			return string(c.Status), stamp(c.LastTransitionTime.Time)
		}
	}
	return "Unknown", "-"
}

func containerState(s corev1.ContainerState) string {
	switch {
	case s.Running != nil:
		return "running since " + stamp(s.Running.StartedAt.Time)
	case s.Waiting != nil:
		return fmt.Sprintf("waiting(%s: %s)", s.Waiting.Reason, oneLine(s.Waiting.Message, diagMessageBytes))
	case s.Terminated != nil:
		return terminated(s.Terminated)
	}
	return "unknown"
}

func terminated(t *corev1.ContainerStateTerminated) string {
	if t == nil {
		return "-"
	}
	return fmt.Sprintf("terminated(reason=%s exit=%d finished=%s msg=%q)",
		t.Reason, t.ExitCode, stamp(t.FinishedAt.Time), oneLine(t.Message, diagMessageBytes))
}

func renewal(l *coordinationv1.Lease) string {
	if l.Spec.RenewTime == nil {
		return "never"
	}
	return fmt.Sprintf("%s (%s ago)", stamp(l.Spec.RenewTime.Time), ago(l.Spec.RenewTime.Time))
}

func stamp(t time.Time) string {
	if t.IsZero() {
		return "-"
	}
	return t.UTC().Format(time.RFC3339)
}

func ago(t time.Time) string { return time.Since(t).Round(time.Second).String() }

func metaTime(t *metav1.Time) string {
	if t == nil {
		return "-"
	}
	return stamp(t.Time)
}

func microTime(t *metav1.MicroTime) string {
	if t == nil {
		return "-"
	}
	return stamp(t.Time)
}

func strOf(p *string) string {
	if p == nil {
		return "-"
	}
	return *p
}

func int32Of(p *int32) int32 {
	if p == nil {
		return 0
	}
	return *p
}

// eventObject names an event's object, namespace-qualified when asked and
// the object has one.
func eventObject(e *corev1.Event, qualify bool) string {
	o := &e.InvolvedObject
	if qualify && o.Namespace != "" {
		return o.Kind + "/" + o.Namespace + "/" + o.Name
	}
	return o.Kind + "/" + o.Name
}

// eventComponent is the recorder that emitted an event: Source.Component for
// the core recorder, ReportingController for the events API one.
func eventComponent(e *corev1.Event) string {
	switch {
	case e.Source.Component != "":
		return e.Source.Component
	case e.ReportingController != "":
		return e.ReportingController
	}
	return "-"
}

// firstSeen is the " first=<time>" suffix of an event that has repeated since
// it was first recorded; empty for one seen once.
func firstSeen(e *corev1.Event) string {
	first := e.FirstTimestamp.Time
	if first.IsZero() || first.Equal(eventTimestamp(e)) {
		return ""
	}
	return " first=" + stamp(first)
}

// recommendationLines renders each recommendation of a cycle on one line,
// labelled with the record's own field names, so every instance's outcome is
// readable without the raw JSON.
func recommendationLines(recs []recommendation) []string {
	lines := make([]string, 0, len(recs))
	for i := range recs {
		r := &recs[i]
		lines = append(lines, fmt.Sprintf(
			"workload=%s component=%s instance=%d policy=%s outcome=%s advisoryReason=%q rejectReason=%q dispatchStatus=%s dispatchReason=%q fromNode=%s target=%s hintTargets=%d",
			r.Workload, orDash(r.Component), r.Instance, orDash(r.Policy), orDash(r.Outcome),
			oneLine(r.AdvisoryReason, diagMessageBytes), oneLine(r.RejectReason, diagMessageBytes),
			orDash(r.DispatchStatus), oneLine(r.DispatchReason, diagMessageBytes),
			orDash(r.FromNode), orDash(r.Target), len(r.HintTargets)))
	}
	return lines
}

// instanceRowLines renders the decoded rows of an InferenceReplica, one
// Instance per line, with the fields the dispatch preflight matches a source
// against.
func instanceRowLines(rows []v1beta1.OMENativeInstanceStatus) []string {
	lines := make([]string, 0, len(rows))
	for i := range rows {
		r := &rows[i]
		op := "-"
		if r.Operation != nil {
			op = orDash(string(r.Operation.Type)) + "/" + orDash(r.Operation.Step)
		}
		lines = append(lines, fmt.Sprintf("row index=%d phase=%s runningRevision=%s incarnation=%d podCount=%d servingPodCount=%d availablePodCount=%d nodes=[%s] operation=%s",
			r.Index, orDash(string(r.Phase)), orDash(r.RunningRevision), r.Incarnation, r.PodCount, r.ServingPodCount, r.AvailablePodCount,
			strings.Join(r.NodesOccupied, " "), op))
	}
	return lines
}

// migrationLines renders the migration records of an InferenceReplica, one
// per line.
func migrationLines(ms []v1beta1.MigrationStatus) []string {
	lines := make([]string, 0, len(ms))
	for i := range ms {
		m := &ms[i]
		surge := "-"
		if m.SurgeInstance != nil {
			surge = fmt.Sprint(*m.SurgeInstance)
		}
		lines = append(lines, fmt.Sprintf("migration requestUUID=%s trigger=%s phase=%s sourceInstance=%d surgeInstance=%s fromNode=%s startedAt=%s completedAt=%s reason=%q",
			orDash(m.RequestUUID), orDash(string(m.Trigger)), orDash(string(m.Phase)), m.SourceInstance, surge, orDash(m.FromNode),
			stamp(m.StartedAt.Time), metaTime(m.CompletedAt), oneLine(m.Reason, diagMessageBytes)))
	}
	return lines
}

// orDash shows an empty field as "-" so a line's fields stay countable.
func orDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

// clip bounds a value to max bytes, noting what was dropped.
func clip(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max] + fmt.Sprintf("... (%d more bytes)", len(s)-max)
}

// oneLine collapses whitespace so a message occupies one bounded log line.
func oneLine(s string, max int) string {
	return clip(strings.Join(strings.Fields(s), " "), max)
}
