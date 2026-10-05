package kindutil

import (
	"context"
	"fmt"
	"io"
	"slices"
	"strings"
	"time"

	"github.com/onsi/ginkgo/v2"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	schedulingv1alpha1 "sigs.k8s.io/scheduler-plugins/apis/scheduling/v1alpha1"
)

// Bounds on the failure report, so one failed spec never floods a lane log.
const (
	// schedulerLogTailLines is how much of the scheduler pod's log the report
	// keeps, counted back from the end of the spec's window.
	schedulerLogTailLines = int64(200)
	// schedulerLogLimitBytes caps one log read whatever its line lengths.
	schedulerLogLimitBytes = int64(256 << 10)
	// specWindowSlack widens the spec-start watermark for logs and events, so
	// a scheduling attempt made just before the spec's first write still shows.
	specWindowSlack = 30 * time.Second
	// diagnosticsBudget caps the wall-clock one report may spend against the
	// apiserver; diagnosticsRequestTimeout bounds each read within it.
	diagnosticsBudget         = 60 * time.Second
	diagnosticsRequestTimeout = 15 * time.Second
	// Line budgets per section: the newest namespaces and events, the first
	// pods and PodGroups by name, and the bytes one message may occupy.
	diagnosticsNamespaces      = 8
	diagnosticsPendingPodLines = 40
	diagnosticsEventLines      = 60
	diagnosticsPodGroupLines   = 40
	diagnosticsMessageBytes    = 240
)

// clusterOwnNamespaces are the namespaces KIND populates itself; no spec
// creates anything there, so the report leaves them out.
var clusterOwnNamespaces = map[string]bool{
	"kube-system": true, "kube-public": true, "kube-node-lease": true, "local-path-storage": true,
}

// failureReport is the cluster state one failed spec is explained with: the
// pods no scheduler bound and their scheduling events, plus, under a
// scheduler install, the PodGroups and the scheduler's own log.
type failureReport struct {
	Spec, State, Phase string
	RunTime            time.Duration
	Since, Now         time.Time
	// Scheduler is the installed scheduler's name; empty under the cluster
	// default scheduler.
	Scheduler      string
	Namespaces     []corev1.Namespace
	MoreNamespaces int
	Pending        []corev1.Pod
	Events         []corev1.Event
	OlderEvents    int
	PodGroups      []schedulingv1alpha1.PodGroup
	SchedulerPods  []corev1.Pod
	SchedulerLogs  []schedulerLog
	Errors         []string
}

// schedulerLog is the tail of one scheduler container's log within the window.
type schedulerLog struct {
	Pod, Container string
	Lines          []string
	Dropped        int
}

// specWindowStart is the watermark logs and events are read from: the spec's
// start, widened by specWindowSlack.
func specWindowStart(specStart time.Time) time.Time {
	return specStart.Add(-specWindowSlack)
}

// describeSpec takes the heading fields from Ginkgo's own spec report.
func (r *failureReport) describeSpec(report ginkgo.SpecReport, phase string, now time.Time) {
	r.Spec = report.FullText()
	r.State = report.State.String()
	r.Phase = phase
	r.RunTime = now.Sub(report.StartTime).Round(time.Second)
	r.Since = specWindowStart(report.StartTime)
	r.Now = now
}

func (r *failureReport) errorf(format string, args ...any) {
	r.Errors = append(r.Errors, fmt.Sprintf(format, args...))
}

// pendingPods are the pods without a node name, in namespace/name order.
func pendingPods(pods []corev1.Pod) []corev1.Pod {
	var pending []corev1.Pod
	for i := range pods {
		if pods[i].Spec.NodeName == "" {
			pending = append(pending, pods[i])
		}
	}
	slices.SortFunc(pending, func(a, b corev1.Pod) int {
		return strings.Compare(a.Namespace+"/"+a.Name, b.Namespace+"/"+b.Name)
	})
	return pending
}

// specNamespaces drops the cluster's own namespaces and keeps the newest
// diagnosticsNamespaces of the rest, since a spec mints its namespace last;
// the count left out is returned with them.
func specNamespaces(namespaces []corev1.Namespace) ([]corev1.Namespace, int) {
	var kept []corev1.Namespace
	for i := range namespaces {
		if !clusterOwnNamespaces[namespaces[i].Name] {
			kept = append(kept, namespaces[i])
		}
	}
	slices.SortFunc(kept, func(a, b corev1.Namespace) int {
		if c := b.CreationTimestamp.Time.Compare(a.CreationTimestamp.Time); c != 0 {
			return c
		}
		return strings.Compare(a.Name, b.Name)
	})
	rest := 0
	if len(kept) > diagnosticsNamespaces {
		rest = len(kept) - diagnosticsNamespaces
		kept = kept[:diagnosticsNamespaces]
	}
	return kept, rest
}

// schedulingEvents keeps the events of the window that explain placement:
// those recorded against a pending pod and, under a scheduler install, those
// the scheduler recorded itself or that concern a PodGroup. Oldest first; the
// newest diagnosticsEventLines are kept and the count dropped is returned.
func schedulingEvents(events []corev1.Event, pending []corev1.Pod, schedulerName string, since time.Time) ([]corev1.Event, int) {
	byUID := map[types.UID]bool{}
	byName := map[string]bool{}
	for i := range pending {
		byUID[pending[i].UID] = true
		byName[pending[i].Namespace+"/"+pending[i].Name] = true
	}
	var kept []corev1.Event
	for i := range events {
		e := &events[i]
		if eventTime(e).Before(since) {
			continue
		}
		o := &e.InvolvedObject
		aboutPending := false
		if o.Kind == "Pod" {
			if o.UID != "" {
				aboutPending = byUID[o.UID]
			} else {
				aboutPending = byName[o.Namespace+"/"+o.Name]
			}
		}
		bySchedulerOrGroup := schedulerName != "" && (eventRecorder(e) == schedulerName || o.Kind == "PodGroup")
		if aboutPending || bySchedulerOrGroup {
			kept = append(kept, *e)
		}
	}
	slices.SortStableFunc(kept, func(a, b corev1.Event) int { return eventTime(&a).Compare(eventTime(&b)) })
	older := 0
	if len(kept) > diagnosticsEventLines {
		older = len(kept) - diagnosticsEventLines
		kept = kept[older:]
	}
	return kept, older
}

// tailLines keeps the last keep lines of raw and counts the ones before them.
func tailLines(raw string, keep int) ([]string, int) {
	raw = strings.TrimSuffix(raw, "\n")
	if raw == "" {
		return nil, 0
	}
	lines := strings.Split(raw, "\n")
	for i := range lines {
		lines[i] = strings.TrimSuffix(lines[i], "\r")
	}
	dropped := 0
	if len(lines) > keep {
		dropped = len(lines) - keep
		lines = lines[dropped:]
	}
	return lines, dropped
}

// schedulerLogOptions bounds one log read to the spec's window, the tail
// budget and the byte cap.
func schedulerLogOptions(container string, since time.Time) *corev1.PodLogOptions {
	return &corev1.PodLogOptions{
		Container:  container,
		SinceTime:  &metav1.Time{Time: since},
		TailLines:  ptr.To(schedulerLogTailLines),
		LimitBytes: ptr.To(schedulerLogLimitBytes),
	}
}

// failureProbe reads one report from the cluster through the suite's clients.
// Every read is bounded by diagnosticsRequestTimeout and a failed one becomes
// a report line, so the probe can neither hang nor fail a spec further.
type failureProbe struct {
	client    client.Client
	podLogs   func(ctx context.Context, namespace, pod string, opts *corev1.PodLogOptions) (io.ReadCloser, error)
	scheduler *SchedulerInstall
}

func (p *failureProbe) read(ctx context.Context, fn func(context.Context) error) error {
	ctx, cancel := context.WithTimeout(ctx, diagnosticsRequestTimeout)
	defer cancel()
	return fn(ctx)
}

// collect reads the spec namespaces' pods, events and PodGroups and, under a
// scheduler install, the scheduler's pods and log tail since the watermark.
func (p *failureProbe) collect(ctx context.Context, since time.Time) *failureReport {
	r := &failureReport{Since: since, Now: time.Now()}
	if p.scheduler != nil {
		r.Scheduler = p.scheduler.Name
	}
	namespaces := &corev1.NamespaceList{}
	if err := p.read(ctx, func(ctx context.Context) error { return p.client.List(ctx, namespaces) }); err != nil {
		r.errorf("list namespaces: %v", err)
	}
	r.Namespaces, r.MoreNamespaces = specNamespaces(namespaces.Items)

	var pods []corev1.Pod
	var events []corev1.Event
	for i := range r.Namespaces {
		ns := r.Namespaces[i].Name
		podList := &corev1.PodList{}
		if err := p.read(ctx, func(ctx context.Context) error {
			return p.client.List(ctx, podList, client.InNamespace(ns))
		}); err != nil {
			r.errorf("list pods in %s: %v", ns, err)
		}
		pods = append(pods, podList.Items...)
		eventList := &corev1.EventList{}
		if err := p.read(ctx, func(ctx context.Context) error {
			return p.client.List(ctx, eventList, client.InNamespace(ns))
		}); err != nil {
			r.errorf("list events in %s: %v", ns, err)
		}
		events = append(events, eventList.Items...)
		if p.scheduler != nil {
			groups, err := p.podGroups(ctx, ns)
			if err != nil {
				r.errorf("list PodGroups in %s: %v", ns, err)
			}
			r.PodGroups = append(r.PodGroups, groups...)
		}
	}
	r.Pending = pendingPods(pods)
	r.Events, r.OlderEvents = schedulingEvents(events, r.Pending, r.Scheduler, since)
	if p.scheduler != nil {
		p.readScheduler(ctx, r, since)
	}
	return r
}

// podGroups lists a namespace's PodGroups as unstructured objects, so the
// read does not depend on the suite's scheme registering the type.
func (p *failureProbe) podGroups(ctx context.Context, ns string) ([]schedulingv1alpha1.PodGroup, error) {
	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(schedulingv1alpha1.SchemeGroupVersion.WithKind("PodGroupList"))
	if err := p.read(ctx, func(ctx context.Context) error {
		return p.client.List(ctx, list, client.InNamespace(ns))
	}); err != nil {
		return nil, err
	}
	groups := make([]schedulingv1alpha1.PodGroup, 0, len(list.Items))
	for i := range list.Items {
		pg := schedulingv1alpha1.PodGroup{}
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(list.Items[i].Object, &pg); err != nil {
			return groups, fmt.Errorf("decode %s: %w", list.Items[i].GetName(), err)
		}
		groups = append(groups, pg)
	}
	slices.SortFunc(groups, func(a, b schedulingv1alpha1.PodGroup) int {
		return strings.Compare(a.Namespace+"/"+a.Name, b.Namespace+"/"+b.Name)
	})
	return groups, nil
}

// readScheduler finds the scheduler's pods through its Deployment's selector,
// so the chart's labels are read rather than repeated here, and tails every
// container's log within the window.
func (p *failureProbe) readScheduler(ctx context.Context, r *failureReport, since time.Time) {
	ns := p.scheduler.namespace()
	key := client.ObjectKey{Namespace: ns, Name: p.scheduler.Name}
	dep := &appsv1.Deployment{}
	if err := p.read(ctx, func(ctx context.Context) error { return p.client.Get(ctx, key, dep) }); err != nil {
		r.errorf("get scheduler deployment %s: %v", key, err)
		return
	}
	selector, err := metav1.LabelSelectorAsSelector(dep.Spec.Selector)
	if err != nil {
		r.errorf("scheduler deployment %s selector: %v", key, err)
		return
	}
	podList := &corev1.PodList{}
	if err := p.read(ctx, func(ctx context.Context) error {
		return p.client.List(ctx, podList, client.InNamespace(ns), client.MatchingLabelsSelector{Selector: selector})
	}); err != nil {
		r.errorf("list scheduler pods in %s: %v", ns, err)
		return
	}
	r.SchedulerPods = podList.Items
	slices.SortFunc(r.SchedulerPods, func(a, b corev1.Pod) int { return strings.Compare(a.Name, b.Name) })
	if p.podLogs == nil {
		r.errorf("no log reader configured; the scheduler log is skipped")
		return
	}
	for i := range r.SchedulerPods {
		pod := &r.SchedulerPods[i]
		for _, c := range pod.Spec.Containers {
			entry, err := p.tailLog(ctx, pod, c.Name, since)
			if err != nil {
				r.errorf("read log %s/%s/%s: %v", pod.Namespace, pod.Name, c.Name, err)
			}
			if entry != nil {
				r.SchedulerLogs = append(r.SchedulerLogs, *entry)
			}
		}
	}
}

// tailLog streams one container's bounded log; nil when the stream never
// opened, the lines read so far alongside the error when it broke midway.
func (p *failureProbe) tailLog(ctx context.Context, pod *corev1.Pod, container string, since time.Time) (*schedulerLog, error) {
	ctx, cancel := context.WithTimeout(ctx, diagnosticsRequestTimeout)
	defer cancel()
	rc, err := p.podLogs(ctx, pod.Namespace, pod.Name, schedulerLogOptions(container, since))
	if err != nil {
		return nil, err
	}
	defer func() { _ = rc.Close() }()
	raw, err := io.ReadAll(io.LimitReader(rc, schedulerLogLimitBytes))
	entry := &schedulerLog{Pod: pod.Namespace + "/" + pod.Name, Container: container}
	entry.Lines, entry.Dropped = tailLines(string(raw), int(schedulerLogTailLines))
	return entry, err
}

// registerFailureDiagnostics adds the root-level Ginkgo nodes that explain a
// failed spec. JustAfterEach runs before the spec's cleanup, while its
// namespaces, pending pods and PodGroups still exist; ReportAfterEach covers
// a failure that cleanup itself raised. Must run before RunSpecs, while Ginkgo
// still accepts top-level nodes.
func registerFailureDiagnostics(state *SuiteState, scheduler *SchedulerInstall) {
	probe := &failureProbe{client: state.Client, scheduler: scheduler}
	if state.Clientset != nil {
		probe.podLogs = func(ctx context.Context, namespace, pod string, opts *corev1.PodLogOptions) (io.ReadCloser, error) {
			return state.Clientset.CoreV1().Pods(namespace).GetLogs(pod, opts).Stream(ctx)
		}
	}
	dump := func(report ginkgo.SpecReport, phase string) {
		defer func() {
			if rec := recover(); rec != nil {
				fmt.Fprintf(ginkgo.GinkgoWriter, "!! KIND failure diagnostics aborted: %v\n", rec)
			}
		}()
		parent := state.Ctx
		if parent == nil {
			parent = context.Background()
		}
		ctx, cancel := context.WithTimeout(parent, diagnosticsBudget)
		defer cancel()
		r := probe.collect(ctx, specWindowStart(report.StartTime))
		r.describeSpec(report, phase, time.Now())
		writeFailureReport(ginkgo.GinkgoWriter, r)
	}
	// dumped is per spec: JustAfterEach writes it before ReportAfterEach reads
	// it, and Ginkgo runs one spec at a time in a process.
	dumped := false
	ginkgo.JustAfterEach(func() {
		report := ginkgo.CurrentSpecReport()
		dumped = report.Failed()
		if dumped {
			dump(report, "before cleanup")
		}
	})
	ginkgo.ReportAfterEach(func(report ginkgo.SpecReport) {
		if report.Failed() && !dumped {
			dump(report, "after cleanup")
		}
	})
}

// writeFailureReport prints the report between two marker lines, state first
// and the long scheduler log last.
func writeFailureReport(w io.Writer, r *failureReport) {
	pr := func(format string, args ...any) { fmt.Fprintf(w, format+"\n", args...) }
	pr("==== KIND failure diagnostics (%s): %s [%s after %s] ====", r.Phase, r.Spec, r.State, r.RunTime)
	pr("%s", schedulerLine(r))
	pr("window: since %s (spec start minus %s), now %s", stamp(r.Since), specWindowSlack, stamp(r.Now))
	pr("namespaces (newest first, outside the cluster's own): %s", namespaceSummary(r))

	pr("--- pending pods (no node name): %d ---", len(r.Pending))
	for i := range r.Pending {
		if i == diagnosticsPendingPodLines {
			pr("... (%d more pending pods)", len(r.Pending)-i)
			break
		}
		pr("%s", pendingPodLine(&r.Pending[i]))
	}

	pr("--- scheduling events since %s: %d ---", stamp(r.Since), len(r.Events)+r.OlderEvents)
	if r.OlderEvents > 0 {
		pr("... (%d older events omitted)", r.OlderEvents)
	}
	for i := range r.Events {
		pr("%s", eventLine(&r.Events[i]))
	}

	if r.Scheduler != "" {
		pr("--- PodGroups: %d ---", len(r.PodGroups))
		for i := range r.PodGroups {
			if i == diagnosticsPodGroupLines {
				pr("... (%d more PodGroups)", len(r.PodGroups)-i)
				break
			}
			pr("%s", podGroupLine(&r.PodGroups[i]))
		}
		for _, l := range r.SchedulerLogs {
			pr("--- scheduler log %s/%s (last %d lines since %s) ---", l.Pod, l.Container, schedulerLogTailLines, stamp(r.Since))
			if l.Dropped > 0 {
				pr("... (%d earlier lines omitted)", l.Dropped)
			}
			if len(l.Lines) == 0 {
				pr("(empty)")
			}
			for _, line := range l.Lines {
				pr("%s", line)
			}
		}
	}

	if len(r.Errors) > 0 {
		pr("--- errors while reading: %d ---", len(r.Errors))
		for _, e := range r.Errors {
			pr("!! %s", oneLine(e, diagnosticsMessageBytes))
		}
	}
	pr("==== end KIND failure diagnostics ====")
}

func schedulerLine(r *failureReport) string {
	if r.Scheduler == "" {
		return "scheduler: cluster default"
	}
	if len(r.SchedulerPods) == 0 {
		return "scheduler: " + r.Scheduler + "; no scheduler pod found"
	}
	parts := make([]string, 0, len(r.SchedulerPods))
	for i := range r.SchedulerPods {
		p := &r.SchedulerPods[i]
		parts = append(parts, fmt.Sprintf("pod %s/%s node=%s phase=%s ready=%t restarts=%d",
			p.Namespace, p.Name, orDash(p.Spec.NodeName), p.Status.Phase, podReady(p), restartCount(p)))
	}
	return "scheduler: " + r.Scheduler + "; " + strings.Join(parts, "; ")
}

// namespaceSummary names each inspected namespace with its age relative to
// the spec start, so a leftover from an earlier run reads as one.
func namespaceSummary(r *failureReport) string {
	if len(r.Namespaces) == 0 {
		return "none"
	}
	specStart := r.Since.Add(specWindowSlack)
	parts := make([]string, 0, len(r.Namespaces))
	for i := range r.Namespaces {
		ns := &r.Namespaces[i]
		s := fmt.Sprintf("%s (created %s", ns.Name, relativeTo(ns.CreationTimestamp.Time, specStart))
		if ns.DeletionTimestamp != nil || ns.Status.Phase == corev1.NamespaceTerminating {
			s += ", terminating"
		}
		parts = append(parts, s+")")
	}
	summary := strings.Join(parts, ", ")
	if r.MoreNamespaces > 0 {
		summary += fmt.Sprintf(" ... (%d older namespaces not inspected)", r.MoreNamespaces)
	}
	return summary
}

func pendingPodLine(p *corev1.Pod) string {
	s := fmt.Sprintf("%s/%s phase=%s scheduler=%s podGroup=%s created=%s",
		p.Namespace, p.Name, orDash(string(p.Status.Phase)), orDash(p.Spec.SchedulerName),
		orDash(p.Labels[schedulingv1alpha1.PodGroupLabel]), stamp(p.CreationTimestamp.Time))
	if p.DeletionTimestamp != nil {
		s += " deleting=true"
	}
	if p.Status.NominatedNodeName != "" {
		s += " nominatedNode=" + p.Status.NominatedNodeName
	}
	if len(p.Spec.SchedulingGates) > 0 {
		gates := make([]string, 0, len(p.Spec.SchedulingGates))
		for _, g := range p.Spec.SchedulingGates {
			gates = append(gates, g.Name)
		}
		s += " gates=[" + strings.Join(gates, " ") + "]"
	}
	return s + " " + podScheduledCondition(p)
}

// podScheduledCondition is the scheduler's verdict as the pod carries it.
func podScheduledCondition(p *corev1.Pod) string {
	for _, c := range p.Status.Conditions {
		if c.Type != corev1.PodScheduled {
			continue
		}
		s := "PodScheduled=" + string(c.Status)
		if c.Reason != "" {
			s += "/" + c.Reason
		}
		if c.Message != "" {
			s += ": " + oneLine(c.Message, diagnosticsMessageBytes)
		}
		return s
	}
	return "PodScheduled=- (no condition yet)"
}

func eventLine(e *corev1.Event) string {
	o := &e.InvolvedObject
	obj := o.Kind + "/" + o.Name
	if o.Namespace != "" {
		obj = o.Kind + "/" + o.Namespace + "/" + o.Name
	}
	count := e.Count
	if e.Series != nil && e.Series.Count > count {
		count = e.Series.Count
	}
	if count < 1 {
		count = 1
	}
	return fmt.Sprintf("%s %s %s %s x%d from %s: %s", stamp(eventTime(e)), e.Type, e.Reason, obj, count,
		orDash(eventRecorder(e)), oneLine(e.Message, diagnosticsMessageBytes))
}

func podGroupLine(pg *schedulingv1alpha1.PodGroup) string {
	s := fmt.Sprintf("%s/%s minMember=%d phase=%s running=%d succeeded=%d failed=%d scheduleStart=%s occupiedBy=%s",
		pg.Namespace, pg.Name, pg.Spec.MinMember, orDash(string(pg.Status.Phase)), pg.Status.Running,
		pg.Status.Succeeded, pg.Status.Failed, stamp(pg.Status.ScheduleStartTime.Time), orDash(pg.Status.OccupiedBy))
	if pg.Spec.ScheduleTimeoutSeconds != nil {
		s += fmt.Sprintf(" scheduleTimeout=%ds", *pg.Spec.ScheduleTimeoutSeconds)
	}
	keys := make([]string, 0, len(pg.Annotations))
	for k := range pg.Annotations {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	annotations := make([]string, 0, len(keys))
	for _, k := range keys {
		annotations = append(annotations, k+"="+pg.Annotations[k])
	}
	return s + " annotations=[" + oneLine(strings.Join(annotations, " "), diagnosticsMessageBytes) + "]"
}

// eventTime is the newest instant an event reports, whichever recorder API
// wrote it.
func eventTime(e *corev1.Event) time.Time {
	switch {
	case e.Series != nil && !e.Series.LastObservedTime.IsZero():
		return e.Series.LastObservedTime.Time
	case !e.LastTimestamp.IsZero():
		return e.LastTimestamp.Time
	case !e.EventTime.IsZero():
		return e.EventTime.Time
	case !e.FirstTimestamp.IsZero():
		return e.FirstTimestamp.Time
	}
	return e.CreationTimestamp.Time
}

// eventRecorder names the component that recorded an event: Source.Component
// for the core recorder, ReportingController for the events API one.
func eventRecorder(e *corev1.Event) string {
	if e.Source.Component != "" {
		return e.Source.Component
	}
	return e.ReportingController
}

func podReady(p *corev1.Pod) bool {
	for _, c := range p.Status.Conditions {
		if c.Type == corev1.PodReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

func restartCount(p *corev1.Pod) int32 {
	var n int32
	for _, cs := range p.Status.ContainerStatuses {
		n += cs.RestartCount
	}
	return n
}

// relativeTo words an instant against the spec start, to the second.
func relativeTo(t, specStart time.Time) string {
	d := t.Sub(specStart)
	if d < 0 {
		return (-d).Round(time.Second).String() + " before spec start"
	}
	return d.Round(time.Second).String() + " after spec start"
}

func stamp(t time.Time) string {
	if t.IsZero() {
		return "-"
	}
	return t.UTC().Format(time.RFC3339)
}

// orDash shows an empty field as "-" so a line's fields stay countable.
func orDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

// oneLine collapses whitespace and bounds a message to limit bytes.
func oneLine(s string, limit int) string {
	s = strings.Join(strings.Fields(s), " ")
	if len(s) <= limit {
		return s
	}
	return s[:limit] + fmt.Sprintf("... (%d more bytes)", len(s)-limit)
}
