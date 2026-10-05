package kindutil

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	ginkgotypes "github.com/onsi/ginkgo/v2/types"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	schedulingv1alpha1 "sigs.k8s.io/scheduler-plugins/apis/scheduling/v1alpha1"
)

// Fixture instants are whole seconds so they survive the fake client's JSON
// round trip unchanged.
var (
	diagSpecStart  = time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
	diagBeforeSpec = diagSpecStart.Add(-10 * time.Minute)
	diagDuringSpec = diagSpecStart.Add(20 * time.Second)
)

const (
	diagSchedulerName = "ome-scheduler"
	diagUnschedulable = "0/3 nodes are available: 1 node(s) had untolerated taint, 2 node(s) didn't match pod topology spread constraints."
)

func diagPendingPod(ns, name, scheduler, group string) *corev1.Pod {
	p := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: ns, UID: types.UID(ns + "/" + name),
			CreationTimestamp: metav1.NewTime(diagDuringSpec),
		},
		Spec: corev1.PodSpec{
			SchedulerName: scheduler,
			Containers:    []corev1.Container{{Name: "main", Image: "serving:v1"}},
		},
		Status: corev1.PodStatus{Phase: corev1.PodPending, Conditions: []corev1.PodCondition{{
			Type: corev1.PodScheduled, Status: corev1.ConditionFalse,
			Reason: corev1.PodReasonUnschedulable, Message: diagUnschedulable,
		}}},
	}
	if group != "" {
		p.Labels = map[string]string{schedulingv1alpha1.PodGroupLabel: group}
	}
	return p
}

func diagBoundPod(ns, name, node string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns, UID: types.UID(ns + "/" + name)},
		Spec: corev1.PodSpec{
			NodeName:   node,
			Containers: []corev1.Container{{Name: "main", Image: "serving:v1"}},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
}

func podRef(p *corev1.Pod) corev1.ObjectReference {
	return corev1.ObjectReference{Kind: "Pod", Namespace: p.Namespace, Name: p.Name, UID: p.UID}
}

func groupRef(ns, name string) corev1.ObjectReference {
	return corev1.ObjectReference{Kind: "PodGroup", Namespace: ns, Name: name}
}

// diagEvent is an event as the events API records it: the recorder is in
// ReportingController and the time in LastTimestamp.
func diagEvent(name string, involved corev1.ObjectReference, reason, recorder string, at time.Time) *corev1.Event {
	return &corev1.Event{
		ObjectMeta:          metav1.ObjectMeta{Name: name, Namespace: involved.Namespace},
		InvolvedObject:      involved,
		Reason:              reason,
		Type:                corev1.EventTypeWarning,
		Message:             reason + " for " + involved.Name,
		Count:               3,
		LastTimestamp:       metav1.NewTime(at),
		ReportingController: recorder,
	}
}

func diagNamespace(name string, created time.Time) *corev1.Namespace {
	return &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name, CreationTimestamp: metav1.NewTime(created)}}
}

func diagPodGroup(ns, name string, minMember int32, phase schedulingv1alpha1.PodGroupPhase) *schedulingv1alpha1.PodGroup {
	return &schedulingv1alpha1.PodGroup{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns, Annotations: map[string]string{"example.com/topology-key": "kubernetes.io/hostname"}},
		Spec:       schedulingv1alpha1.PodGroupSpec{MinMember: minMember},
		Status:     schedulingv1alpha1.PodGroupStatus{Phase: phase, Running: 1},
	}
}

// diagSchedulerInstall is the install whose Deployment and pod the fixtures
// below describe: chart release in the default namespace, selector labels on
// the Deployment, the pod carrying them.
func diagSchedulerInstall() *SchedulerInstall {
	return &SchedulerInstall{Name: diagSchedulerName, Image: diagSchedulerName + ":test"}
}

func diagSchedulerObjects() (*appsv1.Deployment, *corev1.Pod) {
	install := diagSchedulerInstall()
	labels := map[string]string{"app.kubernetes.io/name": "scheduler-chart", "app.kubernetes.io/component": "scheduler"}
	dep := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: install.Name, Namespace: install.namespace()},
		Spec:       appsv1.DeploymentSpec{Selector: &metav1.LabelSelector{MatchLabels: labels}},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: install.Name + "-6d8f-abcde", Namespace: install.namespace(), Labels: labels},
		Spec:       corev1.PodSpec{NodeName: "w0", Containers: []corev1.Container{{Name: "scheduler"}}},
		Status: corev1.PodStatus{
			Phase:      corev1.PodRunning,
			Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
			ContainerStatuses: []corev1.ContainerStatus{
				{Name: "scheduler", Ready: true, RestartCount: 2},
			},
		},
	}
	return dep, pod
}

func podKeys(pods []corev1.Pod) []string {
	keys := make([]string, 0, len(pods))
	for i := range pods {
		keys = append(keys, pods[i].Namespace+"/"+pods[i].Name)
	}
	return keys
}

func eventKeys(events []corev1.Event) []string {
	keys := make([]string, 0, len(events))
	for i := range events {
		keys = append(keys, events[i].InvolvedObject.Name+"/"+events[i].Reason)
	}
	return keys
}

func namespaceNames(namespaces []corev1.Namespace) []string {
	names := make([]string, 0, len(namespaces))
	for i := range namespaces {
		names = append(names, namespaces[i].Name)
	}
	return names
}

func TestPendingPodsAreThoseWithoutANodeName(t *testing.T) {
	deleting := diagPendingPod("b", "gone", "", "")
	deleting.DeletionTimestamp = &metav1.Time{Time: diagDuringSpec}
	pods := []corev1.Pod{
		*diagBoundPod("b", "bound", "w0"),
		*diagPendingPod("b", "zeta", "", ""),
		*deleting,
		*diagPendingPod("a", "alpha", diagSchedulerName, "alpha-0"),
	}
	got := podKeys(pendingPods(pods))
	want := []string{"a/alpha", "b/gone", "b/zeta"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("pendingPods = %v, want %v (unbound pods only, namespace/name order)", got, want)
	}
	if len(pendingPods(nil)) != 0 {
		t.Fatal("no pods must yield no pending pods")
	}
}

func TestSchedulingEventsKeepTheWindowAndTheSchedulerRecorder(t *testing.T) {
	pending := diagPendingPod("ns", "p", diagSchedulerName, "g")
	bound := diagBoundPod("ns", "q", "w0")
	events := []corev1.Event{
		*diagEvent("e4", podRef(pending), "FailedScheduling", diagSchedulerName, diagDuringSpec.Add(3*time.Second)),
		*diagEvent("e0", podRef(pending), "FailedScheduling", "default-scheduler", diagBeforeSpec),
		*diagEvent("e2", podRef(bound), "Scheduled", diagSchedulerName, diagDuringSpec.Add(time.Second)),
		*diagEvent("e5", podRef(bound), "Pulled", "kubelet", diagDuringSpec.Add(4*time.Second)),
		*diagEvent("e3", groupRef("ns", "g"), "GangTimeout", diagSchedulerName, diagDuringSpec.Add(2*time.Second)),
		*diagEvent("e1", groupRef("ns", "g"), "Created", "inferencereplica-controller", diagDuringSpec),
		*diagEvent("e6", podRef(pending), "FailedScheduling", "kube-scheduler", diagDuringSpec.Add(5*time.Second)),
	}
	since := diagSpecStart.Add(-specWindowSlack)
	cases := []struct {
		name      string
		scheduler string
		want      []string
	}{
		{
			name: "cluster default scheduler: the pending pods' events, oldest first",
			want: []string{"p/FailedScheduling", "p/FailedScheduling"},
		},
		{
			name:      "scheduler twin: plus the scheduler's own events and every PodGroup event",
			scheduler: diagSchedulerName,
			want:      []string{"g/Created", "q/Scheduled", "g/GangTimeout", "p/FailedScheduling", "p/FailedScheduling"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, older := schedulingEvents(events, []corev1.Pod{*pending}, tc.scheduler, since)
			if fmt.Sprint(eventKeys(got)) != fmt.Sprint(tc.want) {
				t.Fatalf("schedulingEvents = %v, want %v", eventKeys(got), tc.want)
			}
			if older != 0 {
				t.Fatalf("nothing was dropped for the budget, got older=%d", older)
			}
		})
	}
}

// Events match a pending pod by UID when the recorder set one, and by
// namespace and name otherwise.
func TestSchedulingEventsMatchPodsByUIDOrName(t *testing.T) {
	pending := diagPendingPod("ns", "p", "", "")
	byName := podRef(pending)
	byName.UID = ""
	staleUID := podRef(pending)
	staleUID.UID = "an-earlier-incarnation"
	events := []corev1.Event{
		*diagEvent("e0", byName, "FailedScheduling", "default-scheduler", diagDuringSpec),
		*diagEvent("e1", staleUID, "FailedScheduling", "default-scheduler", diagDuringSpec),
	}
	got, _ := schedulingEvents(events, []corev1.Pod{*pending}, "", diagBeforeSpec)
	if len(got) != 1 || got[0].Name != "e0" {
		t.Fatalf("expected only the event addressed by name, got %v", eventKeys(got))
	}
}

func TestSchedulingEventsKeepTheNewestWithinTheBudget(t *testing.T) {
	pending := diagPendingPod("ns", "p", "", "")
	extra := 5
	var events []corev1.Event
	for i := 0; i < diagnosticsEventLines+extra; i++ {
		events = append(events, *diagEvent(fmt.Sprintf("e%03d", i), podRef(pending), "FailedScheduling", "default-scheduler",
			diagDuringSpec.Add(time.Duration(i)*time.Second)))
	}
	got, older := schedulingEvents(events, []corev1.Pod{*pending}, "", diagBeforeSpec)
	if len(got) != diagnosticsEventLines || older != extra {
		t.Fatalf("kept %d events (older=%d), want %d kept and %d older", len(got), older, diagnosticsEventLines, extra)
	}
	if got[0].Name != fmt.Sprintf("e%03d", extra) {
		t.Fatalf("the oldest kept event must be the first inside the budget, got %s", got[0].Name)
	}
}

func TestSpecNamespacesLeaveOutTheClusterOwnAndKeepTheNewest(t *testing.T) {
	namespaces := []corev1.Namespace{
		*diagNamespace("kube-system", diagBeforeSpec),
		*diagNamespace("kube-public", diagBeforeSpec),
		*diagNamespace("kube-node-lease", diagBeforeSpec),
		*diagNamespace("local-path-storage", diagBeforeSpec),
		*diagNamespace("default", diagBeforeSpec),
		*diagNamespace("spec-b", diagSpecStart.Add(-time.Minute)),
		*diagNamespace("spec-a", diagDuringSpec),
	}
	got, rest := specNamespaces(namespaces)
	want := []string{"spec-a", "spec-b", "default"}
	if fmt.Sprint(namespaceNames(got)) != fmt.Sprint(want) || rest != 0 {
		t.Fatalf("specNamespaces = %v (rest=%d), want %v", namespaceNames(got), rest, want)
	}

	var many []corev1.Namespace
	for i := 0; i < diagnosticsNamespaces+2; i++ {
		many = append(many, *diagNamespace(fmt.Sprintf("spec-%02d", i), diagBeforeSpec.Add(time.Duration(i)*time.Minute)))
	}
	got, rest = specNamespaces(many)
	if len(got) != diagnosticsNamespaces || rest != 2 {
		t.Fatalf("kept %d namespaces (rest=%d), want %d and 2", len(got), rest, diagnosticsNamespaces)
	}
	if got[0].Name != fmt.Sprintf("spec-%02d", diagnosticsNamespaces+1) {
		t.Fatalf("the newest namespace must come first, got %s", got[0].Name)
	}
}

func TestTailLinesKeepTheLastLines(t *testing.T) {
	cases := []struct {
		name    string
		raw     string
		keep    int
		want    []string
		dropped int
	}{
		{name: "empty log", raw: "", keep: 3},
		{name: "within the budget", raw: "a\nb\n", keep: 3, want: []string{"a", "b"}},
		{name: "over the budget keeps the end", raw: "a\nb\nc\nd\n", keep: 3, want: []string{"b", "c", "d"}, dropped: 1},
		{name: "no trailing newline", raw: "a\nb", keep: 1, want: []string{"b"}, dropped: 1},
		{name: "carriage returns are stripped", raw: "a\r\nb\r\n", keep: 5, want: []string{"a", "b"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, dropped := tailLines(tc.raw, tc.keep)
			if fmt.Sprint(got) != fmt.Sprint(tc.want) || dropped != tc.dropped {
				t.Fatalf("tailLines = %q (dropped %d), want %q (dropped %d)", got, dropped, tc.want, tc.dropped)
			}
		})
	}
}

func TestSchedulerLogOptionsBoundTheRead(t *testing.T) {
	opts := schedulerLogOptions("scheduler", diagSpecStart)
	if opts.Container != "scheduler" {
		t.Fatalf("container = %q", opts.Container)
	}
	if opts.TailLines == nil || *opts.TailLines != schedulerLogTailLines {
		t.Fatalf("tailLines = %v, want %d", opts.TailLines, schedulerLogTailLines)
	}
	if opts.LimitBytes == nil || *opts.LimitBytes != schedulerLogLimitBytes {
		t.Fatalf("limitBytes = %v, want %d", opts.LimitBytes, schedulerLogLimitBytes)
	}
	if opts.SinceTime == nil || !opts.SinceTime.Time.Equal(diagSpecStart) {
		t.Fatalf("sinceTime = %v, want %s", opts.SinceTime, diagSpecStart)
	}
	if opts.Previous {
		t.Fatal("the current container's log is wanted, not the previous one")
	}
}

func TestDescribeSpecOpensTheWindowBeforeTheSpecStarted(t *testing.T) {
	report := ginkgotypes.SpecReport{
		ContainerHierarchyTexts: []string{"topology contract"},
		LeafNodeText:            "spreads leaders",
		State:                   ginkgotypes.SpecStateTimedout,
		StartTime:               diagSpecStart,
	}
	r := &failureReport{}
	r.describeSpec(report, "before cleanup", diagSpecStart.Add(301*time.Second))
	if r.Spec != "topology contract spreads leaders" {
		t.Fatalf("spec = %q", r.Spec)
	}
	if r.State != "timedout" || r.Phase != "before cleanup" {
		t.Fatalf("state=%q phase=%q", r.State, r.Phase)
	}
	if !r.Since.Equal(diagSpecStart.Add(-specWindowSlack)) {
		t.Fatalf("since = %s, want the spec start minus %s", r.Since, specWindowSlack)
	}
	if r.RunTime != 301*time.Second {
		t.Fatalf("runTime = %s", r.RunTime)
	}
}

func diagSampleReport(scheduler string) *failureReport {
	pending := diagPendingPod("spec-a", "engine-1-0", scheduler, "engine-1")
	_, schedulerPod := diagSchedulerObjects()
	r := &failureReport{
		Spec: "topology contract spreads leaders", State: "Failed", Phase: "before cleanup",
		RunTime: 301 * time.Second, Since: diagSpecStart.Add(-specWindowSlack), Now: diagSpecStart.Add(301 * time.Second),
		Scheduler:  scheduler,
		Namespaces: []corev1.Namespace{*diagNamespace("spec-a", diagDuringSpec), *diagNamespace("default", diagBeforeSpec)},
		Pending:    []corev1.Pod{*pending},
		Events:     []corev1.Event{*diagEvent("e0", podRef(pending), "FailedScheduling", "default-scheduler", diagDuringSpec.Add(5*time.Second))},
		Errors:     []string{"list events in spec-a: boom"},
	}
	if scheduler != "" {
		r.PodGroups = []schedulingv1alpha1.PodGroup{*diagPodGroup("spec-a", "engine-1", 2, schedulingv1alpha1.PodGroupPending)}
		r.SchedulerPods = []corev1.Pod{*schedulerPod}
		r.SchedulerLogs = []schedulerLog{{Pod: "ome/" + schedulerPod.Name, Container: "scheduler", Lines: []string{"first kept", "last kept"}, Dropped: 3}}
	}
	return r
}

func TestWriteFailureReportUnderTheSchedulerTwin(t *testing.T) {
	var b bytes.Buffer
	writeFailureReport(&b, diagSampleReport(diagSchedulerName))
	out := b.String()
	lines := strings.Split(strings.TrimSuffix(out, "\n"), "\n")
	if !strings.HasPrefix(lines[0], "==== KIND failure diagnostics (before cleanup): topology contract spreads leaders [Failed after 5m1s]") {
		t.Fatalf("first line must open the report, got %q", lines[0])
	}
	if lines[len(lines)-1] != "==== end KIND failure diagnostics ====" {
		t.Fatalf("last line must close the report, got %q", lines[len(lines)-1])
	}
	for _, want := range []string{
		"scheduler: ome-scheduler; pod ome/ome-scheduler-6d8f-abcde node=w0 phase=Running ready=true restarts=2",
		"window: since 2030-01-02T03:03:35Z (spec start minus 30s), now 2030-01-02T03:09:06Z",
		"namespaces (newest first, outside the cluster's own): spec-a (created 20s after spec start), default (created 10m0s before spec start)",
		"--- pending pods (no node name): 1 ---",
		"spec-a/engine-1-0 phase=Pending scheduler=ome-scheduler podGroup=engine-1 created=2030-01-02T03:04:25Z PodScheduled=False/Unschedulable: " + diagUnschedulable,
		"--- scheduling events since 2030-01-02T03:03:35Z: 1 ---",
		"2030-01-02T03:04:30Z Warning FailedScheduling Pod/spec-a/engine-1-0 x3 from default-scheduler: FailedScheduling for engine-1-0",
		"--- PodGroups: 1 ---",
		"spec-a/engine-1 minMember=2 phase=Pending running=1 succeeded=0 failed=0 scheduleStart=- occupiedBy=- annotations=[example.com/topology-key=kubernetes.io/hostname]",
		fmt.Sprintf("--- scheduler log ome/ome-scheduler-6d8f-abcde/scheduler (last %d lines since 2030-01-02T03:03:35Z) ---", schedulerLogTailLines),
		"... (3 earlier lines omitted)",
		"first kept",
		"--- errors while reading: 1 ---",
		"!! list events in spec-a: boom",
	} {
		if !strings.Contains(out, want+"\n") {
			t.Errorf("report lacks the line %q\n%s", want, out)
		}
	}
}

func TestWriteFailureReportUnderTheDefaultScheduler(t *testing.T) {
	var b bytes.Buffer
	writeFailureReport(&b, diagSampleReport(""))
	out := b.String()
	if !strings.Contains(out, "scheduler: cluster default\n") {
		t.Fatalf("report must say the cluster default scheduler placed the pods\n%s", out)
	}
	if !strings.Contains(out, "--- pending pods (no node name): 1 ---\n") || !strings.Contains(out, "--- scheduling events since") {
		t.Fatalf("report must keep the pending pods and their events\n%s", out)
	}
	for _, absent := range []string{"PodGroup", "scheduler log"} {
		if strings.Contains(out, absent) {
			t.Errorf("report under the default scheduler must not carry %q\n%s", absent, out)
		}
	}
}

func TestWriteFailureReportBoundsThePodAndGroupLines(t *testing.T) {
	r := diagSampleReport(diagSchedulerName)
	r.Pending, r.PodGroups = nil, nil
	for i := 0; i < diagnosticsPendingPodLines+2; i++ {
		r.Pending = append(r.Pending, *diagPendingPod("spec-a", fmt.Sprintf("p%03d", i), "", ""))
	}
	for i := 0; i < diagnosticsPodGroupLines+1; i++ {
		r.PodGroups = append(r.PodGroups, *diagPodGroup("spec-a", fmt.Sprintf("g%03d", i), 1, schedulingv1alpha1.PodGroupScheduling))
	}
	var b bytes.Buffer
	writeFailureReport(&b, r)
	out := b.String()
	if !strings.Contains(out, "... (2 more pending pods)\n") || strings.Contains(out, fmt.Sprintf("p%03d", diagnosticsPendingPodLines)) {
		t.Fatalf("pending pods must stop at %d lines\n%s", diagnosticsPendingPodLines, out)
	}
	if !strings.Contains(out, "... (1 more PodGroups)\n") || strings.Contains(out, fmt.Sprintf("g%03d", diagnosticsPodGroupLines)) {
		t.Fatalf("PodGroups must stop at %d lines\n%s", diagnosticsPodGroupLines, out)
	}
}

func TestWriteFailureReportSaysWhenThereIsNothingToShow(t *testing.T) {
	r := &failureReport{Spec: "s", State: "Failed", Phase: "after cleanup", Scheduler: diagSchedulerName}
	var b bytes.Buffer
	writeFailureReport(&b, r)
	out := b.String()
	for _, want := range []string{
		"scheduler: ome-scheduler; no scheduler pod found",
		"namespaces (newest first, outside the cluster's own): none",
		"--- pending pods (no node name): 0 ---",
		"--- PodGroups: 0 ---",
	} {
		if !strings.Contains(out, want+"\n") {
			t.Errorf("report lacks the line %q\n%s", want, out)
		}
	}
	if strings.Contains(out, "errors while reading") {
		t.Errorf("no errors must print no error section\n%s", out)
	}
}

func diagScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(s))
	utilruntime.Must(schedulingv1alpha1.AddToScheme(s))
	return s
}

// diagCluster is the fake cluster the collection tests read: one spec
// namespace with a pending gang member, a bound pod and their events, a
// PodGroup, the cluster's own namespace with an unbound pod that must stay
// out, and the scheduler's Deployment and pod.
func diagCluster(t *testing.T) client.Client {
	t.Helper()
	pending := diagPendingPod("spec-a", "engine-1-0", diagSchedulerName, "engine-1")
	bound := diagBoundPod("spec-a", "engine-0-0", "w1")
	system := diagPendingPod("kube-system", "unbound-system-pod", "", "")
	dep, schedulerPod := diagSchedulerObjects()
	return fake.NewClientBuilder().WithScheme(diagScheme()).WithObjects(
		diagNamespace("kube-system", diagBeforeSpec),
		diagNamespace("spec-a", diagDuringSpec),
		diagNamespace(dep.Namespace, diagBeforeSpec),
		pending, bound, system, schedulerPod, dep,
		diagEvent("e0", podRef(pending), "FailedScheduling", diagSchedulerName, diagDuringSpec.Add(5*time.Second)),
		diagEvent("e1", podRef(bound), "Pulled", "kubelet", diagDuringSpec.Add(6*time.Second)),
		diagEvent("e2", podRef(system), "FailedScheduling", "default-scheduler", diagDuringSpec.Add(7*time.Second)),
		diagPodGroup("spec-a", "engine-1", 2, schedulingv1alpha1.PodGroupPending),
	).Build()
}

func TestCollectFailureReportUnderTheSchedulerTwin(t *testing.T) {
	logLines := int(schedulerLogTailLines) + 7
	var logRequests []string
	probe := &failureProbe{
		client:    diagCluster(t),
		scheduler: diagSchedulerInstall(),
		podLogs: func(_ context.Context, namespace, pod string, opts *corev1.PodLogOptions) (io.ReadCloser, error) {
			logRequests = append(logRequests, fmt.Sprintf("%s/%s/%s tail=%d", namespace, pod, opts.Container, *opts.TailLines))
			return io.NopCloser(strings.NewReader(strings.Repeat("scheduler line\n", logLines))), nil
		},
	}
	r := probe.collect(context.Background(), diagSpecStart.Add(-specWindowSlack))

	if len(r.Errors) != 0 {
		t.Fatalf("unexpected read errors: %v", r.Errors)
	}
	if got := namespaceNames(r.Namespaces); fmt.Sprint(got) != fmt.Sprint([]string{"spec-a", "ome"}) {
		t.Fatalf("namespaces = %v, want the spec's and the scheduler's, newest first", got)
	}
	if got := podKeys(r.Pending); fmt.Sprint(got) != fmt.Sprint([]string{"spec-a/engine-1-0"}) {
		t.Fatalf("pending = %v, want only the unbound pod of the spec namespace", got)
	}
	if got := eventKeys(r.Events); fmt.Sprint(got) != fmt.Sprint([]string{"engine-1-0/FailedScheduling"}) {
		t.Fatalf("events = %v, want the pending pod's scheduling event", got)
	}
	if len(r.PodGroups) != 1 || r.PodGroups[0].Name != "engine-1" || r.PodGroups[0].Spec.MinMember != 2 ||
		r.PodGroups[0].Status.Phase != schedulingv1alpha1.PodGroupPending {
		t.Fatalf("PodGroups = %+v, want the spec namespace's group with its spec and status", r.PodGroups)
	}
	if len(r.SchedulerPods) != 1 || r.SchedulerPods[0].Name != "ome-scheduler-6d8f-abcde" {
		t.Fatalf("scheduler pods = %v, want the pod the Deployment selector matches", podKeys(r.SchedulerPods))
	}
	if fmt.Sprint(logRequests) != fmt.Sprint([]string{fmt.Sprintf("ome/ome-scheduler-6d8f-abcde/scheduler tail=%d", schedulerLogTailLines)}) {
		t.Fatalf("log requests = %v", logRequests)
	}
	if len(r.SchedulerLogs) != 1 || len(r.SchedulerLogs[0].Lines) != int(schedulerLogTailLines) || r.SchedulerLogs[0].Dropped != 7 {
		t.Fatalf("scheduler log must be the last %d lines with the overflow counted, got %d lines dropped=%d",
			schedulerLogTailLines, len(r.SchedulerLogs[0].Lines), r.SchedulerLogs[0].Dropped)
	}
}

func TestCollectFailureReportUnderTheDefaultScheduler(t *testing.T) {
	probe := &failureProbe{
		client: diagCluster(t),
		podLogs: func(context.Context, string, string, *corev1.PodLogOptions) (io.ReadCloser, error) {
			t.Fatal("no log is read under the cluster default scheduler")
			return nil, nil
		},
	}
	r := probe.collect(context.Background(), diagSpecStart.Add(-specWindowSlack))
	if len(r.Errors) != 0 {
		t.Fatalf("unexpected read errors: %v", r.Errors)
	}
	if r.Scheduler != "" || len(r.PodGroups) != 0 || len(r.SchedulerPods) != 0 || len(r.SchedulerLogs) != 0 {
		t.Fatalf("default scheduler must collect pods and events only, got %+v", r)
	}
	if got := podKeys(r.Pending); fmt.Sprint(got) != fmt.Sprint([]string{"spec-a/engine-1-0"}) {
		t.Fatalf("pending = %v", got)
	}
	if got := eventKeys(r.Events); fmt.Sprint(got) != fmt.Sprint([]string{"engine-1-0/FailedScheduling"}) {
		t.Fatalf("events = %v", got)
	}
}

// A read that fails is reported as a line and the rest of the report is
// still assembled; the diagnostics never raise a failure of their own.
func TestCollectFailureReportSurvivesReadErrors(t *testing.T) {
	probe := &failureProbe{
		client:    diagCluster(t),
		scheduler: diagSchedulerInstall(),
		podLogs: func(context.Context, string, string, *corev1.PodLogOptions) (io.ReadCloser, error) {
			return nil, fmt.Errorf("connection refused")
		},
	}
	r := probe.collect(context.Background(), diagSpecStart.Add(-specWindowSlack))
	if len(r.Errors) != 1 || !strings.Contains(r.Errors[0], "connection refused") {
		t.Fatalf("errors = %v, want the log read failure", r.Errors)
	}
	if len(r.Pending) != 1 || len(r.PodGroups) != 1 || len(r.SchedulerPods) != 1 {
		t.Fatalf("the rest of the report must still be collected: %+v", r)
	}

	missing := &failureProbe{
		client:    fake.NewClientBuilder().WithScheme(diagScheme()).Build(),
		scheduler: diagSchedulerInstall(),
	}
	r = missing.collect(context.Background(), diagSpecStart)
	if len(r.Errors) != 1 || !strings.Contains(r.Errors[0], "get scheduler deployment") {
		t.Fatalf("errors = %v, want the missing Deployment", r.Errors)
	}
}
