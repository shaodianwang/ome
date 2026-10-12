package util

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/irprojector"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/coordination"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/escalation"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/podreadiness"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	workloadtypes "github.com/ome-projects/ome/pkg/controller/v1beta1/workload/types"
)

const (
	// InvariantSampleInterval is how often the invariant sampler reads the cluster.
	InvariantSampleInterval = time.Second
	// InvariantSustainWindow is how long a broken invariant must persist before
	// it counts: the same window the pair-alive tracker uses.
	InvariantSustainWindow = time.Duration(PairAliveSustainWindow * float64(time.Second))
	// invariantReadTimeout bounds the apiserver reads of one sample.
	invariantReadTimeout = 15 * time.Second
	// InvariantPodCountTolerance is how long a row's podCount may differ from
	// the pod list before it counts: the engine rewrites the counter when a
	// step completes, not when a pod appears or disappears.
	InvariantPodCountTolerance = 15 * time.Second

	// The invariants by name: enough pairable pods keep serving (I1), every
	// traffic share has a serving pod on its revision (I2), and each Instance
	// row tells the truth about its pods (I4).
	InvariantPairableCapacity = "I1"
	InvariantTrafficBacked    = "I2"
	InvariantStatusTruth      = "I4"
)

// invariantComponents are the Components an InferenceService projects a
// workload for; the first two pair with each other.
var invariantComponents = []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent, v1beta1.RouterComponent}

// ComponentBudget is what one Component owes while it rolls: Desired pods,
// and the disruption budget the engine resolved for it.
type ComponentBudget struct {
	Desired int32
	// MaxUnavailable is escalation.BudgetNoLimit when no layer caps disruption,
	// which leaves the Component without a serving floor.
	MaxUnavailable int32
}

// InvariantTarget names one InferenceService to sample.
type InvariantTarget struct {
	Namespace string
	Name      string
	// Budgets pins the per-Component budgets. nil derives them on every sample
	// from the InferenceReplicas the InferenceService projected and its
	// rollout groups (DeriveComponentBudgets).
	Budgets map[v1beta1.ComponentType]ComponentBudget
}

// InvariantTargetSource lists the InferenceServices to sample right now.
type InvariantTargetSource func(ctx context.Context) ([]InvariantTarget, error)

// InvariantViolation is one invariant that stayed broken for the whole
// sustain window.
type InvariantViolation struct {
	// Invariant is I1 (pairable capacity), I2 (traffic backed) or I4 (status truth).
	Invariant string
	// Target is the namespace/name of the InferenceService.
	Target string
	// Subject is the Component, traffic target or Instance row concerned.
	Subject string
	// FirstDetail describes the first failing sample in words, Detail the
	// latest one; the window between them may have changed shape.
	FirstDetail string
	Detail      string
	FirstSeen   time.Time
	LastSeen    time.Time
	// Count is the number of samples that saw the violation past the window.
	Count int
}

// InvariantSamplerOptions tunes one sampler; zero values take the defaults.
type InvariantSamplerOptions struct {
	Interval time.Duration
	Sustain  time.Duration
	// Enforce makes Report fail the spec on a violation; otherwise Report only
	// writes the violations out.
	Enforce bool
}

// InvariantSampler reads an InferenceService's pods, status and projected
// InferenceReplicas on an interval and records every invariant that stays
// broken for the sustain window. It only reads from the cluster.
type InvariantSampler struct {
	reader  client.Reader
	targets InvariantTargetSource
	opts    InvariantSamplerOptions
	now     func() time.Time

	mu sync.Mutex
	// armed is, per target and Component, the desired count the serving floor
	// was last seen fully met at; the floor applies only while it matches.
	armed    map[string]int32
	open     map[string]*openSpan
	recorded map[string]*InvariantViolation
	samples  int
	readErrs int
	lastErr  string

	started  bool
	stopOnce sync.Once
	done     chan struct{}
	finished chan struct{}
}

// openSpan is a violation seen on every sample since its first failing one.
type openSpan struct {
	since time.Time
	// first is the detail of the first failing sample, detail the latest.
	first, detail string
}

// finding is one broken invariant in one sample.
type finding struct {
	invariant, target, subject, detail string
	// window is how long the finding must stand before it counts; zero
	// takes the sampler's sustain window.
	window time.Duration
}

func (f finding) key() string { return f.invariant + "|" + f.target + "|" + f.subject }

// NewInvariantSampler builds a sampler for one InferenceService.
func NewInvariantSampler(reader client.Reader, target InvariantTarget, opts InvariantSamplerOptions) *InvariantSampler {
	return NewInvariantSamplerFromSource(reader, func(context.Context) ([]InvariantTarget, error) {
		return []InvariantTarget{target}, nil
	}, opts)
}

// NewInvariantSamplerFromSource builds a sampler whose targets source lists
// on every sample, so InferenceServices a spec creates later are picked up.
func NewInvariantSamplerFromSource(reader client.Reader, source InvariantTargetSource, opts InvariantSamplerOptions) *InvariantSampler {
	if opts.Interval <= 0 {
		opts.Interval = InvariantSampleInterval
	}
	if opts.Sustain <= 0 {
		opts.Sustain = InvariantSustainWindow
	}
	return &InvariantSampler{
		reader:   reader,
		targets:  source,
		opts:     opts,
		now:      time.Now,
		armed:    map[string]int32{},
		open:     map[string]*openSpan{},
		recorded: map[string]*InvariantViolation{},
		done:     make(chan struct{}),
		finished: make(chan struct{}),
	}
}

// Start runs the sampling loop until Stop or ctx ends. Returns immediately.
func (s *InvariantSampler) Start(ctx context.Context) {
	s.mu.Lock()
	if s.started {
		s.mu.Unlock()
		return
	}
	s.started = true
	s.mu.Unlock()
	go s.run(ctx)
}

func (s *InvariantSampler) run(ctx context.Context) {
	defer close(s.finished)
	tick := time.NewTicker(s.opts.Interval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.done:
			return
		case <-tick.C:
			// A tick that was ready when Stop closed done must not read
			// the cluster after the spec has handed over to its cleanup.
			select {
			case <-ctx.Done():
				return
			case <-s.done:
				return
			default:
			}
			s.sample(ctx)
		}
	}
}

// Stop ends sampling, waits for a sample in flight, and returns the
// violations recorded so far. Safe to call more than once.
func (s *InvariantSampler) Stop() []InvariantViolation {
	s.stopOnce.Do(func() { close(s.done) })
	s.mu.Lock()
	started := s.started
	s.mu.Unlock()
	if started {
		<-s.finished
	}
	return s.Violations()
}

// Violations returns the recorded violations, earliest first.
func (s *InvariantSampler) Violations() []InvariantViolation {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]InvariantViolation, 0, len(s.recorded))
	for _, v := range s.recorded {
		out = append(out, *v)
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].FirstSeen.Equal(out[j].FirstSeen) {
			return out[i].FirstSeen.Before(out[j].FirstSeen)
		}
		if out[i].Invariant != out[j].Invariant {
			return out[i].Invariant < out[j].Invariant
		}
		return out[i].Target+out[i].Subject < out[j].Target+out[j].Subject
	})
	return out
}

// Report writes the sampler's outcome to the spec's log and, when there are
// violations, attaches them to the spec report. With Enforce set a violation
// fails the spec through g; otherwise the report is advisory.
func (s *InvariantSampler) Report(g gomega.Gomega) {
	violations := s.Violations()
	s.mu.Lock()
	samples, readErrs, lastErr := s.samples, s.readErrs, s.lastErr
	s.mu.Unlock()
	mode := "advisory"
	if s.opts.Enforce {
		mode = "enforced"
	}
	fmt.Fprintf(ginkgo.GinkgoWriter, "OMENative invariant sampler (%s): %d samples, %d read errors, %d violations sustained for %s\n",
		mode, samples, readErrs, len(violations), s.opts.Sustain)
	if readErrs > 0 {
		fmt.Fprintf(ginkgo.GinkgoWriter, "  last read error: %s\n", lastErr)
	}
	if len(violations) == 0 {
		return
	}
	text := formatInvariantViolations(violations)
	fmt.Fprint(ginkgo.GinkgoWriter, text)
	ginkgo.AddReportEntry("OMENative invariant violations ("+mode+")", text)
	if s.opts.Enforce {
		g.Expect(violations).To(gomega.BeEmpty(), "OMENative invariants stayed broken for %s:\n%s", s.opts.Sustain, text)
	}
}

// formatInvariantViolations renders one line per violation, with the first
// sample's message beside the last one's so the whole window is explained.
func formatInvariantViolations(violations []InvariantViolation) string {
	var b strings.Builder
	for _, v := range violations {
		detail := v.Detail + " (same at first and last sample)"
		if v.FirstDetail != v.Detail {
			detail = "first sample: " + v.FirstDetail + "; last sample: " + v.Detail
		}
		fmt.Fprintf(&b, "  %s %s %s: first %s, last %s, %d samples: %s\n",
			v.Invariant, v.Target, v.Subject,
			v.FirstSeen.UTC().Format(time.RFC3339), v.LastSeen.UTC().Format(time.RFC3339), v.Count, detail)
	}
	return b.String()
}

func (s *InvariantSampler) noteError(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.readErrs++
	s.lastErr = err.Error()
}

// sample reads every target once and folds the findings. A read error
// discards the whole sample, so open spans neither grow nor close on it.
func (s *InvariantSampler) sample(ctx context.Context) {
	defer func() {
		if r := recover(); r != nil {
			s.noteError(fmt.Errorf("sample panicked: %v", r))
		}
	}()
	rctx, cancel := context.WithTimeout(ctx, invariantReadTimeout)
	defer cancel()
	targets, err := s.targets(rctx)
	if err != nil {
		s.noteError(fmt.Errorf("list targets: %w", err))
		return
	}
	observations := make([]*targetObservation, 0, len(targets))
	for _, t := range targets {
		obs, err := observeTarget(rctx, s.reader, t)
		if err != nil {
			s.noteError(fmt.Errorf("%s/%s: %w", t.Namespace, t.Name, err))
			return
		}
		observations = append(observations, obs)
	}
	s.mu.Lock()
	var findings []finding
	for _, obs := range observations {
		findings = append(findings, evaluateTarget(obs, s.armed)...)
	}
	s.foldLocked(s.now(), findings)
	s.mu.Unlock()
}

// foldLocked advances the debounce by one sample taken at now: a finding
// opens a span on its first sample, is recorded once the span reaches its
// window (the sustain window, or the finding's own when longer), and closes
// its span on the first sample without it.
func (s *InvariantSampler) foldLocked(now time.Time, findings []finding) {
	s.samples++
	seen := map[string]bool{}
	for _, f := range findings {
		k := f.key()
		seen[k] = true
		span := s.open[k]
		if span == nil {
			span = &openSpan{since: now, first: f.detail}
			s.open[k] = span
		}
		span.detail = f.detail
		window := s.opts.Sustain
		if f.window > window {
			window = f.window
		}
		if now.Sub(span.since) < window {
			continue
		}
		v := s.recorded[k]
		if v == nil {
			v = &InvariantViolation{Invariant: f.invariant, Target: f.target, Subject: f.subject, FirstDetail: span.first, FirstSeen: span.since}
			s.recorded[k] = v
		}
		v.LastSeen = now
		v.Detail = f.detail
		v.Count++
	}
	for k := range s.open {
		if !seen[k] {
			delete(s.open, k)
		}
	}
}

// targetObservation is one read of everything the invariants need about one
// InferenceService.
type targetObservation struct {
	target InvariantTarget
	isvc   *v1beta1.InferenceService
	// pods are every OMENative pod of the InferenceService, any state.
	pods []*corev1.Pod
	// rows are the decoded Instance rows per Component.
	rows    map[v1beta1.ComponentType][]v1beta1.OMENativeInstanceStatus
	budgets map[v1beta1.ComponentType]ComponentBudget
	// setSizes is, per Component, how many pods one Instance owns: the sum
	// of the runner sizes the InferenceReplica declares.
	setSizes map[v1beta1.ComponentType]int32
}

// observeTarget reads the InferenceService, its OMENative pods and its
// projected InferenceReplicas. A deleted InferenceService yields an empty
// observation, which closes every span of the target.
func observeTarget(ctx context.Context, reader client.Reader, t InvariantTarget) (*targetObservation, error) {
	obs := &targetObservation{target: t}
	isvc := &v1beta1.InferenceService{}
	if err := reader.Get(ctx, client.ObjectKey{Namespace: t.Namespace, Name: t.Name}, isvc); err != nil {
		if apierrors.IsNotFound(err) {
			return obs, nil
		}
		return nil, fmt.Errorf("get InferenceService: %w", err)
	}
	obs.isvc = isvc

	podList := &corev1.PodList{}
	if err := reader.List(ctx, podList, client.InNamespace(t.Namespace), client.MatchingLabels{
		LabelManagedByKey:                     ManagedByOMENativeValue,
		constants.InferenceServicePodLabelKey: t.Name,
	}); err != nil {
		return nil, fmt.Errorf("list pods: %w", err)
	}
	for i := range podList.Items {
		obs.pods = append(obs.pods, &podList.Items[i])
	}

	irList := &v1beta1.InferenceReplicaList{}
	if err := reader.List(ctx, irList, client.InNamespace(t.Namespace)); err != nil {
		return nil, fmt.Errorf("list InferenceReplicas: %w", err)
	}
	replicas := map[v1beta1.ComponentType]*v1beta1.InferenceReplica{}
	obs.rows = map[v1beta1.ComponentType][]v1beta1.OMENativeInstanceStatus{}
	obs.setSizes = map[v1beta1.ComponentType]int32{}
	for i := range irList.Items {
		ir := &irList.Items[i]
		if ir.ParentName() != t.Name {
			continue
		}
		for _, c := range invariantComponents {
			if ir.Name != irprojector.InferenceReplicaName(t.Name, c) {
				continue
			}
			rows, err := DecodedInstanceStatuses(ir)
			if err != nil {
				return nil, fmt.Errorf("decode InferenceReplica %s: %w", ir.Name, err)
			}
			replicas[c] = ir
			obs.rows[c] = rows
			obs.setSizes[c] = instanceSetSize(ir)
		}
	}
	obs.budgets = t.Budgets
	if obs.budgets == nil {
		obs.budgets = DeriveComponentBudgets(isvc, replicas)
	}
	return obs, nil
}

// DeriveComponentBudgets reads the engine's own numbers for each Component:
// the replicas it projected onto the InferenceReplica, and the stricter of
// the per-Component rollingUpdate.maxUnavailable and the rollout group's
// maxUnavailable, both resolved by the engine's resolvers (a percent rounds
// up). A Component no layer caps gets escalation.BudgetNoLimit.
func DeriveComponentBudgets(isvc *v1beta1.InferenceService, replicas map[v1beta1.ComponentType]*v1beta1.InferenceReplica) map[v1beta1.ComponentType]ComponentBudget {
	out := map[v1beta1.ComponentType]ComponentBudget{}
	var groups []coordination.ResolvedGroup
	if isvc != nil {
		groups = coordination.ResolveGroups(isvc.Spec.Rollout, coordination.GroupDefaults{})
	}
	for c, ir := range replicas {
		if ir == nil || ir.Spec.Replicas == nil {
			continue
		}
		desired := *ir.Spec.Replicas
		var ru *workloadtypes.RollingUpdate
		if lc := ir.Spec.Lifecycle; lc != nil && lc.UpdateStrategy != nil && lc.UpdateStrategy.RollingUpdate != nil {
			ru = &workloadtypes.RollingUpdate{MaxUnavailable: lc.UpdateStrategy.RollingUpdate.MaxUnavailable}
		}
		budget := escalation.PerComponentMaxUnavailableBudget(ru, desired)
		if g, ok := coordination.MembershipFor(groups, c); ok {
			budget = stricterBudget(budget, coordination.MaxUnavailableBudget(g.Pacing, desired))
		}
		out[c] = ComponentBudget{Desired: desired, MaxUnavailable: budget}
	}
	return out
}

// stricterBudget composes two disruption caps the way the engine does: the
// smaller one wins, and a layer without a cap defers to the other.
func stricterBudget(a, b int32) int32 {
	switch {
	case a == escalation.BudgetNoLimit:
		return b
	case b == escalation.BudgetNoLimit:
		return a
	case b < a:
		return b
	}
	return a
}

// instanceSetSize is the number of pods one Instance of the InferenceReplica
// owns: the sum of its runner sizes. An InferenceReplica that publishes no
// runners is read as single-pod.
func instanceSetSize(ir *v1beta1.InferenceReplica) int32 {
	var n int32
	for _, r := range ir.Spec.Runners {
		n += r.Size
	}
	if n < 1 {
		return 1
	}
	return n
}

// activeSetPods picks the pods of an Instance's current set: those carrying
// the row's incarnation label, and any pod without the label.
func activeSetPods(row v1beta1.OMENativeInstanceStatus, pods []*corev1.Pod) []*corev1.Pod {
	var set []*corev1.Pod
	for _, pod := range pods {
		if inc, ok := query.InstanceIncarnationFromLabels(pod); ok && inc != row.Incarnation {
			continue
		}
		set = append(set, pod)
	}
	return set
}

// failureNamesReplacement reports a Failed row whose failure record names a
// pod other than the serving ones: a replacement attempt that died beside a
// source still carrying traffic, which the row may call Failed.
func failureNamesReplacement(row v1beta1.OMENativeInstanceStatus, serving []*corev1.Pod) bool {
	if row.LastFailure == nil || row.LastFailure.PodName == "" {
		return false
	}
	for _, pod := range serving {
		if pod.Name == row.LastFailure.PodName {
			return false
		}
	}
	return true
}

// remembersCrash reports a row whose failure record is dated after the row
// entered Ready: a failure of the set it promoted, not of one it replaced.
func remembersCrash(row v1beta1.OMENativeInstanceStatus) bool {
	return row.LastFailure != nil && row.ReadySince != nil && row.LastFailure.Time.After(row.ReadySince.Time)
}

// restartedSince reports a pod whose main container ran again after since:
// its last run ended after that time, or its current run began after it. A
// pod created after since is a new pod, not one of the pods that were Ready
// then: the first run of its container began after since by construction and
// is no restart.
func restartedSince(pod *corev1.Pod, since time.Time) bool {
	if pod.DeletionTimestamp != nil || (!pod.CreationTimestamp.IsZero() && pod.CreationTimestamp.After(since)) {
		return false
	}
	for _, cs := range pod.Status.ContainerStatuses {
		if cs.Name != constants.MainContainerName {
			continue
		}
		if t := cs.LastTerminationState.Terminated; t != nil && t.FinishedAt.After(since) {
			return true
		}
		if r := cs.State.Running; r != nil && r.StartedAt.After(since) {
			return true
		}
	}
	return false
}

// unprovenPromotedInstance finds an Instance that was promoted and has not
// proven itself since: its row remembers a crash after it entered Ready, or
// a pod of its current set restarted after that time.
func unprovenPromotedInstance(rows []v1beta1.OMENativeInstanceStatus, byIndex map[int32][]*corev1.Pod) (int32, bool) {
	for _, row := range rows {
		if row.ReadySince == nil || row.ReadySince.IsZero() {
			continue
		}
		if remembersCrash(row) {
			return row.Index, true
		}
		for _, pod := range activeSetPods(row, byIndex[row.Index]) {
			if restartedSince(pod, row.ReadySince.Time) {
				return row.Index, true
			}
		}
	}
	return 0, false
}

// failureNote renders the row's failure record for a violation line.
func failureNote(row v1beta1.OMENativeInstanceStatus) string {
	if row.LastFailure == nil {
		return "no failure record"
	}
	return fmt.Sprintf("failure record names pod %s (%s)", row.LastFailure.PodName, row.LastFailure.Reason)
}

// podInRotation reports a pod that carries traffic: not terminating, Ready,
// and holding the controller's serving gate.
func podInRotation(pod *corev1.Pod) bool {
	return pod != nil && pod.DeletionTimestamp == nil && podreadiness.ReadyAndServing(pod)
}

func podsByIndex(pods []*corev1.Pod) map[int32][]*corev1.Pod {
	out := map[int32][]*corev1.Pod{}
	for _, pod := range pods {
		if idx, ok := query.InstanceIdxFromLabels(pod); ok {
			out[idx] = append(out[idx], pod)
		}
	}
	return out
}

func podsByComponent(pods []*corev1.Pod) map[v1beta1.ComponentType][]*corev1.Pod {
	out := map[v1beta1.ComponentType][]*corev1.Pod{}
	for _, pod := range pods {
		c := v1beta1.ComponentType(pod.Labels[constants.OMEComponentLabel])
		if c == "" {
			continue
		}
		out[c] = append(out[c], pod)
	}
	return out
}

func pairingPeer(c v1beta1.ComponentType) (v1beta1.ComponentType, bool) {
	switch c {
	case v1beta1.EngineComponent:
		return v1beta1.DecoderComponent, true
	case v1beta1.DecoderComponent:
		return v1beta1.EngineComponent, true
	}
	return "", false
}

// pairingTransitionInFlight reports a protocol change under way: serving
// engine and decoder pods show two different non-empty protocols, or the
// InferenceService spec names a protocol a serving pod does not carry.
func pairingTransitionInFlight(byComp map[v1beta1.ComponentType][]*corev1.Pod, specProtocol string) bool {
	protocols := map[string]bool{}
	for _, c := range []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent} {
		for _, pod := range byComp[c] {
			if !podInRotation(pod) {
				continue
			}
			protocol := pod.Labels[query.LabelPairingProtocol]
			if protocol != specProtocol {
				return true
			}
			if protocol != "" {
				protocols[protocol] = true
			}
		}
	}
	return len(protocols) > 1
}

// countPairable counts the serving pods of one pairing Component that can
// pair: an empty protocol, or a serving peer pod with the same or an empty
// protocol.
func countPairable(pods, peers []*corev1.Pod) int32 {
	peerProtocols := map[string]bool{}
	for _, peer := range peers {
		if podInRotation(peer) {
			peerProtocols[peer.Labels[query.LabelPairingProtocol]] = true
		}
	}
	var n int32
	for _, pod := range pods {
		if !podInRotation(pod) {
			continue
		}
		protocol := pod.Labels[query.LabelPairingProtocol]
		if protocol == "" || peerProtocols[""] || peerProtocols[protocol] {
			n++
		}
	}
	return n
}

func sortedComponents[T any](m map[v1beta1.ComponentType]T) []v1beta1.ComponentType {
	out := make([]v1beta1.ComponentType, 0, len(m))
	for c := range m {
		out = append(out, c)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func podNames(pods []*corev1.Pod) string {
	names := make([]string, 0, len(pods))
	for _, pod := range pods {
		names = append(names, pod.Name)
	}
	sort.Strings(names)
	if len(names) == 0 {
		return "none"
	}
	return strings.Join(names, ",")
}

// podRestarts lists each pod's restart count, summed over its containers
// as kubectl shows it.
func podRestarts(pods []*corev1.Pod) string {
	parts := make([]string, 0, len(pods))
	for _, pod := range pods {
		var n int32
		for _, cs := range pod.Status.ContainerStatuses {
			n += cs.RestartCount
		}
		parts = append(parts, fmt.Sprintf("%s=%d", pod.Name, n))
	}
	sort.Strings(parts)
	if len(parts) == 0 {
		return "none"
	}
	return strings.Join(parts, ",")
}

// evaluateTarget checks one observation against the three invariants and
// returns what is broken right now. armed keeps, per target and Component,
// the desired count the serving floor last held at, so a Component that is
// still starting or scaling up is not read as a drop.
func evaluateTarget(obs *targetObservation, armed map[string]int32) []finding {
	if obs == nil || obs.isvc == nil {
		return nil
	}
	target := obs.target.Namespace + "/" + obs.target.Name
	byComp := podsByComponent(obs.pods)
	var out []finding

	// I1: once a Component has served its desired count, at least desired
	// minus maxUnavailable of its pods stay serving and pairable. A shortfall
	// of exactly one while a promoted Instance is unproven is its own
	// subject: the budget plus the one start the engine admitted before that
	// Instance crashed, which the owner may read as the contract or a gap.
	specProtocol := ""
	if obs.isvc.Spec.Rollout != nil && obs.isvc.Spec.Rollout.PairingProtocol != nil {
		specProtocol = *obs.isvc.Spec.Rollout.PairingProtocol
	}
	_, hasEngine := obs.budgets[v1beta1.EngineComponent]
	_, hasDecoder := obs.budgets[v1beta1.DecoderComponent]
	inFlight := hasEngine && hasDecoder && pairingTransitionInFlight(byComp, specProtocol)
	for _, c := range sortedComponents(obs.budgets) {
		budget := obs.budgets[c]
		var serving int32
		for _, pod := range byComp[c] {
			if podInRotation(pod) {
				serving++
			}
		}
		key := target + "/" + string(c)
		if serving >= budget.Desired {
			armed[key] = budget.Desired
		}
		if armed[key] != budget.Desired || budget.MaxUnavailable == escalation.BudgetNoLimit {
			continue
		}
		floor := budget.Desired - budget.MaxUnavailable
		if floor <= 0 {
			continue
		}
		pairable := serving
		pairNote := ""
		if peer, ok := pairingPeer(c); ok && inFlight {
			pairable = countPairable(byComp[c], byComp[peer])
			pairNote = fmt.Sprintf(", pairing transition to %q in flight", specProtocol)
		}
		if pairable < floor {
			subject := string(c)
			detail := fmt.Sprintf("%d pairable serving pods, floor %d (desired %d, maxUnavailable %d, serving %d%s)",
				pairable, floor, budget.Desired, budget.MaxUnavailable, serving, pairNote)
			if floor-pairable == 1 {
				if idx, ok := unprovenPromotedInstance(obs.rows[c], podsByIndex(byComp[c])); ok {
					subject += " (budget plus one while a promoted set is unproven)"
					detail += fmt.Sprintf("; instance %d was promoted and has not proven itself since", idx)
				}
			}
			out = append(out, finding{invariant: InvariantPairableCapacity, target: target, subject: subject, detail: detail})
		}
	}

	// I2: every traffic target with a share has a serving pod on its
	// revision. A Component with no serving pod at all is left to I1 and I4,
	// since no target could be right then. An unbacked target is one of two
	// subjects, each measured as its own window: the revision still has pods
	// and none serves, or it has no pod left and other revisions serve.
	for _, c := range sortedComponents(obs.isvc.Status.Components) {
		var servingPods []*corev1.Pod
		for _, pod := range byComp[c] {
			if podInRotation(pod) {
				servingPods = append(servingPods, pod)
			}
		}
		if len(servingPods) == 0 {
			continue
		}
		for _, tt := range obs.isvc.Status.Components[c].Traffic {
			if tt.Percent <= 0 {
				continue
			}
			hash := query.RevisionFromName(tt.RevisionName).Hash()
			if hash == "" {
				continue
			}
			backed := false
			var revisionPods []*corev1.Pod
			for _, pod := range byComp[c] {
				if pod.Labels[query.LabelRevisionHash] != hash {
					continue
				}
				revisionPods = append(revisionPods, pod)
				if podInRotation(pod) {
					backed = true
				}
			}
			if backed {
				continue
			}
			subject := string(c) + " " + tt.RevisionName
			if len(revisionPods) > 0 {
				out = append(out, finding{
					invariant: InvariantTrafficBacked, target: target, subject: subject + " (no pod serves)",
					detail: fmt.Sprintf("%d%% of traffic, %d pods carry revision hash %s and none serves (revision pods: %s; serving pods: %s)",
						tt.Percent, len(revisionPods), hash, podNames(revisionPods), podNames(servingPods)),
				})
				continue
			}
			out = append(out, finding{
				invariant: InvariantTrafficBacked, target: target, subject: subject + " (a pod of another revision serves)",
				detail: fmt.Sprintf("%d%% of traffic, no pod carries revision hash %s, the serving pods are on other revisions (serving pods: %s)",
					tt.Percent, hash, podNames(servingPods)),
			})
		}
	}

	// I4: a Ready row's set serves, a Failed row's set does not unless the
	// row's failure record names a replacement that died beside the serving
	// source, and podCount is the number of pods the API lists for the index,
	// as the engine counts. A set serves when every pod of it serves: the
	// pods of the index on the row's incarnation, as many as the Component's
	// runners declare.
	for _, c := range sortedComponents(obs.rows) {
		byIndex := podsByIndex(byComp[c])
		size := max(obs.setSizes[c], 1)
		for _, row := range obs.rows[c] {
			pods := byIndex[row.Index]
			set := activeSetPods(row, pods)
			var serving []*corev1.Pod
			for _, pod := range set {
				if podInRotation(pod) {
					serving = append(serving, pod)
				}
			}
			serves := int32(len(serving)) >= size
			subject := string(c) + " instance " + strconv.FormatInt(int64(row.Index), 10)
			switch {
			case row.Phase == v1beta1.OMENativeInstanceReady && !serves:
				out = append(out, finding{
					invariant: InvariantStatusTruth, target: target, subject: subject,
					detail: fmt.Sprintf("row is Ready but %d of %d pods of its set serve (servingPodCount %d; restarts: %s; pods: %s)",
						len(serving), size, row.ServingPodCount, podRestarts(pods), podNames(pods)),
				})
			case row.Phase == v1beta1.OMENativeInstanceFailed && serves && !failureNamesReplacement(row, serving):
				// A failure of the promoted set itself, with its pods back
				// and serving, is the engine's considered verdict on a set
				// that keeps restarting; it is recorded as its own subject.
				if remembersCrash(row) {
					out = append(out, finding{
						invariant: InvariantStatusTruth, target: target, subject: subject + " (Failed after promotion while the set restarts)",
						detail: fmt.Sprintf("row is Failed with every pod of its set serving (%d of %d); %s at %s, after the row entered Ready at %s (restarts: %s; pods: %s)",
							len(serving), size, failureNote(row), row.LastFailure.Time.UTC().Format(time.RFC3339),
							row.ReadySince.UTC().Format(time.RFC3339), podRestarts(pods), podNames(pods)),
					})
				} else {
					out = append(out, finding{
						invariant: InvariantStatusTruth, target: target, subject: subject,
						detail: fmt.Sprintf("row is Failed but every pod of its set serves (%d of %d, pods: %s; %s)",
							len(serving), size, podNames(pods), failureNote(row)),
					})
				}
			}
			if int32(len(pods)) != row.PodCount {
				out = append(out, finding{
					invariant: InvariantStatusTruth, target: target, subject: subject + " podCount",
					detail: fmt.Sprintf("row reports podCount %d, the API lists %d pods for the index (%s)",
						row.PodCount, len(pods), podNames(pods)),
					window: InvariantPodCountTolerance,
				})
			}
		}
	}
	return out
}
