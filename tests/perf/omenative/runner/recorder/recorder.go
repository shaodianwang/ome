// Package recorder collects time-to-Ready measurements for the OMENative
// scale harness and writes a summary the checker asserts against.
//
// Design mirrors the InferenceService harness recorder (itself modeled on
// kueue's): a single Run loop owns all mutable state and drains a bounded
// intake channel. Producers (the readiness observer, and the generator
// stamping creation times) never block — when the channel is full they
// drop the event and bump a counter. Dropping rather than blocking is
// deliberate: a readiness observer that blocks would back-pressure the
// controller-runtime informer and distort the very latency the harness is
// trying to measure. A non-zero drop count fails the checker so a
// saturated run can't masquerade as a clean baseline.
//
// What differs from the ISVC recorder: "ready" here means an
// InferenceReplica reported ReadyReplicas == Replicas (every Instance's
// pods Ready) — pods are real and driven by the fake kubelet — and the
// recorder additionally tracks per-pod create-to-Ready latency. The
// apiserver-op / status-conflict / workqueue-churn / idle-rate fields are
// not event-driven; the runner reads them once from the prometheus
// registry + opcount after convergence and injects them via the With*
// setters before WriteSummary.
package recorder

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/yaml"
)

// eventKind distinguishes the things producers report.
type eventKind int

const (
	// eventISVCCreated stamps the creation time of an InferenceService. The
	// generator emits one per object right after a successful Create.
	eventISVCCreated eventKind = iota
	// eventISVCReady stamps the moment an InferenceService's
	// InferenceReplica first reported ReadyReplicas == Replicas. The
	// observer emits one per object.
	eventISVCReady
	// eventPodCreated stamps a pod's creation timestamp. The observer emits
	// one the first time it sees a harness pod.
	eventPodCreated
	// eventPodReady stamps the moment a harness pod first reached Ready.
	eventPodReady
)

// event is the unit the bounded channel carries.
type event struct {
	kind  eventKind
	key   types.NamespacedName
	class string
	at    time.Time
}

// objState tracks one InferenceService across its create->ready lifecycle.
type objState struct {
	class       string
	createdAt   time.Time
	readyAt     time.Time
	haveCreated bool
	haveReady   bool
}

// podState tracks one pod across its create->ready lifecycle.
type podState struct {
	createdAt   time.Time
	readyAt     time.Time
	haveCreated bool
	haveReady   bool
}

// Recorder owns all measurement state. Construct with New, feed it via
// RecordISVCCreated / RecordISVCReady / RecordPodCreated / RecordPodReady,
// run its loop with Run, inject the registry-derived metrics with the
// With* setters, and emit the report with WriteSummary.
type Recorder struct {
	maxRecording time.Duration
	expectedTot  int
	expectedPods int

	// podsPerClass maps class name -> per-ISVC pod fan-out, supplied by the
	// runner from the config so the summary can relate convergence latency
	// to pod count without re-deriving it.
	podsPerClass map[string]int32

	running atomic.Bool
	dropped atomic.Uint64
	evCh    chan event

	// mu guards objs / pods / readyCount and the injected metrics, which
	// the Run loop mutates and WriteSummary reads after the loop exits.
	mu         sync.Mutex
	objs       map[types.NamespacedName]*objState
	pods       map[types.NamespacedName]*podState
	readyCount int

	// Injected post-convergence by the runner (see With* setters). Not
	// event-driven — read once from the prometheus registry + opcount.
	apiOps          map[string]int64
	apiOpsTotal     int64
	statusUpdates   map[string]*StatusUpdateSummary
	churn           map[string]*ChurnSummary
	idleReconcRate  float64
	haveInjectedOps bool
}

// New constructs a Recorder. maxRecording caps how long Run blocks waiting
// for the run to converge; expectedTotal is the number of
// InferenceServices the generator will create (so Run can exit as soon as
// every one is Ready instead of always waiting out the timeout).
// expectedPods is the total pod count (reported in the summary). podsPerClass
// maps class -> per-ISVC pod fan-out. intakeBuffer is the bounded channel
// depth — small enough to exert drop-on-full back-pressure visibility,
// large enough to absorb normal bursts. The caller supplies it (no in-code
// default).
func New(maxRecording time.Duration, expectedTotal, expectedPods, intakeBuffer int, podsPerClass map[string]int32) *Recorder {
	return &Recorder{
		maxRecording: maxRecording,
		expectedTot:  expectedTotal,
		expectedPods: expectedPods,
		podsPerClass: podsPerClass,
		evCh:         make(chan event, intakeBuffer),
		objs:         make(map[types.NamespacedName]*objState),
		pods:         make(map[types.NamespacedName]*podState),
	}
}

// RecordISVCCreated stamps the creation time of an InferenceService. Safe
// to call from any goroutine; non-blocking. Called by the generator.
func (r *Recorder) RecordISVCCreated(key types.NamespacedName, class string, at time.Time) {
	r.enqueue(event{kind: eventISVCCreated, key: key, class: class, at: at})
}

// RecordISVCReady stamps the moment an InferenceService first became Ready
// (its IR reported ReadyReplicas == Replicas). Safe to call from any
// goroutine; non-blocking. Called by the observer.
func (r *Recorder) RecordISVCReady(key types.NamespacedName, class string, at time.Time) {
	r.enqueue(event{kind: eventISVCReady, key: key, class: class, at: at})
}

// RecordPodCreated stamps a harness pod's creation time. Safe to call from
// any goroutine; non-blocking. Called by the observer.
func (r *Recorder) RecordPodCreated(key types.NamespacedName, at time.Time) {
	r.enqueue(event{kind: eventPodCreated, key: key, at: at})
}

// RecordPodReady stamps the moment a harness pod first became Ready. Safe
// to call from any goroutine; non-blocking. Called by the observer.
func (r *Recorder) RecordPodReady(key types.NamespacedName, at time.Time) {
	r.enqueue(event{kind: eventPodReady, key: key, at: at})
}

// enqueue pushes onto the bounded channel, dropping (and counting) when
// full so producers never block.
func (r *Recorder) enqueue(ev event) {
	if !r.running.Load() {
		// Before Run starts or after it exits we still accept directly under
		// the lock: the ISVC-created stamp (so no time-to-Ready baseline is
		// lost to a startup race) and BOTH pod stamps (the pod roll-up is
		// informational and its map dedupes, so a late pod-ready that lands
		// after the per-ISVC stream already converged should still count).
		// Only the ISVC-ready stamp is dropped outside the run window — it is
		// the convergence signal, already settled by the time Run exits.
		switch ev.kind {
		case eventISVCCreated, eventPodCreated, eventPodReady:
			r.apply(ev)
		}
		return
	}
	select {
	case r.evCh <- ev:
	default:
		r.dropped.Add(1)
	}
}

// Run drains the intake channel until every expected InferenceService is
// Ready, the context is cancelled, or maxRecording elapses. Returns nil on
// clean convergence or context cancel; an error only on timeout so the
// caller can distinguish "ran out of time" from "asked to stop".
func (r *Recorder) Run(ctx context.Context) error {
	r.running.Store(true)
	defer r.running.Store(false)

	ctx, cancel := context.WithTimeout(ctx, r.maxRecording)
	defer cancel()

	// Periodic convergence check so a run that has already recorded every
	// object exits promptly even if no further events arrive.
	tick := time.NewTicker(time.Second)
	defer tick.Stop()

	for {
		select {
		case <-ctx.Done():
			if ctx.Err() == context.DeadlineExceeded {
				return fmt.Errorf("recorder timed out after %s with %d/%d ISVCs ready",
					r.maxRecording, r.currentReady(), r.expectedTot)
			}
			return nil
		case <-tick.C:
			if r.converged() {
				return nil
			}
		case ev := <-r.evCh:
			r.apply(ev)
			if r.converged() {
				return nil
			}
		}
	}
}

// Drain applies every event still buffered in the intake channel and
// returns. It MUST be called only after all producers (observer, manager)
// have stopped — it does not synchronize against concurrent enqueue. The
// runner calls it after tearing the manager down so the pod-lifecycle
// events that were in-flight when Run exited on ISVC convergence (the
// per-ISVC stream converges a beat before the last pod-ready event drains)
// are still folded into the summary. Without it, the pod create-to-Ready
// roll-up systematically under-counts on a fast cold-create run.
func (r *Recorder) Drain() {
	for {
		select {
		case ev := <-r.evCh:
			r.apply(ev)
		default:
			return
		}
	}
}

func (r *Recorder) apply(ev event) {
	switch ev.kind {
	case eventISVCCreated:
		r.applyISVCCreated(ev)
	case eventISVCReady:
		r.applyISVCReady(ev)
	case eventPodCreated:
		r.applyPodCreated(ev)
	case eventPodReady:
		r.applyPodReady(ev)
	}
}

func (r *Recorder) applyISVCCreated(ev event) {
	r.mu.Lock()
	defer r.mu.Unlock()
	st, ok := r.objs[ev.key]
	if !ok {
		st = &objState{}
		r.objs[ev.key] = st
	}
	if !st.haveCreated {
		st.class = ev.class
		st.createdAt = ev.at
		st.haveCreated = true
	}
}

func (r *Recorder) applyISVCReady(ev event) {
	r.mu.Lock()
	defer r.mu.Unlock()
	st, ok := r.objs[ev.key]
	if !ok {
		// Ready arrived before the create stamp (possible if the observer
		// fires before the generator's RecordISVCCreated drained). Seed the
		// state; time-to-Ready is computed once both are present.
		st = &objState{}
		r.objs[ev.key] = st
	}
	if st.class == "" {
		st.class = ev.class
	}
	if !st.haveReady {
		st.readyAt = ev.at
		st.haveReady = true
		if st.haveCreated {
			r.readyCount++
		}
	}
}

func (r *Recorder) applyPodCreated(ev event) {
	r.mu.Lock()
	defer r.mu.Unlock()
	st, ok := r.pods[ev.key]
	if !ok {
		st = &podState{}
		r.pods[ev.key] = st
	}
	if !st.haveCreated {
		st.createdAt = ev.at
		st.haveCreated = true
	}
}

func (r *Recorder) applyPodReady(ev event) {
	r.mu.Lock()
	defer r.mu.Unlock()
	st, ok := r.pods[ev.key]
	if !ok {
		st = &podState{}
		r.pods[ev.key] = st
	}
	if !st.haveReady {
		st.readyAt = ev.at
		st.haveReady = true
	}
}

// converged reports whether every expected ISVC has both a creation and a
// ready stamp. Pod readiness is informational and not part of the
// convergence gate — an ISVC is Ready exactly when its IR reports
// ReadyReplicas == Replicas, which already implies every pod is Ready.
func (r *Recorder) converged() bool {
	return r.currentReady() >= r.expectedTot && r.expectedTot > 0
}

func (r *Recorder) currentReady() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.readyCount
}

// WithAPIOps injects the apiserver op counts (opcount.Flat()) and their
// grand total. Called by the runner after convergence.
func (r *Recorder) WithAPIOps(flat map[string]int64, total int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.apiOps = flat
	r.apiOpsTotal = total
	r.haveInjectedOps = true
}

// WithStatusUpdates injects the per-controller status-write counters read
// from the prometheus registry.
func (r *Recorder) WithStatusUpdates(s map[string]*StatusUpdateSummary) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.statusUpdates = s
}

// WithChurn injects the per-controller workqueue + reconcile counters read
// from the prometheus registry.
func (r *Recorder) WithChurn(c map[string]*ChurnSummary) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.churn = c
}

// WithIdleReconcileRate injects the post-convergence steady-state
// reconcile rate (reconciles/sec).
func (r *Recorder) WithIdleReconcileRate(rate float64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.idleReconcRate = rate
}

// Summarize folds the per-object + per-pod state and the injected metrics
// into a Summary. Safe to call after Run returns.
func (r *Recorder) Summarize() *Summary {
	r.mu.Lock()
	defer r.mu.Unlock()

	sum := &Summary{
		Classes:                 map[string]*ClassSummary{},
		ExpectedTotal:           r.expectedTot,
		ExpectedPods:            r.expectedPods,
		DroppedEvents:           r.dropped.Load(),
		APIOps:                  r.apiOps,
		APIOpsTotal:             r.apiOpsTotal,
		StatusUpdates:           r.statusUpdates,
		Churn:                   r.churn,
		IdleReconcileRatePerSec: r.idleReconcRate,
	}
	if sum.APIOps == nil {
		sum.APIOps = map[string]int64{}
	}
	if sum.StatusUpdates == nil {
		sum.StatusUpdates = map[string]*StatusUpdateSummary{}
	}
	if sum.Churn == nil {
		sum.Churn = map[string]*ChurnSummary{}
	}

	for _, st := range r.objs {
		if !st.haveCreated || !st.haveReady {
			continue
		}
		ms := st.readyAt.Sub(st.createdAt).Milliseconds()
		if ms < 0 {
			ms = 0
		}
		cs, ok := sum.Classes[st.class]
		if !ok {
			cs = &ClassSummary{PodsPerISVC: r.podsPerClass[st.class]}
			sum.Classes[st.class] = cs
		}
		cs.Count++
		cs.totalTimeToReadyMs += ms
		if ms > cs.MaxTimeToReadyMs {
			cs.MaxTimeToReadyMs = ms
		}
		sum.TotalReady++
	}
	for _, cs := range sum.Classes {
		if cs.Count > 0 {
			cs.AverageTimeToReadyMs = cs.totalTimeToReadyMs / int64(cs.Count)
		}
	}

	// Pod create-to-Ready roll-up (informational).
	for _, ps := range r.pods {
		if !ps.haveCreated || !ps.haveReady {
			continue
		}
		ms := ps.readyAt.Sub(ps.createdAt).Milliseconds()
		if ms < 0 {
			ms = 0
		}
		sum.PodCreateToReady.Count++
		sum.PodCreateToReady.totalMs += ms
		if ms > sum.PodCreateToReady.MaxMs {
			sum.PodCreateToReady.MaxMs = ms
		}
	}
	if sum.PodCreateToReady.Count > 0 {
		sum.PodCreateToReady.AverageMs = sum.PodCreateToReady.totalMs / int64(sum.PodCreateToReady.Count)
	}

	return sum
}

// WriteSummary marshals the run summary to path as YAML.
func (r *Recorder) WriteSummary(path string) error {
	b, err := yaml.Marshal(r.Summarize())
	if err != nil {
		return fmt.Errorf("marshal summary: %w", err)
	}
	return os.WriteFile(path, b, 0o644)
}
