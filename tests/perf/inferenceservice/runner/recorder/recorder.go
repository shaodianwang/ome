// Package recorder collects time-to-Ready measurements for the
// InferenceService scale harness and writes a summary the checker
// asserts against.
//
// Design mirrors kueue's performance recorder: a single Run loop owns
// all mutable state and drains a bounded intake channel. Producers (the
// readiness observer, and the generator stamping creation times) never
// block — when the channel is full they drop the event and bump a
// counter. Dropping rather than blocking is deliberate: a readiness
// observer that blocks would back-pressure the controller-runtime
// informer and distort the very latency the harness is trying to
// measure. A non-zero drop count fails the checker so a saturated run
// can't masquerade as a clean baseline.
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

// eventKind distinguishes the two things producers report.
type eventKind int

const (
	// eventCreated stamps the creation time of an InferenceService. The
	// generator emits one per object right after a successful Create.
	eventCreated eventKind = iota
	// eventReady stamps the moment an InferenceService's Ready condition
	// first flipped True. The observer emits one per object.
	eventReady
)

// event is the unit the bounded channel carries.
type event struct {
	kind  eventKind
	key   types.NamespacedName
	class string
	at    time.Time
}

// objState tracks one InferenceService across its create→ready lifecycle.
type objState struct {
	class       string
	createdAt   time.Time
	readyAt     time.Time
	haveCreated bool
	haveReady   bool
}

// Recorder owns all measurement state. Construct with New, feed it via
// RecordCreated / RecordReady, run its loop with Run, and emit the
// report with WriteSummary.
type Recorder struct {
	maxRecording time.Duration
	expectedTot  int

	running atomic.Bool
	dropped atomic.Uint64
	evCh    chan event

	// mu guards objs / readyCount, which only the Run loop mutates but
	// WriteSummary reads after the loop exits.
	mu         sync.Mutex
	objs       map[types.NamespacedName]*objState
	readyCount int
}

// New constructs a Recorder. maxRecording caps how long Run blocks
// waiting for the run to converge; expectedTotal is the number of
// InferenceServices the generator will create (so Run can exit as soon
// as every one is Ready instead of always waiting out the timeout).
// intakeBuffer is the bounded channel depth — small enough to exert
// drop-on-full back-pressure visibility, large enough to absorb normal
// bursts. The caller supplies it (no in-code default).
func New(maxRecording time.Duration, expectedTotal, intakeBuffer int) *Recorder {
	return &Recorder{
		maxRecording: maxRecording,
		expectedTot:  expectedTotal,
		evCh:         make(chan event, intakeBuffer),
		objs:         make(map[types.NamespacedName]*objState),
	}
}

// RecordCreated stamps the creation time of an InferenceService. Safe to
// call from any goroutine; non-blocking. Called by the generator.
func (r *Recorder) RecordCreated(key types.NamespacedName, class string, at time.Time) {
	r.enqueue(event{kind: eventCreated, key: key, class: class, at: at})
}

// RecordReady stamps the moment an InferenceService first became Ready.
// Safe to call from any goroutine; non-blocking. Called by the observer.
func (r *Recorder) RecordReady(key types.NamespacedName, class string, at time.Time) {
	r.enqueue(event{kind: eventReady, key: key, class: class, at: at})
}

// enqueue pushes onto the bounded channel, dropping (and counting) when
// full so producers never block.
func (r *Recorder) enqueue(ev event) {
	if !r.running.Load() {
		// Before Run starts or after it exits we still accept eventCreated
		// from the generator (it can race slightly ahead of Run); buffer
		// it directly under the lock so no creation time is lost. Ready
		// events outside the run window are dropped — they can't affect a
		// converged summary.
		if ev.kind == eventCreated {
			r.applyCreated(ev)
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
// Ready, the context is cancelled, or maxRecording elapses. Returns nil
// on clean convergence or context cancel; an error only on timeout so
// the caller can distinguish "ran out of time" from "asked to stop".
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
				return fmt.Errorf("recorder timed out after %s with %d/%d ready",
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

func (r *Recorder) apply(ev event) {
	switch ev.kind {
	case eventCreated:
		r.applyCreated(ev)
	case eventReady:
		r.applyReady(ev)
	}
}

func (r *Recorder) applyCreated(ev event) {
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

func (r *Recorder) applyReady(ev event) {
	r.mu.Lock()
	defer r.mu.Unlock()
	st, ok := r.objs[ev.key]
	if !ok {
		// Ready arrived before the create stamp (possible if the observer
		// fires before the generator's RecordCreated drained). Seed the
		// state; time-to-Ready will be computed once both are present.
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

// converged reports whether every expected object has both a creation
// and a ready stamp.
func (r *Recorder) converged() bool {
	return r.currentReady() >= r.expectedTot && r.expectedTot > 0
}

func (r *Recorder) currentReady() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.readyCount
}

// Summarize folds the per-object state into a Summary. Safe to call
// after Run returns.
func (r *Recorder) Summarize() *Summary {
	r.mu.Lock()
	defer r.mu.Unlock()

	sum := &Summary{
		Classes:       map[string]*ClassSummary{},
		ExpectedTotal: r.expectedTot,
		DroppedEvents: r.dropped.Load(),
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
			cs = &ClassSummary{}
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
