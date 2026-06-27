// Package recorder collects placement-latency and status-convergence
// measurements for the multi-cluster placement harness and writes a summary
// the checker asserts against.
//
// One Recorder instance measures ONE pass (funnel enabled OR disabled). A
// pass tracks, per source InferenceService, the create timestamp (stamped by
// the generator), the first-fan-out and Placed timestamps (stamped by the
// observer watching the control-plane source status), and the
// winner-admitted timestamp (stamped by the member-side admitter when it
// flips the winning derived admitted). From those it derives placement
// latency, time-to-first-fan-out, race-to-win, and — the headline metric —
// status-convergence latency (winner admitted -> source status reflects it).
//
// Design mirrors the InferenceService harness recorder: a single Run loop
// owns all mutable state and drains a bounded intake channel. Producers
// never block — when the channel is full they drop the event and bump a
// counter. Dropping rather than blocking is deliberate: an observer that
// blocks would back-pressure a controller-runtime informer and distort the
// very latency the harness measures. A non-zero drop count fails the checker.
package recorder

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"k8s.io/apimachinery/pkg/types"
)

// eventKind distinguishes the lifecycle stamps producers report.
type eventKind int

const (
	// eventCreated stamps when the generator created a source ISVC.
	eventCreated eventKind = iota
	// eventFanOut stamps when a derived first appeared on a candidate. Also
	// carries the candidate count for this source so the recorder can bucket
	// placement latency by fleet width.
	eventFanOut
	// eventAdmitted stamps when the member-side admitter flipped the winning
	// derived admitted (the start of the status flow-back the funnel/poll
	// races to converge).
	eventAdmitted
	// eventPlaced stamps when the source status first reported Placed.
	eventPlaced
)

// event is the unit the bounded channel carries.
type event struct {
	kind       eventKind
	key        types.NamespacedName
	class      string
	candidates int
	at         time.Time
}

// objState tracks one source ISVC across its create -> fan-out -> admitted
// -> placed lifecycle within a single pass.
type objState struct {
	class      string
	candidates int

	createdAt   time.Time
	fanOutAt    time.Time
	admittedAt  time.Time
	placedAt    time.Time
	haveCreated bool
	haveFanOut  bool
	haveAdmit   bool
	havePlaced  bool
}

// Recorder owns all measurement state for one pass. Construct with New, feed
// it via the Record* methods, run its loop with Run, and read the folded
// result with Result.
type Recorder struct {
	maxRecording time.Duration
	expectedTot  int

	running atomic.Bool
	dropped atomic.Uint64
	evCh    chan event

	// mu guards objs / placedCount, which only the Run loop mutates but
	// Result reads after the loop exits.
	mu          sync.Mutex
	objs        map[types.NamespacedName]*objState
	placedCount int
}

// New constructs a Recorder for one pass. maxRecording caps how long Run
// blocks waiting for the pass to converge; expectedTotal is the number of
// source ISVCs the generator will create (so Run can exit as soon as every
// one is Placed). intakeBuffer is the bounded channel depth (caller-supplied;
// no in-code default).
func New(maxRecording time.Duration, expectedTotal, intakeBuffer int) *Recorder {
	return &Recorder{
		maxRecording: maxRecording,
		expectedTot:  expectedTotal,
		evCh:         make(chan event, intakeBuffer),
		objs:         make(map[types.NamespacedName]*objState),
	}
}

// RecordCreated stamps the creation time of a source ISVC. Non-blocking.
func (r *Recorder) RecordCreated(key types.NamespacedName, class string, at time.Time) {
	r.enqueue(event{kind: eventCreated, key: key, class: class, at: at})
}

// RecordFanOut stamps when a derived first appeared on a candidate, with the
// candidate count for this source. Non-blocking.
func (r *Recorder) RecordFanOut(key types.NamespacedName, candidates int, at time.Time) {
	r.enqueue(event{kind: eventFanOut, key: key, candidates: candidates, at: at})
}

// RecordAdmitted stamps when the member-side admitter flipped the winning
// derived admitted. Non-blocking.
func (r *Recorder) RecordAdmitted(key types.NamespacedName, at time.Time) {
	r.enqueue(event{kind: eventAdmitted, key: key, at: at})
}

// RecordPlaced stamps when the source status first reported Placed.
// Non-blocking.
func (r *Recorder) RecordPlaced(key types.NamespacedName, at time.Time) {
	r.enqueue(event{kind: eventPlaced, key: key, at: at})
}

// enqueue pushes onto the bounded channel, dropping (and counting) when full
// so producers never block. The create/fan-out/admit stamps that can race
// ahead of Run are applied directly under the lock before Run starts so no
// baseline is lost to a startup race; a placed stamp outside the run window
// is dropped (it can't affect a converged pass).
func (r *Recorder) enqueue(ev event) {
	if !r.running.Load() {
		switch ev.kind {
		case eventCreated:
			r.applyCreated(ev)
		case eventFanOut:
			r.applyFanOut(ev)
		case eventAdmitted:
			r.applyAdmitted(ev)
		}
		return
	}
	select {
	case r.evCh <- ev:
	default:
		r.dropped.Add(1)
	}
}

// Run drains the intake channel until every expected source ISVC is Placed,
// the context is cancelled, or maxRecording elapses. Returns nil on clean
// convergence or context cancel; an error only on timeout so the caller can
// distinguish "ran out of time" from "asked to stop".
func (r *Recorder) Run(ctx context.Context) error {
	r.running.Store(true)
	defer r.running.Store(false)

	ctx, cancel := context.WithTimeout(ctx, r.maxRecording)
	defer cancel()

	// Periodic convergence check so a pass that has already recorded every
	// source exits promptly even if no further events arrive.
	tick := time.NewTicker(time.Second)
	defer tick.Stop()

	for {
		select {
		case <-ctx.Done():
			if ctx.Err() == context.DeadlineExceeded {
				return fmt.Errorf("recorder timed out after %s with %d/%d placed",
					r.maxRecording, r.currentPlaced(), r.expectedTot)
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
	case eventFanOut:
		r.applyFanOut(ev)
	case eventAdmitted:
		r.applyAdmitted(ev)
	case eventPlaced:
		r.applyPlaced(ev)
	}
}

func (r *Recorder) state(key types.NamespacedName) *objState {
	st, ok := r.objs[key]
	if !ok {
		st = &objState{}
		r.objs[key] = st
	}
	return st
}

func (r *Recorder) applyCreated(ev event) {
	r.mu.Lock()
	defer r.mu.Unlock()
	st := r.state(ev.key)
	if !st.haveCreated {
		st.class = ev.class
		st.createdAt = ev.at
		st.haveCreated = true
	}
}

func (r *Recorder) applyFanOut(ev event) {
	r.mu.Lock()
	defer r.mu.Unlock()
	st := r.state(ev.key)
	if !st.haveFanOut {
		st.fanOutAt = ev.at
		st.candidates = ev.candidates
		st.haveFanOut = true
	}
}

func (r *Recorder) applyAdmitted(ev event) {
	r.mu.Lock()
	defer r.mu.Unlock()
	st := r.state(ev.key)
	if !st.haveAdmit {
		st.admittedAt = ev.at
		st.haveAdmit = true
	}
}

func (r *Recorder) applyPlaced(ev event) {
	r.mu.Lock()
	defer r.mu.Unlock()
	st := r.state(ev.key)
	if !st.havePlaced {
		st.placedAt = ev.at
		st.havePlaced = true
		if st.haveCreated {
			r.placedCount++
		}
	}
}

// converged reports whether every expected source has a create and a placed
// stamp.
func (r *Recorder) converged() bool {
	return r.currentPlaced() >= r.expectedTot && r.expectedTot > 0
}

func (r *Recorder) currentPlaced() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.placedCount
}

// DroppedEvents returns the number of intake events dropped because the
// bounded channel was full.
func (r *Recorder) DroppedEvents() uint64 { return r.dropped.Load() }

// Result folds this pass's per-object state into a PassResult. Safe to call
// after Run returns. It is the raw material the runner combines into the
// two-pass Summary.
func (r *Recorder) Result() PassResult {
	r.mu.Lock()
	defer r.mu.Unlock()

	res := PassResult{
		ExpectedTotal: r.expectedTot,
		ByClass:       map[string][]int64{},
		ByCandidates:  map[int][]int64{},
	}
	for _, st := range r.objs {
		if !st.haveCreated || !st.havePlaced {
			continue
		}
		res.TotalPlaced++

		placeMs := nonNeg(st.placedAt.Sub(st.createdAt).Milliseconds())
		res.Placement = append(res.Placement, placeMs)
		if st.class != "" {
			res.ByClass[st.class] = append(res.ByClass[st.class], placeMs)
		}
		if st.haveFanOut {
			res.ByCandidates[st.candidates] = append(res.ByCandidates[st.candidates], placeMs)
			res.FirstFanOut = append(res.FirstFanOut, nonNeg(st.fanOutAt.Sub(st.createdAt).Milliseconds()))
		}
		if st.haveFanOut && st.haveAdmit {
			res.RaceToWin = append(res.RaceToWin, nonNeg(st.admittedAt.Sub(st.fanOutAt).Milliseconds()))
		}
		// Status convergence: the winner-admitted -> source-reflects-it
		// latency. Only meaningful when both stamps exist; the admit stamp is
		// what the funnel/poll races to flow back.
		if st.haveAdmit {
			res.Convergence = append(res.Convergence, nonNeg(st.placedAt.Sub(st.admittedAt).Milliseconds()))
		}
	}
	return res
}

func nonNeg(ms int64) int64 {
	if ms < 0 {
		return 0
	}
	return ms
}
