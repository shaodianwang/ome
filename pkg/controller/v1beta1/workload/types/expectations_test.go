package types

import (
	"testing"
	"time"
)

func TestExpectations_SatisfiedWhenEmpty(t *testing.T) {
	e := NewExpectations()
	if !e.Satisfied("ns", "isvc", ComponentEngine, 0) {
		t.Fatal("empty cache should be satisfied")
	}
}

func TestExpectations_BlocksUntilObserved(t *testing.T) {
	e := NewExpectations()
	e.ExpectCreates("ns", "isvc", ComponentEngine, 0, 2)

	if e.Satisfied("ns", "isvc", ComponentEngine, 0) {
		t.Fatal("should NOT be satisfied with 2 pending adds")
	}

	e.ObservedCreate("ns", "isvc", ComponentEngine, 0)
	if e.Satisfied("ns", "isvc", ComponentEngine, 0) {
		t.Fatal("should NOT be satisfied with 1 pending add still")
	}

	e.ObservedCreate("ns", "isvc", ComponentEngine, 0)
	if !e.Satisfied("ns", "isvc", ComponentEngine, 0) {
		t.Fatal("should be satisfied after observing both creates")
	}
}

func TestExpectations_DeadlineForcesSatisfied(t *testing.T) {
	e := NewExpectations()
	e.ExpectCreates("ns", "isvc", ComponentEngine, 0, 1)
	// Manually expire the entry.
	e.mu.Lock()
	for _, v := range e.entries {
		v.Deadline = time.Now().Add(-1 * time.Second)
	}
	e.mu.Unlock()

	if !e.Satisfied("ns", "isvc", ComponentEngine, 0) {
		t.Fatal("expired entry should be reported as satisfied")
	}
}

func TestExpectations_ScopedByKey(t *testing.T) {
	e := NewExpectations()
	e.ExpectCreates("ns", "isvc", ComponentEngine, 0, 1)
	// Different instance index — should be unaffected.
	if !e.Satisfied("ns", "isvc", ComponentEngine, 1) {
		t.Fatal("instance 1 should still be satisfied")
	}
	// Different component — should be unaffected.
	if !e.Satisfied("ns", "isvc", ComponentDecoder, 0) {
		t.Fatal("decoder instance 0 should still be satisfied")
	}
}

func TestExpectations_Forget(t *testing.T) {
	e := NewExpectations()
	e.ExpectCreates("ns", "isvc", ComponentEngine, 0, 3)
	if e.Satisfied("ns", "isvc", ComponentEngine, 0) {
		t.Fatal("should not be satisfied before Forget")
	}
	e.Forget("ns", "isvc", ComponentEngine, 0)
	if !e.Satisfied("ns", "isvc", ComponentEngine, 0) {
		t.Fatal("Forget should clear the entry")
	}
}

func TestExpectations_DeletesTracked(t *testing.T) {
	e := NewExpectations()
	e.ExpectDeletes("ns", "isvc", ComponentEngine, 0, 1)
	if e.Satisfied("ns", "isvc", ComponentEngine, 0) {
		t.Fatal("should not be satisfied with pending delete")
	}
	e.ObservedDelete("ns", "isvc", ComponentEngine, 0)
	if !e.Satisfied("ns", "isvc", ComponentEngine, 0) {
		t.Fatal("delete observation should satisfy")
	}
}
