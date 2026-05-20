package omenative

import (
	"sync"
	"time"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

// expectationsTTL is how long an Instance's expectation stays in the
// cache before falling back to a fresh observation. Two minutes is
// generous; even slow watch propagation should converge well before.
const expectationsTTL = 2 * time.Minute

// Expectations is a per-Instance counter cache. Before issuing a batch
// of pod creates or deletes, the controller calls ExpectCreates / ExpectDeletes
// so subsequent reconciles can tell when the watch event chain has caught
// up. Subsequent reconciles call Satisfied to check whether they can
// safely issue another batch (or proceed past the create step).
//
// Pattern borrowed from sigs.k8s.io/controller-runtime samples and the
// kubernetes/kubernetes ReplicaSet/StatefulSet controllers.
//
// Concurrency: all methods are safe for concurrent use; an in-process
// singleton (DefaultExpectations) is provided for the dispatch path.
type Expectations struct {
	mu      sync.Mutex
	entries map[expectationKey]*expectationEntry
}

type expectationKey struct {
	Namespace string
	ISVC      string
	Component v1beta1.ComponentType
	Instance  int32
}

type expectationEntry struct {
	Adds     int
	Deletes  int
	Deadline time.Time
}

// NewExpectations builds an empty cache.
func NewExpectations() *Expectations {
	return &Expectations{entries: make(map[expectationKey]*expectationEntry)}
}

// DefaultExpectations is the in-process singleton used by ReconcileComponent.
// Tests construct their own via NewExpectations.
var DefaultExpectations = NewExpectations()

// ExpectCreates records that n pod creates are in flight for the given
// (ISVC, Component, Instance). Satisfied returns false until all of them
// are observed via ObservedCreate, or the deadline elapses.
func (e *Expectations) ExpectCreates(namespace, isvc string, component v1beta1.ComponentType, instance int32, n int) {
	if n <= 0 {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	k := expectationKey{namespace, isvc, component, instance}
	ent, ok := e.entries[k]
	if !ok {
		ent = &expectationEntry{}
		e.entries[k] = ent
	}
	ent.Adds += n
	ent.Deadline = time.Now().Add(expectationsTTL)
}

// ExpectDeletes records that n pod deletes are in flight.
func (e *Expectations) ExpectDeletes(namespace, isvc string, component v1beta1.ComponentType, instance int32, n int) {
	if n <= 0 {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	k := expectationKey{namespace, isvc, component, instance}
	ent, ok := e.entries[k]
	if !ok {
		ent = &expectationEntry{}
		e.entries[k] = ent
	}
	ent.Deletes += n
	ent.Deadline = time.Now().Add(expectationsTTL)
}

// ObservedCreate decrements the create counter, called when a pod
// belonging to (ISVC, Component, Instance) appears in the watch cache.
func (e *Expectations) ObservedCreate(namespace, isvc string, component v1beta1.ComponentType, instance int32) {
	e.observed(namespace, isvc, component, instance, true)
}

// ObservedDelete decrements the delete counter, called when a pod
// belonging to (ISVC, Component, Instance) disappears.
func (e *Expectations) ObservedDelete(namespace, isvc string, component v1beta1.ComponentType, instance int32) {
	e.observed(namespace, isvc, component, instance, false)
}

func (e *Expectations) observed(namespace, isvc string, component v1beta1.ComponentType, instance int32, isAdd bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	k := expectationKey{namespace, isvc, component, instance}
	ent, ok := e.entries[k]
	if !ok {
		return
	}
	if isAdd && ent.Adds > 0 {
		ent.Adds--
	}
	if !isAdd && ent.Deletes > 0 {
		ent.Deletes--
	}
	if ent.Adds == 0 && ent.Deletes == 0 {
		delete(e.entries, k)
	}
}

// Satisfied returns true when no outstanding creates or deletes are
// expected for the Instance — either the watch cache has caught up,
// the entry expired, or no expectations were ever recorded.
func (e *Expectations) Satisfied(namespace, isvc string, component v1beta1.ComponentType, instance int32) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	k := expectationKey{namespace, isvc, component, instance}
	ent, ok := e.entries[k]
	if !ok {
		return true
	}
	if time.Now().After(ent.Deadline) {
		delete(e.entries, k)
		return true
	}
	return ent.Adds <= 0 && ent.Deletes <= 0
}

// Forget clears the entry, e.g., when the Instance is deleted entirely.
func (e *Expectations) Forget(namespace, isvc string, component v1beta1.ComponentType, instance int32) {
	e.mu.Lock()
	defer e.mu.Unlock()
	delete(e.entries, expectationKey{namespace, isvc, component, instance})
}
