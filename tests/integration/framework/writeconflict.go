package framework

import (
	"fmt"
	"strings"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// Write-conflict detection markers.
//
// These are apiserver-protocol sentinels, NOT OME-configurable
// behavior: every Kubernetes apiserver emits an optimistic-concurrency
// rejection as a Conflict status whose message is built from
// conflictMessagePrefix (apimachinery's NewConflict) and ends with
// optimisticLockErrorSuffix (apiserver's
// registry.OptimisticLockErrorMsg). We match the strings rather than
// importing k8s.io/apiserver because (a) only the rendered log string
// is available to a log observer, not the typed error, and (b) pulling
// the apiserver store package in as a direct dependency of the test
// framework just to read one const is not worth the dependency weight.
// If a future apiserver reworded these, the gate would silently stop
// firing — acceptable for a best-effort hygiene check, and the
// framework self-test pins the exact strings so the drift is caught.
const (
	conflictMessagePrefix     = "Operation cannot be fulfilled on"
	optimisticLockErrorSuffix = "the object has been modified; please apply your changes to the latest version and try again"
)

// gvkContextKey is the log context field a controller attaches the
// object's GroupVersionKind under; cert-controller's rotator does,
// controller-runtime's own reconcile logger uses controllerKind
// instead. WriteConflictExemption.GVK is compared against it.
const gvkContextKey = "gvk"

// WriteConflictExemption names one write conflict a suite tolerates.
//
// The gate reads an error-level optimistic-lock rejection as "a
// reconciler lost a race it should have retried". That holds for OME's
// own reconcilers. It does not hold for a vendored controller that
// retries correctly but logs the rejection before requeueing: the suite
// converges anyway, and failing it reports a defect that is not there.
// An exemption names such a conflict precisely, so the gate stays armed
// for everything else in the suite — which is what distinguishes it
// from Framework.SkipWriteConflictCheck, an all-or-nothing switch.
//
// Every field that is set must match; a field left zero is not
// compared. An exemption with no field set matches nothing, so a zero
// value cannot silently disarm the gate.
//
// Matching on a dependency's log message couples the exemption to a
// string that dependency may reword. That direction of failure is the
// safe one: a reworded message stops matching, the gate re-arms, and
// the suite goes red rather than quietly tolerating a new conflict.
type WriteConflictExemption struct {
	// Logger is the entry's logger name — what the controller passed to
	// logr's WithName.
	Logger string
	// Message is the entry's log message, compared in full.
	Message string
	// GVK is the GroupVersionKind the entry carries under its "gvk"
	// context field. An entry with no such field never matches a
	// non-zero GVK.
	GVK schema.GroupVersionKind
}

// matches reports whether le is the conflict this exemption names.
func (e WriteConflictExemption) matches(le observer.LoggedEntry) bool {
	if e.Logger == "" && e.Message == "" && e.GVK.Empty() {
		return false
	}
	if e.Logger != "" && le.LoggerName != e.Logger {
		return false
	}
	if e.Message != "" && le.Message != e.Message {
		return false
	}
	if !e.GVK.Empty() {
		// GroupVersionKind is a fmt.Stringer, so zap stores the field as
		// StringerType and ContextMap renders it via String(). Comparing
		// against GVK.String() uses that same rendering; a gvk logged
		// any other way is not a string here and does not match.
		gvk, ok := le.ContextMap()[gvkContextKey].(string)
		if !ok || gvk != e.GVK.String() {
			return false
		}
	}
	return true
}

// IsWriteConflictMessage reports whether a rendered log/error string
// describes an apiserver optimistic-lock (write-conflict / concurrent
// modification) rejection. The conflict message is sometimes wrapped
// with a controller-supplied prefix or suffix (e.g. "updating status:
// <msg> after ..."), so both markers are matched with Contains rather
// than anchored to the start/end.
func IsWriteConflictMessage(msg string) bool {
	return strings.Contains(msg, conflictMessagePrefix) &&
		strings.Contains(msg, optimisticLockErrorSuffix)
}

// isWriteConflictEntry reports whether an observed log entry at or above
// ErrorLevel describes a write conflict. It inspects the entry message
// and every string-rendered context field (controller-runtime attaches
// the failing error under the "error" key, but suites may log it
// elsewhere), so a conflict surfaced either way is caught.
func isWriteConflictEntry(le observer.LoggedEntry) bool {
	if le.Level < zapcore.ErrorLevel {
		return false
	}
	if IsWriteConflictMessage(le.Message) {
		return true
	}
	for _, v := range le.ContextMap() {
		if s, ok := v.(string); ok && IsWriteConflictMessage(s) {
			return true
		}
	}
	return false
}

// isExempt reports whether any exemption names this entry.
func isExempt(le observer.LoggedEntry, exemptions []WriteConflictExemption) bool {
	for _, e := range exemptions {
		if e.matches(le) {
			return true
		}
	}
	return false
}

// WriteConflictLogs returns the observed >=Error entries that describe
// an apiserver write conflict, starting at from and with the declared
// exemptions removed. Empty slice means the run was clean. Exposed
// (rather than only asserted in Teardown) so suites or the self-test
// can inspect the offending entries directly.
//
// from is an index into the observer's history, taken when the
// Framework started. The observer is process-global and is never
// drained, so without that bound a Framework's Teardown would assert
// over every entry an earlier Framework in the same binary produced.
func WriteConflictLogs(observed *observer.ObservedLogs, from int, exemptions []WriteConflictExemption) []observer.LoggedEntry {
	if observed == nil {
		return nil
	}
	all := observed.All()
	if from < 0 {
		from = 0
	}
	if from > len(all) {
		from = len(all)
	}
	var conflicts []observer.LoggedEntry
	for _, le := range all[from:] {
		if isWriteConflictEntry(le) && !isExempt(le, exemptions) {
			conflicts = append(conflicts, le)
		}
	}
	return conflicts
}

// VerifyNoWriteConflicts fails the suite (via Gomega) if any entry
// logged from from onwards describes an unexempted apiserver write
// conflict.
//
// Called from Teardown after the manager has been stopped, so the
// observer holds the complete log history for the suite. Nil observer
// (logger never wired) is a no-op.
func VerifyNoWriteConflicts(observed *observer.ObservedLogs, from int, exemptions []WriteConflictExemption) {
	ginkgo.GinkgoHelper()
	conflicts := WriteConflictLogs(observed, from, exemptions)
	gomega.Expect(conflicts).To(gomega.BeEmpty(),
		"%d error-level write-conflict (optimistic-lock) rejection(s) logged during this suite. "+
			"From an OME reconciler this means a missing conflict-aware retry path; from a "+
			"dependency's controller that retries and converges, name it in "+
			"Framework.AllowedWriteConflicts. First: %s",
		len(conflicts), firstConflictDetail(conflicts))
}

// firstConflictDetail renders the first offending entry for the failure
// message. Kept tiny so the assertion line stays readable.
func firstConflictDetail(conflicts []observer.LoggedEntry) string {
	if len(conflicts) == 0 {
		return "<none>"
	}
	c := conflicts[0]
	return fmt.Sprintf("logger=%q %q ctx=%v", c.LoggerName, c.Message, c.ContextMap())
}
