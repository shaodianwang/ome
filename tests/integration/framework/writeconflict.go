package framework

import (
	"fmt"
	"strings"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
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

// WriteConflictLogs returns every observed >=Error entry that looks
// like an apiserver write conflict. Empty slice means the run was
// clean. Exposed (rather than only asserted in Teardown) so suites or
// the self-test can inspect the offending entries directly.
func WriteConflictLogs(observed *observer.ObservedLogs) []observer.LoggedEntry {
	if observed == nil {
		return nil
	}
	return observed.Filter(isWriteConflictEntry).All()
}

// VerifyNoWriteConflicts fails the suite (via Gomega) if any observed
// log entry at or above ErrorLevel describes an apiserver write
// conflict. A reconciler that loses an optimistic-lock race and logs
// the rejection at error level signals a missing retry/conflict-aware
// update path — a real bug, not test flake — so the gate turns it into
// a hard failure rather than letting it hide in passing-suite noise.
//
// Called from Teardown after the manager has been stopped, so the
// observer holds the complete log history for the suite. Nil observer
// (logger never wired) is a no-op.
func VerifyNoWriteConflicts(observed *observer.ObservedLogs) {
	ginkgo.GinkgoHelper()
	conflicts := WriteConflictLogs(observed)
	gomega.Expect(conflicts).To(gomega.BeEmpty(),
		"controller logged %d write-conflict (optimistic-lock) error(s); a reconciler is missing conflict-aware retry. First: %s",
		len(conflicts), firstConflictDetail(conflicts))
}

// firstConflictDetail renders the first offending entry for the failure
// message. Kept tiny so the assertion line stays readable.
func firstConflictDetail(conflicts []observer.LoggedEntry) string {
	if len(conflicts) == 0 {
		return "<none>"
	}
	c := conflicts[0]
	return fmt.Sprintf("%q ctx=%v", c.Message, c.ContextMap())
}
