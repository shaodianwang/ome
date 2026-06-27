package framework_test

import (
	"errors"
	"fmt"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/ome-projects/ome/tests/integration/framework"
)

// realConflictError builds the exact error an apiserver returns when an
// Update loses an optimistic-lock race, so the predicate test pins the
// real apimachinery/apiserver wording rather than a hand-copied string.
func realConflictError() error {
	gr := schema.GroupResource{Group: "ome.io", Resource: "inferenceservices"}
	return apierrors.NewConflict(gr, "my-isvc", errors.New(
		"the object has been modified; please apply your changes to the latest version and try again"))
}

var _ = ginkgo.Describe("Write-conflict predicate", func() {

	ginkgo.It("matches a real apiserver Conflict error message", func() {
		Expect(framework.IsWriteConflictMessage(realConflictError().Error())).To(BeTrue())
	})

	ginkgo.It("matches a conflict message wrapped with controller context", func() {
		wrapped := fmt.Sprintf("updating status: %s after clearing finalizer", realConflictError().Error())
		Expect(framework.IsWriteConflictMessage(wrapped)).To(BeTrue())
	})

	ginkgo.It("does not match an unrelated error", func() {
		Expect(framework.IsWriteConflictMessage("connection refused")).To(BeFalse())
	})

	ginkgo.It("does not match a NotFound (different apiserver rejection)", func() {
		gr := schema.GroupResource{Group: "ome.io", Resource: "basemodels"}
		Expect(framework.IsWriteConflictMessage(apierrors.NewNotFound(gr, "x").Error())).To(BeFalse())
	})
})

var _ = ginkgo.Describe("Write-conflict gate over observed logs", func() {

	// newObservedLogger returns an observer plus a logger that writes
	// into it, mirroring how the framework tees controller-runtime logs
	// into an observer for the Teardown gate.
	newObservedLogger := func() (*observer.ObservedLogs, *zap.Logger) {
		core, logs := observer.New(zapcore.InfoLevel)
		return logs, zap.New(core)
	}

	ginkgo.It("flags an error log whose error field is a write conflict", func() {
		logs, logger := newObservedLogger()
		logger.Error("Reconciler error", zap.Error(realConflictError()))

		conflicts := framework.WriteConflictLogs(logs)
		Expect(conflicts).To(HaveLen(1))
	})

	ginkgo.It("flags an error log whose message itself is the conflict", func() {
		logs, logger := newObservedLogger()
		logger.Error(realConflictError().Error())

		Expect(framework.WriteConflictLogs(logs)).To(HaveLen(1))
	})

	ginkgo.It("ignores a conflict logged below error level (info/warn are not gated)", func() {
		logs, logger := newObservedLogger()
		logger.Info("benign", zap.Error(realConflictError()))
		logger.Warn("benign", zap.Error(realConflictError()))

		Expect(framework.WriteConflictLogs(logs)).To(BeEmpty())
	})

	ginkgo.It("ignores error logs that are not write conflicts", func() {
		logs, logger := newObservedLogger()
		logger.Error("Reconciler error", zap.Error(errors.New("dial tcp: connection refused")))

		Expect(framework.WriteConflictLogs(logs)).To(BeEmpty())
	})

	ginkgo.It("VerifyNoWriteConflicts passes on a clean observer", func() {
		logs, logger := newObservedLogger()
		logger.Error("Reconciler error", zap.Error(errors.New("some unrelated failure")))

		// Should not fail the spec.
		framework.VerifyNoWriteConflicts(logs)
	})

	ginkgo.It("WriteConflictLogs tolerates a nil observer", func() {
		Expect(framework.WriteConflictLogs(nil)).To(BeEmpty())
		// And the gate is a no-op on nil (logger never wired).
		framework.VerifyNoWriteConflicts(nil)
	})
})
