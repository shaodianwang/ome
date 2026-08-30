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

		conflicts := framework.WriteConflictLogs(logs, 0, nil)
		Expect(conflicts).To(HaveLen(1))
	})

	ginkgo.It("flags an error log whose message itself is the conflict", func() {
		logs, logger := newObservedLogger()
		logger.Error(realConflictError().Error())

		Expect(framework.WriteConflictLogs(logs, 0, nil)).To(HaveLen(1))
	})

	ginkgo.It("ignores a conflict logged below error level (info/warn are not gated)", func() {
		logs, logger := newObservedLogger()
		logger.Info("benign", zap.Error(realConflictError()))
		logger.Warn("benign", zap.Error(realConflictError()))

		Expect(framework.WriteConflictLogs(logs, 0, nil)).To(BeEmpty())
	})

	ginkgo.It("ignores error logs that are not write conflicts", func() {
		logs, logger := newObservedLogger()
		logger.Error("Reconciler error", zap.Error(errors.New("dial tcp: connection refused")))

		Expect(framework.WriteConflictLogs(logs, 0, nil)).To(BeEmpty())
	})

	ginkgo.It("VerifyNoWriteConflicts passes on a clean observer", func() {
		logs, logger := newObservedLogger()
		logger.Error("Reconciler error", zap.Error(errors.New("some unrelated failure")))

		// Should not fail the spec.
		framework.VerifyNoWriteConflicts(logs, 0, nil)
	})

	ginkgo.It("WriteConflictLogs tolerates a nil observer", func() {
		Expect(framework.WriteConflictLogs(nil, 0, nil)).To(BeEmpty())
		// And the gate is a no-op on nil (logger never wired).
		framework.VerifyNoWriteConflicts(nil, 0, nil)
	})

	ginkgo.It("ignores entries an earlier Framework in the same binary produced", func() {
		// The observer is process-global and never drained, so a
		// Framework's Teardown must assert only from where it started.
		logs, logger := newObservedLogger()
		logger.Error("earlier suite", zap.Error(realConflictError()))
		from := logs.Len()
		logger.Error("this suite", zap.Error(errors.New("unrelated")))

		Expect(framework.WriteConflictLogs(logs, from, nil)).To(BeEmpty())
		Expect(framework.WriteConflictLogs(logs, 0, nil)).To(HaveLen(1))
	})

	ginkgo.It("clamps an out-of-range start index instead of panicking", func() {
		logs, logger := newObservedLogger()
		logger.Error("only entry", zap.Error(realConflictError()))

		Expect(framework.WriteConflictLogs(logs, 99, nil)).To(BeEmpty())
		Expect(framework.WriteConflictLogs(logs, -1, nil)).To(HaveLen(1))
	})
})

// The rotator's own conflict, reproduced field for field. cert-controller
// logs it from ReconcileWH.ensureCerts, which reads the webhook
// configuration from an informer cache and writes it live, so a stale
// resourceVersion is reachable whenever the cache lags its own last write.
// These specs pin the shape the exemption keys on: if a cert-controller bump
// rewords the message, the exemption stops matching, the gate re-arms and the
// quota cert suite goes red.
var _ = ginkgo.Describe("Write-conflict exemptions", func() {

	const rotatorLogger = "cert-rotation"

	validatingWebhookGVK := schema.GroupVersionKind{
		Group:   "admissionregistration.k8s.io",
		Version: "v1",
		Kind:    "ValidatingWebhookConfiguration",
	}

	// rotatorConflict logs the injection failure the way the rotator does:
	// named logger, name + gvk context fields, the apiserver's Conflict error.
	rotatorConflict := func() *observer.ObservedLogs {
		core, logs := observer.New(zapcore.InfoLevel)
		gr := schema.GroupResource{
			Group:    "admissionregistration.k8s.io",
			Resource: "validatingwebhookconfigurations",
		}
		err := apierrors.NewConflict(gr, "ome-quota-manager.ome.io", errors.New(
			"the object has been modified; please apply your changes to the latest version and try again"))
		zap.New(core).Named(rotatorLogger).With(
			zap.String("name", "ome-quota-manager.ome.io"),
			zap.Any("gvk", validatingWebhookGVK),
		).Error("Error updating webhook with certificate", zap.Error(err))
		return logs
	}

	certExemption := framework.WriteConflictExemption{
		Logger:  rotatorLogger,
		Message: "Error updating webhook with certificate",
		GVK:     validatingWebhookGVK,
	}

	ginkgo.It("flags the rotator's injection conflict when unexempted", func() {
		Expect(framework.WriteConflictLogs(rotatorConflict(), 0, nil)).To(HaveLen(1))
	})

	ginkgo.It("exempts it when the suite names logger, message and kind", func() {
		exemptions := []framework.WriteConflictExemption{certExemption}
		Expect(framework.WriteConflictLogs(rotatorConflict(), 0, exemptions)).To(BeEmpty())
	})

	ginkgo.It("still flags the Secret refresh conflict from the same logger", func() {
		// The regression the quota cert suite actually guards: the CA
		// refreshed from two paths at once. Same logger, different message,
		// no gvk — so the injection exemption must not cover it.
		core, logs := observer.New(zapcore.InfoLevel)
		gr := schema.GroupResource{Group: "", Resource: "secrets"}
		err := apierrors.NewConflict(gr, "ome-quota-manager-webhook-cert", errors.New(
			"the object has been modified; please apply your changes to the latest version and try again"))
		zap.New(core).Named(rotatorLogger).
			Error("could not refresh CA and server certs", zap.Error(err))

		exemptions := []framework.WriteConflictExemption{certExemption}
		Expect(framework.WriteConflictLogs(logs, 0, exemptions)).To(HaveLen(1))
	})

	ginkgo.It("stops matching when the message is reworded", func() {
		stale := certExemption
		stale.Message = "Error updating webhook with cert"

		exemptions := []framework.WriteConflictExemption{stale}
		Expect(framework.WriteConflictLogs(rotatorConflict(), 0, exemptions)).To(HaveLen(1))
	})

	ginkgo.It("stops matching when the object kind differs", func() {
		stale := certExemption
		stale.GVK = schema.GroupVersionKind{
			Group:   "admissionregistration.k8s.io",
			Version: "v1",
			Kind:    "MutatingWebhookConfiguration",
		}

		exemptions := []framework.WriteConflictExemption{stale}
		Expect(framework.WriteConflictLogs(rotatorConflict(), 0, exemptions)).To(HaveLen(1))
	})

	ginkgo.It("stops matching when the logger differs", func() {
		stale := certExemption
		stale.Logger = "controller-runtime.manager"

		exemptions := []framework.WriteConflictExemption{stale}
		Expect(framework.WriteConflictLogs(rotatorConflict(), 0, exemptions)).To(HaveLen(1))
	})

	ginkgo.It("a zero-value exemption disarms nothing", func() {
		exemptions := []framework.WriteConflictExemption{{}}
		Expect(framework.WriteConflictLogs(rotatorConflict(), 0, exemptions)).To(HaveLen(1))
	})
})
