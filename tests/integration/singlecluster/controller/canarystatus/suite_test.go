// Package canarystatus hosts integration tests for the InferenceService canary
// analysis STATUS surface against a real apiserver: the generated CRD schema
// must round-trip every analysis field the controller writes, enforce the
// metricResults map-list contract, and keep status behind its subresource.
// Unit tests write these fields into in-memory structs only, so a schema
// regression (a pruned field, a dropped list-type marker) is invisible to them.
package canarystatus

import (
	"context"
	"testing"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/tests/integration/framework"
	"github.com/ome-projects/ome/tests/integration/util"
)

const testNamespace = "canary-a-status"

var (
	suiteCtx    context.Context
	suiteCancel context.CancelFunc
	suiteFwk    *framework.Framework
	k8sClient   client.Client
)

func TestCanaryAnalysisStatusSurface(t *testing.T) {
	util.RunSuite(t, "Canary Analysis Status Surface Integration Suite")
}

var _ = ginkgo.BeforeSuite(func() {
	framework.SetupTestLogger()

	s, err := framework.NewSuite(&framework.Framework{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	suiteFwk = s.Fwk
	k8sClient = s.Client
	suiteCtx, suiteCancel = context.WithCancel(s.Ctx)

	gomega.Expect(k8sClient.Create(suiteCtx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: testNamespace},
	})).To(gomega.Succeed())
})

var _ = ginkgo.AfterSuite(func() {
	if suiteCancel != nil {
		suiteCancel()
	}
	if suiteFwk != nil {
		_ = suiteFwk.Teardown()
	}
})
