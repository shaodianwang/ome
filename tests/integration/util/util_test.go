package util_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/tests/integration/framework"
	"github.com/ome-projects/ome/tests/integration/util"
)

// TestUtil is the Ginkgo entrypoint for self-tests of the integration
// util package.
func TestUtil(t *testing.T) {
	util.RunSuite(t, "Integration Util Self-Test Suite")
}

// Suite-wide envtest shared across specs (boots once, ~5s).
var (
	suiteCtx    context.Context
	suiteFwk    *framework.Framework
	suiteClient client.Client
)

var _ = ginkgo.BeforeSuite(func() {
	s, err := framework.NewSuite(&framework.Framework{})
	Expect(err).NotTo(HaveOccurred())
	suiteFwk = s.Fwk
	suiteClient = s.Client
	suiteCtx = s.Ctx
})

var _ = ginkgo.AfterSuite(func() {
	if suiteFwk != nil {
		_ = suiteFwk.Teardown()
	}
})

var _ = ginkgo.Describe("util.CreateNamespaceFromPrefix", func() {
	ginkgo.It("two consecutive calls produce different names with the same prefix", func() {
		a, err := util.CreateNamespaceFromPrefix(suiteCtx, suiteClient, "ns-uniq-")
		Expect(err).NotTo(HaveOccurred())
		b, err := util.CreateNamespaceFromPrefix(suiteCtx, suiteClient, "ns-uniq-")
		Expect(err).NotTo(HaveOccurred())
		Expect(a.Name).NotTo(Equal(b.Name), "GenerateName must produce unique names")

		ginkgo.DeferCleanup(func() {
			_ = util.DeleteNamespace(suiteCtx, suiteClient, a.Name)
			_ = util.DeleteNamespace(suiteCtx, suiteClient, b.Name)
		})
	})
})

var _ = ginkgo.Describe("util.SeedDefaultConfigMaps", func() {
	ginkgo.It("creates inferenceservice-config in the OME namespace", func() {
		Expect(util.SeedDefaultConfigMaps(suiteCtx, suiteClient)).To(Succeed())

		ginkgo.By("idempotent: a second call returns nil without erroring")
		Expect(util.SeedDefaultConfigMaps(suiteCtx, suiteClient)).To(Succeed())

		ginkgo.By("the helpers in pkg/controller/v1beta1/controllerconfig parse the seeded JSON")
		// We don't call NewIngressConfig directly here (it uses
		// kubernetes.Interface, not controller-runtime client). The
		// real validation of these JSON shapes happens in the ISVC
		// suite that consumes them. Here we only assert the ConfigMap
		// exists with the right keys.
		cm := &corev1.ConfigMap{}
		Expect(suiteClient.Get(suiteCtx,
			types.NamespacedName{Namespace: "ome", Name: "inferenceservice-config"},
			cm)).To(Succeed())
		Expect(cm.Data).To(HaveKey("ingress"))
		Expect(cm.Data).To(HaveKey("deploy"))
		Expect(cm.Data).To(HaveKey("omeAgent"))
		Expect(cm.Data).To(HaveKey("lifecycle"))
		Expect(cm.Data["lifecycle"]).To(MatchJSON(`{"scaleUpPodBatchSize":100,"scaleDownPodBatchSize":100}`))
	})
})

var _ = ginkgo.Describe("util.DeleteNamespace", func() {

	ginkgo.It("runs registered cleanups with the right namespace argument", func() {
		ns, err := util.CreateNamespaceFromPrefix(suiteCtx, suiteClient, "ns-cleanup-")
		Expect(err).NotTo(HaveOccurred())

		var observed atomic.Value
		util.RegisterCleanup(func(ctx context.Context, c client.Client, namespace string) error {
			if namespace == ns.Name {
				observed.Store(namespace)
			}
			return nil
		})

		Expect(util.DeleteNamespace(suiteCtx, suiteClient, ns.Name)).To(Succeed())
		got, _ := observed.Load().(string)
		Expect(got).To(Equal(ns.Name), "cleanup hook must have observed our namespace")
	})

	ginkgo.It("propagates a CleanupFunc error via errors.Is", func() {
		ns, err := util.CreateNamespaceFromPrefix(suiteCtx, suiteClient, "ns-cleanuperr-")
		Expect(err).NotTo(HaveOccurred())

		target := errors.New("ns-cleanuperr-injected")
		util.RegisterCleanup(func(ctx context.Context, c client.Client, namespace string) error {
			if namespace == ns.Name {
				return target
			}
			return nil
		})

		err = util.DeleteNamespace(suiteCtx, suiteClient, ns.Name)
		Expect(err).To(HaveOccurred())
		Expect(errors.Is(err, target)).To(BeTrue(), "wrapped sentinel must surface")
	})

	ginkgo.It("an empty namespace name is a no-op (foot-gun guard for AfterEach with unset state)", func() {
		Expect(util.DeleteNamespace(context.Background(), suiteClient, "")).To(Succeed())
	})
})
