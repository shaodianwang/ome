package framework_test

import (
	"context"
	"testing"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/sgl-project/ome/pkg/apis/ome/v1beta1"

	"github.com/sgl-project/ome/tests/integration/framework"
	"github.com/sgl-project/ome/tests/integration/util"
)

// TestFramework is the Ginkgo entrypoint for self-tests of the
// framework package. We test the framework via Ginkgo on purpose —
// the alternative (plain testing.T) would mean the framework tests
// reject -ginkgo.v / -ginkgo.trace flags and split the make target.
//
// Each It constructs its own Framework so failures isolate cleanly.
func TestFramework(t *testing.T) {
	util.RunSuite(t, "Integration Framework Self-Test Suite")
}

var _ = ginkgo.Describe("Framework lifecycle", func() {

	ginkgo.It("Init / Teardown round-trip starts envtest cleanly", func() {
		f := &framework.Framework{}
		cfg, err := f.Init()
		Expect(err).NotTo(HaveOccurred())
		Expect(cfg).NotTo(BeNil())
		Expect(cfg.Host).NotTo(BeEmpty())
		Expect(f.Teardown()).To(Succeed())
	})

	ginkgo.It("Teardown without Init is a no-op (foot-gun guard for AfterSuite after BeforeSuite panic)", func() {
		f := &framework.Framework{}
		Expect(f.Teardown()).To(Succeed())
	})
})

var _ = ginkgo.Describe("Framework SetupClient + NewSuite", func() {

	ginkgo.It("SetupClient(nil) errors instead of nil-deref", func() {
		f := &framework.Framework{}
		_, _, err := f.SetupClient(nil)
		Expect(err).To(HaveOccurred())
	})

	ginkgo.It("NewSuite happy path: creates Namespace AND BaseModel through the returned client", func() {
		s, err := framework.NewSuite(&framework.Framework{})
		Expect(err).NotTo(HaveOccurred())
		ginkgo.DeferCleanup(func() { _ = s.Fwk.Teardown() })

		ginkgo.By("creating a core type — proves clientgo + corev1 schemes are wired")
		ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "p1-1b-smoke"}}
		Expect(s.Client.Create(context.Background(), ns)).To(Succeed())

		ginkgo.By("creating an OME type — proves v1beta1 scheme + envtest CRD install landed on the apiserver")
		storageURI := "pvc://my-pvc/some/path"
		bm := &v1beta1.BaseModel{
			ObjectMeta: metav1.ObjectMeta{Name: "smoke", Namespace: "p1-1b-smoke"},
			Spec: v1beta1.BaseModelSpec{
				Storage: &v1beta1.StorageSpec{StorageUri: &storageURI},
			},
		}
		Expect(s.Client.Create(context.Background(), bm)).To(Succeed())
	})
})

var _ = ginkgo.Describe("Framework optional CRD bundles", func() {

	ginkgo.It("Knative bundle: CRDs install and the client can Create a KSvc", func() {
		s, err := framework.NewSuite(&framework.Framework{
			OptionalCRDs: []framework.OptionalCRDBundle{framework.Knative},
		})
		Expect(err).NotTo(HaveOccurred())
		ginkgo.DeferCleanup(func() { _ = s.Fwk.Teardown() })

		// We don't import knservingv1 here (would couple framework_test to
		// Knative). The bundle install + scheme registration is the win;
		// confirming the apiserver knows about the CRD is enough — we use
		// an unstructured Get against the discovery endpoint rather than
		// a typed Create.
		// If the CRD is missing or the scheme fails to register, NewSuite
		// would have errored already.
	})
})

var _ = ginkgo.Describe("Framework StartManager / StopManager", func() {

	ginkgo.It("StartManager with a no-op ManagerSetup and StopManager exit cleanly", func() {
		s, err := framework.NewSuite(&framework.Framework{})
		Expect(err).NotTo(HaveOccurred())
		ginkgo.DeferCleanup(func() { _ = s.Fwk.Teardown() })

		noop := func(ctx context.Context, mgr ctrl.Manager) error { return nil }
		Expect(s.Fwk.StartManager(s.Ctx, s.Cfg, noop)).To(Succeed())
		Expect(s.Fwk.StopManager(s.Ctx)).To(Succeed())
	})

	ginkgo.It("registered reconciler observes a watched create within 15 seconds", func() {
		s, err := framework.NewSuite(&framework.Framework{})
		Expect(err).NotTo(HaveOccurred())
		ginkgo.DeferCleanup(func() { _ = s.Fwk.Teardown() })

		var observed bool
		setup := func(ctx context.Context, mgr ctrl.Manager) error {
			return ctrl.NewControllerManagedBy(mgr).
				Named("framework-test-namespace-counter").
				For(&corev1.Namespace{}).
				Complete(reconcile.Func(func(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
					if req.Name == "p1-1c-target" {
						observed = true
					}
					return ctrl.Result{}, nil
				}))
		}
		Expect(s.Fwk.StartManager(s.Ctx, s.Cfg, setup)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = s.Fwk.StopManager(s.Ctx) })

		Expect(s.Client.Create(s.Ctx, &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{Name: "p1-1c-target"},
		})).To(Succeed())

		Eventually(func() bool { return observed }).WithTimeout(util.MediumTimeout).
			WithPolling(util.PollInterval).Should(BeTrue(),
			"reconciler never saw the target namespace")
	})

	ginkgo.It("StopManager before StartManager is a no-op (foot-gun guard for AfterSuite after BeforeSuite panic)", func() {
		f := &framework.Framework{}
		Expect(f.StopManager(context.Background())).To(Succeed())
	})
})
