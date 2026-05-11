package components_test

import (
	"context"
	"testing"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/controllerconfig"
	v1beta1testing "github.com/ome-projects/ome/pkg/utils/testing/v1beta1"

	"github.com/ome-projects/ome/tests/integration/components"
	"github.com/ome-projects/ome/tests/integration/framework"
	"github.com/ome-projects/ome/tests/integration/util"
)

// TestComponents is the Ginkgo entrypoint for components self-tests.
// Each It builds its own framework so wiring failures isolate
// cleanly — the cost (~5s envtest boot per spec) is acceptable for
// the small number of tests in this package.
func TestComponents(t *testing.T) {
	util.RunSuite(t, "Integration Components Self-Test Suite")
}

var _ = ginkgo.Describe("components.SetupBaseModel", func() {

	ginkgo.It("registers BaseModel + ClusterBaseModel reconcilers (proxy: both finalizers added)", func() {
		s, err := framework.NewSuite(&framework.Framework{})
		Expect(err).NotTo(HaveOccurred())
		ginkgo.DeferCleanup(func() { _ = s.Fwk.Teardown() })

		mgrSetup := func(ctx context.Context, mgr ctrl.Manager) error {
			return components.SetupBaseModel(ctx, mgr,
				components.WithOmeAgentConfig(&controllerconfig.OmeAgentConfig{
					Image:          "ghcr.io/test/ome-agent:test",
					ServiceAccount: "ome-model-metadata",
				}))
		}
		Expect(s.Fwk.StartManager(s.Ctx, s.Cfg, mgrSetup)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = s.Fwk.StopManager(s.Ctx) })

		ns, err := util.CreateNamespaceFromPrefix(s.Ctx, s.Client, "p1-4-")
		Expect(err).NotTo(HaveOccurred())

		bm := v1beta1testing.MakeBaseModel("llama-7b", ns.Name).
			StorageURI("pvc://nope/models/llama").Obj()
		Expect(s.Client.Create(s.Ctx, bm)).To(Succeed())
		cbm := v1beta1testing.MakeClusterBaseModel("shared-llama").
			StorageURI("pvc://nope-ns:nope-pvc/models/llama").Obj()
		Expect(s.Client.Create(s.Ctx, cbm)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = s.Client.Delete(context.Background(), cbm) })

		Eventually(func(g Gomega) {
			gotBM := &v1beta1.BaseModel{}
			gotCBM := &v1beta1.ClusterBaseModel{}
			g.Expect(s.Client.Get(s.Ctx, types.NamespacedName{Name: "llama-7b", Namespace: ns.Name}, gotBM)).To(Succeed())
			g.Expect(s.Client.Get(s.Ctx, types.NamespacedName{Name: "shared-llama"}, gotCBM)).To(Succeed())
			g.Expect(gotBM.Finalizers).NotTo(BeEmpty(), "BaseModel reconciler never added a finalizer")
			g.Expect(gotCBM.Finalizers).NotTo(BeEmpty(), "ClusterBaseModel reconciler never added a finalizer")
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.It("without WithOmeAgentConfig, a PVC reconcile surfaces SourceReachable=PVCConfigMissing", func() {
		s, err := framework.NewSuite(&framework.Framework{})
		Expect(err).NotTo(HaveOccurred())
		ginkgo.DeferCleanup(func() { _ = s.Fwk.Teardown() })

		mgrSetup := func(ctx context.Context, mgr ctrl.Manager) error {
			return components.SetupBaseModel(ctx, mgr) // no OmeAgentConfig
		}
		Expect(s.Fwk.StartManager(s.Ctx, s.Cfg, mgrSetup)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = s.Fwk.StopManager(s.Ctx) })

		ns, err := util.CreateNamespaceFromPrefix(s.Ctx, s.Client, "p1-4-noimage-")
		Expect(err).NotTo(HaveOccurred())

		ginkgo.By("pre-binding a PVC so the reconciler reaches the image-config check")
		pvc := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: ns.Name},
			Spec: corev1.PersistentVolumeClaimSpec{
				AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				Resources: corev1.VolumeResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")},
				},
			},
		}
		Expect(s.Client.Create(s.Ctx, pvc)).To(Succeed())
		pvc.Status.Phase = corev1.ClaimBound
		Expect(s.Client.Status().Update(s.Ctx, pvc)).To(Succeed())

		bm := v1beta1testing.MakeBaseModel("llama", ns.Name).
			StorageURI("pvc://p/models/llama").Obj()
		Expect(s.Client.Create(s.Ctx, bm)).To(Succeed())

		Eventually(func(g Gomega) {
			got := &v1beta1.BaseModel{}
			g.Expect(s.Client.Get(s.Ctx, types.NamespacedName{Name: "llama", Namespace: ns.Name}, got)).To(Succeed())
			cond := meta.FindStatusCondition(got.Status.Conditions, v1beta1.ModelConditionSourceReachable)
			g.Expect(cond).NotTo(BeNil())
			g.Expect(cond.Reason).To(Equal(v1beta1.ModelConditionReasonPVCConfigMissing))
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})
})
