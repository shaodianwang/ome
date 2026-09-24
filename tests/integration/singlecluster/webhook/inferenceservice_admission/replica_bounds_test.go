package inferenceservice_admission

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	v1beta1testing "github.com/ome-projects/ome/pkg/utils/testing/v1beta1"
)

// ensureNamespace creates the "admissio-matrix" namespace shared by
// every spec in this suite.
func ensureNamespace() {
	ginkgo.GinkgoHelper()
	_ = k8sClient.Create(suiteCtx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: "admissio-matrix"},
	})
}

// admissionEngine returns a single-pod EngineSpec with one container so
// apiserver schema validation is satisfied.
func admissionEngine() *v1beta1.EngineSpec {
	return &v1beta1.EngineSpec{
		PodSpec: v1beta1.PodSpec{
			Containers: []corev1.Container{{Name: "ome-container", Image: "ghcr.io/example/runtime:v1"}},
		},
	}
}

// admissionRouter returns a single-pod RouterSpec with one container.
func admissionRouter() *v1beta1.RouterSpec {
	return &v1beta1.RouterSpec{
		PodSpec: v1beta1.PodSpec{
			Containers: []corev1.Container{{Name: "ome-container", Image: "ghcr.io/example/router:v1"}},
		},
	}
}

// baselineISVC returns the minimal ISVC that passes the validator: an
// Engine plus a named runtime reference (the runtime-only path accepts
// a reference that does not resolve to a stored runtime), so specs can
// focus on the replica/PDB/policy fields under test.
func baselineISVC(name string) *v1beta1testing.InferenceServiceWrapper {
	return v1beta1testing.MakeInferenceService(name, "admissio-matrix").
		Runtime("admissio-runtime").
		Engine(admissionEngine())
}

// mustGet re-reads the stored object after admission so assertions see
// exactly what the apiserver persisted.
func mustGet(name string) *v1beta1.InferenceService {
	ginkgo.GinkgoHelper()
	got := &v1beta1.InferenceService{}
	Expect(k8sClient.Get(suiteCtx,
		types.NamespacedName{Name: name, Namespace: "admissio-matrix"}, got)).To(Succeed())
	return got
}

// Replica-bounds admission. The validator judges the object as authored:
// minReplicas / maxReplicas are not defaulted at admission (the controller
// resolves deploy.replicas after the runtime merge), so an unset bound is
// stored unset and only explicit values are checked against each other.
var _ = ginkgo.Describe("ISVC admission matrix: replica bounds", func() {

	ginkgo.BeforeEach(ensureNamespace)

	ginkgo.It("create with no replica fields stores the object as authored", func() {
		isvc := baselineISVC("admissio-authored-bounds").Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, isvc) })

		got := mustGet("admissio-authored-bounds")
		Expect(got.Spec.Engine.MinReplicas).To(BeNil(), "unset minReplicas must be stored unset")
		Expect(got.Spec.Engine.MaxReplicas).To(BeZero(), "unset maxReplicas must be stored unset")
		Expect(got.Annotations).NotTo(HaveKey(constants.DeploymentMode),
			"admission must not write a deployment-mode annotation")
	})

	ginkgo.It("create with only minReplicas above the configured default max is admitted as authored", func() {
		// The validator compares only explicit bounds, so an explicit floor
		// above the configured ceiling is stored as written; the controller
		// raises the resolved max to it.
		isvc := baselineISVC("admissio-min-only").Obj()
		isvc.Spec.Engine.MinReplicas = ptr.To(5)
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, isvc) })

		got := mustGet("admissio-min-only")
		Expect(got.Spec.Engine.MinReplicas).To(HaveValue(Equal(5)))
		Expect(got.Spec.Engine.MaxReplicas).To(BeZero())
	})

	ginkgo.It("create with an explicit maxReplicas stores it and leaves minReplicas unset", func() {
		isvc := baselineISVC("admissio-explicit-max").Obj()
		isvc.Spec.Engine.MaxReplicas = 10
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, isvc) })

		got := mustGet("admissio-explicit-max")
		Expect(got.Spec.Engine.MaxReplicas).To(Equal(10))
		Expect(got.Spec.Engine.MinReplicas).To(BeNil())
	})

	ginkgo.It("create with explicit minReplicas greater than explicit maxReplicas is rejected with the engine field path", func() {
		isvc := baselineISVC("admissio-min-gt-max").Obj()
		isvc.Spec.Engine.MinReplicas = ptr.To(5)
		isvc.Spec.Engine.MaxReplicas = 2
		err := k8sClient.Create(suiteCtx, isvc)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("spec.engine.minReplicas"),
			"rejection must name the exact per-Component field path")
		Expect(err.Error()).To(ContainSubstring("must be <= maxReplicas"))
	})

	ginkgo.It("create with a negative router minReplicas is rejected with the router field path", func() {
		isvc := baselineISVC("admissio-negative-min").Router(admissionRouter()).Obj()
		isvc.Spec.Router.MinReplicas = ptr.To(-1)
		err := k8sClient.Create(suiteCtx, isvc)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("spec.router.minReplicas must be >= 0"),
			"negative min must be rejected on the Component that carries it, not a generic path")
	})

	ginkgo.It("create with explicit minReplicas=0 and no KEDA opt-in is rejected", func() {
		isvc := baselineISVC("admissio-scale-to-zero").Obj()
		isvc.Spec.Engine.MinReplicas = ptr.To(0)
		err := k8sClient.Create(suiteCtx, isvc)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("InvalidScaleToZero"))
	})

	ginkgo.It("engine+decoder create stores no deployment-mode annotation and no replica bounds", func() {
		isvc := baselineISVC("admissio-decoder-bounds").
			Decoder(&v1beta1.DecoderSpec{
				PodSpec: v1beta1.PodSpec{
					Containers: []corev1.Container{{Name: "ome-container", Image: "ghcr.io/example/decoder:v1"}},
				},
			}).Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, isvc) })

		got := mustGet("admissio-decoder-bounds")
		Expect(got.Annotations).NotTo(HaveKey(constants.DeploymentMode))
		Expect(got.Spec.Decoder.MinReplicas).To(BeNil())
		Expect(got.Spec.Decoder.MaxReplicas).To(BeZero())
	})

	ginkgo.It("update that clears maxReplicas stores it unset", func() {
		isvc := baselineISVC("admissio-update-clear-max").Obj()
		isvc.Spec.Engine.MaxReplicas = 6
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, isvc) })

		got := mustGet("admissio-update-clear-max")
		Expect(got.Spec.Engine.MaxReplicas).To(Equal(6))
		got.Spec.Engine.MaxReplicas = 0
		Expect(k8sClient.Update(suiteCtx, got)).To(Succeed())

		got = mustGet("admissio-update-clear-max")
		Expect(got.Spec.Engine.MaxReplicas).To(BeZero(),
			"a cleared maxReplicas is stored unset; the controller resolves the configured default")
	})

	ginkgo.It("update raising minReplicas above the stored maxReplicas is rejected", func() {
		isvc := baselineISVC("admissio-update-min-gt-max").Obj()
		isvc.Spec.Engine.MaxReplicas = 3
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, isvc) })

		got := mustGet("admissio-update-min-gt-max")
		got.Spec.Engine.MinReplicas = ptr.To(5)
		err := k8sClient.Update(suiteCtx, got)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("spec.engine.minReplicas"))
		Expect(err.Error()).To(ContainSubstring("must be <= maxReplicas"))
	})

	ginkgo.It("update leaving replica bounds untouched admits unrelated metadata changes", func() {
		isvc := baselineISVC("admissio-update-unrelated").Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, isvc) })

		got := mustGet("admissio-update-unrelated")
		if got.Labels == nil {
			got.Labels = map[string]string{}
		}
		got.Labels["admissio-touched"] = "true"
		Expect(k8sClient.Update(suiteCtx, got)).To(Succeed())

		got = mustGet("admissio-update-unrelated")
		Expect(got.Labels).To(HaveKeyWithValue("admissio-touched", "true"))
		Expect(got.Spec.Engine.MinReplicas).To(BeNil())
		Expect(got.Spec.Engine.MaxReplicas).To(BeZero())
	})
})

// Disruption-budget admission: the shape checks name the exact
// per-Component field path so the controller never builds an invalid
// PodDisruptionBudget.
var _ = ginkgo.Describe("ISVC admission matrix: disruption budgets", func() {

	ginkgo.BeforeEach(ensureNamespace)

	ginkgo.It("create with both minAvailable and maxUnavailable on one Component is rejected", func() {
		isvc := baselineISVC("admissio-pdb-both").Obj()
		isvc.Spec.Engine.MinAvailable = ptr.To(intstr.FromInt32(1))
		isvc.Spec.Engine.MaxUnavailable = ptr.To(intstr.FromString("25%"))
		err := k8sClient.Create(suiteCtx, isvc)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("spec.engine.minAvailable"))
		Expect(err.Error()).To(ContainSubstring("cannot both be set"))
	})

	ginkgo.It("create with a percentage above 100% is rejected", func() {
		isvc := baselineISVC("admissio-pdb-over").Obj()
		isvc.Spec.Engine.MinAvailable = ptr.To(intstr.FromString("150%"))
		err := k8sClient.Create(suiteCtx, isvc)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("spec.engine.minAvailable"))
		Expect(err.Error()).To(ContainSubstring("percentage from 0% to 100%"))
	})

	ginkgo.It("create with a valid percent maxUnavailable is admitted and stored", func() {
		isvc := baselineISVC("admissio-pdb-valid").Obj()
		isvc.Spec.Engine.MaxUnavailable = ptr.To(intstr.FromString("25%"))
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, isvc) })

		got := mustGet("admissio-pdb-valid")
		Expect(got.Spec.Engine.MaxUnavailable).To(HaveValue(Equal(intstr.FromString("25%"))))
		Expect(got.Spec.Engine.MinAvailable).To(BeNil())
	})
})
