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
// exactly what the apiserver persisted (defaulter output included).
func mustGet(name string) *v1beta1.InferenceService {
	ginkgo.GinkgoHelper()
	got := &v1beta1.InferenceService{}
	Expect(k8sClient.Get(suiteCtx,
		types.NamespacedName{Name: name, Namespace: "admissio-matrix"}, got)).To(Succeed())
	return got
}

// Replica-bounds admission through the full defaulter → validator
// chain. The validator judges the DEFAULTED object: minReplicas
// defaults to 1 and an unset maxReplicas is stamped (engine/decoder 3,
// router 2) before any bounds check runs, so these specs pin the
// combinations where defaulting and validation interact.
var _ = ginkgo.Describe("ISVC admission matrix: replica bounds", func() {

	ginkgo.BeforeEach(ensureNamespace)

	ginkgo.It("create with no replica fields stores the defaulted bounds", func() {
		isvc := baselineISVC("admissio-defaulted-bounds").Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, isvc) })

		got := mustGet("admissio-defaulted-bounds")
		Expect(got.Spec.Engine.MinReplicas).To(HaveValue(Equal(1)),
			"unset minReplicas must be defaulted, not stored nil")
		Expect(got.Spec.Engine.MaxReplicas).To(Equal(3),
			"unset maxReplicas must be stamped by the defaulter, not stored 0")
		Expect(got.Annotations).To(HaveKeyWithValue(constants.DeploymentMode, string(constants.RawDeployment)),
			"engine-only shape must inherit the configured default deployment mode")
	})

	ginkgo.It("create with minReplicas equal to the defaulted maxReplicas is admitted", func() {
		// Boundary of the defaulted-max interaction: the validator sees
		// max stamped to the default, so an explicit min at exactly that
		// value must still admit.
		isvc := baselineISVC("admissio-min-eq-defaultmax").Obj()
		isvc.Spec.Engine.MinReplicas = ptr.To(3)
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, isvc) })

		got := mustGet("admissio-min-eq-defaultmax")
		Expect(got.Spec.Engine.MinReplicas).To(HaveValue(Equal(3)))
		Expect(got.Spec.Engine.MaxReplicas).To(Equal(3))
	})

	ginkgo.It("create preserves an explicit maxReplicas instead of re-stamping the default", func() {
		isvc := baselineISVC("admissio-explicit-max").Obj()
		isvc.Spec.Engine.MaxReplicas = 10
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, isvc) })

		got := mustGet("admissio-explicit-max")
		Expect(got.Spec.Engine.MaxReplicas).To(Equal(10),
			"explicit maxReplicas must survive the defaulter unchanged")
		Expect(got.Spec.Engine.MinReplicas).To(HaveValue(Equal(1)),
			"minReplicas still defaults independently of the explicit max")
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

	ginkgo.It("create with explicit minReplicas=0 and no KEDA opt-in is rejected, not silently defaulted", func() {
		// The defaulter only fills a NIL minReplicas — an explicit 0 must
		// reach the validator as 0 and trip the scale-to-zero gate rather
		// than being papered over with the default of 1.
		isvc := baselineISVC("admissio-scale-to-zero").Obj()
		isvc.Spec.Engine.MinReplicas = ptr.To(0)
		err := k8sClient.Create(suiteCtx, isvc)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("InvalidScaleToZero"))
	})

	ginkgo.It("router defaults its own maxReplicas independently of the engine default", func() {
		isvc := baselineISVC("admissio-router-max").Router(admissionRouter()).Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, isvc) })

		got := mustGet("admissio-router-max")
		Expect(got.Spec.Engine.MaxReplicas).To(Equal(3))
		Expect(got.Spec.Router.MaxReplicas).To(Equal(2),
			"router carries its own (smaller) maxReplicas default")
		Expect(got.Spec.Router.MinReplicas).To(HaveValue(Equal(1)))
	})

	ginkgo.It("engine+decoder create defaults decoder bounds and resolves PDDisaggregated", func() {
		isvc := baselineISVC("admissio-decoder-bounds").
			Decoder(&v1beta1.DecoderSpec{
				PodSpec: v1beta1.PodSpec{
					Containers: []corev1.Container{{Name: "ome-container", Image: "ghcr.io/example/decoder:v1"}},
				},
			}).Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, isvc) })

		got := mustGet("admissio-decoder-bounds")
		Expect(got.Annotations).To(HaveKeyWithValue(constants.DeploymentMode, string(constants.PDDisaggregated)))
		Expect(got.Spec.Decoder.MinReplicas).To(HaveValue(Equal(1)))
		Expect(got.Spec.Decoder.MaxReplicas).To(Equal(3))
	})

	ginkgo.It("update that clears maxReplicas re-defaults it rather than storing zero", func() {
		// The defaulter runs on UPDATE too: writing maxReplicas back to
		// its zero value re-stamps the default instead of persisting an
		// unbounded/zero max.
		isvc := baselineISVC("admissio-update-clear-max").Obj()
		isvc.Spec.Engine.MaxReplicas = 6
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, isvc) })

		got := mustGet("admissio-update-clear-max")
		Expect(got.Spec.Engine.MaxReplicas).To(Equal(6))
		got.Spec.Engine.MaxReplicas = 0
		Expect(k8sClient.Update(suiteCtx, got)).To(Succeed())

		got = mustGet("admissio-update-clear-max")
		Expect(got.Spec.Engine.MaxReplicas).To(Equal(3),
			"cleared maxReplicas must be re-defaulted on update, not stored as 0")
	})

	ginkgo.It("update raising minReplicas above the stored maxReplicas is rejected", func() {
		isvc := baselineISVC("admissio-update-min-gt-max").Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, isvc) })

		got := mustGet("admissio-update-min-gt-max")
		Expect(got.Spec.Engine.MaxReplicas).To(Equal(3))
		got.Spec.Engine.MinReplicas = ptr.To(5)
		err := k8sClient.Update(suiteCtx, got)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("spec.engine.minReplicas"))
		Expect(err.Error()).To(ContainSubstring("must be <= maxReplicas"))
	})

	ginkgo.It("update leaving replica bounds untouched admits unrelated metadata changes", func() {
		// A stored, fully-defaulted object must keep accepting writes that
		// do not touch the validated fields — the bounds check re-runs on
		// every update and must not manufacture a rejection from the
		// object's own defaulted state.
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
		Expect(got.Spec.Engine.MinReplicas).To(HaveValue(Equal(1)))
		Expect(got.Spec.Engine.MaxReplicas).To(Equal(3))
	})
})

// Disruption-budget admission through the full chain: the shape checks
// run on the defaulted object and name the exact per-Component field
// path so the controller never builds an invalid PodDisruptionBudget.
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
