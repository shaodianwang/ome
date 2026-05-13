package basemodel

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/types"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/modelagent"
	v1beta1testing "github.com/ome-projects/ome/pkg/utils/testing/v1beta1"

	"github.com/ome-projects/ome/tests/integration/util"
)

// PVC spec auto-fill edge cases. updateSpecWithConfig
// (controller.go:897) is the shared utility that copies fields
// from the agent's ModelConfig into the BaseModel spec. The
// existing pvc_status_test.go covers the positive happy path
// (every field populated when spec starts empty); these specs
// cover the corners.
//
// Important contract: updateSpecWithConfig is "fill-only" — it
// must NEVER overwrite a field the user already populated.
// Otherwise the controller can silently change user-curated
// spec data when the agent re-publishes its CM.
var _ = ginkgo.Describe("BaseModel PVC spec auto-fill edges", ginkgo.Ordered, func() {

	ginkgo.It("user-set ModelType + ModelArchitecture are preserved when agent CM has different values", func() {
		// updateSpecWithConfig checks `if spec.ModelType == nil`
		// before overwriting (controller.go:905). This guard is
		// load-bearing — if it ever flips to overwrite-on-update,
		// every reconcile would clobber operator-curated values.
		ns := "pvc-autofill-preserve"
		makeNamespace(suiteCtx, k8sClient, ns)
		makePVC(suiteCtx, k8sClient, "p", ns)
		bindPVC(suiteCtx, k8sClient, "p", ns)

		bm := v1beta1testing.MakeBaseModel("preset-llama", ns).
			StorageURI("pvc://p/models/llama").
			ModelType("user-curated-type").              // pre-set
			ModelArchitecture("user-curated-arch").Obj() // pre-set
		Expect(k8sClient.Create(suiteCtx, bm)).To(Succeed())

		// Agent publishes DIFFERENT values from what the user set.
		Expect(v1beta1testing.SimulatePVCAgentReady(suiteCtx, k8sClient,
			"preset-llama", ns, false,
			&modelagent.ModelConfig{
				ModelType:          "agent-discovered-type",
				ModelArchitecture:  "agent-discovered-arch",
				ModelParameterSize: "70B", // unset on spec, so this WILL fill
			})).To(Succeed())

		Eventually(func(g Gomega) {
			got := &v1beta1.BaseModel{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "preset-llama", Namespace: ns}, got)).To(Succeed())
			g.Expect(got.Status.State).To(Equal(v1beta1.LifeCycleStateReady))
			// Pre-set fields stay AS-IS.
			g.Expect(got.Spec.ModelType).NotTo(BeNil())
			g.Expect(*got.Spec.ModelType).To(Equal("user-curated-type"),
				"controller MUST NOT overwrite a user-set ModelType")
			g.Expect(got.Spec.ModelArchitecture).NotTo(BeNil())
			g.Expect(*got.Spec.ModelArchitecture).To(Equal("user-curated-arch"),
				"controller MUST NOT overwrite a user-set ModelArchitecture")
			// Unset field IS filled — proves the agent's CM was
			// actually consumed (sanity check for the negative).
			g.Expect(got.Spec.ModelParameterSize).NotTo(BeNil())
			g.Expect(*got.Spec.ModelParameterSize).To(Equal("70B"))
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.It("user-set ModelCapabilities array is preserved (replace-vs-append semantics: NEVER append)", func() {
		// updateSpecWithConfig only fills capabilities when the
		// existing slice is len==0 (controller.go:926). Catches a
		// regression where the controller would append to (or
		// replace) an existing user list.
		ns := "pvc-autofill-caps"
		makeNamespace(suiteCtx, k8sClient, ns)
		makePVC(suiteCtx, k8sClient, "p", ns)
		bindPVC(suiteCtx, k8sClient, "p", ns)

		bm := v1beta1testing.MakeBaseModel("caps-llama", ns).
			StorageURI("pvc://p/models/llama").Obj()
		// Hand-set capabilities since there's no fluent setter.
		bm.Spec.ModelCapabilities = []string{"USER_ONLY_CAP"}
		Expect(k8sClient.Create(suiteCtx, bm)).To(Succeed())

		Expect(v1beta1testing.SimulatePVCAgentReady(suiteCtx, k8sClient,
			"caps-llama", ns, false,
			&modelagent.ModelConfig{
				ModelCapabilities: []string{"AGENT_CAP_A", "AGENT_CAP_B"},
			})).To(Succeed())

		Eventually(func(g Gomega) {
			got := &v1beta1.BaseModel{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "caps-llama", Namespace: ns}, got)).To(Succeed())
			g.Expect(got.Status.State).To(Equal(v1beta1.LifeCycleStateReady))
			g.Expect(got.Spec.ModelCapabilities).To(Equal([]string{"USER_ONLY_CAP"}),
				"user-set ModelCapabilities must be preserved exactly — no append, no replace, no merge")
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.It("agent supplies Quantization → controller propagates it to Spec.Quantization", func() {
		// updateSpecWithConfig now has a Quantization branch (added in #82),
		// so a Quantization value supplied by the agent flows through to
		// the BaseModel spec the same way ModelType and ModelArchitecture do.
		ns := "pvc-autofill-quant"
		makeNamespace(suiteCtx, k8sClient, ns)
		makePVC(suiteCtx, k8sClient, "p", ns)
		bindPVC(suiteCtx, k8sClient, "p", ns)

		bm := v1beta1testing.MakeBaseModel("quant-llama", ns).
			StorageURI("pvc://p/models/llama").Obj()
		Expect(bm.Spec.Quantization).To(BeNil(), "precondition: spec must start with no Quantization")
		Expect(k8sClient.Create(suiteCtx, bm)).To(Succeed())

		Expect(v1beta1testing.SimulatePVCAgentReady(suiteCtx, k8sClient,
			"quant-llama", ns, false,
			&modelagent.ModelConfig{
				ModelType:    "llama",
				Quantization: "fp8", // valid ModelQuantization value
			})).To(Succeed())

		Eventually(func(g Gomega) {
			got := &v1beta1.BaseModel{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "quant-llama", Namespace: ns}, got)).To(Succeed())
			g.Expect(got.Spec.ModelType).NotTo(BeNil(),
				"sanity: ModelType must populate so we know the spec-update reconcile actually ran")
			g.Expect(*got.Spec.ModelType).To(Equal("llama"))
			g.Expect(got.Spec.Quantization).NotTo(BeNil(),
				"controller should propagate ModelConfig.Quantization to Spec.Quantization")
			g.Expect(*got.Spec.Quantization).To(Equal(v1beta1.ModelQuantization("fp8")))
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})
})
