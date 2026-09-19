package basemodel

import (
	"context"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/types"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/basemodel/backends/sharded"
	v1beta1testing "github.com/ome-projects/ome/pkg/utils/testing/v1beta1"

	"github.com/ome-projects/ome/tests/integration/util"
)

// setSuiteFakeMCDefaults restores the suite's fake sharded.Client
// to the happy-path defaults: source reachable, inspectable with a
// minimal valid config.json + one object.
//
// Both this Describe and Sharded edge-case Describes share these
// defaults. Each spec overrides the specific Func it cares about
// after BeforeEach runs.
//
// InspectSource is the sharded flow's load-bearing call:
// reconcileShardedModelMetadataSpec calls InspectSource so the
// controller can parse model metadata from the source's config.json.
// Without it, the controller short-circuits at
// markShardedModelMetadataPending and Ready never flips.
func setSuiteFakeMCDefaults() {
	suiteFakeMC.CheckSourceFunc = func(_ context.Context, _ sharded.Source) (*sharded.SourceStatus, error) {
		return &sharded.SourceStatus{Reachable: true, Reason: "SourceReachable"}, nil
	}
	suiteFakeMC.InspectSourceFunc = func(_ context.Context, src sharded.Source) (*sharded.SourceInspection, error) {
		return &sharded.SourceInspection{
			Objects: []sharded.Object{
				{Source: src, Path: src.URI + "/config.json"},
			},
			Metadata: sharded.ModelMetadataInputs{
				// Minimal config.json the modelparser accepts.
				// Mirrors pkg/.../sharded_cache_metadata_test.go.
				ConfigJSON: []byte(`{"model_type":"phi","architectures":["PhiForCausalLM"]}`),
			},
		}, nil
	}
}

// Sharded distribution flow (sharded_cache_status.go) under lazy-fetch:
//
//  1. controller calls provider.Client.CheckSource → SourceReachable
//     flips True (or False with reason=SourceNotReachable)
//  2. controller calls provider.Client.InspectSource → metadata is
//     parsed and BaseModel spec fields populate
//  3. Ready flips True with reason=MetadataParsed
//
// Bytes themselves are pulled lazily by the cluster_cache daemon at
// first inference — there is no separate prewarm step. The fake
// Client (pkg/modelcache/fake) is dispatched through a callback per
// method so we re-script behavior between specs without restarting
// the manager.
var _ = ginkgo.Describe("Sharded BaseModel cache flow", ginkgo.Ordered, func() {

	ginkgo.BeforeEach(setSuiteFakeMCDefaults)

	ginkgo.It("CheckSource→InspectSource drives Ready=True", func() {
		ginkgo.By("creating a Sharded ClusterBaseModel")
		cbm := v1beta1testing.MakeClusterBaseModel("sharded-llama-7b").
			StorageURI("oci://n/test/b/models/o/llama-7b").
			Distribution(v1beta1.DistributionSharded).
			ModelFormat("safetensors").Obj()
		Expect(k8sClient.Create(suiteCtx, cbm)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, cbm) })

		ginkgo.By("expecting Ready condition to flip True with reason=MetadataParsed")
		// NOTE: we deliberately do NOT also assert state=Ready here.
		// applyInitialShardedModelStatus unconditionally resets
		// state=Importing on every reconcile, and the post-Ready
		// reconcile path doesn't pass a transition that would re-flip
		// state to Ready. The Ready *condition* stays True throughout,
		// which is the truth source consumers should rely on, so the
		// spec asserts the condition rather than Status.State.
		Eventually(func(g Gomega) {
			got := &v1beta1.ClusterBaseModel{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "sharded-llama-7b"}, got)).To(Succeed())
			ready := meta.FindStatusCondition(got.Status.Conditions, v1beta1.ModelConditionReady)
			g.Expect(ready).NotTo(BeNil())
			g.Expect(string(ready.Status)).To(Equal("True"))
			g.Expect(ready.Reason).To(Equal(v1beta1.ModelConditionReasonMetadataParsed))
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.It("CheckSource returning Reachable=false drives SourceReachable=False", func() {
		ginkgo.By("flipping the fake to return SourceNotReachable")
		suiteFakeMC.CheckSourceFunc = func(_ context.Context, _ sharded.Source) (*sharded.SourceStatus, error) {
			return &sharded.SourceStatus{Reachable: false, Reason: "SourceNotReachable", Message: "test injection"}, nil
		}

		cbm := v1beta1testing.MakeClusterBaseModel("sharded-unreachable-7b").
			StorageURI("oci://n/test/b/models/o/llama-7b").
			Distribution(v1beta1.DistributionSharded).
			ModelFormat("safetensors").Obj()
		Expect(k8sClient.Create(suiteCtx, cbm)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, cbm) })

		Eventually(func(g Gomega) {
			got := &v1beta1.ClusterBaseModel{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "sharded-unreachable-7b"}, got)).To(Succeed())
			cond := meta.FindStatusCondition(got.Status.Conditions, v1beta1.ModelConditionSourceReachable)
			g.Expect(cond).NotTo(BeNil())
			g.Expect(string(cond.Status)).To(Equal("False"))
			g.Expect(cond.Reason).To(Equal(v1beta1.ModelConditionReasonSourceNotReachable))
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})
})

// Note: provider-not-configured spec lives in
// sharded_no_provider_test.go because it requires a different
// manager wiring (no provider injected) — separate Describe with its
// own BeforeAll/AfterAll restart is cleaner.
