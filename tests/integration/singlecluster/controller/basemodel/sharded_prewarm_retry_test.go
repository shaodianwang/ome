package basemodel

import (
	"context"
	"sync/atomic"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/types"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/modelcache"
	v1beta1testing "github.com/ome-projects/ome/pkg/utils/testing/v1beta1"

	"github.com/ome-projects/ome/tests/integration/util"
)

var _ = ginkgo.Describe("Sharded BaseModel prewarm retry on transient failure", ginkgo.Ordered, func() {

	ginkgo.It("PrewarmFailed → state stays Importing, controller re-submits StartPrewarm", func() {
		var prewarmCount atomic.Int32
		suiteFakeMC.CheckSourceFunc = func(ctx context.Context, source modelcache.Source) (*modelcache.SourceStatus, error) {
			return &modelcache.SourceStatus{Reachable: true, Reason: v1beta1.ModelConditionReasonSourceReachable}, nil
		}
		suiteFakeMC.InspectSourceFunc = func(ctx context.Context, source modelcache.Source) (*modelcache.SourceInspection, error) {
			return &modelcache.SourceInspection{
				RootURI: source.URI,
				Objects: []modelcache.Object{{Source: source, Path: source.URI + "/config.json"}},
				Metadata: modelcache.ModelMetadataInputs{
					ConfigJSON: []byte(`{"model_type":"phi","architectures":["PhiForCausalLM"]}`),
				},
			}, nil
		}
		suiteFakeMC.StartPrewarmFunc = func(ctx context.Context, req modelcache.PrewarmRequest) (*modelcache.PrewarmOperation, error) {
			prewarmCount.Add(1)
			return &modelcache.PrewarmOperation{ID: "op-transient-1"}, nil
		}
		suiteFakeMC.GetPrewarmStatusFunc = func(ctx context.Context, op modelcache.PrewarmOperation) (*modelcache.PrewarmStatus, error) {
			return &modelcache.PrewarmStatus{
				Operation: op,
				Phase:     modelcache.PrewarmPhaseFailed,
				Reason:    "ClusterCacheFetchFailed",
				Message:   "transient overload",
			}, nil
		}
		ginkgo.DeferCleanup(func() {
			suiteFakeMC.CheckSourceFunc = nil
			suiteFakeMC.InspectSourceFunc = nil
			suiteFakeMC.StartPrewarmFunc = nil
			suiteFakeMC.GetPrewarmStatusFunc = nil
		})

		cbm := v1beta1testing.MakeClusterBaseModel("sharded-retry-transient").
			StorageURI("s3://bucket/retry-transient").
			Distribution(v1beta1.DistributionSharded).
			ModelFormat("safetensors").Obj()
		Expect(k8sClient.Create(suiteCtx, cbm)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, cbm) })

		Eventually(func() int32 {
			return prewarmCount.Load()
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(BeNumerically(">=", 1),
			"first StartPrewarm submit must reach the fake provider")

		// PrewarmAttempts is the durable witness of the re-submit — observing
		// a brief PrewarmID=="" window is racy because the status write that
		// clears it triggers an informer event that re-submits immediately.
		Eventually(func(g Gomega) {
			got := &v1beta1.ClusterBaseModel{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "sharded-retry-transient"}, got)).To(Succeed())

			cached := meta.FindStatusCondition(got.Status.Conditions, v1beta1.ModelConditionCached)
			g.Expect(cached).NotTo(BeNil())
			g.Expect(cached.Reason).To(Equal(v1beta1.ModelConditionReasonPrewarmFailed))
			g.Expect(cached.Message).To(ContainSubstring("will retry"))
			g.Expect(cached.Message).To(ContainSubstring("transient overload"))

			g.Expect(got.Status.State).NotTo(Equal(v1beta1.LifeCycleStateFailed),
				"transient PrewarmFailed must NOT flip state to Failed")

			g.Expect(got.Status.Cache).NotTo(BeNil())
			g.Expect(got.Status.Cache.PrewarmAttempts).NotTo(BeNil())
			g.Expect(*got.Status.Cache.PrewarmAttempts).To(BeNumerically(">=", 2))
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())

		Expect(prewarmCount.Load()).To(BeNumerically(">=", 2))
	})
})
