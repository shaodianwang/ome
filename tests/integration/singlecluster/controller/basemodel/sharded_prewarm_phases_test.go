package basemodel

import (
	"context"
	"sync/atomic"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"

	"github.com/sgl-project/ome/pkg/apis/ome/v1beta1"
	"github.com/sgl-project/ome/pkg/modelcache"
	v1beta1testing "github.com/sgl-project/ome/pkg/util/testing/v1beta1"

	"github.com/sgl-project/ome/tests/integration/util"
)

// Sharded edge-case specs covering PrewarmPhase variants beyond the
// happy/failed pair already in sharded_test.go: Pending, Running,
// Canceled, scripted phase progression, prewarm progress propagation,
// and a namespaced-BaseModel parallel of the cluster-scoped happy path.
//
// Layout: each It is self-contained — sets its own
// suiteFakeMC.GetPrewarmStatusFunc (or other Func overrides) before
// creating its CR. The InspectSourceFunc / CheckSourceFunc /
// StartPrewarmFunc defaults from the suite's BeforeEach in
// sharded_test.go still apply: the spec only overrides what's
// specific to its scenario.
var _ = ginkgo.Describe("Sharded BaseModel prewarm phase variants", ginkgo.Ordered, func() {

	// Reset to happy-path defaults before each spec so per-spec
	// overrides only need to set the specific Func they care about.
	ginkgo.BeforeEach(setSuiteFakeMCDefaults)

	ginkgo.It("PrewarmPhase=Pending → Cached=False reason=PrewarmPending, state=Importing", func() {
		suiteFakeMC.GetPrewarmStatusFunc = func(_ context.Context, op modelcache.PrewarmOperation) (*modelcache.PrewarmStatus, error) {
			return &modelcache.PrewarmStatus{Operation: op, Phase: modelcache.PrewarmPhasePending}, nil
		}

		cbm := v1beta1testing.MakeClusterBaseModel("sharded-pending").
			StorageURI("oci://n/test/b/models/o/llama-pending").
			Distribution(v1beta1.DistributionSharded).
			ModelFormat("safetensors").Obj()
		Expect(k8sClient.Create(suiteCtx, cbm)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, cbm) })

		Eventually(func(g Gomega) {
			got := &v1beta1.ClusterBaseModel{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "sharded-pending"}, got)).To(Succeed())
			cached := meta.FindStatusCondition(got.Status.Conditions, v1beta1.ModelConditionCached)
			g.Expect(cached).NotTo(BeNil())
			g.Expect(string(cached.Status)).To(Equal("False"))
			g.Expect(cached.Reason).To(Equal(v1beta1.ModelConditionReasonPrewarmPending))
			// Pending is non-terminal — controller stays in Importing.
			g.Expect(got.Status.State).To(Equal(v1beta1.LifeCycleStateImporting))
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.It("PrewarmPhase=Running → Cached=False reason=PrewarmRunning, state=Importing", func() {
		suiteFakeMC.GetPrewarmStatusFunc = func(_ context.Context, op modelcache.PrewarmOperation) (*modelcache.PrewarmStatus, error) {
			return &modelcache.PrewarmStatus{Operation: op, Phase: modelcache.PrewarmPhaseRunning}, nil
		}

		cbm := v1beta1testing.MakeClusterBaseModel("sharded-running").
			StorageURI("oci://n/test/b/models/o/llama-running").
			Distribution(v1beta1.DistributionSharded).
			ModelFormat("safetensors").Obj()
		Expect(k8sClient.Create(suiteCtx, cbm)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, cbm) })

		Eventually(func(g Gomega) {
			got := &v1beta1.ClusterBaseModel{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "sharded-running"}, got)).To(Succeed())
			cached := meta.FindStatusCondition(got.Status.Conditions, v1beta1.ModelConditionCached)
			g.Expect(cached).NotTo(BeNil())
			g.Expect(string(cached.Status)).To(Equal("False"))
			g.Expect(cached.Reason).To(Equal(v1beta1.ModelConditionReasonPrewarmRunning))
			g.Expect(got.Status.State).To(Equal(v1beta1.LifeCycleStateImporting))
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.It("PrewarmPhase=Canceled → state=Failed, Cached + Ready conditions both reason=PrewarmCanceled", func() {
		suiteFakeMC.GetPrewarmStatusFunc = func(_ context.Context, op modelcache.PrewarmOperation) (*modelcache.PrewarmStatus, error) {
			return &modelcache.PrewarmStatus{
				Operation: op,
				Phase:     modelcache.PrewarmPhaseCanceled,
				Reason:    "PrewarmCanceled",
				Message:   "operator canceled the prewarm",
			}, nil
		}

		cbm := v1beta1testing.MakeClusterBaseModel("sharded-canceled").
			StorageURI("oci://n/test/b/models/o/llama-canceled").
			Distribution(v1beta1.DistributionSharded).
			ModelFormat("safetensors").Obj()
		Expect(k8sClient.Create(suiteCtx, cbm)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, cbm) })

		Eventually(func(g Gomega) {
			got := &v1beta1.ClusterBaseModel{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "sharded-canceled"}, got)).To(Succeed())
			g.Expect(got.Status.State).To(Equal(v1beta1.LifeCycleStateFailed))

			cached := meta.FindStatusCondition(got.Status.Conditions, v1beta1.ModelConditionCached)
			g.Expect(cached).NotTo(BeNil())
			g.Expect(string(cached.Status)).To(Equal("False"))
			g.Expect(cached.Reason).To(Equal(v1beta1.ModelConditionReasonPrewarmCanceled))

			ready := meta.FindStatusCondition(got.Status.Conditions, v1beta1.ModelConditionReady)
			g.Expect(ready).NotTo(BeNil())
			g.Expect(string(ready.Status)).To(Equal("False"))
			// applyTerminalPrewarmFailure mirrors the same reason on
			// Cached AND Ready (sharded_cache_status.go:539-545).
			g.Expect(ready.Reason).To(Equal(v1beta1.ModelConditionReasonPrewarmCanceled))
			g.Expect(ready.Message).To(ContainSubstring("operator canceled the prewarm"))
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.It("phase progression Pending → Running → Succeeded → Cached=True", func() {
		// Script the fake to walk through phases in sequence: each
		// poll advances one step, mimicking a real backend that
		// reports Pending while queued, Running while transferring,
		// then Succeeded. The controller must observe and reflect
		// each transition.
		var pollCount atomic.Int32
		suiteFakeMC.GetPrewarmStatusFunc = func(_ context.Context, op modelcache.PrewarmOperation) (*modelcache.PrewarmStatus, error) {
			n := pollCount.Add(1)
			var phase modelcache.PrewarmPhase
			switch {
			case n == 1:
				phase = modelcache.PrewarmPhasePending
			case n == 2:
				phase = modelcache.PrewarmPhaseRunning
			default:
				phase = modelcache.PrewarmPhaseSucceeded
			}
			return &modelcache.PrewarmStatus{Operation: op, Phase: phase}, nil
		}

		cbm := v1beta1testing.MakeClusterBaseModel("sharded-progress").
			StorageURI("oci://n/test/b/models/o/llama-progress").
			Distribution(v1beta1.DistributionSharded).
			ModelFormat("safetensors").Obj()
		Expect(k8sClient.Create(suiteCtx, cbm)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, cbm) })

		// Don't try to assert the intermediate phases — the controller
		// poll interval (5s) plus our test poll interval (200ms) make
		// the transitions hard to catch deterministically. Just assert
		// the terminal state: Cached=True with reason=PrewarmSucceeded.
		// At least 3 polls must have happened.
		Eventually(func(g Gomega) {
			got := &v1beta1.ClusterBaseModel{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "sharded-progress"}, got)).To(Succeed())
			cached := meta.FindStatusCondition(got.Status.Conditions, v1beta1.ModelConditionCached)
			g.Expect(cached).NotTo(BeNil())
			g.Expect(string(cached.Status)).To(Equal("True"))
			g.Expect(cached.Reason).To(Equal(v1beta1.ModelConditionReasonPrewarmSucceeded))
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
		Expect(pollCount.Load()).To(BeNumerically(">=", 3),
			"controller must poll the operation across all 3 phases before terminating")
	})

	ginkgo.It("PrewarmProgress fields propagate to Status.Cache (TotalBytes/CachedBytes/TotalChunks/CachedChunks)", func() {
		// Report Running indefinitely with concrete progress numbers.
		// The controller must mirror them into Status.Cache so the
		// CRD reports useful progress to operators.
		suiteFakeMC.GetPrewarmStatusFunc = func(_ context.Context, op modelcache.PrewarmOperation) (*modelcache.PrewarmStatus, error) {
			return &modelcache.PrewarmStatus{
				Operation: op,
				Phase:     modelcache.PrewarmPhaseRunning,
				Progress: modelcache.PrewarmProgress{
					TotalBytes:   ptr.To(int64(1024 * 1024 * 1024)),
					CachedBytes:  ptr.To(int64(512 * 1024 * 1024)),
					TotalChunks:  ptr.To(int32(100)),
					CachedChunks: ptr.To(int32(42)),
				},
			}, nil
		}

		cbm := v1beta1testing.MakeClusterBaseModel("sharded-progress-bytes").
			StorageURI("oci://n/test/b/models/o/llama-progress-bytes").
			Distribution(v1beta1.DistributionSharded).
			ModelFormat("safetensors").Obj()
		Expect(k8sClient.Create(suiteCtx, cbm)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, cbm) })

		Eventually(func(g Gomega) {
			got := &v1beta1.ClusterBaseModel{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "sharded-progress-bytes"}, got)).To(Succeed())
			g.Expect(got.Status.Cache).NotTo(BeNil())
			g.Expect(got.Status.Cache.TotalBytes).NotTo(BeNil())
			g.Expect(*got.Status.Cache.TotalBytes).To(Equal(int64(1024 * 1024 * 1024)))
			g.Expect(got.Status.Cache.CachedBytes).NotTo(BeNil())
			g.Expect(*got.Status.Cache.CachedBytes).To(Equal(int64(512 * 1024 * 1024)))
			g.Expect(got.Status.Cache.TotalChunks).NotTo(BeNil())
			g.Expect(*got.Status.Cache.TotalChunks).To(Equal(int32(100)))
			g.Expect(got.Status.Cache.CachedChunks).NotTo(BeNil())
			g.Expect(*got.Status.Cache.CachedChunks).To(Equal(int32(42)))
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.It("namespaced BaseModel + Sharded distribution: same controller path as ClusterBaseModel", func() {
		// All existing sharded specs use ClusterBaseModel. The
		// namespaced BaseModel reconciler dispatches into the same
		// updateShardedModelStatus → reconcileShardedModelCacheStatus
		// path, but the type switch / object updates have a distinct
		// case branch. This spec proves the namespaced flow reaches
		// Cached=True with reason=PrewarmSucceeded the same way the
		// cluster-scoped one does.
		ns, err := util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "sharded-ns-")
		Expect(err).NotTo(HaveOccurred())
		ginkgo.DeferCleanup(func() {
			Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
		})

		bm := v1beta1testing.MakeBaseModel("sharded-ns-llama", ns.Name).
			StorageURI("oci://n/test/b/models/o/llama-ns").
			Distribution(v1beta1.DistributionSharded).
			ModelFormat("safetensors").Obj()
		Expect(k8sClient.Create(suiteCtx, bm)).To(Succeed())

		Eventually(func(g Gomega) {
			got := &v1beta1.BaseModel{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "sharded-ns-llama", Namespace: ns.Name}, got)).To(Succeed())
			cached := meta.FindStatusCondition(got.Status.Conditions, v1beta1.ModelConditionCached)
			g.Expect(cached).NotTo(BeNil())
			g.Expect(string(cached.Status)).To(Equal("True"))
			g.Expect(cached.Reason).To(Equal(v1beta1.ModelConditionReasonPrewarmSucceeded))

			ready := meta.FindStatusCondition(got.Status.Conditions, v1beta1.ModelConditionReady)
			g.Expect(ready).NotTo(BeNil())
			g.Expect(string(ready.Status)).To(Equal("True"))
			g.Expect(ready.Reason).To(Equal(v1beta1.ModelConditionReasonMetadataParsed))
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})
})
