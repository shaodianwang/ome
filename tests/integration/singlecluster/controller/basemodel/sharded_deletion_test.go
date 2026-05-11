package basemodel

import (
	"context"
	"sync/atomic"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/modelcache"
	v1beta1testing "github.com/ome-projects/ome/pkg/utils/testing/v1beta1"

	"github.com/ome-projects/ome/tests/integration/util"
)

// Sharded BaseModel deletion behavior. Sharded BMs route through
// the SAME handleModelDeletion as per-node BMs (controller.go:87 →
// per-node handshake), but Sharded models never produce per-node
// agent ConfigMaps. Net effect: the handshake's ConfigMap-list
// returns empty, the finalizer clears immediately, and the BM is
// GC'd by the apiserver.
//
// What does NOT happen: no signal goes to the configured cache
// provider that the BM is gone. modelcache.Client has no
// CancelPrewarm method (types.go:20-25 — only CheckSource,
// InspectSource, StartPrewarm, GetPrewarmStatus). So whatever
// cache resources the BM was using continue to accrue until the
// provider's own GC kicks in. These specs lock in that current
// behavior so a future refactor that adds cleanup hooks isn't
// silent — and document the gap for follow-up.
var _ = ginkgo.Describe("Sharded BaseModel deletion", ginkgo.Ordered, func() {

	ginkgo.BeforeEach(setSuiteFakeMCDefaults)

	ginkgo.It("Sharded BM deletion mid-prewarm: finalizer clears immediately (no per-node CMs to wait for)", func() {
		// Drive the prewarm to "Running" so we're definitely
		// mid-operation when we Delete. If we let it finish
		// (Succeeded), the test wouldn't tell us anything about
		// in-flight cleanup.
		suiteFakeMC.GetPrewarmStatusFunc = func(_ context.Context, op modelcache.PrewarmOperation) (*modelcache.PrewarmStatus, error) {
			return &modelcache.PrewarmStatus{Operation: op, Phase: modelcache.PrewarmPhaseRunning}, nil
		}

		cbm := v1beta1testing.MakeClusterBaseModel("sharded-del-running").
			StorageURI("oci://n/test/b/models/o/llama-del-running").
			Distribution(v1beta1.DistributionSharded).
			ModelFormat("safetensors").Obj()
		Expect(k8sClient.Create(suiteCtx, cbm)).To(Succeed())

		// Wait for the controller to have actually started a prewarm
		// — gives us deterministic mid-flight state to delete from.
		Eventually(func(g Gomega) {
			got := &v1beta1.ClusterBaseModel{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "sharded-del-running"}, got)).To(Succeed())
			g.Expect(got.Status.Cache).NotTo(BeNil())
			g.Expect(got.Status.Cache.PrewarmID).NotTo(BeEmpty())
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())

		Expect(k8sClient.Delete(suiteCtx, cbm)).To(Succeed())

		Eventually(func(g Gomega) {
			err := k8sClient.Get(suiteCtx, types.NamespacedName{Name: "sharded-del-running"}, &v1beta1.ClusterBaseModel{})
			g.Expect(apierrors.IsNotFound(err)).To(BeTrue(),
				"Sharded BM should GC immediately on Delete (no per-node CMs gate the handshake)")
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.It("Sharded BM deletion does NOT signal the cache provider (modelcache.Client has no CancelPrewarm method — gap)", func() {
		// This spec documents a real architectural gap: there's no
		// way for the controller to tell the cache provider "stop
		// working on this prewarm, the BM is gone." Whatever was
		// in flight keeps running on the provider side. Pin the
		// current behavior so a future Client interface change
		// surfaces here as a deliberate update.
		var startCount, statusCount, checkCount, inspectCount atomic.Int32
		suiteFakeMC.CheckSourceFunc = func(_ context.Context, _ modelcache.Source) (*modelcache.SourceStatus, error) {
			checkCount.Add(1)
			return &modelcache.SourceStatus{Reachable: true, Reason: "SourceReachable"}, nil
		}
		suiteFakeMC.InspectSourceFunc = func(_ context.Context, src modelcache.Source) (*modelcache.SourceInspection, error) {
			inspectCount.Add(1)
			return &modelcache.SourceInspection{
				Objects: []modelcache.Object{{Source: src, Path: src.URI + "/config.json"}},
				Metadata: modelcache.ModelMetadataInputs{
					ConfigJSON: []byte(`{"model_type":"phi","architectures":["PhiForCausalLM"]}`),
				},
			}, nil
		}
		suiteFakeMC.StartPrewarmFunc = func(_ context.Context, _ modelcache.PrewarmRequest) (*modelcache.PrewarmOperation, error) {
			startCount.Add(1)
			return &modelcache.PrewarmOperation{ID: "fake-op-del-signal"}, nil
		}
		suiteFakeMC.GetPrewarmStatusFunc = func(_ context.Context, op modelcache.PrewarmOperation) (*modelcache.PrewarmStatus, error) {
			statusCount.Add(1)
			return &modelcache.PrewarmStatus{Operation: op, Phase: modelcache.PrewarmPhaseRunning}, nil
		}

		cbm := v1beta1testing.MakeClusterBaseModel("sharded-del-signal").
			StorageURI("oci://n/test/b/models/o/llama-del-signal").
			Distribution(v1beta1.DistributionSharded).
			ModelFormat("safetensors").Obj()
		Expect(k8sClient.Create(suiteCtx, cbm)).To(Succeed())

		// Wait for at least one StartPrewarm + one GetPrewarmStatus
		// so we have a reference point for "calls before delete".
		Eventually(func(g Gomega) {
			g.Expect(startCount.Load()).To(BeNumerically(">=", 1))
			g.Expect(statusCount.Load()).To(BeNumerically(">=", 1))
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())

		startBeforeDelete := startCount.Load()
		statusBeforeDelete := statusCount.Load()

		Expect(k8sClient.Delete(suiteCtx, cbm)).To(Succeed())
		Eventually(func(g Gomega) {
			err := k8sClient.Get(suiteCtx, types.NamespacedName{Name: "sharded-del-signal"}, &v1beta1.ClusterBaseModel{})
			g.Expect(apierrors.IsNotFound(err)).To(BeTrue())
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		// Now: assert that no NEW StartPrewarm was issued (controller
		// stopped reconciling the now-deleted BM, as expected) AND
		// that no cancel-style call surfaced. Since the Client
		// interface has no Cancel method, the only thing we can
		// observe is that calls flatlined — but we ALSO want to
		// document that the provider's in-flight operation is
		// orphaned. A future CancelPrewarm() addition should update
		// this spec to assert the call count.
		Consistently(func(g Gomega) {
			// No further StartPrewarm calls (controller stopped reconciling).
			g.Expect(startCount.Load()).To(Equal(startBeforeDelete),
				"no StartPrewarm calls after Delete — reconciler is no longer touching this BM")
			// GetPrewarmStatus may keep firing briefly until the
			// reconcile queue drains; assert it doesn't keep growing
			// long-term by checking it doesn't grow significantly.
			g.Expect(statusCount.Load()).To(BeNumerically("<=", statusBeforeDelete+5),
				"GetPrewarmStatus should stop firing within a handful of in-flight reconciles after Delete")
		}).WithTimeout(util.ShortTimeout).WithPolling(util.PollInterval).Should(Succeed())

		// NOTE on the gap: there is no observable signal to the
		// provider that this BM was deleted. Operator-side cleanup
		// (cache eviction) currently relies on the provider's own
		// LRU/TTL policy. If we ever add modelcache.Client.Cancel,
		// this Consistently should flip to an Eventually that
		// asserts a Cancel call was issued.
	})
})
