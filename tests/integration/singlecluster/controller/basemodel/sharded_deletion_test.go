package basemodel

import (
	"context"
	"sync/atomic"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/basemodel/backends/sharded"
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
// The lazy-fetch flow makes the deletion story simpler than it was
// under prewarm: there is no separate "operation" to cancel on the
// provider side. CheckSource + InspectSource calls just stop firing
// once the BM is gone.
var _ = ginkgo.Describe("Sharded BaseModel deletion", ginkgo.Ordered, func() {

	ginkgo.BeforeEach(setSuiteFakeMCDefaults)

	ginkgo.It("Sharded BM deletion clears finalizer immediately (no per-node CMs to wait for)", func() {
		cbm := v1beta1testing.MakeClusterBaseModel("sharded-del-running").
			StorageURI("oci://n/test/b/models/o/llama-del-running").
			Distribution(v1beta1.DistributionSharded).
			ModelFormat("safetensors").Obj()
		Expect(k8sClient.Create(suiteCtx, cbm)).To(Succeed())

		// Wait for the controller to have reconciled at least once
		// — Status.Cache.Backend is the first observable signal.
		Eventually(func(g Gomega) {
			got := &v1beta1.ClusterBaseModel{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "sharded-del-running"}, got)).To(Succeed())
			g.Expect(got.Status.Cache).NotTo(BeNil())
			g.Expect(got.Status.Cache.Backend).NotTo(BeEmpty())
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())

		Expect(k8sClient.Delete(suiteCtx, cbm)).To(Succeed())

		Eventually(func(g Gomega) {
			err := k8sClient.Get(suiteCtx, types.NamespacedName{Name: "sharded-del-running"}, &v1beta1.ClusterBaseModel{})
			g.Expect(apierrors.IsNotFound(err)).To(BeTrue(),
				"Sharded BM should GC immediately on Delete (no per-node CMs gate the handshake)")
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.It("Sharded BM deletion stops further cache provider calls", func() {
		// After Delete, the reconciler should stop touching the BM
		// entirely. Pin that no further CheckSource / InspectSource
		// calls fire — easy in the lazy-fetch flow because there's
		// no separate prewarm polling loop to wind down.
		var checkCount, inspectCount atomic.Int32
		suiteFakeMC.CheckSourceFunc = func(_ context.Context, _ sharded.Source) (*sharded.SourceStatus, error) {
			checkCount.Add(1)
			return &sharded.SourceStatus{Reachable: true, Reason: "SourceReachable"}, nil
		}
		suiteFakeMC.InspectSourceFunc = func(_ context.Context, src sharded.Source) (*sharded.SourceInspection, error) {
			inspectCount.Add(1)
			return &sharded.SourceInspection{
				Objects: []sharded.Object{{Source: src, Path: src.URI + "/config.json"}},
				Metadata: sharded.ModelMetadataInputs{
					ConfigJSON: []byte(`{"model_type":"phi","architectures":["PhiForCausalLM"]}`),
				},
			}, nil
		}

		cbm := v1beta1testing.MakeClusterBaseModel("sharded-del-signal").
			StorageURI("oci://n/test/b/models/o/llama-del-signal").
			Distribution(v1beta1.DistributionSharded).
			ModelFormat("safetensors").Obj()
		Expect(k8sClient.Create(suiteCtx, cbm)).To(Succeed())

		// Wait for at least one CheckSource + one InspectSource so we
		// have a reference point for "calls before delete".
		Eventually(func(g Gomega) {
			g.Expect(checkCount.Load()).To(BeNumerically(">=", 1))
			g.Expect(inspectCount.Load()).To(BeNumerically(">=", 1))
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())

		checkBeforeDelete := checkCount.Load()
		inspectBeforeDelete := inspectCount.Load()

		Expect(k8sClient.Delete(suiteCtx, cbm)).To(Succeed())
		Eventually(func(g Gomega) {
			err := k8sClient.Get(suiteCtx, types.NamespacedName{Name: "sharded-del-signal"}, &v1beta1.ClusterBaseModel{})
			g.Expect(apierrors.IsNotFound(err)).To(BeTrue())
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		// No new calls after Delete — reconciler stopped touching it.
		// Allow a small margin for in-flight reconciles that started
		// before the delete propagated.
		Consistently(func(g Gomega) {
			g.Expect(checkCount.Load()).To(BeNumerically("<=", checkBeforeDelete+2),
				"CheckSource should stop firing within a handful of in-flight reconciles after Delete")
			g.Expect(inspectCount.Load()).To(BeNumerically("<=", inspectBeforeDelete+2),
				"InspectSource should stop firing within a handful of in-flight reconciles after Delete")
		}).WithTimeout(util.ShortTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})
})
