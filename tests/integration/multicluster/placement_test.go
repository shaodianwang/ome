package multicluster

import (
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/types"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/placement"
	ometesting "github.com/ome-projects/ome/pkg/utils/testing/v1beta1"
)

// These specs drive the control plane end-to-end across three real
// envtest apiservers: a source InferenceService on the control cluster fans out
// to matching member clusters, the suite admits it on one member (standing in for
// Kueue + the worker reconciler), and the placer declares a winner, sweeps the
// loser, and flows the placement status back.
var _ = ginkgo.Describe("Multi-cluster placement fan-out", func() {
	const (
		// acceleratorLabel is the capability label members advertise and the
		// source ISVC requires; a fixed test value (production resolves it from
		// spec.model + runtime).
		acceleratorLabel = "gpu"
		acceleratorValue = "test-accel"
	)

	var (
		secretNS string
		isvcNS   string

		wc1Name, wc2Name string
		secret1, secret2 string
		isvcName         string
	)

	matchingLabels := func() map[string]string { return map[string]string{acceleratorLabel: acceleratorValue} }
	requirement := func() string { return acceleratorLabel + "=" + acceleratorValue }

	ginkgo.BeforeEach(func() {
		skipIfRealKueue()
		// Namespace for the kubeconfig Secrets + WorkloadClusters live on the
		// control cluster; the workload namespace must exist on the members too,
		// because the placer creates the derived ISVC there.
		secretNS = uniqueName("mc-secrets")
		isvcNS = uniqueName("mc-workload")
		createNamespace(controlCluster, secretNS)
		createNamespace(controlCluster, isvcNS)
		createNamespace(member1Cluster, isvcNS)
		createNamespace(member2Cluster, isvcNS)

		secret1 = uniqueName("kubeconfig-m1")
		secret2 = uniqueName("kubeconfig-m2")
		wc1Name = uniqueName("member1")
		wc2Name = uniqueName("member2")
		isvcName = uniqueName("svc")

		mustCreate(controlCluster, kubeconfigSecretFor(member1Cluster, secretNS, secret1))
		mustCreate(controlCluster, kubeconfigSecretFor(member2Cluster, secretNS, secret2))

		// Create both WorkloadClusters and wait for each to connect to its member
		// apiserver (Ready=True) before it is an eligible placement candidate.
		ginkgo.By("creating the WorkloadClusters and waiting for Ready=True")
		for _, wc := range []struct{ name, secret string }{{wc1Name, secret1}, {wc2Name, secret2}} {
			ometesting.CreateAndWaitReady(controlCluster.ctx, controlCluster.client,
				workloadClusterFor(wc.name, secretNS, wc.secret, matchingLabels()),
				"WorkloadCluster Ready=True",
				func(o *v1beta1.WorkloadCluster) bool {
					return apimeta.IsStatusConditionTrue(o.Status.Conditions, v1beta1.WorkloadClusterReady)
				})
		}
	})

	ginkgo.AfterEach(func() {
		deleteIfExists(controlCluster, sourceISVC(isvcNS, isvcName, requirement()))
		deleteIfExists(controlCluster, workloadClusterFor(wc1Name, secretNS, secret1, nil))
		deleteIfExists(controlCluster, workloadClusterFor(wc2Name, secretNS, secret2, nil))
	})

	ginkgo.It("fans the source ISVC out to every matching member, picks a winner, sweeps the loser, and flows status back", func() {
		ginkgo.By("creating the source InferenceService on the control cluster")
		mustCreate(controlCluster, sourceISVC(isvcNS, isvcName, requirement()))

		ginkgo.By("asserting the derived ISVC fans out to BOTH members")
		for _, m := range []cluster{member1Cluster, member2Cluster} {
			member := m
			gomega.Eventually(func() bool {
				_, ok := getDerivedOn(member, isvcNS, isvcName)
				return ok
			}, eventuallyTimeout, eventuallyPoll).Should(gomega.BeTrue(),
				"derived ISVC should appear on the member via its remote client")
		}

		ginkgo.By("verifying the derived copies carry the placement origin + control-plane stamps")
		derived1, ok := getDerivedOn(member1Cluster, isvcNS, isvcName)
		gomega.Expect(ok).To(gomega.BeTrue())
		gomega.Expect(derived1.Labels).To(gomega.HaveKeyWithValue(placement.PlacementControlPlaneLabel, controlPlaneID))
		gomega.Expect(derived1.Labels).To(gomega.HaveKey(placement.PlacementOriginLabel))
		// The control-plane-only placement selector must NOT ride along.
		gomega.Expect(derived1.Annotations).NotTo(gomega.HaveKey(placement.AcceleratorRequirementsAnnotation))

		ginkgo.By("admitting the derived ISVC on member1 (winner) — member2 stays gated")
		admitDerivedOn(member1Cluster, isvcNS, isvcName)

		ginkgo.By("asserting member1 is selected as the placement winner")
		gomega.Eventually(func() v1beta1.PlacementPhase {
			ps := placementStatusOf(isvcNS, isvcName)
			if ps == nil {
				return ""
			}
			return ps.Phase
		}, eventuallyTimeout, eventuallyPoll).Should(gomega.Equal(v1beta1.PlacementPhasePlaced))
		gomega.Expect(placementStatusOf(isvcNS, isvcName).Cluster).To(gomega.Equal(wc1Name))

		ginkgo.By("asserting the loser's derived ISVC is swept from member2")
		gomega.Eventually(func() error {
			return member2Cluster.client.Get(member2Cluster.ctx,
				types.NamespacedName{Namespace: isvcNS, Name: isvcName}, &v1beta1.InferenceService{})
		}, eventuallyTimeout, eventuallyPoll).Should(ometesting.BeNotFoundError(),
			"the placer should delete the derived ISVC on the losing member")

		ginkgo.By("asserting the winner's derived ISVC survives on member1")
		gomega.Consistently(func() bool {
			_, ok := getDerivedOn(member1Cluster, isvcNS, isvcName)
			return ok
		}, 2*eventuallyPoll, eventuallyPoll).Should(gomega.BeTrue())
	})

	ginkgo.It("re-places onto the surviving member when the winner's derived is lost", func() {
		ginkgo.By("placing the source ISVC and winning on member1")
		mustCreate(controlCluster, sourceISVC(isvcNS, isvcName, requirement()))
		gomega.Eventually(func() bool {
			_, ok := getDerivedOn(member1Cluster, isvcNS, isvcName)
			return ok
		}, eventuallyTimeout, eventuallyPoll).Should(gomega.BeTrue())
		admitDerivedOn(member1Cluster, isvcNS, isvcName)
		gomega.Eventually(func() string {
			ps := placementStatusOf(isvcNS, isvcName)
			if ps == nil {
				return ""
			}
			return ps.Cluster
		}, eventuallyTimeout, eventuallyPoll).Should(gomega.Equal(wc1Name))

		ginkgo.By("deleting the winner's derived out from under the control plane")
		derived, ok := getDerivedOn(member1Cluster, isvcNS, isvcName)
		gomega.Expect(ok).To(gomega.BeTrue())
		gomega.Expect(member1Cluster.client.Delete(member1Cluster.ctx, derived)).To(gomega.Succeed())

		ginkgo.By("asserting the placer re-fans-out (the derived reappears on a candidate)")
		gomega.Eventually(func() bool {
			_, ok1 := getDerivedOn(member1Cluster, isvcNS, isvcName)
			_, ok2 := getDerivedOn(member2Cluster, isvcNS, isvcName)
			return ok1 || ok2
		}, eventuallyTimeout, eventuallyPoll).Should(gomega.BeTrue(),
			"after the winner is lost the placer should re-place onto a candidate")
	})
})
