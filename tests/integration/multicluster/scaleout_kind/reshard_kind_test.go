package scaleout_kind

import (
	"time"

	"github.com/go-logr/logr"
	"github.com/google/go-cmp/cmp"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gatewayapiv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	placementendpoint "github.com/ome-projects/ome/pkg/controller/v1beta1/placement/endpoint"
	placementrouting "github.com/ome-projects/ome/pkg/controller/v1beta1/placement/routing"
	"github.com/ome-projects/ome/tests/integration/kindutil"
)

// Re-sharding and failure handling: what the placement does when a home goes
// away, comes back, loses a pod, or cannot run the workload at all. These are
// the paths where "it converged" is only a real claim if pods actually moved,
// so they belong on a kubelet rather than in envtest.
var _ = ginkgo.Describe("Multi-cluster re-shard and failure (KIND)", func() {
	var env *specEnv

	ginkgo.BeforeEach(func() {
		env = setupSpecEnv()
	})

	ginkgo.DescribeTable("retains exact shares across a member connection loss", func(reconnect bool) {
		model, runtime := env.seedModelRuntimeOnMembers()
		env.createSplitSource(model, runtime, 2)
		gomega.Eventually(func(g gomega.Gomega) {
			for _, m := range env.members {
				g.Expect(runningEnginePodsOn(g, m.c, env.isvcNS, env.isvcName)).To(gomega.Equal(1))
			}
			ps := placementStatusOf(env.isvcNS, env.isvcName)
			g.Expect(ps).NotTo(gomega.BeNil())
			g.Expect(ps.Plan).NotTo(gomega.BeNil())
			g.Expect(ps.Plan.PauseSurge).To(gomega.BeFalse())
		}).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.Succeed())
		survivor, lost := env.members[0], env.members[1]
		incumbent := enginePodsOn(gomega.Default, survivor.c, env.isvcNS, env.isvcName)[0].UID
		planID := placementStatusOf(env.isvcNS, env.isvcName).Plan.ID
		env.disconnectMember(lost)
		ginkgo.DeferCleanup(func() { env.reconnectMember(lost) })
		assertStable := func(g gomega.Gomega) {
			g.Expect(runningEnginePodsOn(g, survivor.c, env.isvcNS, env.isvcName)).To(gomega.Equal(1))
			pods := enginePodsOn(g, survivor.c, env.isvcNS, env.isvcName)
			g.Expect(pods).To(gomega.HaveLen(1))
			g.Expect(cmp.Diff(incumbent, pods[0].UID)).To(gomega.BeEmpty())
			ps := placementStatusOf(env.isvcNS, env.isvcName)
			g.Expect(ps).NotTo(gomega.BeNil())
			g.Expect(ps.Plan).NotTo(gomega.BeNil())
			g.Expect(cmp.Diff(planID, ps.Plan.ID)).To(gomega.BeEmpty())
			shares := map[string]int32{}
			for _, candidate := range ps.Candidates {
				g.Expect(candidate.Allocation).NotTo(gomega.BeNil())
				shares[candidate.Cluster] = candidate.Allocation.DesiredReplicas
			}
			g.Expect(cmp.Diff(map[string]int32{survivor.wcName: 1, lost.wcName: 1}, shares)).To(gomega.BeEmpty())
		}
		gomega.Consistently(assertStable).WithTimeout(10 * time.Second).WithPolling(time.Second).Should(gomega.Succeed())
		if reconnect {
			env.reconnectMember(lost)
			gomega.Eventually(func(g gomega.Gomega) {
				assertStable(g)
				g.Expect(runningEnginePodsOn(g, lost.c, env.isvcNS, env.isvcName)).To(gomega.Equal(1))
			}).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.Succeed())
		}
	},
		ginkgo.Entry("unreachable home retains its share", false),
		ginkgo.Entry("returning home keeps the same allocation", true),
	)

	ginkgo.It("does not move the placement when the winner's pod is replaced", func() {
		model, runtime := env.seedModelRuntimeOnMembers()
		env.createSource(model, runtime, v1beta1.PlacementModeSingle, 1)
		winner := env.awaitWinner()

		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(runningEnginePodsOn(g, winner.c, env.isvcNS, env.isvcName)).
				To(gomega.Equal(1))
			ps := placementStatusOf(env.isvcNS, env.isvcName)
			g.Expect(ps).NotTo(gomega.BeNil())
			g.Expect(ps.Plan).NotTo(gomega.BeNil())
			candidate := candidateFor(ps, winner.wcName)
			g.Expect(candidate).NotTo(gomega.BeNil())
			g.Expect(cmp.Diff(ps.Plan.ID, candidate.AppliedPlanID)).To(gomega.BeEmpty())
			g.Expect(cmp.Diff(int32(1), candidate.ReadyReplicas)).To(gomega.BeEmpty())
			for _, member := range env.members {
				if member.wcName != winner.wcName {
					g.Expect(derivedSweptOn(member.c, env.isvcNS, env.isvcName)).To(gomega.BeTrue())
				}
			}
		}).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.Succeed())
		accepted := placementStatusOf(env.isvcNS, env.isvcName).Plan.DeepCopy()
		derived := &v1beta1.InferenceService{}
		gomega.Expect(winner.c.client.Get(winner.c.ctx, types.NamespacedName{Namespace: env.isvcNS, Name: env.isvcName}, derived)).To(gomega.Succeed())
		derivedUID := derived.UID

		pods := enginePodsOn(gomega.Default, winner.c, env.isvcNS, env.isvcName)
		gomega.Expect(pods).NotTo(gomega.BeEmpty())
		oldUID := pods[0].UID

		// Admission can lapse during Pod replacement. The persisted winner and
		// member identity must survive that gap without opening another race.
		ginkgo.By("deleting the winner's engine pod")
		victim := &corev1.Pod{}
		gomega.Expect(winner.c.client.Get(winner.c.ctx,
			types.NamespacedName{Namespace: env.isvcNS, Name: pods[0].Name}, victim)).To(gomega.Succeed())
		gomega.Expect(winner.c.client.Delete(winner.c.ctx, victim)).To(gomega.Succeed())

		ginkgo.By("asserting the placement never leaves the winner while the pod is replaced")
		gomega.Consistently(func(g gomega.Gomega) {
			ps := placementStatusOf(env.isvcNS, env.isvcName)
			g.Expect(ps).NotTo(gomega.BeNil())
			g.Expect(ps.Plan).NotTo(gomega.BeNil())
			g.Expect(cmp.Diff(winner.wcName, ps.Cluster)).To(gomega.BeEmpty())
			g.Expect(cmp.Diff(accepted.Winner, ps.Plan.Winner)).To(gomega.BeEmpty())
			g.Expect(cmp.Diff(accepted.ID, ps.Plan.ID)).To(gomega.BeEmpty())
			g.Expect(winner.c.client.Get(winner.c.ctx, client.ObjectKeyFromObject(derived), derived)).To(gomega.Succeed())
			g.Expect(cmp.Diff(derivedUID, derived.UID)).To(gomega.BeEmpty())
			g.Expect(derived.DeletionTimestamp.IsZero()).To(gomega.BeTrue())
			for _, member := range env.members {
				if member.wcName != winner.wcName {
					g.Expect(derivedSweptOn(member.c, env.isvcNS, env.isvcName)).To(gomega.BeTrue())
				}
			}
		}).WithTimeout(20*time.Second).WithPolling(time.Second).Should(gomega.Succeed(),
			"pod-UID rotation must not re-race the placement")

		ginkgo.By("asserting a fresh pod took its place")
		gomega.Eventually(func(g gomega.Gomega) {
			current := enginePodsOn(g, winner.c, env.isvcNS, env.isvcName)
			g.Expect(current).NotTo(gomega.BeEmpty())
			var running bool
			for _, p := range current {
				g.Expect(p.UID).NotTo(gomega.Equal(oldUID), "the old pod should be gone")
				if p.Status.Phase == corev1.PodRunning {
					running = true
				}
			}
			g.Expect(running).To(gomega.BeTrue(), "the replacement should reach Running")
			ps := placementStatusOf(env.isvcNS, env.isvcName)
			g.Expect(cmp.Diff(winner.wcName, ps.Cluster)).To(gomega.BeEmpty())
			g.Expect(cmp.Diff(v1beta1.PlacementPhasePlaced, ps.Phase)).To(gomega.BeEmpty())
		}).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.Succeed())
	})

	ginkgo.It("marks a placement Failed when the workload cannot start, and stops re-placing", func() {
		// A short readiness budget so the member's backstop escalates inside the
		// suite's time; production defaults to 30 minutes.
		const readyTimeout = 45 * time.Second

		model, runtime := env.seedModelRuntimeOnMembers()
		env.createFailingSource(model, runtime, readyTimeout)

		ginkgo.By("waiting for the readiness backstop to escalate the placement to Failed")
		var failedOn string
		gomega.Eventually(func(g gomega.Gomega) {
			ps := placementStatusOf(env.isvcNS, env.isvcName)
			g.Expect(ps).NotTo(gomega.BeNil())
			g.Expect(ps.Phase).To(gomega.Equal(v1beta1.PlacementPhaseFailed))
			failedOn = ps.Cluster
		}).WithTimeout(readyTimeout + longTimeout).WithPolling(pollInterval).Should(gomega.Succeed())

		// Failed is terminal on purpose: the spec needs fixing, and re-placing a
		// workload that cannot start just moves the failure around the fleet.
		ginkgo.By("asserting it stays Failed on the same cluster rather than looping")
		gomega.Consistently(func(g gomega.Gomega) {
			ps := placementStatusOf(env.isvcNS, env.isvcName)
			g.Expect(ps).NotTo(gomega.BeNil())
			g.Expect(ps.Phase).To(gomega.Equal(v1beta1.PlacementPhaseFailed))
			g.Expect(ps.Cluster).To(gomega.Equal(failedOn))
		}).WithTimeout(20*time.Second).WithPolling(2*time.Second).Should(gomega.Succeed(),
			"a terminally failed placement must not re-place into a fail loop")
	})
})

var _ = ginkgo.Describe("Placement move with route publication (KIND)", func() {
	ginkgo.It("serves the replacement before retiring the original home", func() {
		env := setupSpecEnv()
		gomega.Expect(kindutil.ApplyCRDs(controlCluster.ctx, controlCluster.cfg, []string{"tests/integration/dep-crds/gateway-api"})).To(gomega.Succeed())
		scheme := kindutil.BaseScheme()
		gomega.Expect(gatewayapiv1.Install(scheme)).To(gomega.Succeed())
		cl, err := client.New(controlCluster.cfg, client.Options{Scheme: scheme})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		routing := &placementrouting.Reconciler{Client: cl, APIReader: cl, Log: logr.Discard(), Config: placementrouting.Config{Enabled: true}}
		publisher := &placementendpoint.TrafficMapPublisherReconciler{
			Client: cl, APIReader: cl, Log: logr.Discard(), Active: true,
			Publisher: placementendpoint.NewGatewayAPITrafficMapPublisher(cl, cl, placementendpoint.Config{
				GlobalHostTemplate: "{{.Name}}.{{.Namespace}}.example.com", GlobalGateway: env.isvcNS + "/gateway", BackendPort: 443,
			}),
		}
		key := client.ObjectKey{Namespace: env.isvcNS, Name: env.isvcName}
		publish := func(g gomega.Gomega) {
			_, err := routing.Reconcile(controlCluster.ctx, ctrl.Request{NamespacedName: key})
			g.Expect(err).NotTo(gomega.HaveOccurred())
			_, err = publisher.Reconcile(controlCluster.ctx, ctrl.Request{NamespacedName: key})
			g.Expect(err).NotTo(gomega.HaveOccurred())
		}
		model, runtime := env.seedModelRuntimeOnMembers()
		source := env.createSplitSource(model, runtime, 1)
		ginkgo.DeferCleanup(func() {
			tm := &v1beta1.TrafficMap{}
			if err := cl.Get(controlCluster.ctx, key, tm); apierrors.IsNotFound(err) {
				return
			} else {
				gomega.Expect(err).NotTo(gomega.HaveOccurred())
			}
			gomega.Expect(cl.Delete(controlCluster.ctx, tm)).To(gomega.Succeed())
			gomega.Eventually(func(g gomega.Gomega) {
				_, err := publisher.Reconcile(controlCluster.ctx, ctrl.Request{NamespacedName: key})
				g.Expect(err).NotTo(gomega.HaveOccurred())
				g.Expect(apierrors.IsNotFound(cl.Get(controlCluster.ctx, key, &v1beta1.TrafficMap{}))).To(gomega.BeTrue())
			}).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.Succeed())
		})
		first := env.preferredMember()
		replacement := env.other(first)
		gomega.Eventually(func(g gomega.Gomega) {
			publish(g)
			g.Expect(runningEnginePodsOn(g, first.c, env.isvcNS, env.isvcName)).To(gomega.Equal(1))
			g.Expect(cl.Get(controlCluster.ctx, key, source)).To(gomega.Succeed())
			candidate := candidateFor(source.Status.Placement, first.wcName)
			g.Expect(candidate).NotTo(gomega.BeNil())
			g.Expect(candidate.ReadyReplicas).To(gomega.Equal(int32(1)))
			g.Expect(source.Status.Placement.Plan.PauseSurge).To(gomega.BeFalse())
		}).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.Succeed())

		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(cl.Get(controlCluster.ctx, key, source)).To(gomega.Succeed())
			source.Spec.Placement.MaxSurge = ptr.To[int32](1)
			source.Spec.Placement.ClusterAffinity = []v1beta1.ClusterAffinityTerm{{MatchFields: []v1beta1.ClusterSelectorRequirement{{Key: "metadata.name", Operator: corev1.NodeSelectorOpIn, Values: []string{replacement.wcName}}}}}
			g.Expect(cl.Update(controlCluster.ctx, source)).To(gomega.Succeed())
		}).WithTimeout(mediumTimeout).WithPolling(pollInterval).Should(gomega.Succeed())
		maxOccupied, lostCoverage := 0, false
		gomega.Eventually(func(g gomega.Gomega) {
			publish(g)
			oldPods := enginePodsOn(g, first.c, env.isvcNS, env.isvcName)
			newPods := enginePodsOn(g, replacement.c, env.isvcNS, env.isvcName)
			maxOccupied = max(maxOccupied, len(oldPods)+len(newPods))
			newReady := 0
			for _, pod := range newPods {
				for _, condition := range pod.Status.Conditions {
					if condition.Type == corev1.PodReady && condition.Status == corev1.ConditionTrue {
						newReady++
					}
				}
			}
			lostCoverage = lostCoverage || len(oldPods) == 0 && newReady == 0
			g.Expect(newReady).To(gomega.Equal(1))
			g.Expect(oldPods).To(gomega.BeEmpty())
			g.Expect(cl.Get(controlCluster.ctx, key, source)).To(gomega.Succeed())
			g.Expect(source.Status.Placement.Plan.PauseSurge).To(gomega.BeFalse())
		}).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.Succeed())
		gomega.Expect(maxOccupied).To(gomega.BeNumerically("<=", 2), "one shared surge replica")
		gomega.Expect(cmp.Diff(false, lostCoverage)).To(gomega.BeEmpty(), "original home must survive until its replacement is ready")
	})
})
