package scaleout_kind

import (
	"slices"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"knative.dev/pkg/apis"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/placement"
	"github.com/ome-projects/ome/pkg/placement/protocol"
	"github.com/ome-projects/ome/tests/integration/multicluster"
)

// Cross-cluster ownership: which objects on a member the control plane is
// allowed to overwrite or delete. These are the guards against the worst
// failure mode in the design — deleting someone else's workload by identity —
// and on a real cluster the stakes are visible: the object under threat has
// pods a kubelet is running.
var _ = ginkgo.Describe("Multi-cluster ownership (KIND)", func() {
	var env *specEnv

	ginkgo.BeforeEach(func() {
		env = setupSpecEnv()
	})

	ginkgo.It("never overwrites or deletes a member-owned ISVC that shares the placed name", func() {
		model, runtime := env.seedModelRuntimeOnMembers()

		// A user creates their own ISVC directly on a member, at exactly the
		// name/namespace the placer is about to derive to. It carries no origin
		// markers, so it is not ours: the placer must neither adopt it (overwrite
		// its spec) nor sweep it (delete it as a loser).
		squatter := env.members[1]
		ginkgo.By("creating a member-owned ISVC on " + squatter.wcName + " at the name the placer will use")
		own := env.createMemberOwnedISVC(squatter, model, runtime)

		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(runningEnginePodsOn(g, squatter.c, env.isvcNS, env.isvcName)).
				To(gomega.BeNumerically(">=", 1))
		}).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.Succeed(),
			"the member-owned workload should be running before the placer arrives")

		podNames := func() []string {
			var out []string
			for _, p := range enginePodsOn(gomega.Default, squatter.c, env.isvcNS, env.isvcName) {
				out = append(out, p.Name)
			}
			return out
		}
		before := podNames()

		ginkgo.By("placing the source, whose derived would collide with the member-owned ISVC")
		env.createSource(model, runtime, v1beta1.PlacementModeSingle, 1)

		// The collision makes the fan-out to this member fail (tolerated per
		// cluster), so the other member is the only viable home.
		ginkgo.By("asserting the placement lands on the OTHER member")
		winner := env.awaitWinner()
		gomega.Expect(winner.wcName).To(gomega.Equal(env.other(squatter).wcName),
			"the placer must not take over the member-owned name")

		ginkgo.By("asserting the member-owned ISVC survives, unadopted and unswept")
		gomega.Consistently(func(g gomega.Gomega) {
			cur := &v1beta1.InferenceService{}
			g.Expect(squatter.c.client.Get(squatter.c.ctx,
				types.NamespacedName{Namespace: env.isvcNS, Name: env.isvcName}, cur)).To(gomega.Succeed(),
				"the member-owned ISVC must not be deleted")
			g.Expect(cur.DeletionTimestamp).To(gomega.BeNil(), "it must not even be marked for deletion")
			g.Expect(multicluster.OriginUID(cur)).To(gomega.BeEmpty(),
				"the placer must not stamp its origin markers onto a foreign object")
			g.Expect(cur.UID).To(gomega.Equal(own.UID), "it must be the same object, not a replacement")
		}).WithTimeout(10 * time.Second).WithPolling(time.Second).Should(gomega.Succeed())

		ginkgo.By("asserting its pods were never disturbed")
		gomega.Expect(podNames()).To(gomega.ConsistOf(before),
			"the member-owned workload's pods must be untouched")
	})

	ginkgo.It("reaps its own orphaned derived and its pods, leaving another control plane's alone", func() {
		model, runtime := env.seedModelRuntimeOnMembers()

		// The sweep treats an empty live-ISVC list as suspect and skips, so a
		// surviving unrelated source must exist for the GC to run at all.
		keeper := env.createKeeperISVC()
		ginkgo.DeferCleanup(func() { deleteIfExists(controlCluster, keeper) })

		// The sweep reaps an orphan at its next tick, which can come before a
		// real kubelet has started the orphan's pod. So the orphan starts life
		// as the derived of a source that is still alive, and the source is
		// deleted only once the pod runs: the reap then has real pods to remove.
		source := env.createUnplacedISVC("source")
		ginkgo.DeferCleanup(func() { deleteIfExists(controlCluster, source) })

		host := env.members[0]
		orphanName := uniqueName("orphan")
		foreignName := uniqueName("foreign")

		ginkgo.By("planting a derived of THIS control plane and a derived of another one")
		env.createDerivedLookalike(host, orphanName, model, runtime, controlPlaneID, string(source.UID))
		env.createDerivedLookalike(host, foreignName, model, runtime, "some-other-control-plane", uniqueName("dead-uid"))

		ginkgo.By("waiting for both to be running, so the reap has real pods to remove")
		for _, name := range []string{orphanName, foreignName} {
			gomega.Eventually(func(g gomega.Gomega) {
				g.Expect(runningEnginePodsOn(g, host.c, env.isvcNS, name)).
					To(gomega.BeNumerically(">=", 1))
			}).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.Succeed(),
				"%s should be running before the sweep", name)
		}

		ginkgo.By("deleting the source, which turns its derived into our orphan")
		gomega.Expect(controlCluster.client.Delete(controlCluster.ctx, source)).To(gomega.Succeed())
		gomega.Eventually(func() bool {
			err := controlCluster.client.Get(controlCluster.ctx,
				client.ObjectKeyFromObject(source), &v1beta1.InferenceService{})
			return apierrors.IsNotFound(err)
		}).WithTimeout(mediumTimeout).WithPolling(pollInterval).Should(gomega.BeTrue(),
			"an unplaced source carries no placement finalizer and should be gone at once")

		ginkgo.By("asserting the GC reaps our orphan and its pods")
		gomega.Eventually(func() bool {
			_, ok := getDerivedOn(host.c, env.isvcNS, orphanName)
			return ok
		}).WithTimeout(gcInterval+longTimeout).WithPolling(pollInterval).Should(gomega.BeFalse(),
			"an orphan stamped with our control-plane ID should be reaped")
		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(enginePodsOn(g, host.c, env.isvcNS, orphanName)).To(gomega.BeEmpty())
		}).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.Succeed(),
			"reaping the orphan must take its pods with it")

		ginkgo.By("asserting the other control plane's derived is left completely alone")
		gomega.Consistently(func(g gomega.Gomega) {
			_, ok := getDerivedOn(host.c, env.isvcNS, foreignName)
			g.Expect(ok).To(gomega.BeTrue(), "another control plane's derived must never be reaped")
			g.Expect(runningEnginePodsOn(g, host.c, env.isvcNS, foreignName)).To(gomega.BeNumerically(">=", 1),
				"its pods must keep running")
		}).WithTimeout(10 * time.Second).WithPolling(time.Second).Should(gomega.Succeed())
	})
})

// Adoption changes execution authority without changing the serving templates.
type adoptionIdentity struct {
	ServiceUID      types.UID
	ReplicaUID      types.UID
	CurrentRevision string
	UpdateRevision  string
	PodUIDs         []string
}

func adoptionIdentityOn(g gomega.Gomega, member memberReg, key client.ObjectKey) adoptionIdentity {
	service := &v1beta1.InferenceService{}
	g.Expect(member.c.client.Get(member.c.ctx, key, service)).To(gomega.Succeed())
	g.Expect(service.DeletionTimestamp.IsZero()).To(gomega.BeTrue())
	replica := &v1beta1.InferenceReplica{}
	g.Expect(member.c.client.Get(member.c.ctx,
		client.ObjectKey{Namespace: key.Namespace, Name: key.Name + "-engine"}, replica)).To(gomega.Succeed())
	g.Expect(replica.DeletionTimestamp.IsZero()).To(gomega.BeTrue())
	g.Expect(replica.Status.CurrentRevision).NotTo(gomega.BeEmpty())
	g.Expect(replica.Status.UpdateRevision).To(gomega.Equal(replica.Status.CurrentRevision))
	pods := enginePodsOn(g, member.c, key.Namespace, key.Name)
	g.Expect(pods).To(gomega.HaveLen(1))
	g.Expect(readyEnginePodsOn(g, member.c, key.Namespace, key.Name)).To(gomega.Equal(1))
	identity := adoptionIdentity{
		ServiceUID: service.UID, ReplicaUID: replica.UID,
		CurrentRevision: replica.Status.CurrentRevision, UpdateRevision: replica.Status.UpdateRevision,
	}
	for _, pod := range pods {
		g.Expect(pod.DeletionTimestamp.IsZero()).To(gomega.BeTrue())
		identity.PodUIDs = append(identity.PodUIDs, string(pod.UID))
	}
	slices.Sort(identity.PodUIDs)
	return identity
}

var _ = ginkgo.DescribeTable("Direct ClusterAffinity adoption (KIND)",
	func(mode v1beta1.PlacementMode) {
		env := setupSpecEnv()
		model, runtime := env.seedModelRuntimeOnMembers()
		members := env.members
		if mode == v1beta1.PlacementModeSingle {
			members = members[:1]
		}
		key := client.ObjectKey{Namespace: env.isvcNS, Name: env.isvcName}
		before := map[string]adoptionIdentity{}
		ginkgo.By("serving member-local workloads before creating a hub source")
		for _, member := range members {
			local := env.createMemberOwnedISVC(member, model, runtime)
			gomega.Expect(protocol.IsMember(local)).To(gomega.BeFalse())
			gomega.Eventually(func(g gomega.Gomega) {
				before[member.wcName] = adoptionIdentityOn(g, member, key)
			}).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.Succeed())
		}

		ginkgo.By("creating an explicit source that cannot overwrite the unowned members")
		source := env.createSource(model, runtime, mode, 1, func(s *v1beta1.InferenceService) {
			names := make([]string, 0, len(members))
			for _, member := range members {
				names = append(names, member.wcName)
			}
			s.Spec.Placement.ClusterAffinity = []v1beta1.ClusterAffinityTerm{{
				MatchFields: []v1beta1.ClusterSelectorRequirement{{
					Key: "metadata.name", Operator: corev1.NodeSelectorOpIn, Values: names,
				}},
			}}
			if mode == v1beta1.PlacementModeSplit {
				s.Spec.Placement.Split = &v1beta1.SplitSpec{
					Replicas: ptr.To(int32(len(members))), MaxReplicasPerCluster: 1,
				}
			}
		})
		assertIdentity := func(g gomega.Gomega) {
			for _, member := range members {
				g.Expect(cmp.Diff(before[member.wcName], adoptionIdentityOn(g, member, key))).
					To(gomega.BeEmpty(), "adoption must preserve the serving workload on %s", member.wcName)
			}
		}
		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(controlCluster.client.Get(controlCluster.ctx, key, source)).To(gomega.Succeed())
			condition := source.Status.GetCondition(v1beta1.PlacementBackendReady)
			g.Expect(condition).NotTo(gomega.BeNil())
			g.Expect(condition.Status).To(gomega.Equal(corev1.ConditionUnknown))
			g.Expect(condition.Reason).To(gomega.Equal("BackendUnknown"))
			assertIdentity(g)
		}).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.Succeed())

		ginkgo.By("handing ownership to the source one member at a time")
		for i, member := range members {
			gomega.Eventually(func(g gomega.Gomega) {
				local := &v1beta1.InferenceService{}
				g.Expect(member.c.client.Get(member.c.ctx, key, local)).To(gomega.Succeed())
				g.Expect(local.UID).To(gomega.Equal(before[member.wcName].ServiceUID))
				base := local.DeepCopy()
				if local.Labels == nil {
					local.Labels = map[string]string{}
				}
				if local.Annotations == nil {
					local.Annotations = map[string]string{}
				}
				local.Labels[placement.PlacementOriginLabel] = string(source.UID)
				local.Labels[placement.PlacementControlPlaneLabel] = controlPlaneID
				local.Annotations[placement.PlacementOriginUIDAnnotation] = string(source.UID)
				g.Expect(member.c.client.Patch(member.c.ctx, local,
					client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{}))).To(gomega.Succeed())
			}).WithTimeout(mediumTimeout).WithPolling(pollInterval).Should(gomega.Succeed())
			if i+1 < len(members) {
				gomega.Consistently(func(g gomega.Gomega) {
					assertIdentity(g)
					unowned := &v1beta1.InferenceService{}
					next := members[i+1]
					g.Expect(next.c.client.Get(next.c.ctx, key, unowned)).To(gomega.Succeed())
					g.Expect(unowned.Labels[placement.PlacementOriginLabel]).To(gomega.BeEmpty())
					g.Expect(unowned.Annotations[placement.PlacementOriginUIDAnnotation]).To(gomega.BeEmpty())
					g.Expect(protocol.IsMember(unowned)).To(gomega.BeFalse())
				}).WithTimeout(3 * placementRequeue).WithPolling(pollInterval).Should(gomega.Succeed())
			}
		}

		assertAdopted := func(g gomega.Gomega) {
			assertIdentity(g)
			g.Expect(controlCluster.client.Get(controlCluster.ctx, key, source)).To(gomega.Succeed())
			for _, conditionType := range []apis.ConditionType{
				v1beta1.PlacementConverged, v1beta1.PlacementSatisfied,
			} {
				condition := source.Status.GetCondition(conditionType)
				g.Expect(condition).NotTo(gomega.BeNil())
				g.Expect(condition.Status).To(gomega.Equal(corev1.ConditionTrue))
			}
			g.Expect(source.Status.Placement).NotTo(gomega.BeNil())
			g.Expect(source.Status.Placement.Plan).NotTo(gomega.BeNil())
			for _, member := range members {
				derived := &v1beta1.InferenceService{}
				g.Expect(member.c.client.Get(member.c.ctx, key, derived)).To(gomega.Succeed())
				g.Expect(derived.Spec.Placement).To(gomega.BeNil())
				g.Expect(derived.Annotations[constants.PlacementPolicy]).To(gomega.Equal(string(v1beta1.PlacementPolicyClusterAffinity)))
				policy, err := protocol.FromDerived(derived)
				g.Expect(err).NotTo(gomega.HaveOccurred())
				g.Expect(policy).NotTo(gomega.BeNil())
				g.Expect(policy.SourceUID).To(gomega.Equal(source.UID))
				g.Expect(policy.PlanID).To(gomega.Equal(source.Status.Placement.Plan.ID))
				replica := &v1beta1.InferenceReplica{}
				g.Expect(member.c.client.Get(member.c.ctx,
					client.ObjectKey{Namespace: key.Namespace, Name: key.Name + "-engine"}, replica)).To(gomega.Succeed())
				g.Expect(cmp.Diff(policy, replica.Spec.PlacementExecution)).To(gomega.BeEmpty())
			}
		}
		ginkgo.By("converging with the same pods, replica identities and serving revisions")
		gomega.Eventually(assertAdopted).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.Succeed())
		gomega.Consistently(assertAdopted).WithTimeout(3 * placementRequeue).WithPolling(pollInterval).Should(gomega.Succeed())
	},
	ginkgo.Entry("Single retains its existing home", v1beta1.PlacementModeSingle),
	ginkgo.Entry("All retains both existing homes", v1beta1.PlacementModeAll),
	ginkgo.Entry("Split retains both exact shares", v1beta1.PlacementModeSplit),
)
