package omenative_coordination_kind

import (
	"fmt"
	"strings"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/podreadiness"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	workloadtypes "github.com/ome-projects/ome/pkg/controller/v1beta1/workload/types"
	"github.com/ome-projects/ome/tests/integration/util"
)

// A finished gang roll charges no retry.
//
// Two engine gangs (a leader and one worker each) roll to a new revision
// one gang at a time under SurgeThenDrain with RecreateInstanceOnPodRestart
// and a retry policy configured. OME builds the new gang at a free index,
// moves traffic to it and deletes the old gang's pods; it promotes the new
// gang only once those pods are gone. While the old pods are on their way
// out (a test finalizer keeps them in Terminating so the window is wide
// enough to act in), the replacement's leader is deleted by hand with its
// grace period and stays in the API, also under the finalizer, so OME sees
// it the way a real kubelet shows a deleted pod for a while: its container
// exited, the pod in a terminal phase. That is the recycle's case: OME
// writes the exit on the old gang's row, removes the dead member itself and
// recreates it under the same attempt, charging nothing. The old pods are
// then released and the new gang is promoted. The retired old row must
// charge nothing to the new revision's retry ladder either, whatever exit
// it still records: no RetryBlock is ever recorded for the new revision,
// no rollout hold names it, the second gang rolls at once and the engine
// converges on the new revision with both gangs whole.
//
// A member whose pod object is already gone when OME looks is a different
// story, a loss after the hand-over, and is charged by its own rule; the
// spec gang_surge_replacement_lost_rule pins that one.

// drainHoldFinalizer keeps a deleted pod in the API until the spec
// releases it, which holds the old gang's drain open and keeps the
// replacement's dead leader visible in its terminal phase.
const drainHoldFinalizer = "kind-test.ome.io/drain-hold"

// Cells: instance/Update/SurgeDrain/pod.phaseTerminal[Failed]/before, instance/Update/SurgeDrain/pod.deleted[source]/before
var _ = ginkgo.Describe("behavior - a gang roll whose replacement lost a member mid-drain charges no retry (KIND)", ginkgo.Ordered, func() {
	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "gang-drain-lost-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("recycles the dead leader, promotes the gang, records no RetryBlock and converges", func() {
		const (
			isvcName   = "gang-drain-lost"
			replicas   = 2
			workerSize = 1
			gangSize   = workerSize + 1
		)
		configureFastUpdateRetry()
		fix := gangSurgeDrainFixture(ns.Name, isvcName, replicas, workerSize)
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		runningRev, runningHash := awaitGangsServing(ns.Name, isvcName, replicas, gangSize)
		holdRunningPods(ns.Name, isvcName)

		newRev, newHash := pushGangRevision(ns.Name, isvcName, runningRev)

		// From the push to the end: the new revision's ladder is never
		// charged and no hold names it. A roll that succeeds owes nothing.
		stop := util.StartSampler(suiteCtx, kindPollInterval, func(g Gomega) {
			ir := freshInferenceReplica(g, ns.Name, isvcName, v1beta1.EngineComponent)
			if block := findRetryBlock(ir.Status.RetryBlocks, newRev); block != nil {
				g.Expect(block).To(BeNil(),
					"a RetryBlock was recorded for the new revision %s (state=%s reason=%q): a roll that succeeded was charged as a failed attempt", newRev, block.State, block.Reason)
			}
			if hold := ir.Status.RolloutHold; hold != nil && hold.Target == newRev {
				g.Expect(hold.Gate).NotTo(BeElementOf(v1beta1.RolloutHoldGateRetryBlock, v1beta1.RolloutHoldGateHeld),
					"the roll to %s is held by its own ladder: %s", newRev, hold.Reason)
			}
		})
		defer stop()

		sourceIdx, surgeIdx, sourcePods := awaitFirstGangDraining(ns.Name, isvcName, runningHash, newHash, gangSize)

		// The dead leader stays in the API under the hold, so OME reads it in
		// its terminal phase rather than finding its name free.
		ginkgo.By(fmt.Sprintf("deleting the replacement's leader (instance %d) by hand while the old gang drains, keeping its object in the API", surgeIdx))
		var killed *corev1.Pod
		Eventually(func(g Gomega) {
			leader := gangLeaderPod(g, ns.Name, isvcName, surgeIdx, newHash)
			setDrainHold(leader.Name, ns.Name, true)
			g.Expect(k8sClient.Delete(suiteCtx, leader)).To(Succeed())
			killed = leader
		}).WithTimeout(kindShortTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("OME reads the dead leader in its terminal phase and recycles it: the exit is written on the old gang's row")
		Eventually(func(g Gomega) {
			pod := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(killed), pod)).To(Succeed(), "the hold must keep the dead leader in the API")
			g.Expect(pod.UID).To(Equal(killed.UID))
			g.Expect(query.IsTerminalPod(pod)).To(BeTrue(), "the dead leader must read a terminal phase, got %s", pod.Status.Phase)
			for _, row := range instanceStatuses(g, ns.Name, isvcName, v1beta1.EngineComponent) {
				if row.Index != sourceIdx {
					continue
				}
				g.Expect(row.Operation).NotTo(BeNil(), "the old gang's row must keep its roll open")
				g.Expect(row.LastFailure).NotTo(BeNil(), "the recycle must write the dead member's exit on the old gang's row")
				g.Expect(row.LastFailure.PodName).To(Equal(killed.Name), "the recycle's record must name the dead leader")
			}
			g.Expect(eventsNaming(g, ns.Name, workloadtypes.EventReasonTerminalPodRecycled, killed.Name)).NotTo(BeEmpty(),
				"the recycle must announce the dead leader")
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("releasing the dead leader: the member is recreated under the same attempt")
		setDrainHold(killed.Name, ns.Name, false)
		Eventually(func(g Gomega) {
			leader := gangLeaderPod(g, ns.Name, isvcName, surgeIdx, newHash)
			g.Expect(leader.UID).NotTo(Equal(killed.UID), "the recreated leader must be a new pod")
			g.Expect(leader.DeletionTimestamp).To(BeNil())
			g.Expect(podIsReady(leader)).To(BeTrue())
			g.Expect(podreadiness.IsServing(leader)).To(BeTrue())
			snap, err := util.SnapshotPods(suiteCtx, k8sClient, ns.Name, isvcName)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(servingGangMembers(snap, surgeIdx, newHash)).To(Equal(gangSize), "the replacement gang must be whole again")
			for _, name := range sourcePods {
				pod := &corev1.Pod{}
				g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: ns.Name, Name: name}, pod)).To(Succeed(), "the hold must keep old pod %s in the API", name)
				g.Expect(pod.DeletionTimestamp).NotTo(BeNil())
			}
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("releasing the old gang's pods so the promotion can land")
		for _, name := range sourcePods {
			setDrainHold(name, ns.Name, false)
		}

		ginkgo.By("the roll finishes on both gangs with no retry charged")
		awaitGangsConvergedOn(ns.Name, isvcName, newRev, newHash, runningHash, replicas, gangSize)
		Consistently(func(g Gomega) {
			ir := freshInferenceReplica(g, ns.Name, isvcName, v1beta1.EngineComponent)
			g.Expect(ir.Status.CurrentRevision).To(Equal(newRev))
			g.Expect(ir.Status.RolloutHold).To(BeNil())
			g.Expect(findRetryBlock(ir.Status.RetryBlocks, newRev)).To(BeNil())
			g.Expect(decodedInstanceStatuses(g, ir)).To(HaveLen(replicas))
		}, kindSteadyStateWindow, kindPollInterval).Should(Succeed())

		samples := stop()
		Expect(samples).To(BeNumerically(">", 0), "the ladder must have been sampled across the roll")
	})
})

// gangSurgeDrainFixture is an engine-only ISVC of replicas gangs (a leader
// and workerSize workers each) under SurgeThenDrain maxSurge 1 /
// maxUnavailable 0 with RecreateInstanceOnPodRestart.
func gangSurgeDrainFixture(namespace, isvcName string, replicas, workerSize int) *coordFixture {
	maxSurge := intstr.FromInt(1)
	maxUnavailable := intstr.FromInt(0)
	restartPolicy := v1beta1.InstanceRestartPolicyRecreateInstance
	fix := newCoordFixture(namespace, WithISVCName(isvcName),
		WithoutDecoder(),
		WithEngineReplicas(replicas),
		WithEngineWorkerSize(workerSize),
		WithEngineImage("fake-serving:v1"))
	fix.ISVC.Spec.Engine.Lifecycle = &v1beta1.LifecycleSpec{
		UpdateStrategy: &v1beta1.UpdateStrategy{
			Type:          v1beta1.UpdateStrategySurgeThenDrain,
			RollingUpdate: &v1beta1.RollingUpdate{MaxSurge: &maxSurge, MaxUnavailable: &maxUnavailable},
		},
		RestartPolicy: &restartPolicy,
	}
	return fix
}

// awaitGangsServing blocks until every gang is Ready with no attempt open
// on one promoted revision and every member serves, then returns that
// revision's name and hash.
func awaitGangsServing(namespace, isvcName string, replicas, gangSize int) (revision, hash string) {
	EventuallyWithOffset(1, func(g Gomega) {
		ir := freshInferenceReplica(g, namespace, isvcName, v1beta1.EngineComponent)
		g.Expect(ir.Status.CurrentRevision).NotTo(BeEmpty())
		g.Expect(ir.Status.CurrentRevision).To(Equal(ir.Status.UpdateRevision))
		rows := decodedInstanceStatuses(g, ir)
		g.Expect(rows).To(HaveLen(replicas))
		for _, row := range rows {
			g.Expect(row.Phase).To(Equal(v1beta1.OMENativeInstanceReady))
			g.Expect(row.Operation).To(BeNil())
		}
		revision = ir.Status.CurrentRevision
		hash = query.RevisionHashFromControllerRevisionName(revision)
		snap, err := util.SnapshotPods(suiteCtx, k8sClient, namespace, isvcName)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(snap.Serving(v1beta1.EngineComponent)).To(Equal(replicas * gangSize))
	}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	return revision, hash
}

// holdRunningPods puts the drain-hold finalizer on every pod of the service
// so a drain's delete leaves them Terminating until the spec releases them,
// and releases whatever is left on any exit so the namespace can go away.
func holdRunningPods(namespace, isvcName string) {
	for _, pod := range listOMENativePods(Default, namespace, isvcName) {
		setDrainHold(pod.Name, namespace, true)
	}
	ginkgo.DeferCleanup(func() {
		for _, pod := range listOMENativePods(Default, namespace, isvcName) {
			setDrainHold(pod.Name, namespace, false)
		}
	})
}

// pushGangRevision bumps both gang templates to fake-serving:v2 and returns
// the new target revision's name and hash.
func pushGangRevision(namespace, isvcName, runningRev string) (revision, hash string) {
	EventuallyWithOffset(1, func(g Gomega) {
		cur := freshISVC(g, namespace, isvcName)
		cur.Spec.Engine.Leader.PodSpec.Containers[0].Image = "fake-serving:v2"
		cur.Spec.Engine.Worker.PodSpec.Containers[0].Image = "fake-serving:v2"
		g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
	}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())
	EventuallyWithOffset(1, func(g Gomega) {
		ir := freshInferenceReplica(g, namespace, isvcName, v1beta1.EngineComponent)
		g.Expect(ir.Status.UpdateRevision).NotTo(BeEmpty())
		g.Expect(ir.Status.UpdateRevision).NotTo(Equal(runningRev))
		revision = ir.Status.UpdateRevision
		hash = query.RevisionHashFromControllerRevisionName(revision)
	}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())
	return revision, hash
}

// awaitFirstGangDraining blocks until the first gang is in its drain: its
// replacement serves whole at a fresh index and its own pods are deleted
// but still in the API under the hold. Returns the source index, the
// replacement index and the source pods' names.
func awaitFirstGangDraining(namespace, isvcName, runningHash, newHash string, gangSize int) (sourceIdx, surgeIdx int32, sourcePods []string) {
	EventuallyWithOffset(1, func(g Gomega) {
		ir := freshInferenceReplica(g, namespace, isvcName, v1beta1.EngineComponent)
		var source *v1beta1.OMENativeInstanceStatus
		for _, row := range decodedInstanceStatuses(g, ir) {
			// The source names its replacement through SurgeIndex; the
			// replacement's own marker row names no other index.
			if row.Operation != nil && row.Operation.Type == v1beta1.InstanceOperationUpdate &&
				row.Operation.SurgeIndex != nil && *row.Operation.SurgeIndex != row.Index {
				r := row
				source = &r
			}
		}
		g.Expect(source).NotTo(BeNil(), "a gang must be rolling with a surge replacement")
		sourceIdx, surgeIdx = source.Index, *source.Operation.SurgeIndex
		snap, err := util.SnapshotPods(suiteCtx, k8sClient, namespace, isvcName)
		g.Expect(err).NotTo(HaveOccurred())
		sourcePods = sourcePods[:0]
		draining := 0
		for _, pod := range snap.ByComponent[v1beta1.EngineComponent] {
			idx, ok := query.InstanceIdxFromLabels(pod)
			if !ok || idx != sourceIdx || pod.Labels[query.LabelRevisionHash] != runningHash {
				continue
			}
			sourcePods = append(sourcePods, pod.Name)
			if pod.DeletionTimestamp != nil {
				draining++
			}
		}
		g.Expect(sourcePods).To(HaveLen(gangSize), "the old gang's pods must still be in the API under the hold")
		g.Expect(draining).To(Equal(gangSize), "the drain must have deleted the old gang's pods")
		g.Expect(servingGangMembers(snap, surgeIdx, newHash)).To(Equal(gangSize), "the replacement gang must be serving before the drain")
	}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	return sourceIdx, surgeIdx, sourcePods
}

// awaitGangsConvergedOn blocks until the engine has promoted revision and
// every gang is whole on it: the retired rows gone, no attempt open, no
// hold, no pod left on supersededHash. Old pods still held by the
// drain-hold finalizer once their drain deletes them are released on the
// way, so the second gang's drain can finish too.
func awaitGangsConvergedOn(namespace, isvcName, revision, hash, supersededHash string, replicas, gangSize int) {
	EventuallyWithOffset(1, func(g Gomega) {
		for _, pod := range listOMENativePods(g, namespace, isvcName) {
			if pod.DeletionTimestamp != nil && controllerutil.ContainsFinalizer(&pod, drainHoldFinalizer) {
				setDrainHold(pod.Name, namespace, false)
			}
		}
		ir := freshInferenceReplica(g, namespace, isvcName, v1beta1.EngineComponent)
		g.Expect(ir.Status.CurrentRevision).To(Equal(revision), "the engine must promote the new revision")
		g.Expect(ir.Status.RolloutHold).To(BeNil(), "nothing may hold a roll that finished")
		rows := decodedInstanceStatuses(g, ir)
		g.Expect(rows).To(HaveLen(replicas), "the retired source rows must be gone")
		for _, row := range rows {
			g.Expect(row.Phase).To(Equal(v1beta1.OMENativeInstanceReady), "instance %d must end Ready", row.Index)
			g.Expect(row.RunningRevision).To(Equal(revision), "instance %d must end on the new revision", row.Index)
			g.Expect(row.Operation).To(BeNil(), "instance %d must end with no attempt open", row.Index)
		}
		snap, err := util.SnapshotPods(suiteCtx, k8sClient, namespace, isvcName)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(snap.OnRevision(v1beta1.EngineComponent, supersededHash)).To(BeZero(), "no pod may still carry the running revision")
		g.Expect(snap.Serving(v1beta1.EngineComponent)).To(Equal(replicas*gangSize), "both gangs must serve whole on the new revision")
	}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
}

// setDrainHold adds or removes the drain-hold finalizer on the named pod,
// tolerating a pod that is already gone.
func setDrainHold(name, namespace string, hold bool) {
	EventuallyWithOffset(1, func(g Gomega) {
		pod := &corev1.Pod{}
		err := k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: namespace, Name: name}, pod)
		if client.IgnoreNotFound(err) == nil && err != nil {
			return
		}
		g.Expect(err).NotTo(HaveOccurred())
		changed := false
		if hold {
			changed = controllerutil.AddFinalizer(pod, drainHoldFinalizer)
		} else {
			changed = controllerutil.RemoveFinalizer(pod, drainHoldFinalizer)
		}
		if changed {
			g.Expect(k8sClient.Update(suiteCtx, pod)).To(Succeed())
		}
	}).WithTimeout(kindShortTimeout).WithPolling(kindPollInterval).Should(Succeed())
}

// gangLeaderPod returns the live leader pod of the gang at idx on hash.
func gangLeaderPod(g Gomega, namespace, isvcName string, idx int32, hash string) *corev1.Pod {
	var leader *corev1.Pod
	for _, pod := range listOMENativePods(g, namespace, isvcName) {
		if pod.DeletionTimestamp != nil || pod.Labels[query.LabelRunner] != "leader" || pod.Labels[query.LabelRevisionHash] != hash {
			continue
		}
		if i, ok := query.InstanceIdxFromLabels(&pod); ok && i == idx {
			p := pod
			g.ExpectWithOffset(1, leader).To(BeNil(), "instance %d must have one live leader on %s", idx, hash)
			leader = &p
		}
	}
	g.ExpectWithOffset(1, leader).NotTo(BeNil(), "instance %d must have a live leader on %s", idx, hash)
	return leader
}

// servingGangMembers counts the live, in-rotation engine pods of the gang
// at idx carrying hash.
func servingGangMembers(snap *util.PodSnapshot, idx int32, hash string) int {
	n := 0
	for _, pod := range snap.ByComponent[v1beta1.EngineComponent] {
		i, ok := query.InstanceIdxFromLabels(pod)
		if !ok || i != idx || pod.DeletionTimestamp != nil || pod.Labels[query.LabelRevisionHash] != hash {
			continue
		}
		if podIsReady(pod) && podreadiness.IsServing(pod) {
			n++
		}
	}
	return n
}

// eventsNaming lists the messages of the events with reason on the
// InferenceService whose message names name.
func eventsNaming(g Gomega, namespace string, reason workloadtypes.EventReason, name string) []string {
	var out []string
	for _, e := range eventsWithReason(g, namespace, string(reason)) {
		if strings.Contains(e.Message, name) {
			out = append(out, e.Message)
		}
	}
	return out
}
