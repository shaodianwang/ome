package omenative_coordination_kind

import (
	"strconv"
	"sync"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/podreadiness"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	"github.com/ome-projects/ome/tests/integration/util"
)

// A one-down, no-surge roll stops at the Instance a bad push breaks, and
// the next push lands there first.
//
// Three single-pod engine Instances under RecreatePod with maxSurge=0 /
// maxUnavailable=1: the shape of a service on machines with no spare
// capacity, where no extra pod can be placed and at most one Instance may
// be out of rotation at a time. A push whose container crashes on every
// start takes one Instance down and leaves it there: the two others keep
// serving the running revision, nothing takes a second one down, and the
// retry ladder parks the revision (a Failed row, a Held block naming the
// crash loop). The push that follows lands on that Instance first,
// because replacing a pod set that serves nothing costs no capacity, then
// on the two healthy Instances one at a time; nothing rebuilds the
// crashing revision once the fix is the target, and the fix's
// per-revision Services end with the three pods as their only ready
// endpoints.
//
// The mirror: the crash is on the running revision before any push — a
// live pod pointed at an image the cluster cannot pull, since a running
// pod's image is its only mutable field and the fake serving image has no
// per-pod crash switch — and then a sound push arrives. The wedged
// Instance takes the fix first under the same one-down rule, and no
// repair is recorded for it.
//
// "Never two down" is measured on the pods, not on status: at every poll
// from the first push to the end, the number of Instances with no pod in
// rotation on any revision is at most one; the peak is recorded and
// asserted at the end.

// Cells: instance/Update/Drain/pod.waiting[CrashLoopBackOff]/before, instance/Ready/pod.waiting[ImagePullBackOff]/boundary@T-ready-recreate
var _ = ginkgo.Describe("behavior — a one-down, no-surge roll stops at a bad push and lands the fix on the broken Instance first", ginkgo.Ordered, func() {
	const replicas = 3

	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "onedown-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("the Instance that failed the bad push lands the good push first and no second Instance is ever down", func() {
		const isvcName = "onedown-badpush"
		configureLifecycleConfig(crashLoopBoundLifecycleJSON)

		fix := oneDownNoSurgeFixture(ns.Name, isvcName, replicas)
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)
		runningRev, runningHash := awaitEngineConvergedOnOneRevision(ns.Name, isvcName, replicas)

		ledger := newRollLedger()
		stop := startOneDownSampler(ns.Name, isvcName, replicas, ledger)
		defer stop()

		ginkgo.By("pushing a revision whose container crashes at every start")
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, isvcName)
			engine := &cur.Spec.Engine.PodSpec.Containers[0]
			engine.Env = append(engine.Env, corev1.EnvVar{Name: "FAIL_AT", Value: "startup"})
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())
		awaitEngineTargetMoved(ns.Name, isvcName, runningRev)
		crashRev := engineTargetRevision(Default, ns.Name, isvcName)
		crashHash := query.RevisionHashFromControllerRevisionName(crashRev)

		// The roll stops at one Instance: its row is Failed with no attempt
		// open, the crashing revision is Held with the crash loop named, and
		// the two other Instances still serve the running revision.
		ginkgo.By("the roll stops at one Failed Instance with the crashing revision held")
		var failedIndex string
		Eventually(func(g Gomega) {
			ir := freshInferenceReplica(g, ns.Name, isvcName, v1beta1.EngineComponent)
			block := findRetryBlock(ir.Status.RetryBlocks, crashRev)
			g.Expect(block).NotTo(BeNil(), "a RetryBlock must be recorded for the crashing revision %s", crashRev)
			g.Expect(block.State).To(Equal(v1beta1.RetryBlockHeld), "the ladder must end Held, not retry the crash forever")
			g.Expect(block.Reason).To(Equal("CrashLoopBackOff"))
			g.Expect(ir.Status.RolloutHold).NotTo(BeNil(), "status.rolloutHold must say why the roll stopped")
			g.Expect(ir.Status.RolloutHold.Gate).To(Equal(v1beta1.RolloutHoldGateHeld))
			g.Expect(ir.Status.RolloutHold.Target).To(Equal(crashRev))
			g.Expect(ir.Status.RolloutHold.Reason).To(ContainSubstring("CrashLoopBackOff"))
			rows := decodedInstanceStatuses(g, ir)
			g.Expect(rows).To(HaveLen(replicas))
			failed := 0
			for _, row := range rows {
				g.Expect(row.Phase).To(BeElementOf(v1beta1.OMENativeInstanceReady, v1beta1.OMENativeInstanceFailed),
					"instance %d must be Ready on the running revision or the one Failed Instance", row.Index)
				if row.Phase == v1beta1.OMENativeInstanceFailed {
					failed++
					failedIndex = strconv.Itoa(int(row.Index))
					g.Expect(row.Operation).To(BeNil(), "a held revision leaves no attempt open on instance %d", row.Index)
					continue
				}
				g.Expect(row.RunningRevision).To(Equal(runningRev), "instance %d must still run the running revision", row.Index)
			}
			g.Expect(failed).To(Equal(1), "the bad push may cost exactly one Instance")
			snap, err := util.SnapshotPods(suiteCtx, k8sClient, ns.Name, isvcName)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(servingEnginePodsOn(snap, runningHash)).To(Equal(replicas-1),
				"every Instance but the failed one must keep serving the running revision")
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// Held has no time bound: across two operation requeues nothing
		// opens another attempt at the crashing revision and no second
		// Instance leaves the running revision.
		ginkgo.By("holding: no second Instance leaves the running revision and no further pod of the crashing revision is created")
		crashPodsWhileHeld := ledger.podsOnRevision(crashHash)
		Consistently(func(g Gomega) {
			snap, err := util.SnapshotPods(suiteCtx, k8sClient, ns.Name, isvcName)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(servingEnginePodsOn(snap, runningHash)).To(Equal(replicas-1),
				"the healthy Instances must stay on the running revision while the crashing one is held")
			for uid, index := range ledger.podsOnRevision(crashHash) {
				_, known := crashPodsWhileHeld[uid]
				g.Expect(known).To(BeTrue(), "no pod on the crashing revision may be created while it is Held (new pod %s on instance %s)", uid, index)
			}
			for _, row := range instanceStatuses(g, ns.Name, isvcName, v1beta1.EngineComponent) {
				g.Expect(row.Operation).To(BeNil(), "no attempt may reopen on instance %d while the revision is Held", row.Index)
			}
		}).WithTimeout(kindSteadyStateWindow).WithPolling(kindPollInterval).Should(Succeed())
		Expect(instanceIndicesOf(crashPodsWhileHeld)).To(ConsistOf(failedIndex),
			"only the Instance the roll took down may ever carry the crashing revision")
		Expect(len(crashPodsWhileHeld)).To(BeNumerically("<=", int(crashLoopBoundAttempts)),
			"distinct pods on the crashing revision (%d) must not exceed the ladder's bound (%d)", len(crashPodsWhileHeld), crashLoopBoundAttempts)

		ginkgo.By("pushing a good revision")
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, isvcName)
			engine := &cur.Spec.Engine.PodSpec.Containers[0]
			engine.Image = "fake-serving:v2"
			engine.Env = withoutEnv(engine.Env, "FAIL_AT")
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())
		fixSeenAt := awaitEngineTargetMoved(ns.Name, isvcName, crashRev)
		fixRev := engineTargetRevision(Default, ns.Name, isvcName)
		fixHash := query.RevisionHashFromControllerRevisionName(fixRev)
		Expect(fixRev).NotTo(Equal(runningRev), "the good push must be a revision of its own, not a return to the running one")

		// Read in one snapshot so the two facts are of one instant: the
		// first pod on the good revision belongs to the failed Instance,
		// and both healthy Instances still serve the running revision.
		ginkgo.By("the failed Instance receives the first pod on the good revision")
		healthyIndices := otherIndices(replicas, failedIndex)
		Eventually(func(g Gomega) {
			snap, err := util.SnapshotPods(suiteCtx, k8sClient, ns.Name, isvcName)
			g.Expect(err).NotTo(HaveOccurred())
			fixed := enginePodsOn(snap, fixHash)
			g.Expect(fixed).NotTo(BeEmpty(), "the roll must open on the good revision")
			for _, pod := range fixed {
				g.Expect(pod.Labels[query.LabelInstanceIdx]).To(Equal(failedIndex),
					"the first pod on the good revision must belong to the Instance that failed the bad push, got %s", pod.Name)
			}
			for _, index := range healthyIndices {
				g.Expect(servingEnginePodOfInstanceOn(snap, index, runningHash)).To(BeTrue(),
					"healthy instance %s must still serve the running revision while the failed Instance is replaced", index)
			}
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("every Instance ends Ready and serving on the good revision")
		expectEngineServingOnlyRevision(ns.Name, isvcName, replicas, fixRev, fixHash, []string{runningHash, crashHash})

		samples := stop()
		Expect(samples).To(BeNumerically(">", 0), "the one-down invariant must have been sampled during the roll")
		Expect(ledger.peakDown()).To(Equal(1),
			"at most one Instance may be out of rotation at any poll under maxUnavailable=1, and the roll must have taken one down")
		for uid, index := range ledger.podsOnRevision(crashHash) {
			_, known := crashPodsWhileHeld[uid]
			Expect(known).To(BeTrue(), "pod %s on instance %s was created on the crashing revision after the good push", uid, index)
		}
		Expect(restartsRecordedForInstance(ns.Name, isvcName, failedIndex, fixSeenAt)).To(BeEmpty(),
			"the failed Instance is the update pass's once the fix is the target; a repair on it is the defect")
	})

	ginkgo.It("an Instance wedged on the running revision lands the good push first and no second Instance is ever down", func() {
		const isvcName = "onedown-wedged"
		const wedgedIndex = "2"
		configureLifecycleConfig(offTargetRollLifecycleJSON)

		fix := oneDownNoSurgeFixture(ns.Name, isvcName, replicas)
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)
		runningRev, runningHash := awaitEngineConvergedOnOneRevision(ns.Name, isvcName, replicas)

		ginkgo.By("wedging instance " + wedgedIndex + " on the running revision")
		wedgeEngineInstanceOnRunningRevision(ns.Name, isvcName, wedgedIndex)

		ledger := newRollLedger()
		stop := startOneDownSampler(ns.Name, isvcName, replicas, ledger)
		defer stop()

		ginkgo.By("pushing a good revision while the wedge stands")
		Eventually(func(g Gomega) {
			cur := freshISVC(g, ns.Name, isvcName)
			cur.Spec.Engine.PodSpec.Containers[0].Image = "fake-serving:v2"
			g.Expect(k8sClient.Update(suiteCtx, cur)).To(Succeed())
		}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())
		pushSeenAt := awaitEngineTargetMoved(ns.Name, isvcName, runningRev)
		fixRev := engineTargetRevision(Default, ns.Name, isvcName)
		fixHash := query.RevisionHashFromControllerRevisionName(fixRev)

		ginkgo.By("the wedged Instance receives the first pod on the good revision")
		healthyIndices := otherIndices(replicas, wedgedIndex)
		Eventually(func(g Gomega) {
			snap, err := util.SnapshotPods(suiteCtx, k8sClient, ns.Name, isvcName)
			g.Expect(err).NotTo(HaveOccurred())
			fixed := enginePodsOn(snap, fixHash)
			g.Expect(fixed).NotTo(BeEmpty(), "the roll must open on the good revision")
			for _, pod := range fixed {
				g.Expect(pod.Labels[query.LabelInstanceIdx]).To(Equal(wedgedIndex),
					"the first pod on the good revision must belong to the wedged Instance, got %s", pod.Name)
			}
			for _, index := range healthyIndices {
				g.Expect(servingEnginePodOfInstanceOn(snap, index, runningHash)).To(BeTrue(),
					"healthy instance %s must still serve the running revision while the wedged Instance is replaced", index)
			}
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		ginkgo.By("every Instance ends Ready and serving on the good revision")
		expectEngineServingOnlyRevision(ns.Name, isvcName, replicas, fixRev, fixHash, []string{runningHash})

		samples := stop()
		Expect(samples).To(BeNumerically(">", 0), "the one-down invariant must have been sampled during the roll")
		Expect(ledger.peakDown()).To(Equal(1),
			"at most one Instance may be out of rotation at any poll under maxUnavailable=1, and the wedged one was down throughout its replacement")
		Expect(restartsRecordedForInstance(ns.Name, isvcName, wedgedIndex, pushSeenAt)).To(BeEmpty(),
			"an Instance off the roll target is the update pass's; a repair on the running revision is the defect")
	})
})

// oneDownNoSurgeFixture is an engine-only ISVC of replicas single-pod
// Instances under RecreatePod with maxSurge=0 / maxUnavailable=1.
func oneDownNoSurgeFixture(namespace, isvcName string, replicas int) *coordFixture {
	maxSurge := intstr.FromInt(0)
	maxUnavailable := intstr.FromInt(1)
	fix := newCoordFixture(namespace, WithISVCName(isvcName),
		WithoutDecoder(),
		WithEngineReplicas(replicas),
		WithEngineImage("fake-serving:v1"))
	fix.ISVC.Spec.Engine.Lifecycle = &v1beta1.LifecycleSpec{
		UpdateStrategy: &v1beta1.UpdateStrategy{
			Type: v1beta1.UpdateStrategyRecreatePod,
			RollingUpdate: &v1beta1.RollingUpdate{
				MaxSurge:       &maxSurge,
				MaxUnavailable: &maxUnavailable,
			},
		},
	}
	return fix
}

// awaitEngineConvergedOnOneRevision blocks until the engine
// InferenceReplica has every Instance Ready on its promoted revision with
// a serving pod each, then returns that revision's name and hash.
func awaitEngineConvergedOnOneRevision(namespace, isvcName string, replicas int) (revision, hash string) {
	EventuallyWithOffset(1, func(g Gomega) {
		ir := freshInferenceReplica(g, namespace, isvcName, v1beta1.EngineComponent)
		g.Expect(ir.Status.CurrentRevision).NotTo(BeEmpty(), "engine CurrentRevision must be promoted before the push")
		g.Expect(ir.Status.CurrentRevision).To(Equal(ir.Status.UpdateRevision))
		rows := decodedInstanceStatuses(g, ir)
		g.Expect(rows).To(HaveLen(replicas))
		for _, row := range rows {
			g.Expect(row.Phase).To(Equal(v1beta1.OMENativeInstanceReady))
			g.Expect(row.RunningRevision).To(Equal(ir.Status.CurrentRevision))
		}
		revision = ir.Status.CurrentRevision
		hash = query.RevisionHashFromControllerRevisionName(revision)
		snap, err := util.SnapshotPods(suiteCtx, k8sClient, namespace, isvcName)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(servingEnginePodsOn(snap, hash)).To(Equal(replicas), "every Instance must serve before the push")
	}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
	return revision, hash
}

// wedgeEngineInstanceOnRunningRevision points the live engine pod of the
// Instance at index at an image the cluster cannot pull and blocks until
// the kubelet reports it out of ContainersReady in a terminal waiting
// reason: the pod set now serves nothing on the running revision.
func wedgeEngineInstanceOnRunningRevision(namespace, isvcName, index string) {
	var wedgedPod string
	EventuallyWithOffset(1, func(g Gomega) {
		pod := singleEnginePodOfInstance(g, namespace, isvcName, index)
		pod.Spec.Containers[0].Image = "ghcr.io/ome-projects/this-image-does-not-exist:v0"
		g.Expect(k8sClient.Update(suiteCtx, pod)).To(Succeed())
		wedgedPod = pod.Name
	}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(Succeed())
	EventuallyWithOffset(1, func(g Gomega) {
		pod := &corev1.Pod{}
		g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: namespace, Name: wedgedPod}, pod)).To(Succeed())
		g.Expect(podreadiness.IsContainersReady(pod)).To(BeFalse(),
			"the wedged pod must leave ContainersReady before the push")
		g.Expect(terminalWaitingReasons(pod)).NotTo(BeEmpty(),
			"kubelet must report a terminal waiting reason on the wedged pod")
	}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
}

// expectEngineServingOnlyRevision blocks until the engine has converged
// on revision: every row Ready on it with no attempt open and no hold
// left, every Instance serving a pod of it, no pod left on any of the
// superseded hashes, and the per-revision Services routing to its pods
// alone.
func expectEngineServingOnlyRevision(namespace, isvcName string, replicas int, revision, hash string, supersededHashes []string) {
	EventuallyWithOffset(1, func(g Gomega) {
		ir := freshInferenceReplica(g, namespace, isvcName, v1beta1.EngineComponent)
		g.Expect(ir.Status.CurrentRevision).To(Equal(revision), "the engine must promote the good revision")
		g.Expect(ir.Status.RolloutHold).To(BeNil(), "status.rolloutHold must clear once the good revision converges")
		rows := decodedInstanceStatuses(g, ir)
		g.Expect(rows).To(HaveLen(replicas))
		for _, row := range rows {
			g.Expect(row.Phase).To(Equal(v1beta1.OMENativeInstanceReady), "instance %d must end Ready", row.Index)
			g.Expect(row.RunningRevision).To(Equal(revision), "instance %d must end on the good revision", row.Index)
			g.Expect(row.Operation).To(BeNil(), "instance %d must end with no attempt open", row.Index)
		}
		snap, err := util.SnapshotPods(suiteCtx, k8sClient, namespace, isvcName)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(servingEnginePodsOn(snap, hash)).To(Equal(replicas), "every Instance must serve on the good revision")
		for index := 0; index < replicas; index++ {
			g.Expect(servingEnginePodOfInstanceOn(snap, strconv.Itoa(index), hash)).To(BeTrue(),
				"instance %d must serve on the good revision under its own index", index)
		}
		for _, superseded := range supersededHashes {
			g.Expect(snap.OnRevision(v1beta1.EngineComponent, superseded)).To(Equal(0),
				"no pod may still carry the superseded revision %s", superseded)
		}
		expectPerRevisionServicesRouteOnly(g, namespace, hash, replicas)
	}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())
}

// expectPerRevisionServicesRouteOnly asserts the per-revision Services
// route to hash alone: every Service selecting it publishes replicas ready
// endpoints and every other per-revision Service publishes none.
func expectPerRevisionServicesRouteOnly(g Gomega, namespace, hash string, replicas int) {
	selectingHash := 0
	for _, svc := range listPerRevisionServices(g, namespace) {
		ready := readyEndpointCount(g, namespace, svc.Name)
		if svc.Spec.Selector[query.LabelRevisionHash] == hash {
			selectingHash++
			g.ExpectWithOffset(1, ready).To(Equal(replicas),
				"Service %s must publish every Instance of the good revision as a ready endpoint", svc.Name)
			continue
		}
		g.ExpectWithOffset(1, ready).To(BeZero(),
			"Service %s selects a superseded revision and must publish no ready endpoint", svc.Name)
	}
	g.ExpectWithOffset(1, selectingHash).NotTo(BeZero(), "the good revision must have its per-revision Services")
}

// rollLedger is what the one-down sampler records across a roll: the most
// Instances out of rotation at any one poll, and every engine pod seen
// with the Instance and revision it carried.
type rollLedger struct {
	mu      sync.Mutex
	peak    int
	podSeen map[types.UID]sampledPod
}

type sampledPod struct {
	index string
	hash  string
}

func newRollLedger() *rollLedger {
	return &rollLedger{podSeen: map[types.UID]sampledPod{}}
}

func (l *rollLedger) record(down int, pods []*corev1.Pod) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if down > l.peak {
		l.peak = down
	}
	for _, pod := range pods {
		l.podSeen[pod.UID] = sampledPod{index: pod.Labels[query.LabelInstanceIdx], hash: pod.Labels[query.LabelRevisionHash]}
	}
}

// peakDown is the most Instances out of rotation at any one poll.
func (l *rollLedger) peakDown() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.peak
}

// podsOnRevision maps the UID of every pod seen on hash to its Instance
// index.
func (l *rollLedger) podsOnRevision(hash string) map[types.UID]string {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := map[types.UID]string{}
	for uid, pod := range l.podSeen {
		if pod.hash == hash {
			out[uid] = pod.index
		}
	}
	return out
}

// startOneDownSampler polls the engine pods at the suite cadence and fails
// the spec the moment a poll shows more than one Instance out of rotation
// or more pods than Instances; every poll is recorded on ledger.
func startOneDownSampler(namespace, isvcName string, replicas int, ledger *rollLedger) func() int {
	return util.StartSampler(suiteCtx, kindPollInterval, func(g Gomega) {
		snap, err := util.SnapshotPods(suiteCtx, k8sClient, namespace, isvcName)
		g.Expect(err).NotTo(HaveOccurred())
		pods := snap.ByComponent[v1beta1.EngineComponent]
		down := instancesOutOfRotation(snap, replicas)
		ledger.record(down, pods)
		g.Expect(down).To(BeNumerically("<=", 1),
			"maxUnavailable=1 admits one Instance out of rotation; observed %d of %d with no pod in rotation on any revision", down, replicas)
		g.Expect(len(pods)).To(BeNumerically("<=", replicas),
			"maxSurge=0 under RecreatePod admits no pod beyond the Instance count; observed %d", len(pods))
	})
}

// instancesOutOfRotation counts the Instances in [0, replicas) with no
// engine pod in rotation (ContainersReady plus the serving gate) on any
// revision.
func instancesOutOfRotation(snap *util.PodSnapshot, replicas int) int {
	inRotation := map[string]bool{}
	for _, pod := range snap.ByComponent[v1beta1.EngineComponent] {
		if podreadiness.IsContainersReady(pod) && podreadiness.IsServing(pod) {
			inRotation[pod.Labels[query.LabelInstanceIdx]] = true
		}
	}
	down := 0
	for index := 0; index < replicas; index++ {
		if !inRotation[strconv.Itoa(index)] {
			down++
		}
	}
	return down
}

// enginePodsOn lists the engine pods, Terminating or not, carrying hash.
func enginePodsOn(snap *util.PodSnapshot, hash string) []*corev1.Pod {
	var out []*corev1.Pod
	for _, pod := range snap.ByComponent[v1beta1.EngineComponent] {
		if pod.Labels[query.LabelRevisionHash] == hash {
			out = append(out, pod)
		}
	}
	return out
}

// servingEnginePodsOn counts the engine pods in rotation (ContainersReady
// plus the serving gate) carrying hash.
func servingEnginePodsOn(snap *util.PodSnapshot, hash string) int {
	n := 0
	for _, pod := range enginePodsOn(snap, hash) {
		if podreadiness.IsContainersReady(pod) && podreadiness.IsServing(pod) {
			n++
		}
	}
	return n
}

// instanceIndicesOf lists the distinct Instance indices in a
// podsOnRevision map.
func instanceIndicesOf(pods map[types.UID]string) []string {
	seen := map[string]struct{}{}
	var out []string
	for _, index := range pods {
		if _, dup := seen[index]; dup {
			continue
		}
		seen[index] = struct{}{}
		out = append(out, index)
	}
	return out
}

// otherIndices lists the Instance indices in [0, replicas) other than
// index.
func otherIndices(replicas int, index string) []string {
	var out []string
	for candidate := 0; candidate < replicas; candidate++ {
		if s := strconv.Itoa(candidate); s != index {
			out = append(out, s)
		}
	}
	return out
}

// withoutEnv returns envs without the variable named name.
func withoutEnv(envs []corev1.EnvVar, name string) []corev1.EnvVar {
	kept := envs[:0]
	for _, env := range envs {
		if env.Name != name {
			kept = append(kept, env)
		}
	}
	return kept
}
