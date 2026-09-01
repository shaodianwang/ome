package scaleout_kind

import (
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/placement"
	ometesting "github.com/ome-projects/ome/pkg/utils/testing/v1beta1"
	"github.com/ome-projects/ome/tests/integration/framework"
	"github.com/ome-projects/ome/tests/integration/kindfix"
	"github.com/ome-projects/ome/tests/integration/multicluster"
	"github.com/ome-projects/ome/tests/integration/util"
)

// The two fake-serving tags every member preloads. Distinct build args give them
// distinct digests, which an image-bump spec needs — kubelet short-circuits the
// image comparison when two tags resolve to the same sha256.
const (
	engineImageV1 = "fake-serving:v1"
	engineImageV2 = "fake-serving:v2"
)

// memberReg is one member as the control plane sees it (its WorkloadCluster and
// kubeconfig Secret) joined to the KIND cluster behind it. Specs need all three:
// the client to assert on pods, the WorkloadCluster name to read placement
// status, and the KIND cluster name to pause its node.
type memberReg struct {
	c           cluster
	wcName      string
	secretName  string
	kindCluster string
}

// specEnv is one spec's isolated slice of the shared members: its own
// namespaces, its own WorkloadCluster registrations, and the ISVC name it will
// place. Built by setupSpecEnv in a BeforeEach.
type specEnv struct {
	secretNS string
	isvcNS   string
	isvcName string
	members  []memberReg

	// kueueManaged labels the workload namespace so Kueue gates its pods.
	kueueManaged bool
	// sourceQueue overrides the queue a SOURCE is routed to, without touching
	// the queue grantQuota funds. The two differ only when a spec is about a
	// workload pointed somewhere its quota is not.
	sourceQueue string

	// engineCPU is the per-replica CPU request. Empty means none, which is
	// what most specs want; a spec asserting on admission MUST set it, since
	// Kueue admits a zero-request pod against even a zero quota.
	engineCPU string

	// queueName is this spec's own Kueue LocalQueue, and by extension its own
	// ClusterQueue. Set only for Kueue-managed specs.
	//
	// ClusterQueues are CLUSTER-scoped, so sharing the default one couples every
	// spec's quota to every other's: with specs re-running in a randomised order
	// against reused clusters, one spec's budget or its not-yet-reaped Workloads
	// decide whether the next one admits. A per-spec queue removes the coupling
	// entirely.
	queueName string
}

// setupSpecEnv gives a spec fresh namespaces on all three clusters and registers
// both KIND members as WorkloadClusters, waiting until the control plane reports
// them Ready.
//
// Deregistration is a DeferCleanup rather than an AfterEach so cleanup unwinds
// LIFO: the spec's own cleanup (delete the source, wait for its deriveds to be
// swept) runs FIRST, and the members are deregistered only after. Deregistering
// first would disconnect them, and the placer's delete path skips clusters it is
// no longer connected to — it would drop the finalizer and strand the deriveds.
// specEnvOption adjusts how a spec's environment is built.
type specEnvOption func(*specEnv)

// withKueueManagedNamespaces puts the spec's workload namespace under Kueue
// management on every member. Kueue runs everywhere but manages only namespaces
// carrying this label, so admission is opt-in: without it a spec's pods are
// never gated, even though every OME-derived pod carries a queue-name label.
func withKueueManagedNamespaces() specEnvOption {
	return func(e *specEnv) { e.kueueManaged = true }
}

// withEngineCPU gives each engine replica a CPU request, so quota is something
// Kueue can actually run out of. Without a request a pod consumes nothing and
// is admitted against a zero quota, which silently turns an admission spec into
// a no-op that passes.
// withSourceQueue routes created sources to a queue other than the one this
// spec funds. Quota still lands on the real queue, so a workload that fails to
// run under this option failed for want of routing rather than of budget.
func withSourceQueue(name string) specEnvOption {
	return func(e *specEnv) { e.sourceQueue = name }
}

func withEngineCPU(quantity string) specEnvOption {
	return func(e *specEnv) { e.engineCPU = quantity }
}

func setupSpecEnv(opts ...specEnvOption) *specEnv {
	e := &specEnv{
		secretNS: uniqueName("mc-secrets"),
		isvcNS:   uniqueName("mc-workload"),
		isvcName: uniqueName("svc"),
	}
	for _, opt := range opts {
		opt(e)
	}
	if e.kueueManaged {
		e.queueName = uniqueName("lq")
	}

	// Registered before anything is created, so it unwinds LAST: the source
	// teardown, model/runtime cleanup, and member deregistration all run first.
	// Without this the members accumulate three namespaces per spec, and the
	// clusters are reused across runs by default (KIND_KEEP_CLUSTER).
	ginkgo.DeferCleanup(func() {
		for _, m := range []cluster{member1, member2} {
			gomega.Expect(util.DeleteNamespace(m.ctx, m.client, e.isvcNS)).To(gomega.Succeed(),
				"delete %s on %s", e.isvcNS, m.name)
		}
		for _, ns := range []string{e.isvcNS, e.secretNS} {
			gomega.Expect(util.DeleteNamespace(controlCluster.ctx, controlCluster.client, ns)).
				To(gomega.Succeed(), "delete %s on the control plane", ns)
		}
	})

	createNamespace(controlCluster, e.secretNS)
	createNamespace(controlCluster, e.isvcNS)

	for i, m := range []cluster{member1, member2} {
		createNamespace(m, e.isvcNS)
		if e.kueueManaged {
			labelNamespace(m, e.isvcNS, kindfix.KueueManagedLabel, "true")
		}

		reg := memberReg{
			c:           m,
			wcName:      uniqueName("member"),
			secretName:  uniqueName("kubeconfig"),
			kindCluster: []string{member1ClusterName, member2ClusterName}[i],
		}
		mustCreate(controlCluster, kubeconfigSecretFor(m, e.secretNS, reg.secretName))

		ometesting.CreateAndWaitReady(controlCluster.ctx, controlCluster.client,
			multicluster.WorkloadClusterFor(reg.wcName, e.secretNS, reg.secretName, memberLabels()),
			"WorkloadCluster Ready=True",
			func(o *v1beta1.WorkloadCluster) bool {
				return apimeta.IsStatusConditionTrue(o.Status.Conditions, v1beta1.WorkloadClusterReady)
			})

		name := reg.wcName
		ginkgo.DeferCleanup(func() {
			deleteIfExists(controlCluster, multicluster.WorkloadClusterFor(name, "", "", nil))
		})

		e.members = append(e.members, reg)
	}
	return e
}

// seedModelRuntimeOnMembers creates the ClusterBaseModel + ClusterServingRuntime
// on BOTH members, so a derived ISVC placed on either is runnable, and waits for
// each member's controller cache to observe them. Returns the names to reference
// from the source ISVC.
//
// The cleanup it registers runs AFTER the source teardown a spec registers later
// (LIFO), which matters: dropping the model while a derived still references it
// leaves the member's reconciler error-looping on "ClusterBaseModel not found".
func (e *specEnv) seedModelRuntimeOnMembers() (model, runtime string) {
	model = uniqueName("cbm")
	runtime = uniqueName("csr")

	for _, m := range e.members {
		cbm := kindfix.StandardClusterBaseModel(model, kindfix.StandardModelStorageURI)
		csr := kindfix.StandardClusterServingRuntime(runtime)
		kindfix.ApplyModelRuntime(m.c.ctx, gomega.Default, m.c.client, cbm, csr, kindfix.ApplyModelRuntimeOpts{
			CachedReader: m.c.manager.GetClient(),
		})
		member := m.c
		ginkgo.DeferCleanup(func() {
			kindfix.CleanupModelRuntime(member.ctx, member.client, cbm, csr, nil)
		})
	}
	return model, runtime
}

// createSource creates the control-plane source ISVC and registers the teardown
// every spec needs: delete it, then wait for the placer to sweep its derived off
// every member before the model and runtime go away.
func (e *specEnv) createSource(model, runtime string, mode v1beta1.PlacementMode, replicas int) *v1beta1.InferenceService {
	src := runnableSourceISVC(e.isvcNS, e.isvcName, model, runtime, mode, replicas)
	e.applyEngineResources(src)
	e.routeToSpecQueue(src)
	mustCreate(controlCluster, src)

	ginkgo.DeferCleanup(func() {
		deleteIfExists(controlCluster, src)
		for _, m := range e.members {
			member := m
			gomega.Eventually(func() bool {
				return derivedOfSourceSwept(member.c, e.isvcNS, e.isvcName, string(src.UID))
			}).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.BeTrue(),
				"our derived should be swept from %s before the model/runtime are deleted", member.wcName)
		}
	})
	return src
}

// awaitWinner blocks until Single-mode placement resolves and returns the member
// that won. Which member wins is not fixed — with no Kueue gating, whichever
// member's pods appear first is admitted first — so specs must ask rather than
// assume.
func (e *specEnv) awaitWinner() memberReg {
	var winner string
	gomega.EventuallyWithOffset(1, func(g gomega.Gomega) {
		ps := placementStatusOf(e.isvcNS, e.isvcName)
		g.Expect(ps).NotTo(gomega.BeNil())
		g.Expect(ps.Phase).To(gomega.Equal(v1beta1.PlacementPhasePlaced))
		g.Expect(ps.Cluster).NotTo(gomega.BeEmpty(), "Placed must name a winning cluster")
		winner = ps.Cluster
	}).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.Succeed())

	for _, m := range e.members {
		if m.wcName == winner {
			return m
		}
	}
	gomega.ExpectWithOffset(1, winner).To(gomega.BeElementOf(e.memberNames()),
		"the winner must be one of this spec's registered members")
	return memberReg{}
}

// other returns the member that is not m — the loser, for a Single-mode spec.
func (e *specEnv) other(m memberReg) memberReg {
	for _, candidate := range e.members {
		if candidate.wcName != m.wcName {
			return candidate
		}
	}
	gomega.ExpectWithOffset(1, false).To(gomega.BeTrue(), "no other member registered")
	return memberReg{}
}

// memberNames lists the registered WorkloadCluster names, for assertion messages.
func (e *specEnv) memberNames() []string {
	out := make([]string, 0, len(e.members))
	for _, m := range e.members {
		out = append(out, m.wcName)
	}
	return out
}

// createKeeperISVC creates a control-plane ISVC that declares no placement
// requirement, so it matches no cluster and is never fanned out. It exists only
// to keep the GC's live-ISVC list non-empty: the sweep treats an empty list as
// suspect and skips it, so without a survivor no orphan is ever reaped.
func (e *specEnv) createKeeperISVC() *v1beta1.InferenceService {
	keeper := runnableSourceISVC(e.isvcNS, uniqueName("keeper"), "unused-model", "unused-runtime",
		v1beta1.PlacementModeSingle, 1)
	keeper.Spec.Placement = nil // no requirements => no candidates => never placed
	mustCreate(controlCluster, keeper)
	return keeper
}

// createSplitSource creates a Split-mode source that spreads its replicas evenly
// across the members. Balanced (spread) is what makes the distribution
// observable without Kueue: Packed would fill the first home and sweep the
// second, since with no admission gate every requested replica is admitted.
func (e *specEnv) createSplitSource(model, runtime string, replicas int) *v1beta1.InferenceService {
	return e.createSplitSourceWith(model, runtime, replicas, &v1beta1.SplitSpec{Spread: true})
}

// createSplitSourceWith is createSplitSource with the Split policy spelled out,
// for the specs that exercise Packed order, the per-cluster ceiling, or the
// anti-sliver floor.
func (e *specEnv) createSplitSourceWith(model, runtime string, replicas int, split *v1beta1.SplitSpec) *v1beta1.InferenceService {
	src := runnableSourceISVC(e.isvcNS, e.isvcName, model, runtime, v1beta1.PlacementModeSplit, replicas)
	src.Spec.Placement.Split = split
	e.applyEngineResources(src)
	e.routeToSpecQueue(src)
	mustCreate(controlCluster, src)

	ginkgo.DeferCleanup(func() {
		deleteIfExists(controlCluster, src)
		for _, m := range e.members {
			gomega.Eventually(func() bool {
				return derivedSweptOn(m.c, e.isvcNS, e.isvcName)
			}).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.BeTrue(),
				"derived should be swept from %s", m.wcName)
		}
	})
	return src
}

// createMemberOwnedISVC creates a runnable InferenceService directly on a member,
// carrying NO placement-origin markers — the "a user made this themselves"
// object the cross-cluster origin guards must never touch. It deliberately uses
// the same name and namespace the placer would derive to, which is the only
// shape that can trigger a same-name collision.
func (e *specEnv) createMemberOwnedISVC(m memberReg, model, runtime string) *v1beta1.InferenceService {
	own := runnableSourceISVC(e.isvcNS, e.isvcName, model, runtime, v1beta1.PlacementModeSingle, 1)
	own.Spec.Placement = nil // a member-local ISVC declares no placement
	mustCreate(m.c, own)
	ginkgo.DeferCleanup(func() { deleteIfExists(m.c, own) })
	return own
}

// createDerivedLookalike creates a runnable ISVC on a member stamped as a
// placement derived of controlPlane with source UID originUID. A UID with no
// live source on the control plane makes it an orphan; a controlPlane that is
// not ours makes it another control plane's business.
func (e *specEnv) createDerivedLookalike(m memberReg, name, model, runtime, controlPlane, originUID string) *v1beta1.InferenceService {
	d := runnableSourceISVC(e.isvcNS, name, model, runtime, v1beta1.PlacementModeSingle, 1)
	d.Spec.Placement = nil
	d.Labels = map[string]string{
		placement.PlacementOriginLabel:       originUID,
		placement.PlacementControlPlaneLabel: controlPlane,
	}
	d.Annotations[placement.PlacementOriginUIDAnnotation] = originUID
	mustCreate(m.c, d)
	ginkgo.DeferCleanup(func() { deleteIfExists(m.c, d) })
	return d
}

// candidateFor returns the per-home placement entry for a member, or nil when
// the control plane has not recorded that home yet.
func candidateFor(ps *v1beta1.PlacementStatus, wcName string) *v1beta1.CandidatePlacement {
	if ps == nil {
		return nil
	}
	for i := range ps.Candidates {
		if ps.Candidates[i].Cluster == wcName {
			return &ps.Candidates[i]
		}
	}
	return nil
}

// disconnectMember makes a member unreachable to the control plane by corrupting
// its kubeconfig Secret, and blocks until the WorkloadCluster reports NotReady.
// A bad kubeconfig is rejected outright rather than treated as a probe blip, so
// it takes effect immediately — unlike pausing the node, which the connection
// grace period deliberately rides out.
func (e *specEnv) disconnectMember(m memberReg) {
	rotateSecretTo(controlCluster, e.secretNS, m.secretName, []byte("not-a-kubeconfig"))
	gomega.EventuallyWithOffset(1, func() bool {
		return workloadClusterReady(m.wcName)
	}).WithTimeout(mediumTimeout).WithPolling(pollInterval).Should(gomega.BeFalse(),
		"%s should go NotReady on an invalid kubeconfig", m.wcName)
}

// reconnectMember restores a member's kubeconfig and blocks until the control
// plane reports it Ready again.
func (e *specEnv) reconnectMember(m memberReg) {
	rotateSecretTo(controlCluster, e.secretNS, m.secretName, kubeconfigBytesFor(m.c))
	gomega.EventuallyWithOffset(1, func() bool {
		return workloadClusterReady(m.wcName)
	}).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.BeTrue(),
		"%s should return to Ready once its kubeconfig is restored", m.wcName)
}

// readyReplicasAcrossHomes sums the ready-replica weight every home reports —
// the fleet-wide count Split converges toward.
func (e *specEnv) readyReplicasAcrossHomes() int32 {
	ps := placementStatusOf(e.isvcNS, e.isvcName)
	if ps == nil {
		return 0
	}
	var total int32
	for i := range ps.Candidates {
		total += ps.Candidates[i].ReadyReplicas
	}
	return total
}

// createFailingSource creates a Single-mode source whose engine image does not
// exist, with a short InstanceReadyTimeout so the member's readiness backstop
// escalates the Instance to Failed inside the suite's budget rather than the
// 30-minute production default.
func (e *specEnv) createFailingSource(model, runtime string, readyTimeout time.Duration) *v1beta1.InferenceService {
	src := runnableSourceISVC(e.isvcNS, e.isvcName, model, runtime, v1beta1.PlacementModeSingle, 1)
	src.Spec.Engine.PodSpec.Containers[0].Image = "fake-serving:does-not-exist"
	src.Spec.Engine.Lifecycle = &v1beta1.LifecycleSpec{
		InstanceReadyTimeout: &metav1.Duration{Duration: readyTimeout},
	}
	mustCreate(controlCluster, src)

	ginkgo.DeferCleanup(func() {
		deleteIfExists(controlCluster, src)
		for _, m := range e.members {
			member := m
			gomega.Eventually(func() bool {
				return derivedOfSourceSwept(member.c, e.isvcNS, e.isvcName, string(src.UID))
			}).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.BeTrue(),
				"our derived should be swept from %s", member.wcName)
		}
	})
	return src
}

// applyEngineResources stamps the spec's CPU request onto the engine container,
// when one was requested.
func (e *specEnv) applyEngineResources(src *v1beta1.InferenceService) {
	if e.engineCPU == "" {
		return
	}
	src.Spec.Engine.PodSpec.Containers[0].Resources = corev1.ResourceRequirements{
		Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse(e.engineCPU)},
	}
}

// grantQuota sets a member's CPU quota for this spec's namespace. Re-callable:
// the underlying ClusterQueue is upserted, so a spec can tighten or release
// quota mid-flight and watch the placement respond.
func (e *specEnv) grantQuota(m memberReg, cpu string) {
	gomega.ExpectWithOffset(1, e.queueName).NotTo(gomega.BeEmpty(),
		"grantQuota needs a per-spec queue: build the env with withKueueManagedNamespaces()")
	gomega.ExpectWithOffset(1, framework.ApplyKueueQuota(
		m.c.ctx, m.c.client, e.isvcNS, e.queueName, cpu,
	)).To(gomega.Succeed(), "grant %s CPU quota on %s", cpu, m.wcName)
}

// cpuForReplicas is the exact quota that admits n replicas of engineCPU and no
// more. Specs state capacity in replicas; Kueue counts CPU.
func (e *specEnv) cpuForReplicas(n int) string {
	q := resource.MustParse(e.engineCPU)
	total := resource.NewMilliQuantity(q.MilliValue()*int64(n), resource.DecimalSI)
	return total.String()
}

// admittedAcrossHomes sums the admitted replicas every home reports — the
// fleet-wide count Split converges toward.
func (e *specEnv) admittedAcrossHomes() int32 {
	ps := placementStatusOf(e.isvcNS, e.isvcName)
	if ps == nil {
		return 0
	}
	var total int32
	for i := range ps.Candidates {
		total += ps.Candidates[i].AdmittedReplicas
	}
	return total
}

// preferredMember returns the member the placer fills first under Packed.
//
// MatchCandidates returns candidates sorted by WorkloadCluster name, and
// splitApportion walks that slice in order — so preference is lexicographic on
// the name, NOT the order this suite happened to register them in. Those two
// coincide only until a uniqueName counter crosses a digit boundary.
func (e *specEnv) preferredMember() memberReg {
	preferred := e.members[0]
	for _, m := range e.members[1:] {
		if m.wcName < preferred.wcName {
			preferred = m
		}
	}
	return preferred
}

// routeToSpecQueue points the derived workload at this spec's own LocalQueue.
//
// DeriveISVC reads ome.io/local-queue and stamps that name onto every component's
// pods, defaulting to a single shared queue. Naming it per spec is what keeps one
// spec's quota from deciding another's admissions.
func (e *specEnv) routeToSpecQueue(src *v1beta1.InferenceService) {
	if e.queueName == "" {
		return
	}
	if src.Annotations == nil {
		src.Annotations = map[string]string{}
	}
	queue := e.queueName
	if e.sourceQueue != "" {
		queue = e.sourceQueue
	}
	src.Annotations[placement.LocalQueueAnnotation] = queue
}
