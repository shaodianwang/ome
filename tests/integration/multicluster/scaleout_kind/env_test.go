package scaleout_kind

import (
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/placement"
	ometesting "github.com/ome-projects/ome/pkg/utils/testing/v1beta1"
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
func setupSpecEnv() *specEnv {
	e := &specEnv{
		secretNS: uniqueName("mc-secrets"),
		isvcNS:   uniqueName("mc-workload"),
		isvcName: uniqueName("svc"),
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
	src := runnableSourceISVC(e.isvcNS, e.isvcName, model, runtime, v1beta1.PlacementModeSplit, replicas)
	src.Spec.Placement.Split = &v1beta1.SplitSpec{Spread: true}
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
