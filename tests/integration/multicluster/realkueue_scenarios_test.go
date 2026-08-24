package multicluster

import (
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/placement"
	ometesting "github.com/ome-projects/ome/pkg/utils/testing/v1beta1"
	"github.com/ome-projects/ome/tests/integration/framework"
)

// These specs (S1–S7) drive the control-plane → member placement race end to end
// against REAL Kueue quota — no fakes. The sim specs in placement_test.go /
// gc_test.go are the structural template; here the member-side admission that the
// sim path forged with admitDerivedOn is produced for real:
//
//	source ISVC (control plane: accelerator + local-queue annotations + runnable
//	  OMENative engine/decoder shape)
//	  → CP placer fans the derived ISVC out to every matching member
//	  → each member's ISVC/IR reconcilers create queue-labeled engine pod(s)
//	  → Kueue gates them; the member's quota ("1") ungates, quota ("0") holds
//	  → IR controller flips InstanceStatus.Admitted on the ungated component
//	  → CP placer sees AllComponentsAdmitted on the winner, records the winner,
//	    sweeps the losers, and flows placement status back.
//
// All behind skipUnlessRealKueue() (members are bare when the gate is off).
var _ = ginkgo.Describe("Real-Kueue placement scenarios", ginkgo.Ordered, func() {
	const (
		// acceleratorLabel/Value is the capability both members advertise and the
		// source ISVC requires (a fixed test value; production resolves it from
		// spec.model + runtime).
		acceleratorLabel = "gpu"
		acceleratorValue = "test-accel"
		// engineCPU is the per-component CPU request stamped on each engine/decoder
		// pod. quota "1" admits it, quota "0" blocks it.
		engineCPU = "500m"
	)

	var (
		secretNS string
		isvcNS   string

		wc1Name, wc2Name string
		secret1, secret2 string
	)

	matchingLabels := func() map[string]string { return map[string]string{acceleratorLabel: acceleratorValue} }
	requirement := func() string { return acceleratorLabel + "=" + acceleratorValue }

	ginkgo.BeforeEach(func() {
		skipUnlessRealKueue()

		secretNS = uniqueName("rk-secrets")
		isvcNS = uniqueName("rk-workload")
		createNamespace(controlCluster, secretNS)
		createNamespace(controlCluster, isvcNS)
		createNamespace(member1Cluster, isvcNS)
		createNamespace(member2Cluster, isvcNS)

		secret1 = uniqueName("kubeconfig-m1")
		secret2 = uniqueName("kubeconfig-m2")
		wc1Name = uniqueName("member1")
		wc2Name = uniqueName("member2")

		mustCreate(controlCluster, kubeconfigSecretFor(member1Cluster, secretNS, secret1))
		mustCreate(controlCluster, kubeconfigSecretFor(member2Cluster, secretNS, secret2))

		ginkgo.By("creating the WorkloadClusters and waiting for Ready=True")
		for _, wc := range []struct{ name, secret string }{{wc1Name, secret1}, {wc2Name, secret2}} {
			ometesting.CreateAndWaitReady(controlCluster.ctx, controlCluster.client,
				WorkloadClusterFor(wc.name, secretNS, wc.secret, matchingLabels()),
				"WorkloadCluster Ready=True",
				func(o *v1beta1.WorkloadCluster) bool {
					return apimeta.IsStatusConditionTrue(o.Status.Conditions, v1beta1.WorkloadClusterReady)
				})
		}
	})

	ginkgo.AfterEach(func() {
		deleteIfExists(controlCluster, WorkloadClusterFor(wc1Name, secretNS, secret1, nil))
		deleteIfExists(controlCluster, WorkloadClusterFor(wc2Name, secretNS, secret2, nil))
	})

	ginkgo.It("S1 happy race: member1 admits, member2 blocked → winner=member1, loser swept", func() {
		isvcName := uniqueName("svc")
		queue := uniqueName("rk-lq")

		seedRuntimeOnMembers(queue) // (queue name reused only for unique runtime name)
		csrName := runtimeNameFor(queue)

		ginkgo.By("granting an admissible quota on member1 and a zero quota on member2")
		applyQuota(member1Cluster, isvcNS, queue, "1")
		applyQuota(member2Cluster, isvcNS, queue, "0")

		ginkgo.By("creating the runnable source ISVC on the control plane")
		mustCreate(controlCluster, runnableSourceISVC(isvcNS, isvcName, csrName, requirement(), queue, false))
		ginkgo.DeferCleanup(func() {
			deleteIfExists(controlCluster, runnableSourceISVC(isvcNS, isvcName, csrName, requirement(), queue, false))
		})

		// The derived fans out to both members, but member1 admits near-instantly
		// (quota "1") and the placer sweeps member2's loser derived promptly, so
		// "member2's derived ever existed" is racy to observe here — the fan-out to
		// a non-winning member is asserted deterministically in S3/S4 (where no
		// member wins fast). Here we assert member1's derived appears, member1 wins,
		// and member2 ends up swept.
		ginkgo.By("asserting the derived fans out to member1")
		expectDerivedOn(member1Cluster, isvcNS, isvcName, true)

		ginkgo.By("asserting member1 (the admitting member) is selected as the placement winner")
		gomega.Eventually(func() v1beta1.PlacementPhase {
			ps := placementStatusOf(isvcNS, isvcName)
			if ps == nil {
				return ""
			}
			return ps.Phase
		}, eventuallyTimeout, eventuallyPoll).Should(gomega.Equal(v1beta1.PlacementPhasePlaced))
		ps := placementStatusOf(isvcNS, isvcName)
		gomega.Expect(ps.Cluster).To(gomega.Equal(wc1Name))
		gomega.Expect(CandidateClusters(ps)).To(gomega.ContainElement(wc1Name),
			"the winner must appear in the candidates list")

		ginkgo.By("asserting member2's loser derived is swept by the placer (deleted or Terminating)")
		expectDerivedSweptOn(member2Cluster, isvcNS, isvcName)

		ginkgo.By("asserting the winner's derived survives on member1")
		gomega.Consistently(func() bool {
			_, ok := getDerivedOn(member1Cluster, isvcNS, isvcName)
			return ok
		}, 2*eventuallyPoll, eventuallyPoll).Should(gomega.BeTrue())
	})

	ginkgo.It("S2 PD all-components: no winner until BOTH engine+decoder admit", func() {
		isvcName := uniqueName("svc")
		queue := uniqueName("rk-lq")
		seedRuntimeOnMembers(queue)
		csrName := runtimeNameFor(queue)

		// derive.go stamps the SAME queue on engine AND decoder, so a single
		// LocalQueue gates both PD components. We therefore exercise the
		// AllComponentsAdmitted invariant by START with quota that is too small
		// for BOTH pods (admits at most one), assert NO winner while a component
		// is still gated, then RELEASE full quota and assert the winner appears
		// only once every component has admitted. member2 stays blocked.
		ginkgo.By("granting member1 a quota that admits at most ONE of the two PD pods")
		applyQuota(member1Cluster, isvcNS, queue, engineCPU) // 500m: fits one 500m pod, not two
		applyQuota(member2Cluster, isvcNS, queue, "0")

		ginkgo.By("creating the runnable PD source ISVC (engine + decoder)")
		mustCreate(controlCluster, runnableSourceISVC(isvcNS, isvcName, csrName, requirement(), queue, true))
		ginkgo.DeferCleanup(func() {
			deleteIfExists(controlCluster, runnableSourceISVC(isvcNS, isvcName, csrName, requirement(), queue, true))
		})

		ginkgo.By("asserting the derived fans out to member1")
		expectDerivedOn(member1Cluster, isvcNS, isvcName, true)

		ginkgo.By("asserting NO winner is declared while a component is still gated under-quota")
		gomega.Consistently(func() v1beta1.PlacementPhase {
			ps := placementStatusOf(isvcNS, isvcName)
			if ps == nil {
				return ""
			}
			return ps.Phase
		}, "5s", eventuallyPoll).ShouldNot(gomega.Equal(v1beta1.PlacementPhasePlaced),
			"the placer must not declare a winner while any PD component is gated")

		ginkgo.By("releasing full quota on member1 so BOTH PD components can admit")
		applyQuota(member1Cluster, isvcNS, queue, "2") // 2 CPU: admits both 500m pods

		ginkgo.By("asserting member1 wins only once ALL components have admitted")
		gomega.Eventually(func() string {
			ps := placementStatusOf(isvcNS, isvcName)
			if ps == nil || ps.Phase != v1beta1.PlacementPhasePlaced {
				return ""
			}
			return ps.Cluster
		}, eventuallyTimeout, eventuallyPoll).Should(gomega.Equal(wc1Name))
	})

	ginkgo.It("S3 gated-deadline: member2 blocked, member1 wins; gated loser writes status without apiserver error", func() {
		isvcName := uniqueName("svc")
		queue := uniqueName("rk-lq")
		seedRuntimeOnMembers(queue)
		csrName := runtimeNameFor(queue)

		// Block BOTH members initially so neither wins and both deriveds persist —
		// this lets us deterministically observe member2's gated instance status
		// (the null-deadline path) BEFORE any winner-sweep removes it. We then release
		// member1 to resolve the race.
		ginkgo.By("granting a zero quota on BOTH members (no winner yet)")
		applyQuota(member1Cluster, isvcNS, queue, "0")
		applyQuota(member2Cluster, isvcNS, queue, "0")

		mustCreate(controlCluster, runnableSourceISVC(isvcNS, isvcName, csrName, requirement(), queue, false))
		ginkgo.DeferCleanup(func() {
			deleteIfExists(controlCluster, runnableSourceISVC(isvcNS, isvcName, csrName, requirement(), queue, false))
		})

		ginkgo.By("asserting the derived fans out to BOTH members (both stay gated)")
		expectDerivedOn(member1Cluster, isvcNS, isvcName, true)
		expectDerivedOn(member2Cluster, isvcNS, isvcName, true)

		// The null-deadline path: member2's instance is long-gated, so its Operation.Deadline
		// is null/parked. The IR controller must still be able to write the gated
		// instance's status — if the apiserver rejected the null-deadline status the
		// write would fail and no InstanceStatus would ever appear.
		ginkgo.By("asserting member2's gated instance status is written (no apiserver rejection of the null-deadline status)")
		gomega.Eventually(func(g gomega.Gomega) {
			// The authoritative per-instance status now lives on the component's
			// InferenceReplica (<isvcName>-engine), not the ISVC's LifecycleStatus.
			ir := &v1beta1.InferenceReplica{}
			g.Expect(member2Cluster.client.Get(member2Cluster.ctx,
				types.NamespacedName{Namespace: isvcNS, Name: isvcName + "-" + string(v1beta1.EngineComponent)}, ir)).
				To(gomega.Succeed(), "engine InferenceReplica status must be written for the gated instance")
			g.Expect(ir.Status.InstanceStatuses).NotTo(gomega.BeEmpty(),
				"a gated instance must still record an InstanceStatus (the null-deadline write path)")
			for _, is := range ir.Status.InstanceStatuses {
				g.Expect(is.Admitted).To(gomega.BeFalse(), "member2's instance must stay un-admitted under zero quota")
			}
		}, eventuallyTimeout, eventuallyPoll).Should(gomega.Succeed())

		ginkgo.By("releasing member1's quota so it admits and wins the race")
		applyQuota(member1Cluster, isvcNS, queue, "1")

		ginkgo.By("asserting the race resolves to member1 and member2's derived does not strand (swept as loser)")
		gomega.Eventually(func() string {
			ps := placementStatusOf(isvcNS, isvcName)
			if ps == nil || ps.Phase != v1beta1.PlacementPhasePlaced {
				return ""
			}
			return ps.Cluster
		}, eventuallyTimeout, eventuallyPoll).Should(gomega.Equal(wc1Name))
		ginkgo.By("asserting member2's gated loser derived is swept by the placer (deleted or Terminating), not stranded")
		expectDerivedSweptOn(member2Cluster, isvcNS, isvcName)
	})

	ginkgo.It("S4 no-quota-anywhere: both members blocked → no winner, nothing swept", func() {
		isvcName := uniqueName("svc")
		queue := uniqueName("rk-lq")
		seedRuntimeOnMembers(queue)
		csrName := runtimeNameFor(queue)

		ginkgo.By("granting a zero quota on BOTH members")
		applyQuota(member1Cluster, isvcNS, queue, "0")
		applyQuota(member2Cluster, isvcNS, queue, "0")

		mustCreate(controlCluster, runnableSourceISVC(isvcNS, isvcName, csrName, requirement(), queue, false))
		ginkgo.DeferCleanup(func() {
			deleteIfExists(controlCluster, runnableSourceISVC(isvcNS, isvcName, csrName, requirement(), queue, false))
		})

		ginkgo.By("asserting the derived fans out to BOTH members")
		expectDerivedOn(member1Cluster, isvcNS, isvcName, true)
		expectDerivedOn(member2Cluster, isvcNS, isvcName, true)

		ginkgo.By("asserting NO winner is ever declared and neither derived is swept")
		gomega.Consistently(func(g gomega.Gomega) {
			ps := placementStatusOf(isvcNS, isvcName)
			if ps != nil {
				g.Expect(ps.Phase).NotTo(gomega.Equal(v1beta1.PlacementPhasePlaced),
					"no member admits, so no winner")
				g.Expect(ps.Cluster).To(gomega.BeEmpty())
			}
			_, ok1 := getDerivedOn(member1Cluster, isvcNS, isvcName)
			_, ok2 := getDerivedOn(member2Cluster, isvcNS, isvcName)
			g.Expect(ok1).To(gomega.BeTrue(), "member1 derived must not be swept")
			g.Expect(ok2).To(gomega.BeTrue(), "member2 derived must not be swept")
		}, "6s", eventuallyPoll).Should(gomega.Succeed())
	})

	ginkgo.It("S5 re-placement: the winning cluster is removed → the race re-resolves to the surviving member", func() {
		isvcName := uniqueName("svc")
		queue := uniqueName("rk-lq")
		seedRuntimeOnMembers(queue)
		csrName := runtimeNameFor(queue)

		// Both members can admit. member1 wins the first race; we then remove
		// member1 from the candidate set (delete its WorkloadCluster) and assert the
		// placer re-races onto the only surviving candidate, member2, which admits
		// and becomes the new winner. Removing the winning cluster — rather than
		// flipping its quota — is the deterministic re-placement trigger here:
		// lowering a ClusterQueue's quota does NOT evict an already-admitted
		// workload, so a quota-flip alone would let member1's re-fanned-out derived
		// re-admit and win again (a real Kueue no-preemption behavior, surfaced
		// while building this spec).
		ginkgo.By("granting an admissible quota on BOTH members")
		applyQuota(member1Cluster, isvcNS, queue, "1")
		applyQuota(member2Cluster, isvcNS, queue, "1")

		mustCreate(controlCluster, runnableSourceISVC(isvcNS, isvcName, csrName, requirement(), queue, false))
		ginkgo.DeferCleanup(func() {
			deleteIfExists(controlCluster, runnableSourceISVC(isvcNS, isvcName, csrName, requirement(), queue, false))
		})

		ginkgo.By("waiting for member1 to win the first race")
		gomega.Eventually(func() string {
			ps := placementStatusOf(isvcNS, isvcName)
			if ps == nil || ps.Phase != v1beta1.PlacementPhasePlaced {
				return ""
			}
			return ps.Cluster
		}, eventuallyTimeout, eventuallyPoll).Should(gomega.Equal(wc1Name))

		ginkgo.By("removing member1 from the candidate set (deleting its WorkloadCluster)")
		gomega.Expect(controlCluster.client.Delete(controlCluster.ctx,
			WorkloadClusterFor(wc1Name, secretNS, secret1, nil))).To(gomega.Succeed())

		ginkgo.By("asserting the placer re-races and member2 becomes the new winner")
		gomega.Eventually(func() string {
			ps := placementStatusOf(isvcNS, isvcName)
			if ps == nil || ps.Phase != v1beta1.PlacementPhasePlaced {
				return ""
			}
			return ps.Cluster
		}, eventuallyTimeout, eventuallyPoll).Should(gomega.Equal(wc2Name))
	})

	ginkgo.It("S6 GC: an orphan derived with a live keeper source is reaped by the control plane", func() {
		// A live keeper source keeps the GC sweep's live-UID set non-empty so the
		// safety guard (skip-on-empty) does not fire.
		keeperName := uniqueName("rk-keeper")
		mustCreate(controlCluster, keeperISVC(isvcNS, keeperName))
		ginkgo.DeferCleanup(func() { deleteIfExists(controlCluster, keeperISVC(isvcNS, keeperName)) })

		orphanName := uniqueName("orphan")
		ginkgo.By("planting an orphan derived (this control plane, no live source) on member1")
		mustCreate(member1Cluster, orphanDerived(isvcNS, orphanName, controlPlaneID, "33333333-3333-3333-3333-333333333333"))

		ginkgo.By("asserting the GC reaps the orphan whose source UID is not live")
		gomega.Eventually(func() error {
			return member1Cluster.client.Get(member1Cluster.ctx,
				types.NamespacedName{Namespace: isvcNS, Name: orphanName}, &v1beta1.InferenceService{})
		}, eventuallyTimeout, eventuallyPoll).Should(ometesting.BeNotFoundError())
	})

	ginkgo.It("S7 origin-guard: a derived owned by ANOTHER control plane survives the GC sweep", func() {
		// Mirrors gc_test.go's origin-scoping assertion, but in the real-Kueue
		// suite: the GC sweep reaps only deriveds stamped for THIS control plane.
		// A live keeper source keeps the sweep's live-UID set non-empty so the
		// skip-on-empty safety guard does not fire. We plant TWO objects on
		// member1: an orphan owned by THIS control plane (must be reaped) and a
		// foreign derived stamped for a DIFFERENT control plane (must be left
		// intact across the sweep). The loser-sweep path is not the right vehicle
		// here — placeOn re-applies the derived spec onto every fanned-out
		// candidate by name, so a same-named object on a candidate is owned by the
		// placer; the origin guard that protects a *foreign* object is the GC's
		// control-plane scoping, which never runs placeOn.
		keeperName := uniqueName("rk-keeper")
		mustCreate(controlCluster, keeperISVC(isvcNS, keeperName))
		ginkgo.DeferCleanup(func() { deleteIfExists(controlCluster, keeperISVC(isvcNS, keeperName)) })

		ownOrphan := uniqueName("orphan")
		foreignName := uniqueName("foreign")

		ginkgo.By("planting this control plane's orphan + a foreign control plane's derived on member1")
		mustCreate(member1Cluster, orphanDerived(isvcNS, ownOrphan, controlPlaneID, "44444444-4444-4444-4444-444444444444"))
		mustCreate(member1Cluster, orphanDerived(isvcNS, foreignName, "other-control-plane", "55555555-5555-5555-5555-555555555555"))
		ginkgo.DeferCleanup(func() {
			deleteIfExists(member1Cluster, orphanDerived(isvcNS, foreignName, "other-control-plane", ""))
		})

		ginkgo.By("asserting the GC reaps THIS control plane's orphan")
		gomega.Eventually(func() error {
			return member1Cluster.client.Get(member1Cluster.ctx,
				types.NamespacedName{Namespace: isvcNS, Name: ownOrphan}, &v1beta1.InferenceService{})
		}, eventuallyTimeout, eventuallyPoll).Should(ometesting.BeNotFoundError())

		ginkgo.By("asserting the foreign control plane's derived is left intact across a sweep")
		gomega.Consistently(func(g gomega.Gomega) {
			d, ok := getDerivedOn(member1Cluster, isvcNS, foreignName)
			g.Expect(ok).To(gomega.BeTrue(), "the GC must not reap a derived stamped for another control plane")
			g.Expect(d.Labels).To(gomega.HaveKeyWithValue(placement.PlacementControlPlaneLabel, "other-control-plane"))
		}, gcInterval+eventuallyPoll, eventuallyPoll).Should(gomega.Succeed())
	})
})

// --- file-scope helpers for the real-Kueue scenarios ---

// runtimeNameFor derives a stable, unique ClusterServingRuntime name from a
// per-spec unique queue name, so each spec's runtime is isolated from the others
// (the runtime is cluster-scoped and watched on every member).
func runtimeNameFor(queue string) string { return "rt-" + queue }

// seedRuntimeOnMembers creates the lean ClusterServingRuntime on BOTH members so
// each member's ISVC reconciler can resolve the derived's named runtime. Mirrors
// realkueue_test.go's leanRuntime: a no-model runtime carrying an engine runner.
func seedRuntimeOnMembers(queue string) {
	name := runtimeNameFor(queue)
	for _, m := range []cluster{member1Cluster, member2Cluster} {
		mc := m
		csr := ometesting.MakeClusterServingRuntime(name).
			SupportsModelFormat("safetensors", true, 10).
			WithLastFormatArchitecture("LlamaForCausalLM").
			EngineRunner(corev1.Container{Name: "ome-container", Image: "example/model-server:test"}).
			Obj()
		err := mc.client.Create(mc.ctx, csr)
		if err == nil {
			ginkgo.DeferCleanup(func() { _ = mc.client.Delete(mc.ctx, csr) })
		}
		// AlreadyExists across Ordered specs is fine; a real error fails the spec.
		gomega.Expect(client.IgnoreAlreadyExists(err)).To(gomega.Succeed(), "seed runtime on member")
	}
}

// applyQuota applies a Kueue quota on a member, failing the spec on error.
func applyQuota(c cluster, ns, queue, cpu string) {
	gomega.ExpectWithOffset(1, framework.ApplyKueueQuota(c.ctx, c.client, ns, queue, cpu)).
		To(gomega.Succeed())
}

// expectDerivedOn waits until the derived ISVC is present (or absent) on a member.
func expectDerivedOn(member cluster, ns, name string, present bool) {
	gomega.EventuallyWithOffset(1, func() bool {
		_, ok := getDerivedOn(member, ns, name)
		return ok
	}, eventuallyTimeout, eventuallyPoll).Should(gomega.Equal(present))
}

// expectDerivedSweptOn waits until the placer has SWEPT the loser derived on a
// member — i.e. it is gone OR marked for deletion (Terminating). The placer's
// contract is to ISSUE the origin-guarded delete; full removal is the member's
// eventually-consistent finalizer teardown (its OME reconciler tears down the
// derived's IR + Kueue-gated pods, the latter carrying Kueue's own pod
// finalizer), which is NOT the placer's responsibility and can lag under load.
// Asserting "deleted or Terminating" tests the placer's actual contract without
// coupling the assertion to that teardown latency. A loser the placer never
// deleted would be present with no DeletionTimestamp, so this still fails on a
// genuine missed sweep.
func expectDerivedSweptOn(member cluster, ns, name string) {
	gomega.EventuallyWithOffset(1, func() bool {
		d, ok := getDerivedOn(member, ns, name)
		return !ok || d.DeletionTimestamp != nil
	}, eventuallyTimeout, eventuallyPoll).Should(gomega.BeTrue(),
		"the placer must sweep the loser derived (delete it); full reap is the member's finalizer teardown")
}

// runnableSourceISVC builds a control-plane SOURCE ISVC that the placer derives
// into a runnable member workload: it carries the accelerator-requirement
// selector (so it fans out to matching members) and the LocalQueue annotation
// (so derive.go stamps the queue label on each component), plus a runnable
// OMENative engine (and optional decoder) shape with a CPU request — so each
// member's IR controller produces queue-labeled pods that Kueue gates/ungates.
//
// derive.go stamps the queue-name LABEL on each component itself; we only set the
// per-component OMENative deploymentMode annotation + the CPU request here.
func runnableSourceISVC(ns, name, runtime, accelReq, queue string, withDecoder bool) *v1beta1.InferenceService {
	component := func() v1beta1.ComponentExtensionSpec {
		return v1beta1.ComponentExtensionSpec{
			Annotations: map[string]string{constants.DeploymentMode: string(constants.OMENative)},
		}
	}
	podSpec := func() v1beta1.PodSpec {
		return v1beta1.PodSpec{
			Containers: []corev1.Container{{
				Name:  "ome-container",
				Image: "example/model-server:test",
				Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("500m")},
				},
			}},
		}
	}

	w := ometesting.MakeInferenceService(name, ns).
		Runtime(runtime).
		DeploymentMode(constants.OMENative).
		Annotation(placement.AcceleratorRequirementsAnnotation, accelReq).
		Annotation(placement.LocalQueueAnnotation, queue).
		Engine(&v1beta1.EngineSpec{
			ComponentExtensionSpec: component(),
			PodSpec:                podSpec(),
		})
	if withDecoder {
		w = w.Decoder(&v1beta1.DecoderSpec{
			ComponentExtensionSpec: component(),
			PodSpec:                podSpec(),
		})
	}
	return w.Obj()
}
