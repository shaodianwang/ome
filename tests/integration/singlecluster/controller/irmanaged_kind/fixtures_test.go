package irmanaged_kind

import (
	"fmt"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"knative.dev/pkg/apis"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/irprojector"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/podreadiness"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	v1beta1testing "github.com/ome-projects/ome/pkg/utils/testing/v1beta1"
	"github.com/ome-projects/ome/tests/integration/kindfix"
	"github.com/ome-projects/ome/tests/integration/kindutil"
)

// irFixture bundles the cluster-scoped CBM + CSR and the namespaced
// ISVC for one test. Self-contained (this package can't import
// omenative_coordination_kind's _test.go helpers).
type irFixture struct {
	CBM  *v1beta1.ClusterBaseModel
	CSR  *v1beta1.ClusterServingRuntime
	ISVC *v1beta1.InferenceService
}

// irFixtureOpts is the minimal knob set the IR-managed repro needs.
type irFixtureOpts struct {
	isvcName       string
	engineImage    string
	engineReplicas int
	engineAnnots   map[string]string
	// updateStrategy, when non-empty, is stamped on engine.Lifecycle so a
	// spec can opt into InPlaceOnly / RecreatePod instead of the default
	// SurgeThenDrain.
	updateStrategy v1beta1.UpdateStrategyType
}

type irFixtureOption func(*irFixtureOpts)

func withISVCName(n string) irFixtureOption { return func(o *irFixtureOpts) { o.isvcName = n } }
func withEngineImage(i string) irFixtureOption {
	return func(o *irFixtureOpts) { o.engineImage = i }
}
func withEngineReplicas(n int) irFixtureOption {
	return func(o *irFixtureOpts) { o.engineReplicas = n }
}
func withUpdateStrategy(t v1beta1.UpdateStrategyType) irFixtureOption {
	return func(o *irFixtureOpts) { o.updateStrategy = t }
}

// newIRFixture builds a minimum-viable single-Component (engine-only)
// OMENative ISVC plus its CBM/CSR. Default strategy is left UNSET so the
// workload planner resolves it to SurgeThenDrain (the affected path).
func newIRFixture(namespace string, opts ...irFixtureOption) *irFixture {
	o := &irFixtureOpts{
		isvcName:       "ir-isvc",
		engineImage:    "fake-serving:v1",
		engineReplicas: 1,
	}
	for _, opt := range opts {
		opt(o)
	}
	cbmName := o.isvcName + "-cbm"
	csrName := o.isvcName + "-csr"

	cbm := kindfix.StandardClusterBaseModel(cbmName, kindfix.StandardModelStorageURI)
	csr := kindfix.StandardClusterServingRuntime(csrName)

	engineAnnots := map[string]string{
		constants.DeploymentMode: string(constants.OMENative),
	}
	for k, v := range o.engineAnnots {
		engineAnnots[k] = v
	}
	engine := &v1beta1.EngineSpec{
		ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
			Annotations: engineAnnots,
		},
		PodSpec: v1beta1.PodSpec{Containers: []corev1.Container{{
			Name:  "ome-container",
			Image: o.engineImage,
		}}},
	}
	if o.engineReplicas != 1 {
		mr := o.engineReplicas
		engine.ComponentExtensionSpec.MinReplicas = &mr
	}
	if o.updateStrategy != "" {
		st := o.updateStrategy
		engine.Lifecycle = &v1beta1.LifecycleSpec{
			UpdateStrategy: &v1beta1.UpdateStrategy{Type: st},
		}
	}

	isvc := v1beta1testing.MakeInferenceService(o.isvcName, namespace).
		Model(cbmName).
		Runtime(csrName).
		DeploymentMode(constants.OMENative).
		Engine(engine).
		Obj()

	return &irFixture{CBM: cbm, CSR: csr, ISVC: isvc}
}

// applyIRFixture creates CBM + CSR, labels nodes model-Ready, waits for the
// in-process manager's cache to observe them, then creates the ISVC. The cache
// wait passes the manager's cached client so the ISVC is not created before the
// reconciler can see its model + runtime.
func applyIRFixture(g gomega.Gomega, f *irFixture) {
	kindfix.ApplyModelRuntime(suiteCtx, g, k8sClient, f.CBM, f.CSR, kindfix.ApplyModelRuntimeOpts{
		CachedReader: kindutil.Suite().Manager.GetClient(),
	})
	g.ExpectWithOffset(1, k8sClient.Create(suiteCtx, f.ISVC)).To(gomega.Succeed(), "create ISVC")
}

// deferIRFixtureCleanup tears the fixture down ISVC-first (waiting for full
// deletion) then CBM/CSR, avoiding the cross-test "ClusterBaseModel not found"
// backoff storm.
func deferIRFixtureCleanup(f *irFixture) {
	ginkgo.DeferCleanup(func() {
		if f == nil {
			return
		}
		kindfix.CleanupModelRuntime(suiteCtx, k8sClient, f.CBM, f.CSR, f.ISVC)
	})
}

// freshISVC re-reads the ISVC from the (direct) apiserver client.
func freshISVC(g gomega.Gomega, namespace, name string) *v1beta1.InferenceService {
	out := &v1beta1.InferenceService{}
	g.ExpectWithOffset(1, k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: namespace, Name: name}, out)).To(gomega.Succeed())
	return out
}

// engineIR returns the InferenceReplica CR the projector creates for the
// engine Component (name = <isvc>-engine). ok=false when absent.
func engineIR(namespace, isvcName string) (*v1beta1.InferenceReplica, bool) {
	ir := &v1beta1.InferenceReplica{}
	key := client.ObjectKey{Namespace: namespace, Name: irprojector.InferenceReplicaName(isvcName, v1beta1.EngineComponent)}
	if err := k8sClient.Get(suiteCtx, key, ir); err != nil {
		return nil, false
	}
	return ir, true
}

// engineOMENative returns the ISVC's engine OMENative lifecycle status,
// or nil when not yet populated.
func engineOMENative(isvc *v1beta1.InferenceService) *v1beta1.LifecycleStatus {
	if isvc.Status.Components == nil {
		return nil
	}
	cs, ok := isvc.Status.Components[v1beta1.EngineComponent]
	if !ok {
		return nil
	}
	return cs.Lifecycle
}

// =========================================================================
// Diagnostic snapshot — the fork-resolving payload (capture EVERYTHING)
// =========================================================================

// dumpDiag prints a full timestamped snapshot to GinkgoWriter: pods (with
// revision-hash + serving gate), the IR CR status + instanceStatuses, the
// ISVC OMENative rollup, and the routed Services + EndpointSlices. Reading
// one transcript answers: does Phase reach Ready? does readyReplicas track
// Phase? is the old pod's routed slice still Ready=true (drain hang)? is it
// a transient (recovers) or a wedge (flat 0)?
func dumpDiag(tick int, ns, isvcName string) {
	w := ginkgo.GinkgoWriter
	fmt.Fprintf(w, "\n========== DIAG tick=%d ns=%s isvc=%s ==========\n", tick, ns, isvcName)

	// (a) PODS
	pods := &corev1.PodList{}
	if err := k8sClient.List(suiteCtx, pods, client.InNamespace(ns)); err != nil {
		fmt.Fprintf(w, "  pods: LIST ERR %v\n", err)
	} else {
		fmt.Fprintf(w, "  PODS (%d):\n", len(pods.Items))
		for i := range pods.Items {
			p := &pods.Items[i]
			ord := "-"
			if o, ok := query.PodOrdinalFromLabels(p); ok {
				ord = fmt.Sprintf("%d", o)
			}
			rev := p.Labels[query.LabelRevisionHash]
			inc := p.Labels[query.LabelInstanceIncarnation]
			containersReady := condStatus(p, corev1.ContainersReady)
			ready := condStatus(p, corev1.PodReady)
			serving := condStatus(p, podreadiness.ConditionType)
			del := ""
			if p.DeletionTimestamp != nil {
				del = " DELETING"
			}
			fmt.Fprintf(w, "    - %s ord=%s rev=%s inc=%s phase=%s cReady=%s ready=%s serving=%s isServing=%t%s\n",
				p.Name, ord, rev, inc, p.Status.Phase, containersReady, ready, serving, podreadiness.IsServing(p), del)
			for _, cs := range p.Status.ContainerStatuses {
				fmt.Fprintf(w, "        ctr %s ready=%t image=%s\n", cs.Name, cs.Ready, cs.Image)
			}
		}
	}

	// (b)+(c) IR CR status + instanceStatuses
	if ir, ok := engineIR(ns, isvcName); ok {
		s := ir.Status
		fmt.Fprintf(w, "  IR %s: gen=%d obsGen=%d replicas=%d ready=%d updated=%d updatedReady=%d serving=%d avail=%d curRev=%s updRev=%s\n",
			ir.Name, ir.Generation, s.ObservedGeneration, s.Replicas, s.ReadyReplicas, s.UpdatedReplicas,
			s.UpdatedReadyReplicas, s.ServingReplicas, s.AvailableReplicas, s.CurrentRevision, s.UpdateRevision)
		fmt.Fprintf(w, "    IR Ready cond: %s\n", metaReadyCond(s.Conditions))
		for _, is := range s.InstanceStatuses {
			op := "nil"
			if is.Operation != nil {
				op = fmt.Sprintf("{type=%s step=%s target=%s}", is.Operation.Type, is.Operation.Step, is.Operation.TargetRevision)
			}
			fmt.Fprintf(w, "    inst[%d] phase=%s runningRev=%s activeOrd=%d podCount=%d inc=%d op=%s\n",
				is.Index, is.Phase, is.RunningRevision, is.ActiveOrdinal, is.PodCount, is.Incarnation, op)
		}
	} else {
		fmt.Fprintf(w, "  IR %s-engine: NOT FOUND\n", isvcName)
	}

	// (d) ISVC OMENative rollup + top-level Ready
	isvc := &v1beta1.InferenceService{}
	if err := k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: ns, Name: isvcName}, isvc); err != nil {
		fmt.Fprintf(w, "  ISVC: GET ERR %v\n", err)
	} else {
		if on := engineOMENative(isvc); on != nil {
			fmt.Fprintf(w, "  ISVC engine omenative: ready=%d updatedReady=%d replicas=%d curRev=%s updRev=%s\n",
				on.ReadyReplicas, on.UpdatedReadyReplicas, on.Replicas, on.CurrentRevision, on.UpdateRevision)
		} else {
			fmt.Fprintf(w, "  ISVC engine omenative: nil\n")
		}
		fmt.Fprintf(w, "  ISVC top Ready cond: %s\n", knativeReadyCond(isvc))
	}

	// (e) routed Services + EndpointSlices (drain-gate ground truth)
	svcs := &corev1.ServiceList{}
	if err := k8sClient.List(suiteCtx, svcs, client.InNamespace(ns)); err == nil {
		for i := range svcs.Items {
			s := &svcs.Items[i]
			if _, routed := s.Spec.Selector[query.LabelRevisionHash]; routed {
				fmt.Fprintf(w, "  routedSvc %s selector=%v\n", s.Name, s.Spec.Selector)
			}
		}
	}
	slices := &discoveryv1.EndpointSliceList{}
	if err := k8sClient.List(suiteCtx, slices, client.InNamespace(ns)); err == nil {
		for i := range slices.Items {
			sl := &slices.Items[i]
			svcName := sl.Labels["kubernetes.io/service-name"]
			for _, ep := range sl.Endpoints {
				refName := ""
				if ep.TargetRef != nil {
					refName = ep.TargetRef.Name
				}
				epReady := "?"
				if ep.Conditions.Ready != nil {
					epReady = fmt.Sprintf("%t", *ep.Conditions.Ready)
				}
				fmt.Fprintf(w, "  epslice svc=%s target=%s ready=%s\n", svcName, refName, epReady)
			}
		}
	}
	fmt.Fprintf(w, "========== /DIAG tick=%d ==========\n", tick)
}

func condStatus(p *corev1.Pod, t corev1.PodConditionType) string {
	for _, c := range p.Status.Conditions {
		if c.Type == t {
			return string(c.Status)
		}
	}
	return "-"
}

// metaReadyCond renders the "Ready" entry of a []metav1.Condition slice
// (the IR controller's condition shape).
func metaReadyCond(conds []metav1.Condition) string {
	for _, c := range conds {
		if c.Type == "Ready" {
			return fmt.Sprintf("status=%s reason=%s msg=%q", c.Status, c.Reason, c.Message)
		}
	}
	return "(no Ready)"
}

// knativeReadyCond renders the ISVC's top-level knative Ready condition.
func knativeReadyCond(isvc *v1beta1.InferenceService) string {
	c := isvc.Status.GetCondition(apis.ConditionReady)
	if c == nil {
		return "(no Ready)"
	}
	return fmt.Sprintf("status=%s reason=%s msg=%q", c.Status, c.Reason, c.Message)
}
