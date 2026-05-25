package coordination

import (
	"fmt"
	"strings"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/coordination"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/query"
	v1beta1testing "github.com/ome-projects/ome/pkg/utils/testing/v1beta1"
	"github.com/ome-projects/ome/tests/integration/util"
)

// Per-revision Service specs for the OEP-0007.1 coordination layer.
// The producer side of the API: `coordination.Reconcile` lists pods
// labeled `ome.io/managed-by=OMENative`, groups by `ome.io/revision-hash`,
// and ensures a routing + headless Service pair per (component, hash):
//
//   <isvc>-<component>-rev-<hash>             ClusterIP   (routing)
//   <isvc>-<component>-rev-<hash>-headless    ClusterIPNone
//
// The selector keys are inferenceservice + component + revision-hash +
// managed-by=OMENative — narrow enough to pick exactly one revision's
// pods without leaking to siblings.
//
// Several specs here cross the simulator boundary: pods need to reach
// ContainersReady (kubeletSim handles that) before AggregateAndWriteStatus
// stamps RunningRevision; only then does coordination's
// observePerRevisionPods see a hash label to bucket on. Each Eventually
// gives the kubelet sim + the coordination tail enough time to converge.

// readyPerRevisionService re-fetches a per-revision Service by name and
// fails the assertion if absent. Returns the live object so subsequent
// assertions can read it.
func readyPerRevisionService(g Gomega, ns, name string) *corev1.Service {
	svc := &corev1.Service{}
	g.ExpectWithOffset(1, k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: ns, Name: name}, svc)).
		To(Succeed(), fmt.Sprintf("per-revision Service %s/%s should exist", ns, name))
	return svc
}

// observedRevisionHashes lists the distinct `ome.io/revision-hash`
// label values across the OMENative-owned pods in the namespace. Used
// by specs that need to know which hashes coordination should have
// emitted Services for.
func observedRevisionHashes(g Gomega, ns string) []string {
	pods := &corev1.PodList{}
	g.ExpectWithOffset(1, k8sClient.List(suiteCtx, pods,
		client.InNamespace(ns),
		client.MatchingLabels{util.LabelManagedByKey: util.ManagedByOMENativeValue},
	)).To(Succeed())
	seen := map[string]struct{}{}
	for _, p := range pods.Items {
		if h := p.Labels[query.LabelRevisionHash]; h != "" {
			seen[h] = struct{}{}
		}
	}
	out := make([]string, 0, len(seen))
	for h := range seen {
		out = append(out, h)
	}
	return out
}

var _ = ginkgo.Describe("Coordination per-revision Services (OEP-0007.1)", ginkgo.Ordered, func() {
	var ns *corev1.Namespace

	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "coord-svc-")
		Expect(err).NotTo(HaveOccurred())
	})

	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	// SVC-1: the most fundamental producer assertion — coordination
	// emits the routing per-revision Service for the steady-state
	// revision once the first pod's hash is observed.
	ginkgo.PIt("G1: SVC-1 emits <isvc>-<c>-rev-<hash> routing Service for the steady revision", func() {
		cbm := v1beta1testing.MakeClusterBaseModel("svc1-cbm").
			StorageURI("oci://n/test/b/models/o/llama-7b").
			ModelFormat("safetensors").ModelType("llama").
			ModelArchitecture("LlamaForCausalLM").ModelFramework("Transformers").Obj()
		Expect(k8sClient.Create(suiteCtx, cbm)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, cbm) })
		csr := v1beta1testing.MakeClusterServingRuntime("svc1-csr").
			SupportsModelFormat("safetensors", true, 10).
			WithLastFormatArchitecture("LlamaForCausalLM").Obj()
		Expect(k8sClient.Create(suiteCtx, csr)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, csr) })
		isvc := v1beta1testing.MakeInferenceService("svc1", ns.Name).
			Model("svc1-cbm").Runtime("svc1-csr").
			DeploymentMode(constants.OMENative).
			Engine(&v1beta1.EngineSpec{
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
					Annotations: map[string]string{constants.DeploymentMode: string(constants.OMENative)},
				},
				PodSpec: v1beta1.PodSpec{Containers: []corev1.Container{{Name: "ome-container", Image: "ghcr.io/test/runtime:test"}}},
			}).Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, isvc) })

		Eventually(func(g Gomega) {
			hashes := observedRevisionHashes(g, ns.Name)
			g.Expect(hashes).To(HaveLen(1), "expected exactly one observed revision hash")
			name := coordination.PerRevisionServiceName("svc1", v1beta1.EngineComponent, hashes[0])
			readyPerRevisionService(g, ns.Name, name)
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	// SVC-2: the headless counterpart is emitted alongside the routing
	// Service, with ClusterIPNone for peer-DNS resolution.
	ginkgo.PIt("G1: SVC-2 emits <isvc>-<c>-rev-<hash>-headless ClusterIPNone Service", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("svc2"), WithoutDecoder())
		applyFixture(Default, fix)
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.ISVC) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CBM) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CSR) })

		Eventually(func(g Gomega) {
			hashes := observedRevisionHashes(g, ns.Name)
			g.Expect(hashes).To(HaveLen(1))
			name := coordination.PerRevisionHeadlessServiceName("svc2", v1beta1.EngineComponent, hashes[0])
			svc := readyPerRevisionService(g, ns.Name, name)
			g.Expect(svc.Spec.ClusterIP).To(Equal(corev1.ClusterIPNone),
				"headless variant must have ClusterIP=None")
			g.Expect(svc.Spec.PublishNotReadyAddresses).To(BeTrue(),
				"headless must publish not-ready addresses for peer-DNS gang init")
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	// SVC-3: the selector narrows to one revision via the four required
	// label keys. Without LabelRevisionHash the selector would match
	// every revision's pods; without LabelManagedBy it could pick up
	// non-OMENative pods sharing the Component label.
	ginkgo.PIt("G1: SVC-3 routing Service selector has the four required keys", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("svc3"), WithoutDecoder())
		applyFixture(Default, fix)
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.ISVC) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CBM) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CSR) })

		Eventually(func(g Gomega) {
			hashes := observedRevisionHashes(g, ns.Name)
			g.Expect(hashes).To(HaveLen(1))
			name := coordination.PerRevisionServiceName("svc3", v1beta1.EngineComponent, hashes[0])
			svc := readyPerRevisionService(g, ns.Name, name)
			g.Expect(svc.Spec.Selector).To(HaveKeyWithValue(constants.InferenceServicePodLabelKey, "svc3"))
			g.Expect(svc.Spec.Selector).To(HaveKeyWithValue(constants.OMEComponentLabel, string(v1beta1.EngineComponent)))
			g.Expect(svc.Spec.Selector).To(HaveKeyWithValue(query.LabelRevisionHash, hashes[0]))
			g.Expect(svc.Spec.Selector).To(HaveKeyWithValue(query.LabelManagedBy, query.ManagedByOMENative))
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	// SVC-4: per-revision Services own back through the ISVC, so
	// deleting the ISVC cascades them away via K8s garbage collection.
	ginkgo.PIt("G1: SVC-4 per-revision Services carry an ISVC OwnerReference and cascade-delete", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("svc4"), WithoutDecoder())
		applyFixture(Default, fix)
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CBM) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CSR) })

		// Wait for the per-revision Service to exist + verify OwnerRef.
		var svcName string
		var isvcUID types.UID
		Eventually(func(g Gomega) {
			hashes := observedRevisionHashes(g, ns.Name)
			g.Expect(hashes).To(HaveLen(1))
			svcName = coordination.PerRevisionServiceName("svc4", v1beta1.EngineComponent, hashes[0])
			svc := readyPerRevisionService(g, ns.Name, svcName)
			g.Expect(svc.OwnerReferences).To(HaveLen(1))
			g.Expect(svc.OwnerReferences[0].Kind).To(Equal("InferenceService"))
			g.Expect(svc.OwnerReferences[0].Name).To(Equal("svc4"))
			g.Expect(svc.OwnerReferences[0].Controller).NotTo(BeNil())
			g.Expect(*svc.OwnerReferences[0].Controller).To(BeTrue())

			// Capture the live ISVC UID so the cascade-delete assertion
			// can verify the OwnerRef points at the real object (not a
			// stale UID from a recreate).
			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: ns.Name, Name: "svc4"}, fresh)).To(Succeed())
			isvcUID = fresh.UID
			g.Expect(svc.OwnerReferences[0].UID).To(Equal(isvcUID))
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		// Cascade-delete: nuke ISVC, expect Service to be GC'd by
		// kube-controller-manager (envtest's gc controller runs in-suite).
		Expect(k8sClient.Delete(suiteCtx, fix.ISVC)).To(Succeed())
		Eventually(func(g Gomega) {
			svc := &corev1.Service{}
			err := k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: ns.Name, Name: svcName}, svc)
			g.Expect(apierrors.IsNotFound(err)).To(BeTrue(),
				"per-revision Service %s should cascade-delete after ISVC delete; err=%v svc=%+v",
				svcName, err, svc)
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	// SVC-7: per-reconcile idempotency. Two reconciles in a row must
	// not bump the per-revision Service's ResourceVersion — coordination
	// patches only when the desired shape differs.
	ginkgo.PIt("G1: SVC-7 idempotent: second reconcile doesn't bump ResourceVersion", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("svc7"), WithoutDecoder())
		applyFixture(Default, fix)
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.ISVC) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CBM) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CSR) })

		var svcName string
		var rvAfterFirst string
		Eventually(func(g Gomega) {
			hashes := observedRevisionHashes(g, ns.Name)
			g.Expect(hashes).To(HaveLen(1))
			svcName = coordination.PerRevisionServiceName("svc7", v1beta1.EngineComponent, hashes[0])
			svc := readyPerRevisionService(g, ns.Name, svcName)
			rvAfterFirst = svc.ResourceVersion
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		// Force a re-reconcile by re-fetching ISVC and updating an
		// inconsequential annotation. Coordination should run again
		// and observe the desired shape already matches → no patch.
		fresh := &v1beta1.InferenceService{}
		Expect(k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: ns.Name, Name: "svc7"}, fresh)).To(Succeed())
		if fresh.Annotations == nil {
			fresh.Annotations = map[string]string{}
		}
		fresh.Annotations["ome.io/poke"] = "1"
		Expect(k8sClient.Update(suiteCtx, fresh)).To(Succeed())

		// Wait a few reconcile cycles to make sure coordination ran.
		// ResourceVersion of the Service should stay stable.
		Consistently(func(g Gomega) {
			svc := &corev1.Service{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: ns.Name, Name: svcName}, svc)).To(Succeed())
			g.Expect(svc.ResourceVersion).To(Equal(rvAfterFirst),
				"per-revision Service was patched on re-reconcile; first RV=%s now=%s",
				rvAfterFirst, svc.ResourceVersion)
		}).WithTimeout(util.ShortTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	// SVC-8: drift correction on the Selector. Hand-mutate the
	// per-revision Service to a bogus selector; coordination's next
	// reconcile must restore the correct one.
	ginkgo.PIt("G1: SVC-8 drift-corrects a hand-mutated selector", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("svc8"), WithoutDecoder())
		applyFixture(Default, fix)
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.ISVC) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CBM) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CSR) })

		var svcName string
		var origSelector map[string]string
		Eventually(func(g Gomega) {
			hashes := observedRevisionHashes(g, ns.Name)
			g.Expect(hashes).To(HaveLen(1))
			svcName = coordination.PerRevisionServiceName("svc8", v1beta1.EngineComponent, hashes[0])
			svc := readyPerRevisionService(g, ns.Name, svcName)
			origSelector = svc.Spec.Selector
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		// Mutate selector to a clearly-bogus key set.
		mutated := &corev1.Service{}
		Expect(k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: ns.Name, Name: svcName}, mutated)).To(Succeed())
		mutated.Spec.Selector = map[string]string{"hacked": "true"}
		Expect(k8sClient.Update(suiteCtx, mutated)).To(Succeed())

		// Poke a reconcile via a benign annotation; coordination should
		// drift-correct.
		fresh := &v1beta1.InferenceService{}
		Expect(k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: ns.Name, Name: "svc8"}, fresh)).To(Succeed())
		if fresh.Annotations == nil {
			fresh.Annotations = map[string]string{}
		}
		fresh.Annotations["ome.io/poke"] = "drift"
		Expect(k8sClient.Update(suiteCtx, fresh)).To(Succeed())

		Eventually(func(g Gomega) {
			svc := &corev1.Service{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: ns.Name, Name: svcName}, svc)).To(Succeed())
			g.Expect(svc.Spec.Selector).To(Equal(origSelector),
				"selector should be restored after drift; got %v want %v", svc.Spec.Selector, origSelector)
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	// SVC-10: ISVCs without rolloutCoordination still get per-revision
	// Services. The producer is opt-out, not opt-in — the HTTPRoute
	// weighted-backendRef consumer reads from Status.Components.<c>.Traffic[]
	// regardless of whether the operator declared coordination.
	ginkgo.PIt("G1: SVC-10 emits per-revision Services even without rolloutCoordination block", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("svc10"), WithoutDecoder())
		// No WithCoordinationGroup call → ISVC.Spec.RolloutCoordination is nil.
		Expect(fix.ISVC.Spec.RolloutCoordination).To(BeNil())
		applyFixture(Default, fix)
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.ISVC) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CBM) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CSR) })

		Eventually(func(g Gomega) {
			hashes := observedRevisionHashes(g, ns.Name)
			g.Expect(hashes).To(HaveLen(1))
			name := coordination.PerRevisionServiceName("svc10", v1beta1.EngineComponent, hashes[0])
			readyPerRevisionService(g, ns.Name, name)
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	// SVC-11: Multi-Component ISVC gets a Service set per Component.
	ginkgo.PIt("G1: SVC-11 multi-Component ISVC gets per-Component per-revision Services", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("svc11")) // engine + decoder default
		applyFixture(Default, fix)
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.ISVC) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CBM) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CSR) })

		Eventually(func(g Gomega) {
			svcs := listPerRevisionServices(g, ns.Name)
			byComponent := map[v1beta1.ComponentType]int{}
			for _, s := range svcs {
				c := v1beta1.ComponentType(s.Spec.Selector[constants.OMEComponentLabel])
				byComponent[c]++
			}
			// Each Component should have 2 Services (routing + headless).
			g.Expect(byComponent[v1beta1.EngineComponent]).To(Equal(2),
				"engine missing per-revision Service pair; got=%v all=%s",
				byComponent, describeServices(svcs))
			g.Expect(byComponent[v1beta1.DecoderComponent]).To(Equal(2),
				"decoder missing per-revision Service pair; got=%v all=%s",
				byComponent, describeServices(svcs))
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	// SVC-12 (PIt for G4): per-revision Services accumulate forever.
	// Coordination doesn't sweep stale Services even after pods are
	// gone — GCPerRevisionServices is defined but never called.
	//
	// **Gap G4** — add a GC sweep at the end of coordination.Reconcile.
	// Test forces a hash transition by re-rendering the engine PodSpec,
	// waits for the new revision's Service to appear, then asserts the
	// OLD revision's Service is gone once the old pod set is. Today
	// the old Service lingers indefinitely.
	ginkgo.PIt("SVC-12 (G4) GCs per-revision Service for a hash with no live pods", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("svc12"), WithoutDecoder(),
			WithEngineImage("ghcr.io/test/runtime:v1"))
		applyFixture(Default, fix)
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.ISVC) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CBM) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CSR) })

		var oldHash string
		Eventually(func(g Gomega) {
			hashes := observedRevisionHashes(g, ns.Name)
			g.Expect(hashes).To(HaveLen(1))
			oldHash = hashes[0]
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		// Bump image to force a new CR → new revision-hash on subsequent pods.
		fresh := &v1beta1.InferenceService{}
		Expect(k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: ns.Name, Name: "svc12"}, fresh)).To(Succeed())
		fresh.Spec.Engine.PodSpec.Containers[0].Image = "ghcr.io/test/runtime:v2"
		Expect(k8sClient.Update(suiteCtx, fresh)).To(Succeed())

		// Wait until only the new hash's pods are alive.
		Eventually(func(g Gomega) {
			hashes := observedRevisionHashes(g, ns.Name)
			g.Expect(hashes).To(HaveLen(1))
			g.Expect(hashes[0]).NotTo(Equal(oldHash))
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())

		// Old per-revision Service should be GC'd.
		oldName := coordination.PerRevisionServiceName("svc12", v1beta1.EngineComponent, oldHash)
		Eventually(func(g Gomega) {
			svc := &corev1.Service{}
			err := k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: ns.Name, Name: oldName}, svc)
			g.Expect(apierrors.IsNotFound(err)).To(BeTrue(),
				"old per-revision Service %s should be GC'd after pods migrated away", oldName)
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	// SVC-13: K8s Service name limit is 63 chars. Long ISVC names may
	// push the per-revision Service name past the limit. Pin observed
	// behavior so a fix (truncation/hashing) doesn't silently regress.
	ginkgo.PIt("G1: SVC-13 long ISVC name: behavior pinned vs K8s 63-char Service limit", func() {
		// Construct an ISVC name that would push the per-revision Service
		// name over 63 chars. Service name format:
		//   <isvc>-<component>-rev-<hash>
		// Engine = 6 chars + "-rev-" + 10-char hash + 2 hyphens = 23 chars overhead.
		// Leaving ~40 chars for isvc, choose 50 chars to exceed.
		longISVC := "very-long-isvc-name-pushing-past-fifty-characters-aaa" // 53 chars
		fix := newCoordFixture(ns.Name, WithISVCName(longISVC), WithoutDecoder())
		applyFixture(Default, fix)
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.ISVC) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CBM) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CSR) })

		Eventually(func(g Gomega) {
			hashes := observedRevisionHashes(g, ns.Name)
			g.Expect(hashes).To(HaveLen(1))
			name := coordination.PerRevisionServiceName(longISVC, v1beta1.EngineComponent, hashes[0])
			if len(name) > 63 {
				// Coordination should either: (a) truncate/hash to fit,
				// (b) reject at admission, or (c) fail with a clear error.
				// Today (b) and (c) don't fire and (a) isn't implemented,
				// so the Service create returns an apiserver-side error.
				svc := &corev1.Service{}
				err := k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: ns.Name, Name: name}, svc)
				if err == nil {
					ginkgo.Fail(fmt.Sprintf("expected length-limit handling, got Service %s (%d chars)", name, len(name)))
				}
				// Pin current behavior: Service simply doesn't exist
				// because apiserver rejected it. No truncation, no
				// rejection at admission. Operators with long ISVCs need
				// to know coordination silently fails.
				g.Expect(apierrors.IsNotFound(err)).To(BeTrue(),
					"current behavior: Service create silently fails; name=%s len=%d err=%v",
					name, len(name), err)
				return
			}
			readyPerRevisionService(g, ns.Name, name)
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	// SVC-14: pods with revision-hash="" (a label-stamping miss) are
	// not selectable by any per-revision Service. The selector requires
	// the label to be present + non-empty.
	ginkgo.PIt("G1: SVC-14 per-revision Service selector excludes pods missing revision-hash", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("svc14"), WithoutDecoder())
		applyFixture(Default, fix)
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.ISVC) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CBM) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CSR) })

		var hash string
		Eventually(func(g Gomega) {
			hashes := observedRevisionHashes(g, ns.Name)
			g.Expect(hashes).To(HaveLen(1))
			hash = hashes[0]
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		// Synthesize an orphan pod with the OMENative selector but no
		// revision-hash. It should not match the per-revision Service's
		// selector even though it has the other 2 of 4 keys.
		orphan := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "orphan-no-hash",
				Namespace: ns.Name,
				Labels: map[string]string{
					constants.InferenceServicePodLabelKey: "svc14",
					constants.OMEComponentLabel:           string(v1beta1.EngineComponent),
					query.LabelManagedBy:                  query.ManagedByOMENative,
					// revision-hash deliberately absent
				},
			},
			Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "x", Image: "x"}}},
		}
		Expect(k8sClient.Create(suiteCtx, orphan)).To(Succeed())
		ginkgo.DeferCleanup(func() {
			zero := int64(0)
			_ = k8sClient.Delete(suiteCtx, orphan, &client.DeleteOptions{GracePeriodSeconds: &zero})
		})

		// Service selector should contain LabelRevisionHash; the orphan
		// doesn't satisfy it. (We assert on the selector contract; envtest
		// doesn't run kube-proxy so endpoint matching isn't observable.)
		name := coordination.PerRevisionServiceName("svc14", v1beta1.EngineComponent, hash)
		svc := readyPerRevisionService(Default, ns.Name, name)
		Expect(svc.Spec.Selector).To(HaveKey(query.LabelRevisionHash),
			"per-revision Service must require LabelRevisionHash in its selector to exclude unlabeled pods")
	})

	// SVC-19: cross-ISVC scoping. Two ISVCs in the same namespace with
	// the same Component get distinct per-revision Services scoped by
	// the inferenceservice label.
	ginkgo.PIt("G1: SVC-19 two ISVCs in same namespace: Services scoped via inferenceservice label", func() {
		fixA := newCoordFixture(ns.Name,
			WithISVCName("svc19a"), WithoutDecoder(),
			WithCBMName("svc19-cbm"), WithCSRName("svc19-csr"),
		)
		fixB := newCoordFixture(ns.Name,
			WithISVCName("svc19b"), WithoutDecoder(),
			WithCBMName("svc19-cbm"), WithCSRName("svc19-csr"),
		)
		// Create fixtures, but share CBM/CSR between them.
		Expect(k8sClient.Create(suiteCtx, fixA.CBM)).To(Succeed())
		Expect(k8sClient.Create(suiteCtx, fixA.CSR)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fixA.CBM) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fixA.CSR) })
		Expect(k8sClient.Create(suiteCtx, fixA.ISVC)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fixA.ISVC) })
		Expect(k8sClient.Create(suiteCtx, fixB.ISVC)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fixB.ISVC) })

		Eventually(func(g Gomega) {
			svcs := listPerRevisionServices(g, ns.Name)
			byISVC := map[string]int{}
			for _, s := range svcs {
				byISVC[s.Spec.Selector[constants.InferenceServicePodLabelKey]]++
			}
			g.Expect(byISVC["svc19a"]).To(BeNumerically(">=", 2),
				"svc19a should own at least 2 per-revision Services; got %v", byISVC)
			g.Expect(byISVC["svc19b"]).To(BeNumerically(">=", 2),
				"svc19b should own at least 2 per-revision Services; got %v", byISVC)
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	// SVC-20: routing Service is Type=ClusterIP with a Cluster IP, while
	// headless variant carries ClusterIP=None. Both are Type=ClusterIP
	// from K8s's typing perspective.
	ginkgo.PIt("G1: SVC-20 routing has ClusterIP set, headless has ClusterIPNone", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("svc20"), WithoutDecoder())
		applyFixture(Default, fix)
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.ISVC) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CBM) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CSR) })

		Eventually(func(g Gomega) {
			hashes := observedRevisionHashes(g, ns.Name)
			g.Expect(hashes).To(HaveLen(1))
			routing := readyPerRevisionService(g, ns.Name,
				coordination.PerRevisionServiceName("svc20", v1beta1.EngineComponent, hashes[0]))
			headless := readyPerRevisionService(g, ns.Name,
				coordination.PerRevisionHeadlessServiceName("svc20", v1beta1.EngineComponent, hashes[0]))

			g.Expect(routing.Spec.Type).To(Equal(corev1.ServiceTypeClusterIP))
			g.Expect(routing.Spec.ClusterIP).NotTo(Equal(corev1.ClusterIPNone))
			g.Expect(routing.Spec.ClusterIP).NotTo(BeEmpty(),
				"routing Service should have a cluster-assigned ClusterIP")

			g.Expect(headless.Spec.Type).To(Equal(corev1.ServiceTypeClusterIP))
			g.Expect(headless.Spec.ClusterIP).To(Equal(corev1.ClusterIPNone))
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	// SVC-22: revisions in flight: while pods of both revisions exist
	// during a rollout, both Service pairs coexist. After the old pods
	// drain, the old Service stays (G4 is the GC fix; SVC-12 pins that).
	ginkgo.PIt("G1: SVC-22 both revisions' Service pairs coexist mid-rollout", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("svc22"), WithoutDecoder(),
			WithEngineImage("ghcr.io/test/runtime:v1"))
		applyFixture(Default, fix)
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.ISVC) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CBM) })
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fix.CSR) })

		var v1Hash string
		Eventually(func(g Gomega) {
			hashes := observedRevisionHashes(g, ns.Name)
			g.Expect(hashes).To(HaveLen(1))
			v1Hash = hashes[0]
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		// Bump image → new revision in flight. Whether old pods drain
		// before the new ones are Ready depends on update strategy. We
		// just need to observe a window where both hashes coexist OR
		// the new hash has its Service emitted.
		fresh := &v1beta1.InferenceService{}
		Expect(k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: ns.Name, Name: "svc22"}, fresh)).To(Succeed())
		fresh.Spec.Engine.PodSpec.Containers[0].Image = "ghcr.io/test/runtime:v2"
		Expect(k8sClient.Update(suiteCtx, fresh)).To(Succeed())

		// Eventually a Service for some new hash exists; old Service
		// (G4 pending) still around regardless of pod state.
		Eventually(func(g Gomega) {
			svcs := listPerRevisionServices(g, ns.Name)
			byHash := map[string]int{}
			for _, s := range svcs {
				byHash[s.Spec.Selector[query.LabelRevisionHash]]++
			}
			// Old hash's Service pair still present.
			g.Expect(byHash[v1Hash]).To(Equal(2),
				"old revision's Service pair gone; svcs=%s", describeServices(svcs))
			// At least one OTHER hash appeared with a Service pair too.
			otherHashes := 0
			for h, n := range byHash {
				if h != v1Hash && n == 2 {
					otherHashes++
				}
			}
			g.Expect(otherHashes).To(BeNumerically(">=", 1),
				"new revision didn't get its Service pair; svcs=%s", describeServices(svcs))
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})
})

// Sanity: ensure spec doc references stay readable when SVC-13 fires
// the length-limit fallback assertion.
var _ = ginkgo.Describe("Coordination per-revision Service naming sanity", func() {
	ginkgo.It("PerRevisionServiceName format is <isvc>-<c>-rev-<hash>", func() {
		name := coordination.PerRevisionServiceName("foo", v1beta1.EngineComponent, "abcd1234")
		Expect(name).To(Equal("foo-engine-rev-abcd1234"))
	})

	ginkgo.It("PerRevisionHeadlessServiceName appends -headless", func() {
		name := coordination.PerRevisionHeadlessServiceName("foo", v1beta1.DecoderComponent, "abcd1234")
		Expect(name).To(Equal("foo-decoder-rev-abcd1234-headless"))
		Expect(strings.HasSuffix(name, "-headless")).To(BeTrue())
	})
})
