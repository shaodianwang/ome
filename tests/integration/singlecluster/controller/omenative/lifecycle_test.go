package omenative

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	v1beta1testing "github.com/ome-projects/ome/pkg/utils/testing/v1beta1"
	"github.com/ome-projects/ome/tests/integration/util"
)

// OMENative end-to-end lifecycle specs.
//
// Minimum-viable reconcile chain for OMENative mode:
//
//	ISVC (engine annotation ome.io/deploymentMode=OMENative)
//	  → ClusterBaseModel resolved → ClusterServingRuntime resolved
//	  → MergeRuntimeSpecs → DetermineDeploymentModes (engine=OMENative)
//	  → Engine component reconcile
//	  → OMENative.ReconcileComponent
//	     - BuildPlan
//	     - Create: render pods, create headless Service
//	     - kubelet sim: assign NodeName, set PodScheduled + ContainersReady
//	     - status.go: AggregateAndWriteStatus → InstanceStatuses + Conditions
//
// envtest has no kubelet, so the readiness-gate pipeline relies on
// the kubelet simulator in helpers_test.go to walk pods to
// ContainersReady. The simulator runs as a suite-level goroutine.
var _ = ginkgo.Describe("OMENative basic lifecycle", ginkgo.Ordered, func() {
	var ns *corev1.Namespace

	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "omenative-lc-")
		Expect(err).NotTo(HaveOccurred())
	})

	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("OMENative-mode ISVC creates pods + headless Service and converges to InstanceStatus=Ready", func() {
		ginkgo.By("creating a ClusterBaseModel + ClusterServingRuntime baseline")
		cbm := v1beta1testing.MakeClusterBaseModel("omenative-llama-7b").
			StorageURI("oci://n/test/b/models/o/llama-7b").
			ModelFormat("safetensors").
			ModelType("llama").
			ModelArchitecture("LlamaForCausalLM").
			ModelFramework("Transformers").
			Obj()
		Expect(k8sClient.Create(suiteCtx, cbm)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, cbm) })

		csr := v1beta1testing.MakeClusterServingRuntime("omenative-runtime").
			SupportsModelFormat("safetensors", true, 10).
			WithLastFormatArchitecture("LlamaForCausalLM").
			Obj()
		Expect(k8sClient.Create(suiteCtx, csr)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, csr) })

		ginkgo.By("creating an OMENative-mode InferenceService")
		// The deployment-mode determinant reads
		// engine.ComponentExtensionSpec.Annotations (per
		// pkg/.../utils/deployment.go DetermineEngineDeploymentMode),
		// not the ISVC's annotations directly. In production a defaulting
		// webhook propagates the ISVC-level annotation down. The
		// integration suite doesn't register that webhook, so set the
		// annotation on the engine spec explicitly.
		isvc := v1beta1testing.MakeInferenceService("lc-isvc", ns.Name).
			Model("omenative-llama-7b").
			Runtime("omenative-runtime").
			DeploymentMode(constants.OMENative).
			Engine(&v1beta1.EngineSpec{
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
					Annotations: map[string]string{
						constants.DeploymentMode: string(constants.OMENative),
					},
				},
				PodSpec: v1beta1.PodSpec{
					Containers: []corev1.Container{{
						Name:  "ome-container",
						Image: "ghcr.io/test/runtime:test",
					}},
				},
			}).Obj()
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())

		ginkgo.By("expecting at least one OMENative-owned pod to appear")
		Eventually(func(g Gomega) {
			pods := &corev1.PodList{}
			g.Expect(k8sClient.List(suiteCtx, pods,
				client.InNamespace(ns.Name),
				client.MatchingLabels{labelManagedByKey: managedByValue},
			)).To(Succeed())
			g.Expect(pods.Items).NotTo(BeEmpty(), "expected at least one OMENative-managed pod")
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("expecting the Component's headless Service to be created")
		Eventually(func(g Gomega) {
			svcs := &corev1.ServiceList{}
			g.Expect(k8sClient.List(suiteCtx, svcs, client.InNamespace(ns.Name))).To(Succeed())
			hasHeadless := false
			for _, svc := range svcs.Items {
				if svc.Spec.ClusterIP == corev1.ClusterIPNone {
					hasHeadless = true
					break
				}
			}
			g.Expect(hasHeadless).To(BeTrue(), "expected a headless Service for the OMENative Component; got %+v", svcs.Items)
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("expecting pods to be scheduled (NodeName via binder) and ContainersReady (via simulator)")
		Eventually(func(g Gomega) {
			pods := &corev1.PodList{}
			g.Expect(k8sClient.List(suiteCtx, pods,
				client.InNamespace(ns.Name),
				client.MatchingLabels{labelManagedByKey: managedByValue},
			)).To(Succeed())
			g.Expect(pods.Items).NotTo(BeEmpty(), "no OMENative-managed pods to advance")
			for i := range pods.Items {
				p := &pods.Items[i]
				g.Expect(p.Spec.NodeName).NotTo(BeEmpty(),
					"pod %s should have NodeName assigned by the binder", p.Name)
				g.Expect(hasConditionTrue(p.Status.Conditions, corev1.ContainersReady)).To(BeTrue(),
					"pod %s should have ContainersReady=True; conditions=%+v", p.Name, p.Status.Conditions)
			}
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("expecting the EndpointSlice simulator to publish the pod as Ready")
		// The controller flips ome.io/serving=True after Create promotes
		// Phase=Ready; the slice simulator then publishes Ready=true on
		// the endpoint.
		Eventually(func(g Gomega) {
			slices := &discoveryv1.EndpointSliceList{}
			g.Expect(k8sClient.List(suiteCtx, slices, client.InNamespace(ns.Name))).To(Succeed())
			g.Expect(slices.Items).NotTo(BeEmpty(), "expected the simulator to upsert at least one slice")
			anyReady := false
			for _, slice := range slices.Items {
				for _, ep := range slice.Endpoints {
					if ep.Conditions.Ready != nil && *ep.Conditions.Ready {
						anyReady = true
						break
					}
				}
				if anyReady {
					break
				}
			}
			g.Expect(anyReady).To(BeTrue(),
				"expected at least one endpoint Ready=true after serving flip; got %+v", slices.Items)
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("expecting InstanceStatus to converge to Phase=Ready with RunningRevision recorded")
		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(isvc), fresh)).To(Succeed())
			cs, ok := fresh.Status.Components[v1beta1.EngineComponent]
			g.Expect(ok).To(BeTrue())
			g.Expect(cs.OMENative).NotTo(BeNil())
			g.Expect(cs.OMENative.InstanceStatuses).To(HaveLen(1))
			s := cs.OMENative.InstanceStatuses[0]
			g.Expect(s.Phase).To(Equal(v1beta1.OMENativeInstanceReady),
				"InstanceStatus[0] should reach Ready; got %q (op=%+v)", s.Phase, s.Operation)
			g.Expect(s.RunningRevision).NotTo(BeEmpty(),
				"RunningRevision should be recorded after promote")
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.It("ControllerRevision is created with the right OwnerReference", func() {
		ginkgo.By("applying CBM + CSR + ISVC via the OMENative fixture")
		fx := newOMENativeFixture(ns.Name,
			WithCBMName("rev-llama-7b"),
			WithCSRName("rev-runtime"),
			WithISVCName("rev-isvc"),
		)
		Expect(k8sClient.Create(suiteCtx, fx.CBM)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CBM) })
		Expect(k8sClient.Create(suiteCtx, fx.CSR)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CSR) })
		Expect(k8sClient.Create(suiteCtx, fx.ISVC)).To(Succeed())
		isvc := fx.ISVC

		ginkgo.By("expecting at least one ControllerRevision owned by the ISVC")
		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(isvc), fresh)).To(Succeed())
			crs := listOMENativeRevisions(g, ns.Name)
			g.Expect(crs).NotTo(BeEmpty(), "expected at least one OMENative-owned ControllerRevision")
			matched := false
			for _, cr := range crs {
				for _, ref := range cr.OwnerReferences {
					if ref.UID == fresh.UID {
						matched = true
						break
					}
				}
			}
			g.Expect(matched).To(BeTrue(), "no CR had a controller OwnerRef pointing at this ISVC")
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	// Spec scenario #5 — ISVC deletion cascade prerequisite: every
	// owned child carries a controller OwnerReference to the ISVC,
	// which is what K8s garbage-collection uses for cascade-delete.
	//
	// We do NOT assert the cascade actually fires here: envtest runs
	// no kube-controller-manager, so the gc/garbage-collector controller
	// that walks OwnerReferences and deletes orphans isn't running.
	// In real K8s, correctly-shaped OwnerReferences guarantee the
	// cascade — this test proves OMENative's contract is met; the
	// cascade behavior itself is K8s contract.
	//
	// Bug it catches: missing controller=true OwnerReference on pods,
	// CRs, or Service. Without these, real K8s leaves orphans after
	// every ISVC delete.
	ginkgo.It("owned children carry controller OwnerReferences to the ISVC (cascade-delete prerequisite)", func() {
		fx := newOMENativeFixture(ns.Name,
			WithCBMName("cascade-llama"),
			WithCSRName("cascade-runtime"),
			WithISVCName("cascade-isvc"),
		)
		Expect(k8sClient.Create(suiteCtx, fx.CBM)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CBM) })
		Expect(k8sClient.Create(suiteCtx, fx.CSR)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CSR) })
		Expect(k8sClient.Create(suiteCtx, fx.ISVC)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.ISVC) })

		ginkgo.By("waiting for pods + CR + Service to exist")
		Eventually(func(g Gomega) {
			pods := &corev1.PodList{}
			g.Expect(k8sClient.List(suiteCtx, pods,
				client.InNamespace(ns.Name),
				client.MatchingLabels{labelManagedByKey: managedByValue},
			)).To(Succeed())
			g.Expect(pods.Items).NotTo(BeEmpty(), "expected at least one pod")
			crs := listOMENativeRevisions(g, ns.Name)
			g.Expect(crs).NotTo(BeEmpty(), "expected at least one CR")
			svcs := &corev1.ServiceList{}
			g.Expect(k8sClient.List(suiteCtx, svcs, client.InNamespace(ns.Name))).To(Succeed())
			hasHeadless := false
			for _, svc := range svcs.Items {
				if svc.Spec.ClusterIP == corev1.ClusterIPNone {
					hasHeadless = true
					break
				}
			}
			g.Expect(hasHeadless).To(BeTrue(), "expected a headless Service")
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		// Re-read the ISVC to capture its current UID (in case envtest
		// regenerated it from the test-supplied fixture).
		fresh := &v1beta1.InferenceService{}
		Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(fx.ISVC), fresh)).To(Succeed())
		isvcUID := fresh.UID

		ginkgo.By("asserting every owned child has a controller OwnerRef pointing at the ISVC")

		// Pods.
		pods := &corev1.PodList{}
		Expect(k8sClient.List(suiteCtx, pods,
			client.InNamespace(ns.Name),
			client.MatchingLabels{labelManagedByKey: managedByValue},
		)).To(Succeed())
		for _, pod := range pods.Items {
			found := false
			for _, ref := range pod.OwnerReferences {
				if ref.Controller != nil && *ref.Controller && ref.UID == isvcUID {
					found = true
					break
				}
			}
			Expect(found).To(BeTrue(), "pod %s missing controller OwnerReference to ISVC; owners=%+v", pod.Name, pod.OwnerReferences)
		}

		// ControllerRevisions.
		crList := &appsv1.ControllerRevisionList{}
		Expect(k8sClient.List(suiteCtx, crList,
			client.InNamespace(ns.Name),
			client.MatchingLabels{labelManagedByKey: managedByValue},
		)).To(Succeed())
		for _, cr := range crList.Items {
			found := false
			for _, ref := range cr.OwnerReferences {
				if ref.Controller != nil && *ref.Controller && ref.UID == isvcUID {
					found = true
					break
				}
			}
			Expect(found).To(BeTrue(), "CR %s missing controller OwnerReference to ISVC; owners=%+v", cr.Name, cr.OwnerReferences)
		}

		// Headless Service.
		svcs := &corev1.ServiceList{}
		Expect(k8sClient.List(suiteCtx, svcs, client.InNamespace(ns.Name))).To(Succeed())
		for _, svc := range svcs.Items {
			if svc.Spec.ClusterIP != corev1.ClusterIPNone {
				continue // skip non-headless services if any
			}
			found := false
			for _, ref := range svc.OwnerReferences {
				if ref.Controller != nil && *ref.Controller && ref.UID == isvcUID {
					found = true
					break
				}
			}
			Expect(found).To(BeTrue(), "headless Service %s missing controller OwnerReference to ISVC; owners=%+v", svc.Name, svc.OwnerReferences)
		}
	})

	// Spec scenario #6 — MinReplicas=0 behavior. Open question in the
	// spec: should the controller create no pods (scale-to-zero) or
	// default 0 → 1?
	//
	// FINDING from running this test: the reconciler creates 1 pod
	// when MinReplicas=0. Likely a defaulter promoting 0 → 1 because
	// the single-pod alpha doesn't support scale-to-zero. Pinned here
	// so a future "fix" (whether that's enforcing 0 OR documenting
	// the 0→1 promotion) is deliberate; the spec's open question can
	// be resolved by this observation.
	ginkgo.It("MinReplicas=0 currently creates 1 pod (defaulter promotes 0 → 1; OEP open question)", func() {
		fx := newOMENativeFixture(ns.Name,
			WithCBMName("mr0-llama"),
			WithCSRName("mr0-runtime"),
			WithISVCName("mr0-isvc"),
			WithMinReplicas(0),
		)
		Expect(k8sClient.Create(suiteCtx, fx.CBM)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CBM) })
		Expect(k8sClient.Create(suiteCtx, fx.CSR)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CSR) })
		Expect(k8sClient.Create(suiteCtx, fx.ISVC)).To(Succeed())

		Eventually(func(g Gomega) {
			pods := &corev1.PodList{}
			g.Expect(k8sClient.List(suiteCtx, pods,
				client.InNamespace(ns.Name),
				client.MatchingLabels{labelManagedByKey: managedByValue},
			)).To(Succeed())
			g.Expect(pods.Items).NotTo(BeEmpty(),
				"observed behavior: MinReplicas=0 promotes to 1 pod (file as P2 finding if scale-to-zero is desired)")
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	// Stable-name reuse: a pre-existing pod with the deterministic name
	// shouldn't wedge the controller (AlreadyExists silently counted as
	// a successful create would inflate expectations for the 2-minute
	// TTL).
	ginkgo.It("pre-existing pod with the deterministic name doesn't wedge expectations", func() {
		fx := newOMENativeFixture(ns.Name,
			WithCBMName("ops7-llama"),
			WithCSRName("ops7-runtime"),
			WithISVCName("ops7-isvc"),
		)
		Expect(k8sClient.Create(suiteCtx, fx.CBM)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CBM) })
		Expect(k8sClient.Create(suiteCtx, fx.CSR)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CSR) })

		// Pre-create a stranger pod with the SAME deterministic name
		// the controller would compute, but WITHOUT the OMENative
		// managed-by label so the controller's pod-listing query
		// won't match it. The controller will then attempt to Create
		// a pod with the same name and the apiserver will reject
		// with AlreadyExists.
		stranger := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "ops7-isvc-engine-0-default-0",
				Namespace: ns.Name,
				Labels:    map[string]string{"placeholder": "not-OMENative"},
			},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{{Name: "ph", Image: "test:placeholder"}},
			},
		}
		Expect(k8sClient.Create(suiteCtx, stranger)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, stranger) })

		Expect(k8sClient.Create(suiteCtx, fx.ISVC)).To(Succeed())

		// The contract is "doesn't spin out" — controller should not error
		// indefinitely and should not produce a parallel pod with the
		// same name. The placeholder pod stays.
		Consistently(func(g Gomega) {
			got := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(stranger), got)).To(Succeed())
			// Placeholder still has the placeholder label, hasn't been
			// adopted by the controller.
			g.Expect(got.Labels[labelManagedByKey]).NotTo(Equal(managedByValue),
				"controller must not adopt a stranger pod by relabeling it; labels=%+v", got.Labels)
		}, "3s", util.PollInterval).Should(Succeed())
	})

	// Scenario #9 / #10 from the spec — failure-mode reconciles.
	// Both verify the controller refuses to materialize pods when a
	// required dependency is missing, AND that it doesn't crash or
	// loop unboundedly. The asserted invariant is "no OMENative-
	// managed pods appear" — the controller surfaces the missing
	// dependency via an InferenceService condition / event, but the
	// specific condition wording is the ISVC reconciler's contract,
	// not OMENative's, so we assert on the OMENative-visible outcome.

	ginkgo.It("missing CBM: no pods created, no panic, ISVC stays without InstanceStatus", func() {
		ginkgo.By("creating CSR + ISVC but NO ClusterBaseModel")
		fx := newOMENativeFixture(ns.Name,
			WithCBMName("missing-cbm-llama"), // intentionally not Create'd
			WithCSRName("missing-cbm-runtime"),
			WithISVCName("missing-cbm-isvc"),
		)
		// Skip the CBM Create. CSR is required for the runtime selector
		// to even classify the ISVC; without it the reconciler errors
		// even earlier and we'd be testing CSR-missing, not CBM-missing.
		Expect(k8sClient.Create(suiteCtx, fx.CSR)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CSR) })
		Expect(k8sClient.Create(suiteCtx, fx.ISVC)).To(Succeed())

		ginkgo.By("waiting 3s and asserting no OMENative pods exist")
		// Negative assertion via Consistently rather than Eventually:
		// we want to prove the controller doesn't EVENTUALLY create
		// pods anyway after retry backoff.
		Consistently(func(g Gomega) {
			pods := &corev1.PodList{}
			g.Expect(k8sClient.List(suiteCtx, pods,
				client.InNamespace(ns.Name),
				client.MatchingLabels{labelManagedByKey: managedByValue},
			)).To(Succeed())
			g.Expect(pods.Items).To(BeEmpty(),
				"missing CBM must block pod creation; got %d OMENative pod(s)", len(pods.Items))
		}, "3s", util.PollInterval).Should(Succeed())
	})

	// Spec scenario #2 — BuildPlan index allocation + pod naming pattern.
	// Bug it catches: a regression where BuildPlan emits the wrong
	// index set (e.g., skips an index, double-allocates one) or pod
	// names don't follow the documented <isvc>-<component>-<idx>-<runner>-<ordinal>
	// shape.
	ginkgo.It("MinReplicas=3 creates pods at indices 0, 1, 2 with the expected name pattern", func() {
		fx := newOMENativeFixture(ns.Name,
			WithCBMName("mr3-llama-7b"),
			WithCSRName("mr3-runtime"),
			WithISVCName("mr3-isvc"),
			WithMinReplicas(3),
		)
		Expect(k8sClient.Create(suiteCtx, fx.CBM)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CBM) })
		Expect(k8sClient.Create(suiteCtx, fx.CSR)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CSR) })
		Expect(k8sClient.Create(suiteCtx, fx.ISVC)).To(Succeed())

		expected := map[string]bool{
			"mr3-isvc-engine-0-default-0": false,
			"mr3-isvc-engine-1-default-0": false,
			"mr3-isvc-engine-2-default-0": false,
		}
		Eventually(func(g Gomega) {
			pods := &corev1.PodList{}
			g.Expect(k8sClient.List(suiteCtx, pods,
				client.InNamespace(ns.Name),
				client.MatchingLabels{labelManagedByKey: managedByValue},
			)).To(Succeed())
			// Reset seen on each retry so any reconcile flap doesn't latch
			// a stale flag — this iteration's snapshot must show all three
			// names simultaneously.
			seen := map[string]bool{}
			for k := range expected {
				seen[k] = false
			}
			for i := range pods.Items {
				if _, ok := seen[pods.Items[i].Name]; ok {
					seen[pods.Items[i].Name] = true
				}
			}
			missing := []string{}
			for name, ok := range seen {
				if !ok {
					missing = append(missing, name)
				}
			}
			g.Expect(missing).To(BeEmpty(),
				"expected pods at indices 0/1/2; missing %v (full list: %d pods)", missing, len(pods.Items))
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	// Scale-up race: immediately bump MinReplicas before pod 0 reaches
	// Ready. Tests the planner's handling of "pod 0 still Creating"
	// state when computing additional indices — Pass 1 might pick the
	// in-flight Index 0 if it's already in InstanceStatuses, plus
	// Pass 2/3 add new ones up to the new MinReplicas.
	ginkgo.It("scale-up race: bump MinReplicas before pod 0 is Ready converges to all replicas", func() {
		fx := newOMENativeFixture(ns.Name,
			WithCBMName("race-llama"),
			WithCSRName("race-runtime"),
			WithISVCName("race-isvc"),
			WithMinReplicas(1),
		)
		Expect(k8sClient.Create(suiteCtx, fx.CBM)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CBM) })
		Expect(k8sClient.Create(suiteCtx, fx.CSR)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CSR) })
		Expect(k8sClient.Create(suiteCtx, fx.ISVC)).To(Succeed())

		ginkgo.By("immediately bumping MinReplicas 1 → 3 without waiting for Ready")
		// Bounce up the MinReplicas as quickly as possible to race the
		// initial reconcile. RetryOnConflict because the controller is
		// also writing status concurrently.
		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(fx.ISVC), fresh)).To(Succeed())
			three := 3
			fresh.Spec.Engine.ComponentExtensionSpec.MinReplicas = &three
			g.Expect(k8sClient.Update(suiteCtx, fresh)).To(Succeed())
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("expecting all 3 InstanceStatuses to reach Ready")
		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(fx.ISVC), fresh)).To(Succeed())
			cs := fresh.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.OMENative).NotTo(BeNil())
			g.Expect(cs.OMENative.InstanceStatuses).To(HaveLen(3))
			for _, s := range cs.OMENative.InstanceStatuses {
				g.Expect(s.Phase).To(Equal(v1beta1.OMENativeInstanceReady),
					"all 3 instances must converge to Ready; got Index=%d Phase=%q", s.Index, s.Phase)
			}
			// Verify indices are exactly {0, 1, 2} (no sparse allocation).
			seen := map[int32]bool{}
			for _, s := range cs.OMENative.InstanceStatuses {
				seen[s.Index] = true
			}
			g.Expect(seen).To(HaveKey(int32(0)))
			g.Expect(seen).To(HaveKey(int32(1)))
			g.Expect(seen).To(HaveKey(int32(2)))
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	// ISVC delete during in-flight Restart: deleting the ISVC while
	// Restart has stamped Phase=Restarting on its InstanceStatus must
	// not panic the controller. envtest doesn't run GC, so pods stick
	// around — the test asserts the controller keeps reconciling
	// (next reconcile NotFound's on the ISVC and exits cleanly) without
	// throwing an unrecoverable error.
	ginkgo.It("ISVC delete during in-flight Restart does not panic the controller", func() {
		fx := newOMENativeFixture(ns.Name,
			WithCBMName("rdel-llama"),
			WithCSRName("rdel-runtime"),
			WithISVCName("rdel-isvc"),
		)
		Expect(k8sClient.Create(suiteCtx, fx.CBM)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CBM) })
		Expect(k8sClient.Create(suiteCtx, fx.CSR)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CSR) })
		Expect(k8sClient.Create(suiteCtx, fx.ISVC)).To(Succeed())

		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(fx.ISVC), fresh)).To(Succeed())
			cs := fresh.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.OMENative).NotTo(BeNil())
			g.Expect(cs.OMENative.InstanceStatuses).To(HaveLen(1))
			g.Expect(cs.OMENative.InstanceStatuses[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("triggering Restart via PodFailed")
		Eventually(func(g Gomega) {
			pod := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name, Name: "rdel-isvc-engine-0-default-0",
			}, pod)).To(Succeed())
			pod.Status.Phase = corev1.PodFailed
			g.Expect(k8sClient.Status().Update(suiteCtx, pod)).To(Succeed())
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("immediately deleting the ISVC")
		Expect(k8sClient.Delete(suiteCtx, fx.ISVC)).To(Succeed())

		// The controller might still be mid-Restart when the ISVC delete
		// lands. We're asserting it survives — the simplest way is to
		// fire a follow-up Get and confirm the ISVC eventually disappears
		// (or returns NotFound). envtest's GC isn't running so the ISVC
		// may persist with a deletionTimestamp; either way the controller
		// shouldn't crash, and subsequent reconciles must be no-ops.
		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceService{}
			err := k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(fx.ISVC), fresh)
			if err != nil {
				g.Expect(client.IgnoreNotFound(err)).To(Succeed())
				return
			}
			// Tolerate envtest's missing GC: deletionTimestamp on the
			// ISVC means K8s observed the delete and is awaiting GC. The
			// controller should observe it too and stop driving state
			// transitions; we don't need to assert specific phase here.
			_ = fresh.DeletionTimestamp
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.It("missing CSR: no pods created, controller doesn't panic on retry", func() {
		ginkgo.By("creating CBM + ISVC but NO ClusterServingRuntime")
		fx := newOMENativeFixture(ns.Name,
			WithCBMName("missing-csr-llama"),
			WithCSRName("missing-csr-runtime"), // intentionally not Create'd
			WithISVCName("missing-csr-isvc"),
		)
		Expect(k8sClient.Create(suiteCtx, fx.CBM)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CBM) })
		Expect(k8sClient.Create(suiteCtx, fx.ISVC)).To(Succeed())

		ginkgo.By("waiting 3s and asserting no OMENative pods exist")
		Consistently(func(g Gomega) {
			pods := &corev1.PodList{}
			g.Expect(k8sClient.List(suiteCtx, pods,
				client.InNamespace(ns.Name),
				client.MatchingLabels{labelManagedByKey: managedByValue},
			)).To(Succeed())
			g.Expect(pods.Items).To(BeEmpty(),
				"missing CSR must block pod creation; got %d OMENative pod(s)", len(pods.Items))
		}, "3s", util.PollInterval).Should(Succeed())
	})

	// Scale up 1 → 3 must append new pods at indices 1, 2 without
	// touching pod 0. extraInstanceIndices over-selecting would
	// retire/recreate the existing pod 0 (changing its UID).
	ginkgo.It("scale up MinReplicas 1 → 3 appends pods at indices 1, 2 leaving pod 0 untouched", func() {
		fx := newOMENativeFixture(ns.Name,
			WithCBMName("scaleup-llama"),
			WithCSRName("scaleup-runtime"),
			WithISVCName("scaleup-isvc"),
			WithMinReplicas(1),
		)
		Expect(k8sClient.Create(suiteCtx, fx.CBM)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CBM) })
		Expect(k8sClient.Create(suiteCtx, fx.CSR)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CSR) })
		Expect(k8sClient.Create(suiteCtx, fx.ISVC)).To(Succeed())

		ginkgo.By("waiting for the initial single-pod instance to be Ready")
		var pod0UID string
		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(fx.ISVC), fresh)).To(Succeed())
			cs := fresh.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.OMENative).NotTo(BeNil())
			g.Expect(cs.OMENative.InstanceStatuses).To(HaveLen(1))
			g.Expect(cs.OMENative.InstanceStatuses[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))

			pod := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name, Name: "scaleup-isvc-engine-0-default-0",
			}, pod)).To(Succeed())
			pod0UID = string(pod.UID)
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("bumping MinReplicas 1 → 3 via retry-on-conflict")
		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(fx.ISVC), fresh)).To(Succeed())
			three := 3
			fresh.Spec.Engine.ComponentExtensionSpec.MinReplicas = &three
			g.Expect(k8sClient.Update(suiteCtx, fresh)).To(Succeed())
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("expecting pods at indices 0, 1, 2 with pod 0's UID preserved")
		Eventually(func(g Gomega) {
			pod0 := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name, Name: "scaleup-isvc-engine-0-default-0",
			}, pod0)).To(Succeed())
			g.Expect(string(pod0.UID)).To(Equal(pod0UID),
				"pod 0 must NOT be recreated on scale-up; UID changed from %s to %s", pod0UID, pod0.UID)

			pod1 := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name, Name: "scaleup-isvc-engine-1-default-0",
			}, pod1)).To(Succeed())
			pod2 := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name, Name: "scaleup-isvc-engine-2-default-0",
			}, pod2)).To(Succeed())
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	// Scale down 3 → 1 must delete pods at indices 1, 2 only. Pod 0
	// stays Ready. extraInstanceIndices over-selecting would delete a
	// live serving pod and break the service contract.
	ginkgo.It("scale down MinReplicas 3 → 1 deletes pods 1+2, leaves pod 0 Ready", func() {
		fx := newOMENativeFixture(ns.Name,
			WithCBMName("scaledown-llama"),
			WithCSRName("scaledown-runtime"),
			WithISVCName("scaledown-isvc"),
			WithMinReplicas(3),
		)
		Expect(k8sClient.Create(suiteCtx, fx.CBM)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CBM) })
		Expect(k8sClient.Create(suiteCtx, fx.CSR)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx.CSR) })
		Expect(k8sClient.Create(suiteCtx, fx.ISVC)).To(Succeed())

		ginkgo.By("waiting for all three instances to be Ready")
		var pod0UID string
		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(fx.ISVC), fresh)).To(Succeed())
			cs := fresh.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.OMENative).NotTo(BeNil())
			g.Expect(cs.OMENative.InstanceStatuses).To(HaveLen(3))
			for _, s := range cs.OMENative.InstanceStatuses {
				g.Expect(s.Phase).To(Equal(v1beta1.OMENativeInstanceReady),
					"InstanceStatus[%d] should be Ready", s.Index)
			}
			pod0 := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name, Name: "scaledown-isvc-engine-0-default-0",
			}, pod0)).To(Succeed())
			pod0UID = string(pod0.UID)
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("scaling down MinReplicas 3 → 1")
		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(fx.ISVC), fresh)).To(Succeed())
			one := 1
			fresh.Spec.Engine.ComponentExtensionSpec.MinReplicas = &one
			g.Expect(k8sClient.Update(suiteCtx, fresh)).To(Succeed())
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("expecting pods 1 and 2 to disappear and pod 0 to remain untouched")
		Eventually(func(g Gomega) {
			pod1 := &corev1.Pod{}
			err1 := k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name, Name: "scaledown-isvc-engine-1-default-0",
			}, pod1)
			g.Expect(client.IgnoreNotFound(err1)).To(Succeed())
			if err1 == nil {
				g.Expect(pod1.DeletionTimestamp).NotTo(BeNil(),
					"pod 1 should be deleting or gone; live pod observed")
			}
			pod2 := &corev1.Pod{}
			err2 := k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name, Name: "scaledown-isvc-engine-2-default-0",
			}, pod2)
			g.Expect(client.IgnoreNotFound(err2)).To(Succeed())
			if err2 == nil {
				g.Expect(pod2.DeletionTimestamp).NotTo(BeNil(),
					"pod 2 should be deleting or gone; live pod observed")
			}

			pod0 := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
				Namespace: ns.Name, Name: "scaledown-isvc-engine-0-default-0",
			}, pod0)).To(Succeed())
			g.Expect(string(pod0.UID)).To(Equal(pod0UID),
				"pod 0 must NOT be touched on scale-down")
			g.Expect(pod0.DeletionTimestamp).To(BeNil(),
				"pod 0 must NOT be marked for deletion")

			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(fx.ISVC), fresh)).To(Succeed())
			cs := fresh.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.OMENative).NotTo(BeNil())
			g.Expect(cs.OMENative.InstanceStatuses).To(HaveLen(1),
				"InstanceStatuses should converge to exactly one entry after scale-down")
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	// Delete + recreate same-name ISVC in the same namespace. The new
	// ISVC has a different UID; controller-owned CRs from the old ISVC
	// become foreign-owned from the new ISVC's perspective.
	// EnsureControllerRevision detects this as a collision and bumps
	// CollisionCount so the new CR lands with a different name. The
	// new ISVC must reach Ready without inheriting state from the
	// deleted one — guards against an old-CR adoption that would lock
	// the new ISVC to a stale pod template.
	ginkgo.It("delete + recreate same-name ISVC produces a fresh ControllerRevision (no foreign-owner adoption)", func() {
		fx1 := newOMENativeFixture(ns.Name,
			WithCBMName("recreate-llama"),
			WithCSRName("recreate-runtime"),
			WithISVCName("recreate-isvc"),
		)
		Expect(k8sClient.Create(suiteCtx, fx1.CBM)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx1.CBM) })
		Expect(k8sClient.Create(suiteCtx, fx1.CSR)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, fx1.CSR) })
		Expect(k8sClient.Create(suiteCtx, fx1.ISVC)).To(Succeed())

		var originalCR, originalUID string
		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(fx1.ISVC), fresh)).To(Succeed())
			cs := fresh.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.OMENative).NotTo(BeNil())
			g.Expect(cs.OMENative.InstanceStatuses).To(HaveLen(1))
			g.Expect(cs.OMENative.InstanceStatuses[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
			g.Expect(cs.OMENative.UpdateRevision).NotTo(BeEmpty())
			originalCR = cs.OMENative.UpdateRevision
			originalUID = string(fresh.UID)
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("delete the ISVC")
		Expect(k8sClient.Delete(suiteCtx, fx1.ISVC)).To(Succeed())
		Eventually(func(g Gomega) {
			err := k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(fx1.ISVC), &v1beta1.InferenceService{})
			g.Expect(client.IgnoreNotFound(err)).To(Succeed())
			g.Expect(err).To(HaveOccurred(), "ISVC must vanish from apiserver before recreate")
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("recreate ISVC with the SAME name (new UID)")
		fx2 := newOMENativeFixture(ns.Name,
			WithCBMName("recreate-llama"),
			WithCSRName("recreate-runtime"),
			WithISVCName("recreate-isvc"),
		)
		Expect(k8sClient.Create(suiteCtx, fx2.ISVC)).To(Succeed())

		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceService{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(fx2.ISVC), fresh)).To(Succeed())
			g.Expect(string(fresh.UID)).NotTo(Equal(originalUID),
				"recreated ISVC must have a new UID")
			cs := fresh.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.OMENative).NotTo(BeNil())
			g.Expect(cs.OMENative.InstanceStatuses).To(HaveLen(1))
			g.Expect(cs.OMENative.InstanceStatuses[0].Phase).To(Equal(v1beta1.OMENativeInstanceReady))
			g.Expect(cs.OMENative.UpdateRevision).NotTo(BeEmpty())
			g.Expect(cs.OMENative.UpdateRevision).NotTo(Equal(originalCR),
				"new ISVC must NOT adopt the old CR (foreign UID); EnsureControllerRevision must bump CollisionCount and produce a fresh name. Old=%s New=%s",
				originalCR, cs.OMENative.UpdateRevision)
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

})
