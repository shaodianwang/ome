package inferencereplicatpuslice

import (
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/sliceprovision"
	workloadtypes "github.com/ome-projects/ome/pkg/controller/v1beta1/workload/types"
	"github.com/ome-projects/ome/pkg/tpuslice"
	"github.com/ome-projects/ome/pkg/tpuslice/gke"
	"github.com/ome-projects/ome/tests/integration/util"
)

const (
	waitTimeout  = 2 * time.Minute
	pollInterval = 250 * time.Millisecond
	// holdWindow is how long a spec watches for a release or a removal that
	// must not happen.
	holdWindow = 3 * time.Second
	// podFinalizer keeps a deleted pod in place. A pod that never bound is
	// removed as soon as it is deleted; the finalizer stands in for the
	// graceful shutdown of a running pod.
	podFinalizer = "example.com/graceful-shutdown"
	// sliceFinalizer keeps a released slice in place, as the provider does
	// while it frees the slice's partitions.
	sliceFinalizer = "example.com/partition-release"
	// irKind is the owner kind the controller records on the slices it
	// provisions for an InferenceReplica.
	irKind = "InferenceReplica"
)

// sliceIR returns an InferenceReplica of one Instance, opted in to TPU
// slice provisioning, whose pods select accelerator and topology and
// request chips each. workers > 0 runs a leader and that many workers;
// otherwise the Instance is one default pod. Like the retention suite's
// IR, it is controller-write stamped and controller-owned by a synthetic
// parent ISVC identity, which envtest's missing garbage collector leaves
// inert.
func sliceIR(parent, namespace, accelerator, topology string, chips int64, workers int32) *v1beta1.InferenceReplica {
	template := func() corev1.PodTemplateSpec {
		return corev1.PodTemplateSpec{
			ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{
				constants.TPUSliceProvisioningAnnotationKey: "true",
			}},
			Spec: corev1.PodSpec{
				NodeSelector: map[string]string{
					sliceConfig.NodeLabels.Accelerator: accelerator,
					sliceConfig.NodeLabels.Topology:    topology,
				},
				Containers: []corev1.Container{{
					Name:  "ome-container",
					Image: "registry.example.com/fake-serving:v1",
					Resources: corev1.ResourceRequirements{Limits: corev1.ResourceList{
						sliceConfig.ChipResource: *resource.NewQuantity(chips, resource.DecimalSI),
					}},
				}},
			},
		}
	}
	runners := []v1beta1.Runner{{Name: v1beta1.RunnerNameDefault, Size: 1, Template: template()}}
	if workers > 0 {
		runners = []v1beta1.Runner{
			{Name: v1beta1.RunnerNameLeader, Size: 1, Template: template()},
			{Name: v1beta1.RunnerNameWorker, Size: workers, Template: template()},
		}
	}
	return &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{
			Name:      parent + "-engine",
			Namespace: namespace,
			Annotations: map[string]string{
				constants.InferenceReplicaControllerWriteAnnotationKey: constants.InferenceReplicaControllerWriteAnnotationVal,
			},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: v1beta1.SchemeGroupVersion.String(),
				Kind:       "InferenceService",
				Name:       parent,
				UID:        types.UID(parent + "-isvc-uid"),
				Controller: ptr.To(true),
			}},
		},
		Spec: v1beta1.InferenceReplicaSpec{
			ParentRef: &v1beta1.ParentReference{Name: parent},
			Component: v1beta1.EngineComponent,
			Replicas:  ptr.To(int32(1)),
			Runners:   runners,
		},
	}
}

func createNamespace(name string) string {
	Expect(k8sClient.Create(suiteCtx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: name},
	})).To(Succeed())
	return name
}

// ownedSlices lists the slices provisioned for ir, sorted by name.
func ownedSlices(g Gomega, ir *v1beta1.InferenceReplica) []unstructured.Unstructured {
	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(gke.GroupVersion.WithKind(gke.Kind + "List"))
	g.Expect(k8sClient.List(suiteCtx, list,
		client.MatchingLabels{sliceprovision.LabelOwnerUID: string(ir.UID)})).To(Succeed())
	items := list.Items
	slices.SortFunc(items, func(a, b unstructured.Unstructured) int {
		return strings.Compare(a.GetName(), b.GetName())
	})
	return items
}

// awaitOwnedSlices waits until exactly n slices are provisioned for ir and
// returns their names.
func awaitOwnedSlices(ir *v1beta1.InferenceReplica, n int) []string {
	var names []string
	Eventually(func(g Gomega) {
		owned := ownedSlices(g, ir)
		g.Expect(owned).To(HaveLen(n))
		names = names[:0]
		for i := range owned {
			names = append(names, owned[i].GetName())
		}
	}).WithTimeout(waitTimeout).WithPolling(pollInterval).Should(Succeed())
	return names
}

func getSlice(g Gomega, name string) *unstructured.Unstructured {
	slice := gke.NewObject()
	g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{Name: name}, slice)).To(Succeed())
	return slice
}

// reportSliceState writes state as the slice's Ready condition reason, as
// the provider does. Before reporting a ready state, it also exposes the
// slice's ready hosts with the labels the provider adds during activation.
func reportSliceState(name, state, message string) {
	status := metav1.ConditionFalse
	if slices.Contains(sliceConfig.Slice.ReadyStates, state) {
		status = metav1.ConditionTrue
		exposeSliceHosts(name)
	}
	Eventually(func(g Gomega) {
		slice := getSlice(g, name)
		slice.Object["status"] = map[string]interface{}{
			"conditions": []interface{}{map[string]interface{}{
				"type":               "Ready",
				"status":             string(status),
				"reason":             state,
				"message":            message,
				"lastTransitionTime": metav1.Now().UTC().Format(time.RFC3339),
			}},
		}
		g.Expect(k8sClient.Status().Update(suiteCtx, slice)).To(Succeed())
	}).WithTimeout(waitTimeout).WithPolling(pollInterval).Should(Succeed())
}

// exposeSliceHosts creates the nodes that back a ready slice. The suite has
// no provider, so this stands in for the provider labeling the activated
// partition's hosts before it reports the slice ready.
func exposeSliceHosts(name string) {
	slice := getSlice(Default, name)
	topologyName, found, err := unstructured.NestedString(slice.Object, "spec", "topology")
	Expect(err).NotTo(HaveOccurred())
	Expect(found).To(BeTrue())
	sliceType, found, err := unstructured.NestedString(slice.Object, "spec", "type")
	Expect(err).NotTo(HaveOccurred())
	Expect(found).To(BeTrue())

	acceleratorName := ""
	chipsPerHost := int64(0)
	for candidate, accelerator := range sliceConfig.Accelerators {
		if accelerator.SliceType == sliceType {
			acceleratorName = candidate
			chipsPerHost = accelerator.ChipsPerHost
			break
		}
	}
	Expect(acceleratorName).NotTo(BeEmpty(), "slice type %s must name a configured accelerator", sliceType)
	topology, err := tpuslice.ParseTopology(topologyName)
	Expect(err).NotTo(HaveOccurred())
	hosts, err := topology.Hosts(chipsPerHost)
	Expect(err).NotTo(HaveOccurred())

	for i := range hosts {
		node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{
			Name: fmt.Sprintf("%s-host-%d", name, i),
			Labels: map[string]string{
				sliceConfig.NodeLabels.Accelerator: acceleratorName,
				sliceConfig.NodeLabels.Topology:    topologyName,
				sliceConfig.NodeLabels.Slice:       name,
			},
		}}
		err := k8sClient.Create(suiteCtx, node)
		if !apierrors.IsAlreadyExists(err) {
			Expect(err).To(Succeed())
		}
		Eventually(func(g Gomega) {
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(node), node)).To(Succeed())
			node.Status.Conditions = []corev1.NodeCondition{{
				Type: corev1.NodeReady, Status: corev1.ConditionTrue,
			}}
			g.Expect(k8sClient.Status().Update(suiteCtx, node)).To(Succeed())
		}).WithTimeout(waitTimeout).WithPolling(pollInterval).Should(Succeed())
	}
}

// setFinalizer adds finalizer to, or with present false removes it from,
// the object at key; obj is the empty object to read it into.
func setFinalizer(obj client.Object, key client.ObjectKey, finalizer string, present bool) {
	Eventually(func(g Gomega) {
		g.Expect(k8sClient.Get(suiteCtx, key, obj)).To(Succeed())
		if present {
			controllerutil.AddFinalizer(obj, finalizer)
		} else {
			controllerutil.RemoveFinalizer(obj, finalizer)
		}
		g.Expect(k8sClient.Update(suiteCtx, obj)).To(Succeed())
	}).WithTimeout(waitTimeout).WithPolling(pollInterval).Should(Succeed())
}

// provisionForRowlessInstance provisions, as the controller does, the slice
// of ir's Instance idx and returns its name. It stands in for the slice a
// create pass provisions for a new Instance when the status write that
// would add the Instance's row does not land.
func provisionForRowlessInstance(ir *v1beta1.InferenceReplica, idx int32) string {
	var pods []*corev1.PodSpec
	for i := range ir.Spec.Runners {
		for range ir.Spec.Runners[i].Size {
			pods = append(pods, &ir.Spec.Runners[i].Template.Spec)
		}
	}
	demand, needed, err := sliceprovision.Resolve(suiteCtx, k8sClient, &sliceConfig, pods)
	Expect(err).NotTo(HaveOccurred())
	Expect(needed).To(BeTrue())
	p, err := sliceprovision.New(&sliceConfig, k8sClient, k8sClient, k8sClient, sliceprovision.Owner{
		Kind:      irKind,
		Namespace: ir.Namespace,
		Name:      ir.Name,
		UID:       ir.UID,
	})
	Expect(err).NotTo(HaveOccurred())
	placement, err := p.Ensure(suiteCtx, demand, sliceprovision.Slot{Instance: idx})
	Expect(err).NotTo(HaveOccurred())
	return placement.Slice.Name
}

func listPods(g Gomega, namespace string) []corev1.Pod {
	pods := &corev1.PodList{}
	g.Expect(k8sClient.List(suiteCtx, pods, client.InNamespace(namespace))).To(Succeed())
	return pods.Items
}

// awaitPods waits until namespace has exactly n pods and returns them.
func awaitPods(namespace string, n int) []corev1.Pod {
	var pods []corev1.Pod
	Eventually(func(g Gomega) {
		pods = listPods(g, namespace)
		g.Expect(pods).To(HaveLen(n))
	}).WithTimeout(waitTimeout).WithPolling(pollInterval).Should(Succeed())
	return pods
}

// awaitWithheld waits until ir's only Instance reports that a pod is
// withheld for its slice, with an explanation containing each of words.
func awaitWithheld(ir *v1beta1.InferenceReplica, words ...string) {
	Eventually(func(g Gomega) {
		rows, err := util.GetInstanceStatuses(suiteCtx, k8sClient, client.ObjectKeyFromObject(ir))
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(rows).To(HaveLen(1))
		g.Expect(rows[0].Operation).NotTo(BeNil())
		g.Expect(rows[0].Operation.Waiting).To(Equal(workloadtypes.WaitingReasonCapacityProvisioning))
		g.Expect(rows[0].LastFailure).NotTo(BeNil())
		for _, w := range words {
			g.Expect(rows[0].LastFailure.Message).To(ContainSubstring(w))
		}
	}).WithTimeout(waitTimeout).WithPolling(pollInterval).Should(Succeed())
}

func expectIRPresent(g Gomega, ir *v1beta1.InferenceReplica, why string) {
	g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(ir), &v1beta1.InferenceReplica{})).To(Succeed(), why)
}

func expectConfinedTo(pod corev1.Pod, slice, topology string) {
	ExpectWithOffset(1, pod.Spec.NodeSelector).To(Equal(map[string]string{
		sliceConfig.NodeLabels.Accelerator: onDemandAccelerator,
		sliceConfig.NodeLabels.Topology:    topology,
		sliceConfig.NodeLabels.Slice:       slice,
	}), "pod %s must keep its shape selectors and be confined to slice %s", pod.Name, slice)
}

var _ = ginkgo.Describe("InferenceReplica TPU slice provisioning", func() {
	ginkgo.It("provisions the slice before the pod and creates the pod on it once it is ready", func() {
		ns := createNamespace("tpuslice-ready")
		ir := sliceIR("llama", ns, onDemandAccelerator, "2x2x1", 4, 0)
		Expect(k8sClient.Create(suiteCtx, ir)).To(Succeed())

		ginkgo.By("provisioning the Instance's slice and withholding its pod")
		name := awaitOwnedSlices(ir, 1)[0]
		slice := getSlice(Default, name)
		Expect(slice.Object["spec"]).To(Equal(map[string]interface{}{
			"type":         sliceConfig.Accelerators[onDemandAccelerator].SliceType,
			"topology":     "2x2x1",
			"partitionIds": []interface{}{},
		}))
		Expect(slice.GetLabels()).To(HaveKeyWithValue(sliceConfig.Slice.OwnerKindLabel, irKind))
		Expect(slice.GetLabels()).To(HaveKeyWithValue(sliceConfig.Slice.OwnerNameLabel, ir.Name))
		for k, v := range sliceConfig.Slice.Annotations {
			Expect(slice.GetAnnotations()).To(HaveKeyWithValue(k, v))
		}
		Expect(slice.GetAnnotations()).To(HaveKeyWithValue(sliceprovision.AnnotationOwner, ns+"/"+ir.Name))
		awaitWithheld(ir, name)
		Expect(listPods(Default, ns)).To(BeEmpty())

		ginkgo.By("withholding the pod while the slice reports a state that is not ready")
		reportSliceState(name, stateCreating, "binding partitions")
		awaitWithheld(ir, name, stateCreating, "binding partitions")
		Expect(listPods(Default, ns)).To(BeEmpty())

		ginkgo.By("creating the pod on the slice once it is ready")
		reportSliceState(name, stateActive, "")
		pods := awaitPods(ns, 1)
		expectConfinedTo(pods[0], name, "2x2x1")
		Eventually(func(g Gomega) {
			rows, err := util.GetInstanceStatuses(suiteCtx, k8sClient, client.ObjectKeyFromObject(ir))
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(rows).To(HaveLen(1))
			if op := rows[0].Operation; op != nil {
				g.Expect(op.Waiting).NotTo(Equal(workloadtypes.WaitingReasonCapacityProvisioning),
					"the provisioning hold must lift once the pod exists")
			}
		}).WithTimeout(waitTimeout).WithPolling(pollInterval).Should(Succeed())
	})

	ginkgo.It("confines every pod of a multi-pod Instance to one slice", func() {
		ns := createNamespace("tpuslice-gang")
		// The leader is at ordinal 0 and the workers at 0, 1 and 2: one
		// slice serves pods at different ordinals.
		ir := sliceIR("mixtral", ns, onDemandAccelerator, "2x2x4", 4, 3)
		Expect(k8sClient.Create(suiteCtx, ir)).To(Succeed())

		name := awaitOwnedSlices(ir, 1)[0]
		topology, _, err := unstructured.NestedString(getSlice(Default, name).Object, "spec", "topology")
		Expect(err).NotTo(HaveOccurred())
		Expect(topology).To(Equal("2x2x4"))
		Expect(listPods(Default, ns)).To(BeEmpty())

		reportSliceState(name, stateDegraded, "one partition is degraded")
		for _, pod := range awaitPods(ns, 4) {
			expectConfinedTo(pod, name, "2x2x4")
		}
		Expect(awaitOwnedSlices(ir, 1)).To(Equal([]string{name}))
	})

	ginkgo.It("releases every slice once the pods are gone and removes the InferenceReplica only after they are", func() {
		ns := createNamespace("tpuslice-teardown")
		ir := sliceIR("gemma", ns, onDemandAccelerator, "2x2x1", 4, 0)
		Expect(k8sClient.Create(suiteCtx, ir)).To(Succeed())
		name := awaitOwnedSlices(ir, 1)[0]
		setFinalizer(gke.NewObject(), client.ObjectKey{Name: name}, sliceFinalizer, true)
		reportSliceState(name, stateActive, "")
		pod := awaitPods(ns, 1)[0]
		expectConfinedTo(pod, name, "2x2x1")

		ginkgo.By("deleting the InferenceReplica while its pod is still shutting down")
		setFinalizer(&corev1.Pod{}, client.ObjectKeyFromObject(&pod), podFinalizer, true)
		Expect(k8sClient.Delete(suiteCtx, ir)).To(Succeed())
		Eventually(func(g Gomega) {
			fresh := &corev1.Pod{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(&pod), fresh)).To(Succeed())
			g.Expect(fresh.DeletionTimestamp).NotTo(BeNil(), "teardown must delete the pod")
		}).WithTimeout(waitTimeout).WithPolling(pollInterval).Should(Succeed())

		ginkgo.By("provisioning a slice for an Instance that has no status row")
		rowless := provisionForRowlessInstance(ir, 1)

		Consistently(func(g Gomega) {
			g.Expect(getSlice(g, name).GetDeletionTimestamp()).To(BeNil(),
				"a slice must not be released while a pod holds it")
			expectIRPresent(g, ir, "the InferenceReplica must stay while its pod remains")
		}).WithTimeout(holdWindow).WithPolling(pollInterval).Should(Succeed())

		ginkgo.By("releasing every slice once the pod is gone")
		setFinalizer(&corev1.Pod{}, client.ObjectKeyFromObject(&pod), podFinalizer, false)
		Eventually(func(g Gomega) {
			g.Expect(getSlice(g, name).GetDeletionTimestamp()).NotTo(BeNil(),
				"the slice must be released once no pod holds it")
			err := k8sClient.Get(suiteCtx, client.ObjectKey{Name: rowless}, gke.NewObject())
			g.Expect(apierrors.IsNotFound(err)).To(BeTrue(),
				"the slice of the Instance without a row must be released, got %v", err)
		}).WithTimeout(waitTimeout).WithPolling(pollInterval).Should(Succeed())

		ginkgo.By("keeping the InferenceReplica until the provider has freed the slice")
		Consistently(func(g Gomega) {
			expectIRPresent(g, ir, "the InferenceReplica must stay while its slice is terminating")
		}).WithTimeout(holdWindow).WithPolling(pollInterval).Should(Succeed())
		setFinalizer(gke.NewObject(), client.ObjectKey{Name: name}, sliceFinalizer, false)
		Eventually(func(g Gomega) {
			err := k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(ir), &v1beta1.InferenceReplica{})
			g.Expect(apierrors.IsNotFound(err)).To(BeTrue(), "the InferenceReplica must be removed, got %v", err)
		}).WithTimeout(waitTimeout).WithPolling(pollInterval).Should(Succeed())
		Expect(ownedSlices(Default, ir)).To(BeEmpty())
	})

	ginkgo.It("releases the slice of a scaled-in Instance and keeps the others", func() {
		ns := createNamespace("tpuslice-scale-in")
		ir := sliceIR("qwen", ns, onDemandAccelerator, "2x2x1", 4, 0)
		ir.Spec.Replicas = ptr.To(int32(2))
		Expect(k8sClient.Create(suiteCtx, ir)).To(Succeed())
		names := awaitOwnedSlices(ir, 2)
		for _, name := range names {
			reportSliceState(name, stateActive, "")
		}
		pods := awaitPods(ns, 2)
		confined := map[string]bool{}
		for _, pod := range pods {
			confined[pod.Spec.NodeSelector[sliceConfig.NodeLabels.Slice]] = true
		}
		Expect(confined).To(Equal(map[string]bool{names[0]: true, names[1]: true}),
			"each Instance's pod must be confined to its own slice")

		ginkgo.By("scaling in to one Instance")
		Eventually(func(g Gomega) {
			fresh := &v1beta1.InferenceReplica{}
			g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(ir), fresh)).To(Succeed())
			fresh.Spec.Replicas = ptr.To(int32(1))
			g.Expect(k8sClient.Update(suiteCtx, fresh)).To(Succeed())
		}).WithTimeout(waitTimeout).WithPolling(pollInterval).Should(Succeed())
		Eventually(func(g Gomega) {
			owned := ownedSlices(g, ir)
			g.Expect(owned).To(HaveLen(1))
			remaining := listPods(g, ns)
			g.Expect(remaining).To(HaveLen(1))
			g.Expect(remaining[0].Spec.NodeSelector).To(
				HaveKeyWithValue(sliceConfig.NodeLabels.Slice, owned[0].GetName()),
				"the remaining Instance must keep its slice")
		}).WithTimeout(waitTimeout).WithPolling(pollInterval).Should(Succeed())
	})

	ginkgo.It("provisions no slice for pods on a static pool", func() {
		ns := createNamespace("tpuslice-static")
		ir := sliceIR("phi", ns, staticAccelerator, "2x2", 4, 0)
		Expect(k8sClient.Create(suiteCtx, ir)).To(Succeed())

		pod := awaitPods(ns, 1)[0]
		Expect(pod.Spec.NodeSelector).To(Equal(map[string]string{
			sliceConfig.NodeLabels.Accelerator: staticAccelerator,
			sliceConfig.NodeLabels.Topology:    "2x2",
		}), "a static pool's pod must not be confined to a slice")
		Expect(ownedSlices(Default, ir)).To(BeEmpty())
	})
})
