package scaleout_kind

import (
	"fmt"
	"os/exec"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/tests/integration/multicluster"
	"github.com/ome-projects/ome/tests/integration/util"
)

// acceleratorLabel/Value is the capability both members advertise and every
// source ISVC selects through explicit placement affinity.
const (
	acceleratorLabel = "gpu"
	acceleratorValue = "test-accel"
)

// nameCounter makes per-spec object names unique within a suite run.
var nameCounter atomic.Int64

// runID disambiguates object names ACROSS runs, not just within one. The KIND
// members are reused between `go test` invocations (KIND_KEEP_CLUSTER), so a
// cluster-scoped ClusterBaseModel or ClusterServingRuntime left behind by an
// aborted run would collide with a plain per-process counter. Base-36 seconds
// keeps the suffix short enough to stay well inside the name length limit.
var runID = strconv.FormatInt(time.Now().Unix(), 36)

// uniqueName returns a name unique to this spec AND this run.
func uniqueName(prefix string) string {
	return fmt.Sprintf("%s-%s-%d", prefix, runID, nameCounter.Add(1))
}

// memberLabels is the label set selected by source placement affinity.
func memberLabels() map[string]string {
	return map[string]string{acceleratorLabel: acceleratorValue}
}

// kubeconfigSecretFor is the cluster-typed wrapper over multicluster.KubeConfigSecret.
func kubeconfigSecretFor(member cluster, namespace, name string) *corev1.Secret {
	s, err := multicluster.KubeConfigSecret(member.cfg, namespace, name)
	gomega.ExpectWithOffset(1, err).NotTo(gomega.HaveOccurred(), "kubeconfig Secret for %s", member.name)
	return s
}

// runnableSourceISVC builds a control-plane source ISVC that a member can
// actually run: OMENative deployment mode (so the member dispatches through the
// InferenceReplica path and creates pods), the fake-serving image, and the model
// + runtime the members have. mode selects the placement cardinality;
// label affinity makes both members candidates.
func runnableSourceISVC(namespace, name, model, runtime string, mode v1beta1.PlacementMode, replicas int) *v1beta1.InferenceService {
	return &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   namespace,
			Annotations: map[string]string{constants.DeploymentMode: string(constants.OMENative)},
		},
		Spec: v1beta1.InferenceServiceSpec{
			Placement: &v1beta1.PlacementSpec{
				Mode:            mode,
				ClusterAffinity: []v1beta1.ClusterAffinityTerm{{MatchExpressions: []v1beta1.ClusterSelectorRequirement{{Key: acceleratorLabel, Operator: corev1.NodeSelectorOpIn, Values: []string{acceleratorValue}}}}},
			},
			Model:   &v1beta1.ModelRef{Name: model},
			Runtime: &v1beta1.ServingRuntimeRef{Name: runtime},
			Engine: &v1beta1.EngineSpec{
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
					// Per-Component OMENative annotation is the highest-priority
					// mode selector, so the engine takes the InferenceReplica path
					// on the member rather than emitting a Deployment.
					Annotations: map[string]string{constants.DeploymentMode: string(constants.OMENative)},
					MinReplicas: &replicas,
				},
				PodSpec: v1beta1.PodSpec{
					Containers: []corev1.Container{{
						Name:  constants.MainContainerName,
						Image: "fake-serving:v1",
					}},
				},
			},
		},
	}
}

// createNamespace creates a namespace on a cluster, tolerating one that already
// exists.
func createNamespace(c cluster, name string) {
	gomega.ExpectWithOffset(1, multicluster.CreateNamespace(c.ctx, c.client, name)).
		To(gomega.Succeed(), "create namespace %q on %s", name, c.name)
}

// mustCreate creates an object and fails the spec on error.
func mustCreate(c cluster, obj client.Object) {
	gomega.ExpectWithOffset(1, c.client.Create(c.ctx, obj)).To(gomega.Succeed())
}

// deleteIfExists best-effort deletes an object, for cleanup paths.
func deleteIfExists(c cluster, obj client.Object) {
	_ = c.client.Delete(c.ctx, obj)
}

// getDerivedOn fetches the derived ISVC the placer created on a member, failing
// the spec if the member's apiserver errors on anything but NotFound.
func getDerivedOn(member cluster, namespace, name string) (*v1beta1.InferenceService, bool) {
	derived, ok, err := multicluster.GetDerived(member.ctx, member.client, namespace, name)
	gomega.ExpectWithOffset(1, err).NotTo(gomega.HaveOccurred(), "get derived ISVC on %s", member.name)
	return derived, ok
}

// derivedSweptOn reports whether the placer has torn down its derived on a
// member (deleted, or Terminating while the member finalizes it).
func derivedSweptOn(member cluster, namespace, name string) bool {
	return multicluster.DerivedSwept(member.ctx, member.client, namespace, name)
}

// placementStatusOf reads the source ISVC's placement status from the control
// plane.
func placementStatusOf(namespace, name string) *v1beta1.PlacementStatus {
	return multicluster.PlacementStatus(controlCluster.ctx, controlCluster.client, namespace, name)
}

// enginePodsOn lists the OMENative engine pods a member created for an ISVC.
//
// It asserts the List succeeded through the caller's Gomega rather than
// swallowing the error. Returning an empty slice on failure would make "no pods"
// indistinguishable from "could not ask", so a member whose apiserver is down
// would satisfy every pods-are-gone assertion in this suite. Inside an
// Eventually the assertion simply retries; at the deadline it reports the API
// error instead of a bare count mismatch.
func enginePodsOn(g gomega.Gomega, member cluster, namespace, isvcName string) []corev1.Pod {
	pods := &corev1.PodList{}
	g.Expect(member.client.List(member.ctx, pods,
		client.InNamespace(namespace),
		client.MatchingLabels{
			util.LabelManagedByKey:          util.ManagedByOMENativeValue,
			constants.InferenceServiceLabel: isvcName,
		})).To(gomega.Succeed(), "list engine pods on %s", member.name)
	return pods.Items
}

// runningEnginePodsOn counts the engine pods a real kubelet has taken all the
// way to Running on a member.
func runningEnginePodsOn(g gomega.Gomega, member cluster, namespace, isvcName string) int {
	var n int
	for _, p := range enginePodsOn(g, member, namespace, isvcName) {
		if p.Status.Phase == corev1.PodRunning {
			n++
		}
	}
	return n
}

// readyEnginePodsOn counts the engine pods a member reports Ready — a stronger
// claim than Running, since only a passing readiness probe gets a pod there.
func readyEnginePodsOn(g gomega.Gomega, member cluster, namespace, isvcName string) int {
	var n int
	for _, p := range enginePodsOn(g, member, namespace, isvcName) {
		for _, cond := range p.Status.Conditions {
			if cond.Type == corev1.PodReady && cond.Status == corev1.ConditionTrue {
				n++
			}
		}
	}
	return n
}

// engineImagesOn returns the main-container image of every engine pod on a
// member, for asserting that a spec change actually rolled the workload.
func engineImagesOn(g gomega.Gomega, member cluster, namespace, isvcName string) []string {
	var out []string
	for _, p := range enginePodsOn(g, member, namespace, isvcName) {
		for _, ctr := range p.Spec.Containers {
			if ctr.Name == constants.MainContainerName {
				out = append(out, ctr.Image)
			}
		}
	}
	return out
}

// rotateSecretTo overwrites a kubeconfig Secret's payload, retrying on conflict.
// Writing garbage makes the WorkloadCluster reconciler reject the kubeconfig and
// disconnect the member; writing the real bytes back restores it.
func rotateSecretTo(c cluster, namespace, name string, kubeconfig []byte) {
	gomega.EventuallyWithOffset(1, func(g gomega.Gomega) {
		s := &corev1.Secret{}
		g.Expect(c.client.Get(c.ctx, types.NamespacedName{Namespace: namespace, Name: name}, s)).To(gomega.Succeed())
		s.Data = map[string][]byte{multicluster.KubeConfigSecretKey: kubeconfig}
		g.Expect(c.client.Update(c.ctx, s)).To(gomega.Succeed())
	}).WithTimeout(mediumTimeout).WithPolling(pollInterval).Should(gomega.Succeed())
}

// kubeconfigBytesFor re-serializes a member's rest.Config, for restoring a
// Secret that a spec deliberately corrupted.
func kubeconfigBytesFor(member cluster) []byte {
	s, err := multicluster.KubeConfigSecret(member.cfg, "", "")
	gomega.ExpectWithOffset(1, err).NotTo(gomega.HaveOccurred(), "serialize kubeconfig for %s", member.name)
	return s.Data[multicluster.KubeConfigSecretKey]
}

// workloadClusterReady reports the Ready condition the control plane currently
// records for a registered member.
func workloadClusterReady(wcName string) bool {
	wc := &v1beta1.WorkloadCluster{}
	if err := controlCluster.client.Get(controlCluster.ctx, types.NamespacedName{Name: wcName}, wc); err != nil {
		return false
	}
	return apimeta.IsStatusConditionTrue(wc.Status.Conditions, v1beta1.WorkloadClusterReady)
}

// nodeContainer is the Docker container backing a single-node KIND cluster's
// only node. Pausing it makes the member's apiserver stop answering WITHOUT
// changing its registration — the transient blip a probe-failure grace window
// exists to ride out, which no envtest suite can produce.
func nodeContainer(kindClusterName string) string {
	return kindClusterName + "-control-plane"
}

// pauseNode/unpauseNode freeze and thaw a member's node container. Keep pauses
// well under the WorkloadCluster connection grace period, or the control plane
// stops treating the member as a candidate and the placement legitimately moves.
func pauseNode(kindClusterName string) {
	out, err := exec.Command("docker", "pause", nodeContainer(kindClusterName)).CombinedOutput()
	gomega.ExpectWithOffset(1, err).NotTo(gomega.HaveOccurred(), "docker pause: %s", out)
}

func unpauseNode(kindClusterName string) {
	out, err := exec.Command("docker", "unpause", nodeContainer(kindClusterName)).CombinedOutput()
	gomega.ExpectWithOffset(1, err).NotTo(gomega.HaveOccurred(), "docker unpause: %s", out)
}

// derivedOfSourceSwept reports whether nothing belonging to srcUID remains on a
// member: the object is gone, is Terminating, or is present but is NOT ours.
// The last case matters for specs that deliberately park a member-owned object
// at the placed name — waiting for THAT to disappear would wait forever, since
// the origin guard is supposed to leave it alone.
func derivedOfSourceSwept(member cluster, namespace, name, srcUID string) bool {
	derived, ok := getDerivedOn(member, namespace, name)
	if !ok || derived.DeletionTimestamp != nil {
		return true
	}
	return multicluster.OriginUID(derived) != srcUID
}

// labelNamespace stamps a label onto an existing namespace, retrying on
// conflict. Used to opt a spec's workload namespace into Kueue management.
func labelNamespace(c cluster, name, key, value string) {
	gomega.EventuallyWithOffset(1, func(g gomega.Gomega) {
		ns := &corev1.Namespace{}
		g.Expect(c.client.Get(c.ctx, types.NamespacedName{Name: name}, ns)).To(gomega.Succeed())
		if ns.Labels == nil {
			ns.Labels = map[string]string{}
		}
		ns.Labels[key] = value
		g.Expect(c.client.Update(c.ctx, ns)).To(gomega.Succeed())
	}).WithTimeout(mediumTimeout).WithPolling(pollInterval).Should(gomega.Succeed(),
		"label namespace %s on %s", name, c.name)
}
