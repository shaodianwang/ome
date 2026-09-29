package multicluster

import (
	"fmt"
	"strings"
	"sync/atomic"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/irprojector"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/placement"
	"github.com/ome-projects/ome/tests/integration/util"
)

// kubeConfigBytesFor serializes a member cluster's rest.Config to kubeconfig bytes.
func kubeConfigBytesFor(member cluster) []byte {
	raw, err := util.RestConfigToKubeConfig(member.cfg)
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), "RestConfigToKubeConfig")
	return raw
}

// kubeconfigSecretFor serializes a member cluster's rest.Config into a kubeconfig
// Secret on the control cluster. The control plane's WorkloadCluster reconciler
// reads this Secret to connect to the member.
func kubeconfigSecretFor(member cluster, namespace, name string) *corev1.Secret {
	s, err := KubeConfigSecret(member.cfg, namespace, name)
	gomega.ExpectWithOffset(1, err).NotTo(gomega.HaveOccurred(), "kubeconfig Secret for member")
	return s
}

// rotateSecret overwrites a kubeconfig Secret's data on the given cluster,
// retrying on conflict — used to simulate kubeconfig rotation.
func rotateSecret(c cluster, namespace, name string, kubeconfig []byte) {
	gomega.EventuallyWithOffset(1, func(g gomega.Gomega) {
		s := &corev1.Secret{}
		g.Expect(c.client.Get(c.ctx, types.NamespacedName{Namespace: namespace, Name: name}, s)).To(gomega.Succeed())
		s.Data = map[string][]byte{KubeConfigSecretKey: kubeconfig}
		g.Expect(c.client.Update(c.ctx, s)).To(gomega.Succeed())
	}, eventuallyTimeout, eventuallyPoll).Should(gomega.Succeed())
}

const placementRuntimeName = "placement-runtime"

// sourceISVC declares Single placement with one Engine and label affinity.
func sourceISVC(namespace, name, acceleratorRequirements string) *v1beta1.InferenceService {
	return &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: v1beta1.InferenceServiceSpec{
			DeploymentMode: ptr.To(constants.OMENative),
			Runtime:        &v1beta1.ServingRuntimeRef{Name: placementRuntimeName},
			Placement:      &v1beta1.PlacementSpec{Policy: v1beta1.PlacementPolicyClusterAffinity, Mode: v1beta1.PlacementModeSingle, ClusterAffinity: affinityForRequirement(acceleratorRequirements)},
			Engine: &v1beta1.EngineSpec{
				Runner: &v1beta1.RunnerSpec{
					Container: corev1.Container{Name: "ome-container", Image: "example/model-server:test"},
				},
			},
		},
	}
}

// affinityForRequirement converts the equality requirement used by these fixtures.
func affinityForRequirement(requirement string) []v1beta1.ClusterAffinityTerm {
	key, value, ok := strings.Cut(requirement, "=")
	gomega.ExpectWithOffset(1, ok && key != "" && value != "").To(gomega.BeTrue(), "fixture requires key=value")
	return []v1beta1.ClusterAffinityTerm{{MatchExpressions: []v1beta1.ClusterSelectorRequirement{{
		Key: key, Operator: corev1.NodeSelectorOpIn, Values: []string{value},
	}}}}
}

// keeperISVC builds a minimal control-plane InferenceService with no accelerator
// requirement, so the placer finds no candidates and never fans it out. It exists
// only to keep the GC sweep's live-UID set non-empty (the sweep treats an empty
// set as suspect and skips, so an orphan would never be reaped without one).
func keeperISVC(namespace, name string) *v1beta1.InferenceService {
	return &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec: v1beta1.InferenceServiceSpec{
			Engine: &v1beta1.EngineSpec{
				Runner: &v1beta1.RunnerSpec{
					Container: corev1.Container{Name: "ome-container", Image: "example/model-server:test"},
				},
			},
		},
	}
}

// orphanDerived builds an InferenceService shaped like a placer-created derived
// (origin label + origin-UID annotation + control-plane stamp), for direct
// creation on a member to exercise the GC sweep. originUID is the source UID the
// GC checks against the live control-plane set — a UID with no live source makes
// it an orphan; controlPlaneID is the owning control plane (the GC reaps only its
// own deriveds).
func orphanDerived(namespace, name, controlPlaneID, originUID string) *v1beta1.InferenceService {
	d := keeperISVC(namespace, name)
	d.Labels = map[string]string{
		placement.PlacementOriginLabel:       originUID,
		placement.PlacementControlPlaneLabel: controlPlaneID,
	}
	d.Annotations = map[string]string{
		placement.PlacementOriginUIDAnnotation: originUID,
	}
	return d
}

// createNamespace creates a namespace on the given cluster (idempotent: an
// already-existing namespace is fine).
func createNamespace(c cluster, name string) {
	gomega.ExpectWithOffset(1, CreateNamespace(c.ctx, c.client, name)).
		To(gomega.Succeed(), "create namespace %q", name)
}

// mustCreate creates an object and fails the spec on error.
func mustCreate(c cluster, obj client.Object) {
	gomega.ExpectWithOffset(1, c.client.Create(c.ctx, obj)).To(gomega.Succeed())
}

// deleteIfExists best-effort deletes an object (used in AfterEach cleanup).
func deleteIfExists(c cluster, obj client.Object) {
	_ = c.client.Delete(c.ctx, obj)
}

// getDerivedOn fetches the derived InferenceService that the placer created on a
// member cluster, returning (nil, false) when it is absent.
func getDerivedOn(member cluster, namespace, name string) (*v1beta1.InferenceService, bool) {
	derived, ok, err := GetDerived(member.ctx, member.client, namespace, name)
	gomega.ExpectWithOffset(1, err).NotTo(gomega.HaveOccurred(), "get derived ISVC on member")
	return derived, ok
}

// admitDerivedOn simulates the member-side admission path: it stamps the Engine
// component's InferenceReplica (<name>-engine) with an admitted instance, which
// is the authoritative signal the control plane's placer reads to declare this
// cluster the race winner. No OME reconciler runs on the members, so the suite
// plays the role Kueue + the worker reconciler would in production.
func admitDerivedOn(member cluster, namespace, name string) {
	irName := name + "-" + string(v1beta1.EngineComponent)
	gomega.EventuallyWithOffset(1, func(g gomega.Gomega) {
		ir := &v1beta1.InferenceReplica{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: irName},
			// Minimal valid spec so the Create passes CRD schema validation on
			// bare members (the sim path has no manager to pre-create the IR).
			Spec: v1beta1.InferenceReplicaSpec{
				ParentRef: v1beta1.ParentReference{Name: name},
				Component: v1beta1.EngineComponent,
				Runners: []v1beta1.Runner{{
					Name: v1beta1.RunnerNameDefault,
					Size: 1,
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: "sim:latest"}}},
					},
				}},
			},
		}
		g.Expect(client.IgnoreAlreadyExists(member.client.Create(member.ctx, ir))).To(gomega.Succeed())
		g.Expect(member.client.Get(member.ctx, types.NamespacedName{Namespace: namespace, Name: irName}, ir)).To(gomega.Succeed())
		g.Expect(util.WriteDenseInstanceStatuses(member.ctx, member.client, ir, []v1beta1.OMENativeInstanceStatus{
			{Index: 0, Phase: v1beta1.OMENativeInstanceReady, Admitted: true},
		})).To(gomega.Succeed())
	}, eventuallyTimeout, eventuallyPoll).Should(gomega.Succeed())
}

// placementStatusOf reads the source ISVC's placement status from the control
// cluster, returning nil when not yet written.
func placementStatusOf(namespace, name string) *v1beta1.PlacementStatus {
	return PlacementStatus(controlCluster.ctx, controlCluster.client, namespace, name)
}

// skipIfRealKueue skips a sim-path spec when the real-Kueue member-manager path
// is enabled. The two paths are mutually exclusive: in the real-Kueue path the
// members run real managers that write derived status, so the sim-path
// admitDerivedOn fake (a second writer of the same status) must not run.
func skipIfRealKueue() {
	if realKueueEnabled() {
		ginkgo.Skip(realKueueEnvVar + "=1: real-Kueue member managers own derived status; skipping sim-path spec")
	}
}

// skipUnlessRealKueue skips a real-Kueue spec when the gate is off (the members
// are bare in that mode, so there is no real reconcile→Kueue chain to exercise).
func skipUnlessRealKueue() {
	if !realKueueEnabled() {
		ginkgo.Skip(realKueueEnvVar + " not set: member managers not running; skipping real-Kueue spec")
	}
}

// nameCounter backs uniqueName so every spec gets a distinct, DNS-safe resource
// name. The controllers watch ALL InferenceServices / WorkloadClusters on the
// control cluster, so specs that reuse a name would otherwise observe each
// other's objects.
var nameCounter atomic.Int64

// uniqueName builds a per-spec unique, DNS-1123-safe resource name.
func uniqueName(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, nameCounter.Add(1))
}

// upsertPlannedEngineStatusOn uses the member projector to preserve ownership
// and placement authority, then simulates admission on the bare member API.
func upsertPlannedEngineStatusOn(member cluster, namespace, name string, instances []v1beta1.OMENativeInstanceStatus) {
	gomega.EventuallyWithOffset(1, func(g gomega.Gomega) {
		derived := &v1beta1.InferenceService{}
		g.Expect(member.client.Get(member.ctx, client.ObjectKey{Namespace: namespace, Name: name}, derived)).To(gomega.Succeed())
		g.Expect(derived.Spec.Engine).NotTo(gomega.BeNil())
		ir, err := irprojector.EnsureInferenceReplica(member.ctx, irprojector.Params{
			Client: member.client, ISVC: derived, Component: v1beta1.EngineComponent,
			ComponentExt: &derived.Spec.Engine.ComponentExtensionSpec,
			ObjectMeta:   metav1.ObjectMeta{Labels: map[string]string{constants.InferenceServicePodLabelKey: name}},
			PodSpec:      &corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: "example/model-server:test"}}},
		})
		g.Expect(err).NotTo(gomega.HaveOccurred())
		ir.Status.ObservedGeneration = ir.Generation
		ir.Status.PlacementObservedGeneration = ir.Generation
		g.Expect(util.WriteDenseInstanceStatuses(member.ctx, member.client, ir, instances)).To(gomega.Succeed())
	}, eventuallyTimeout, eventuallyPoll).Should(gomega.Succeed())
}
