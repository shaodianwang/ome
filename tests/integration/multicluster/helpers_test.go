package multicluster

import (
	"fmt"
	"sync/atomic"

	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/placement"
	"github.com/ome-projects/ome/tests/integration/util"
)

// kubeConfigSecretKey is the Secret data key the WorkloadCluster kubeConfig
// source reads. It matches the CRD's `kubeConfig.key` default.
const kubeConfigSecretKey = "kubeconfig"

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
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Data:       map[string][]byte{kubeConfigSecretKey: kubeConfigBytesFor(member)},
	}
}

// rotateSecret overwrites a kubeconfig Secret's data on the given cluster,
// retrying on conflict — used to simulate kubeconfig rotation.
func rotateSecret(c cluster, namespace, name string, kubeconfig []byte) {
	gomega.EventuallyWithOffset(1, func(g gomega.Gomega) {
		s := &corev1.Secret{}
		g.Expect(c.client.Get(c.ctx, types.NamespacedName{Namespace: namespace, Name: name}, s)).To(gomega.Succeed())
		s.Data = map[string][]byte{kubeConfigSecretKey: kubeconfig}
		g.Expect(c.client.Update(c.ctx, s)).To(gomega.Succeed())
	}, eventuallyTimeout, eventuallyPoll).Should(gomega.Succeed())
}

// workloadClusterFor builds a WorkloadCluster CR pointing at a kubeconfig Secret,
// carrying the given labels (matched against an ISVC's accelerator requirements).
func workloadClusterFor(name, secretNamespace, secretName string, labels map[string]string) *v1beta1.WorkloadCluster {
	return &v1beta1.WorkloadCluster{
		ObjectMeta: metav1.ObjectMeta{Name: name, Labels: labels},
		Spec: v1beta1.WorkloadClusterSpec{
			ClusterSource: v1beta1.ClusterConnectionSource{
				KubeConfig: &v1beta1.KubeConfigSource{
					SecretRef: corev1.SecretReference{Name: secretName, Namespace: secretNamespace},
					Key:       kubeConfigSecretKey,
				},
			},
		},
	}
}

// sourceISVC builds a minimal control-plane source InferenceService that declares
// a single Engine component and the accelerator requirement selector that makes
// it eligible for fan-out onto matching candidate clusters.
func sourceISVC(namespace, name, acceleratorRequirements string) *v1beta1.InferenceService {
	return &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Annotations: map[string]string{
				placement.AcceleratorRequirementsAnnotation: acceleratorRequirements,
			},
		},
		Spec: v1beta1.InferenceServiceSpec{
			Engine: &v1beta1.EngineSpec{
				Runner: &v1beta1.RunnerSpec{
					Container: corev1.Container{Name: "ome-container", Image: "example/model-server:test"},
				},
			},
		},
	}
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
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}}
	err := c.client.Create(c.ctx, ns)
	if err != nil && !apierrors.IsAlreadyExists(err) {
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "create namespace %q", name)
	}
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
	derived := &v1beta1.InferenceService{}
	err := member.client.Get(member.ctx, types.NamespacedName{Namespace: namespace, Name: name}, derived)
	if apierrors.IsNotFound(err) {
		return nil, false
	}
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), "get derived ISVC on member")
	return derived, true
}

// admitDerivedOn simulates the member-side admission path: it marks the derived
// ISVC's Engine instance Admitted in status, which is the signal the control
// plane's placer reads to declare this cluster the race winner. No OME reconciler
// runs on the members, so the suite plays the role Kueue + the worker reconciler
// would in production.
func admitDerivedOn(member cluster, namespace, name string) {
	gomega.EventuallyWithOffset(1, func(g gomega.Gomega) {
		derived := &v1beta1.InferenceService{}
		g.Expect(member.client.Get(member.ctx, types.NamespacedName{Namespace: namespace, Name: name}, derived)).To(gomega.Succeed())
		derived.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
			v1beta1.EngineComponent: {
				Lifecycle: &v1beta1.LifecycleStatus{
					InstanceStatuses: []v1beta1.OMENativeInstanceStatus{
						{Index: 0, Phase: v1beta1.OMENativeInstanceReady, Admitted: true},
					},
				},
			},
		}
		g.Expect(member.client.Status().Update(member.ctx, derived)).To(gomega.Succeed())
	}, eventuallyTimeout, eventuallyPoll).Should(gomega.Succeed())
}

// placementStatusOf reads the source ISVC's placement status from the control
// cluster, returning nil when not yet written.
func placementStatusOf(namespace, name string) *v1beta1.PlacementStatus {
	isvc := &v1beta1.InferenceService{}
	if err := controlCluster.client.Get(controlCluster.ctx, types.NamespacedName{Namespace: namespace, Name: name}, isvc); err != nil {
		return nil
	}
	return isvc.Status.Placement
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
