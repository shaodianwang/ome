// This file is the multi-cluster scaffolding shared by the suites in this
// directory tree: kubeconfig Secret and WorkloadCluster construction, plus the
// derived-ISVC and placement-status accessors the specs assert against. It is
// the only non-test file in the package, so the nested KIND suites can import
// it while this package's own _test.go files call it unqualified.
//
// Every helper takes (ctx, client) rather than a suite's own cluster struct, so
// the same code serves an envtest apiserver (this package) and a KIND member
// (scaleout_kind). Suite-specific scaffolding — how a cluster is stood up, how
// admission is produced or faked, and the ISVC shapes a given suite needs —
// stays in the suite.

package multicluster

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/placement"
	"github.com/ome-projects/ome/tests/integration/util"
)

// KubeConfigSecretKey is the Secret data key the WorkloadCluster kubeConfig
// source reads. It matches the CRD's `kubeConfig.key` default.
const KubeConfigSecretKey = "kubeconfig"

// KubeConfigSecret serializes a member's rest.Config into the kubeconfig Secret
// the control plane's WorkloadCluster reconciler reads to connect to it. Only
// the fields the connection validator accepts are emitted (inline CA and client
// cert/key, no file paths or exec plugin) — true of both an envtest rest.Config
// and the one `kind get kubeconfig` yields.
func KubeConfigSecret(memberCfg *rest.Config, namespace, name string) (*corev1.Secret, error) {
	raw, err := util.RestConfigToKubeConfig(memberCfg)
	if err != nil {
		return nil, fmt.Errorf("multicluster: serialize member kubeconfig: %w", err)
	}
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Data:       map[string][]byte{KubeConfigSecretKey: raw},
	}, nil
}

// WorkloadClusterFor builds a WorkloadCluster CR pointing at a kubeconfig
// Secret, carrying the capability labels an ISVC's placement requirements are
// matched against.
func WorkloadClusterFor(name, secretNamespace, secretName string, labels map[string]string) *v1beta1.WorkloadCluster {
	return &v1beta1.WorkloadCluster{
		ObjectMeta: metav1.ObjectMeta{Name: name, Labels: labels},
		Spec: v1beta1.WorkloadClusterSpec{
			ClusterSource: v1beta1.ClusterConnectionSource{
				KubeConfig: &v1beta1.KubeConfigSource{
					SecretRef: corev1.SecretReference{Name: secretName, Namespace: secretNamespace},
					Key:       KubeConfigSecretKey,
				},
			},
		},
	}
}

// CreateNamespace creates a namespace, treating an existing one as success so
// specs sharing a cluster can each ensure their namespace.
func CreateNamespace(ctx context.Context, c client.Client, name string) error {
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}}
	if err := c.Create(ctx, ns); err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("multicluster: create namespace %q: %w", name, err)
	}
	return nil
}

// GetDerived fetches the derived InferenceService the placer created on a
// member. A NotFound is reported as ok=false with a nil error — the expected
// "not placed (yet)" answer. Any other error is returned so a caller polling
// inside an Eventually can distinguish absence from an unhealthy apiserver
// rather than silently retrying against a broken connection.
func GetDerived(ctx context.Context, c client.Client, namespace, name string) (*v1beta1.InferenceService, bool, error) {
	isvc := &v1beta1.InferenceService{}
	err := c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, isvc)
	switch {
	case apierrors.IsNotFound(err):
		return nil, false, nil
	case err != nil:
		return nil, false, fmt.Errorf("multicluster: get derived ISVC %s/%s: %w", namespace, name, err)
	}
	return isvc, true, nil
}

// DerivedSwept reports whether the placer has torn down its derived ISVC on a
// member. A derived still present but carrying a DeletionTimestamp counts as
// swept: the placer's contract is that it issued the delete, while the member's
// own finalizer teardown is eventually-consistent and can lag.
func DerivedSwept(ctx context.Context, c client.Client, namespace, name string) bool {
	derived, ok, err := GetDerived(ctx, c, namespace, name)
	if err != nil {
		// A read failure is not evidence of a sweep; report "still there" so the
		// caller keeps polling instead of declaring teardown complete.
		return false
	}
	return !ok || derived.DeletionTimestamp != nil
}

// PlacementStatus reads a source ISVC's placement status from the control
// plane, or nil when the ISVC or the status block is absent.
func PlacementStatus(ctx context.Context, c client.Client, namespace, name string) *v1beta1.PlacementStatus {
	isvc := &v1beta1.InferenceService{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, isvc); err != nil {
		return nil
	}
	return isvc.Status.Placement
}

// CandidateClusters returns the cluster names recorded in a placement status.
func CandidateClusters(ps *v1beta1.PlacementStatus) []string {
	if ps == nil {
		return nil
	}
	out := make([]string, 0, len(ps.Candidates))
	for _, c := range ps.Candidates {
		out = append(out, c.Cluster)
	}
	return out
}

// OriginUID returns the source-UID stamp the placer wrote on a derived, which is
// what the cross-cluster origin guards key on. Empty for a nil derived or an
// object the placer did not create.
func OriginUID(derived *v1beta1.InferenceService) string {
	if derived == nil {
		return ""
	}
	return derived.Annotations[placement.PlacementOriginUIDAnnotation]
}
