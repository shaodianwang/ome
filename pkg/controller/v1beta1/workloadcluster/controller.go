package workloadcluster

import (
	"context"
	"fmt"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

// DefaultHealthInterval is the fallback re-probe cadence used when
// Reconciler.HealthInterval is unset — how often a WorkloadCluster is re-probed
// for reachability when nothing else triggers a reconcile. The operative value
// is supplied by the manager flag / chart; this is the graceful-degradation
// default.
const DefaultHealthInterval = time.Minute

// +kubebuilder:rbac:groups=ome.io,resources=workloadclusters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=ome.io,resources=workloadclusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch

// Reconciler reconciles a WorkloadCluster: it resolves the cluster's
// kubeconfig, probes reachability, and stamps the Ready condition. It does NOT
// read capacity (OEP-0018) — that is a placer concern in a later phase.
type Reconciler struct {
	client.Client
	Scheme *runtime.Scheme
	Log    logr.Logger
	// Probe reports whether the cluster described by these kubeconfig bytes is
	// reachable. Defaults to probeViaServerVersion; overridden in tests.
	Probe func(ctx context.Context, kubeconfig []byte) error
	// HealthInterval is the re-probe cadence; defaults to DefaultHealthInterval.
	HealthInterval time.Duration
	// Manager holds the live per-cluster clients; the reconciler connects a
	// cluster when it becomes Reachable and disconnects it on removal. Optional
	// (nil-safe) so the health-only path keeps working without transport.
	Manager *Manager
	// ExecPolicy is the exec credential policy applied during validation.
	ExecPolicy ExecCredentialPolicy
}

func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	wc := &v1beta1.WorkloadCluster{}
	if err := r.Get(ctx, req.NamespacedName, wc); err != nil {
		if apierrors.IsNotFound(err) {
			if r.Manager != nil {
				r.Manager.Disconnect(req.Name)
			}
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	status, reason, msg, raw := r.assess(ctx, wc)
	apimeta.SetStatusCondition(&wc.Status.Conditions, metav1.Condition{
		Type:               v1beta1.WorkloadClusterReady,
		Status:             status,
		Reason:             reason,
		Message:            msg,
		ObservedGeneration: wc.Generation,
	})
	if r.Manager != nil {
		if status == metav1.ConditionTrue {
			if err := r.Manager.Connect(ctx, wc.Name, raw); err != nil {
				apimeta.SetStatusCondition(&wc.Status.Conditions, metav1.Condition{
					Type:               v1beta1.WorkloadClusterReady,
					Status:             metav1.ConditionFalse,
					Reason:             "ConnectionFailed",
					Message:            err.Error(),
					ObservedGeneration: wc.Generation,
				})
			}
		} else {
			r.Manager.Disconnect(wc.Name)
		}
	}
	if err := r.Status().Update(ctx, wc); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("workloadcluster %s: update status: %w", wc.Name, err)
	}
	return ctrl.Result{RequeueAfter: r.healthInterval()}, nil
}

// assess resolves the connection and probes it, returning the Ready condition
// fields. The Probe is injected so this is unit-testable without a real cluster.
func (r *Reconciler) assess(ctx context.Context, wc *v1beta1.WorkloadCluster) (status metav1.ConditionStatus, reason, msg string, kubeconfig []byte) {
	src := wc.Spec.ClusterSource
	if src.KubeConfig == nil {
		return metav1.ConditionFalse, "ClusterProfileUnsupported",
			"clusterProfileRef is not yet resolved (Phase 1 supports kubeConfig only)", nil
	}

	key := src.KubeConfig.Key
	if key == "" {
		key = "kubeconfig"
	}
	secret := &corev1.Secret{}
	nn := types.NamespacedName{Name: src.KubeConfig.SecretRef.Name, Namespace: src.KubeConfig.SecretRef.Namespace}
	if err := r.Get(ctx, nn, secret); err != nil {
		if apierrors.IsNotFound(err) {
			return metav1.ConditionFalse, "SecretNotFound",
				fmt.Sprintf("kubeconfig Secret %s/%s not found", nn.Namespace, nn.Name), nil
		}
		return metav1.ConditionFalse, "SecretNotFound", err.Error(), nil
	}
	raw, ok := secret.Data[key]
	if !ok || len(raw) == 0 {
		return metav1.ConditionFalse, "BadKubeConfig",
			fmt.Sprintf("Secret %s/%s has no %q key", nn.Namespace, nn.Name, key), nil
	}
	// Validate the kubeconfig (parse + security: reject exec/token-file/insecure)
	// BEFORE probing. This keeps "malformed/unsafe kubeconfig" (BadKubeConfig)
	// distinct from "cluster unreachable" (ConnectionFailed), and ensures the
	// security validation runs regardless of which probe is in use.
	if _, err := RESTConfigFromKubeConfig(raw, r.ExecPolicy); err != nil {
		return metav1.ConditionFalse, "BadKubeConfig", err.Error(), nil
	}
	if err := r.Probe(ctx, raw); err != nil {
		return metav1.ConditionFalse, "ConnectionFailed", err.Error(), nil
	}
	return metav1.ConditionTrue, "Reachable", "", raw
}

func (r *Reconciler) healthInterval() time.Duration {
	if r.HealthInterval > 0 {
		return r.HealthInterval
	}
	return DefaultHealthInterval
}

// SetupWithManager wires the reconciler: watch WorkloadClusters, and re-enqueue
// when a referenced kubeconfig Secret changes.
func (r *Reconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.Probe == nil {
		r.Probe = func(ctx context.Context, raw []byte) error {
			return probeViaServerVersion(ctx, raw, r.ExecPolicy)
		}
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1beta1.WorkloadCluster{}).
		Watches(&corev1.Secret{}, handler.EnqueueRequestsFromMapFunc(r.workloadClustersForSecret)).
		Complete(r)
}

// workloadClustersForSecret maps a Secret change to the WorkloadClusters whose
// kubeConfig.secretRef points at it.
func (r *Reconciler) workloadClustersForSecret(ctx context.Context, obj client.Object) []ctrl.Request {
	list := &v1beta1.WorkloadClusterList{}
	if err := r.List(ctx, list); err != nil {
		return nil
	}
	var reqs []ctrl.Request
	for i := range list.Items {
		kc := list.Items[i].Spec.ClusterSource.KubeConfig
		if kc != nil && kc.SecretRef.Name == obj.GetName() && kc.SecretRef.Namespace == obj.GetNamespace() {
			reqs = append(reqs, ctrl.Request{NamespacedName: types.NamespacedName{Name: list.Items[i].Name}})
		}
	}
	return reqs
}
