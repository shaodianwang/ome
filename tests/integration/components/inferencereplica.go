package components

import (
	"context"
	"fmt"

	"k8s.io/client-go/kubernetes"
	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/ome-projects/ome/pkg/controller/v1beta1/controllerconfig"
	inferencereplica "github.com/ome-projects/ome/pkg/controller/v1beta1/inferencereplica"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/irstatus"
	"github.com/ome-projects/ome/pkg/runtimeselector"
)

// InferenceReplicaOption mutates the IR reconciler before it is
// registered. Functional-options pattern mirrors
// SetupInferenceService so the signature stays stable as the
// reconciler accumulates dependencies.
type InferenceReplicaOption func(*inferenceReplicaOpts)

type inferenceReplicaOpts struct {
	maxConcurrentReconciles int
}

// WithIRMaxConcurrentReconciles sets the IR reconciler's
// MaxConcurrentReconciles. Zero (the default when unset) preserves
// controller-runtime's single-worker default.
func WithIRMaxConcurrentReconciles(n int) InferenceReplicaOption {
	return func(o *inferenceReplicaOpts) { o.maxConcurrentReconciles = n }
}

// SetupInferenceReplica registers the InferenceReplica reconciler with
// the supplied manager. Mirrors SetupInferenceService and the
// production wiring in cmd/manager/main.go: the reconciler is a plain
// struct (no constructor) carrying Client / Log / APIReader / Recorder.
//
// The IR reconciler is the second half of the OMENative path: the ISVC
// controller projects a per-Component InferenceReplica object, and this
// controller turns it into pods + status (and flips
// InstanceStatus.Admitted once the pods are created and not admission-
// gated). SetupWithManager discovers the scheduler-plugins PodGroup CRD
// and seeds an Expectations cache. TPU slices are provisioned only when
// the ConfigMap carries a tpuSliceProvisioning block and the Slice CRD is
// served, as in production.
func SetupInferenceReplica(ctx context.Context, mgr ctrl.Manager, opts ...InferenceReplicaOption) error {
	cfg := &inferenceReplicaOpts{}
	for _, o := range opts {
		o(cfg)
	}
	clientset, err := kubernetes.NewForConfig(mgr.GetConfig())
	if err != nil {
		return fmt.Errorf("SetupInferenceReplica: create clientset: %w", err)
	}
	podBatchSizes, err := controllerconfig.LoadPodBatchSizes(clientset)
	if err != nil {
		return fmt.Errorf("SetupInferenceReplica: load lifecycle scale settings: %w", err)
	}
	statusConfig, err := controllerconfig.NewOMENativeStatusConfig(clientset)
	if err != nil {
		return fmt.Errorf("SetupInferenceReplica: load omenativeStatus configuration: %w", err)
	}
	tpuSliceConfig, err := controllerconfig.NewTPUSliceProvisioningConfig(clientset)
	if err != nil {
		return fmt.Errorf("SetupInferenceReplica: load tpuSliceProvisioning configuration: %w", err)
	}

	r := &inferencereplica.Reconciler{
		Client:                   mgr.GetClient(),
		Clientset:                clientset,
		Log:                      ctrl.Log.WithName("InferenceReplica"),
		APIReader:                mgr.GetAPIReader(),
		RuntimeSelector:          runtimeselector.New(mgr.GetClient()),
		InstanceStatusDecoder:    irstatus.NewDecoder(statusConfig.DecodeBound()),
		InstanceStatusTarget:     statusConfig.InstanceStatusEncoding,
		Recorder:                 mgr.GetEventRecorderFor("InferenceReplica"),
		MaxConcurrentReconciles:  cfg.maxConcurrentReconciles,
		ScaleUpPodBatchSize:      podBatchSizes.ScaleUp,
		ScaleDownPodBatchSize:    podBatchSizes.ScaleDown,
		ScaleDownRequeueInterval: podBatchSizes.ScaleDownRequeueInterval,
		TPUSliceProvisioning:     tpuSliceConfig,
	}
	if err := r.SetupWithManager(mgr); err != nil {
		return fmt.Errorf("SetupInferenceReplica: SetupWithManager: %w", err)
	}
	return nil
}
