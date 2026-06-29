package components

import (
	"context"
	"fmt"

	ctrl "sigs.k8s.io/controller-runtime"

	inferencereplica "github.com/ome-projects/ome/pkg/controller/v1beta1/inferencereplica"
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
// and seeds an Expectations cache.
func SetupInferenceReplica(ctx context.Context, mgr ctrl.Manager, opts ...InferenceReplicaOption) error {
	cfg := &inferenceReplicaOpts{}
	for _, o := range opts {
		o(cfg)
	}

	r := &inferencereplica.Reconciler{
		Client:                  mgr.GetClient(),
		Log:                     ctrl.Log.WithName("InferenceReplica"),
		APIReader:               mgr.GetAPIReader(),
		Recorder:                mgr.GetEventRecorderFor("InferenceReplica"),
		MaxConcurrentReconciles: cfg.maxConcurrentReconciles,
	}
	if err := r.SetupWithManager(mgr); err != nil {
		return fmt.Errorf("SetupInferenceReplica: SetupWithManager: %w", err)
	}
	return nil
}
