package sharded

import (
	"context"
	"time"

	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/basemodel/shared"
)

// Backend handles Distribution=Sharded BaseModels: probes the
// configured Provider for source reachability, parses metadata through
// the cache, bypasses the per-node ConfigMap handshake. An
// unconfigured Provider surfaces as ProviderNotConfigured via status.
type Backend struct {
	provider Provider
}

func New(provider Provider) Backend {
	return Backend{provider: provider}
}

func (Backend) Name() string { return "sharded" }

func (Backend) Matches(spec *v1beta1.BaseModelSpec) bool {
	return IsDistribution(spec.Distribution)
}

func (b Backend) Reconcile(ctx context.Context, a shared.BackendArgs) (ctrl.Result, error) {
	result, err := Reconcile(ctx, a.Client, a.Log, b.provider, a.Obj, a.Kind)
	if err != nil {
		a.Log.Error(err, "Failed to update Sharded "+a.Kind+" status")
		return ctrl.Result{RequeueAfter: time.Minute}, err
	}
	return result, nil
}

func (Backend) HandleDeletion(ctx context.Context, a shared.BackendArgs) (ctrl.Result, error) {
	return HandleModelDeletion(ctx, a.Client, a.Log, a.Obj, a.IsClusterScoped, a.Finalizer)
}
