// Package components contributes ManagerSetup-shaped functions, one
// per OME controller. Suites import this package and combine the
// SetupX functions they need into their own ManagerSetup callback:
//
//	mgrSetup := func(ctx context.Context, mgr ctrl.Manager) error {
//	    if err := components.SetupBaseModel(ctx, mgr, components.WithOmeAgentConfig(cfg)); err != nil {
//	        return err
//	    }
//	    return components.SetupInferenceService(ctx, mgr)
//	}
//	fwk.StartManager(s.Ctx, s.Cfg, mgrSetup)
//
// Each suite picks exactly the controllers it needs; the framework
// knows nothing about specific OME controllers. Adding a new
// controller = one Setup* func in this package, no edit to framework
// code.
package components

import (
	"context"
	"fmt"

	ctrl "sigs.k8s.io/controller-runtime"

	v1beta1basemodelcontroller "github.com/sgl-project/ome/pkg/controller/v1beta1/basemodel"
	"github.com/sgl-project/ome/pkg/controller/v1beta1/controllerconfig"
	"github.com/sgl-project/ome/pkg/modelcache"
)

// BaseModelOption mutates the BaseModel reconciler before it is
// registered with the manager. Functional-options pattern keeps the
// SetupBaseModel signature stable as the reconciler accumulates
// dependencies.
type BaseModelOption func(*baseModelOpts)

type baseModelOpts struct {
	omeAgentConfig     *controllerconfig.OmeAgentConfig
	modelCacheProvider modelcache.Provider
}

// WithOmeAgentConfig provides the metadata-Job config the BaseModel
// reconciler reads when it spawns a metadata extraction Job for a
// PVC-backed model. Without this, PVC reconciles transition to
// LifeCycleStateFailed with reason=PVCConfigMissing — useful for
// tests of the missing-config error path.
func WithOmeAgentConfig(cfg *controllerconfig.OmeAgentConfig) BaseModelOption {
	return func(o *baseModelOpts) { o.omeAgentConfig = cfg }
}

// WithModelCacheProvider injects a model-cache provider used by the
// Sharded distribution flow. Tests pass a fake provider (e.g.
// pkg/modelcache/fake) so the controller doesn't make real HTTP
// calls. Without this, BaseModels with Distribution=Sharded transition
// to Ready=False with reason=ProviderNotConfigured.
func WithModelCacheProvider(p modelcache.Provider) BaseModelOption {
	return func(o *baseModelOpts) { o.modelCacheProvider = p }
}

// SetupBaseModel registers BaseModelReconciler + ClusterBaseModelReconciler
// with the supplied manager. Both reconcilers share the same OmeAgentConfig.
//
// Test suites that don't drive PVC-backed models can call SetupBaseModel
// without WithOmeAgentConfig — the reconciler will surface a clear
// PVCConfigMissing failure via model status the first time a PVC model
// reconciles, which is fine for tests that don't exercise that path.
func SetupBaseModel(ctx context.Context, mgr ctrl.Manager, opts ...BaseModelOption) error {
	cfg := &baseModelOpts{}
	for _, o := range opts {
		o(cfg)
	}

	bm := &v1beta1basemodelcontroller.BaseModelReconciler{
		Client:         mgr.GetClient(),
		Log:            ctrl.Log.WithName("BaseModel"),
		Scheme:         mgr.GetScheme(),
		ModelCache:     cfg.modelCacheProvider,
		OmeAgentConfig: cfg.omeAgentConfig,
	}
	if err := bm.SetupWithManager(mgr); err != nil {
		return fmt.Errorf("SetupBaseModel: BaseModelReconciler: %w", err)
	}

	cbm := &v1beta1basemodelcontroller.ClusterBaseModelReconciler{
		Client:         mgr.GetClient(),
		Log:            ctrl.Log.WithName("ClusterBaseModel"),
		Scheme:         mgr.GetScheme(),
		ModelCache:     cfg.modelCacheProvider,
		OmeAgentConfig: cfg.omeAgentConfig,
	}
	if err := cbm.SetupWithManager(mgr); err != nil {
		return fmt.Errorf("SetupBaseModel: ClusterBaseModelReconciler: %w", err)
	}
	return nil
}
