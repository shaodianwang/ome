package components

import (
	"context"
	"fmt"

	"github.com/go-logr/logr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	v1beta1servingruntimecontroller "github.com/ome-projects/ome/pkg/controller/v1beta1/servingruntime"
	servingruntimewebhook "github.com/ome-projects/ome/pkg/webhook/admission/servingruntime"
)

// SetupServingRuntimeWebhooks registers both ServingRuntime
// validators. Paths must match the installed
// ValidatingWebhookConfiguration YAML.
func SetupServingRuntimeWebhooks(mgr ctrl.Manager) error {
	server := mgr.GetWebhookServer()
	if server == nil {
		return fmt.Errorf("SetupServingRuntimeWebhooks: manager has no webhook server (set Framework.WebhookPath)")
	}
	decoder := admission.NewDecoder(mgr.GetScheme())

	server.Register("/validate-ome-io-v1beta1-servingruntime", &webhook.Admission{
		Handler: &servingruntimewebhook.ServingRuntimeValidator{
			Client: mgr.GetAPIReader(), Decoder: decoder,
		},
	})
	server.Register("/validate-ome-io-v1beta1-clusterservingruntime", &webhook.Admission{
		Handler: &servingruntimewebhook.ClusterServingRuntimeValidator{
			Client: mgr.GetAPIReader(), Decoder: decoder,
		},
	})
	return nil
}

// SetupServingRuntimeInheritanceControllers wires the unified
// inheritance reconciler (one source per GVK; reconciles both CSR + SR
// types by branching on request namespace).
func SetupServingRuntimeInheritanceControllers(ctx context.Context, mgr ctrl.Manager) error {
	r := &v1beta1servingruntimecontroller.InheritanceReconciler{
		Client:   mgr.GetClient(),
		Log:      logr.Discard(),
		Scheme:   mgr.GetScheme(),
		Recorder: mgr.GetEventRecorderFor("ServingRuntimeInheritance"),
	}
	if err := r.SetupWithManager(mgr); err != nil {
		return fmt.Errorf("setup ServingRuntime inheritance reconciler: %w", err)
	}
	return nil
}
