package components

import (
	"fmt"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"github.com/ome-projects/ome/pkg/webhook/admission/runtimepreset"
)

// SetupRuntimePresetWebhooks registers the OEP-0012 Part C engine-preset
// mutators (namespaced + cluster scope). Paths must match the installed
// MutatingWebhookConfiguration.
func SetupRuntimePresetWebhooks(mgr ctrl.Manager) error {
	server := mgr.GetWebhookServer()
	if server == nil {
		return fmt.Errorf("SetupRuntimePresetWebhooks: manager has no webhook server (set Framework.WebhookPath)")
	}
	decoder := admission.NewDecoder(mgr.GetScheme())
	server.Register("/mutate-ome-io-v1beta1-servingruntime-preset", &webhook.Admission{
		Handler: &runtimepreset.ServingRuntimeMutator{Decoder: decoder},
	})
	server.Register("/mutate-ome-io-v1beta1-clusterservingruntime-preset", &webhook.Admission{
		Handler: &runtimepreset.ClusterServingRuntimeMutator{Decoder: decoder},
	})
	return nil
}
