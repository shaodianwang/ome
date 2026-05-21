package components

import (
	"fmt"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"github.com/ome-projects/ome/pkg/webhook/admission/runtimerevision"
)

// SetupRuntimeRevisionWebhooks registers the apps/v1 ControllerRevision
// immutability validator. Path must match the installed
// ValidatingWebhookConfiguration.
func SetupRuntimeRevisionWebhooks(mgr ctrl.Manager) error {
	server := mgr.GetWebhookServer()
	if server == nil {
		return fmt.Errorf("SetupRuntimeRevisionWebhooks: manager has no webhook server (set Framework.WebhookPath)")
	}
	server.Register("/validate-apps-v1-controllerrevision", &webhook.Admission{
		Handler: &runtimerevision.ImmutabilityValidator{Decoder: admission.NewDecoder(mgr.GetScheme())},
	})
	return nil
}
