package components

import (
	"fmt"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	basemodelwebhook "github.com/sgl-project/ome/pkg/webhook/admission/basemodel"
)

// SetupBaseModelWebhooks registers BOTH the BaseModel and
// ClusterBaseModel ValidatingWebhook handlers on the supplied
// manager's webhook server. The manager must have been constructed
// with a webhook server (Framework.WebhookPath set on Init).
//
// Path strings here MUST match the corresponding rule paths in the
// installed ValidatingWebhookConfiguration YAML — a mismatch yields
// "no path matches" 404s from the apiserver and silently-passing
// admission, which would defeat the test.
func SetupBaseModelWebhooks(mgr ctrl.Manager) error {
	server := mgr.GetWebhookServer()
	if server == nil {
		return fmt.Errorf("SetupBaseModelWebhooks: manager has no webhook server (set Framework.WebhookPath)")
	}
	decoder := admission.NewDecoder(mgr.GetScheme())

	server.Register("/validate-ome-io-v1beta1-basemodel",
		&webhook.Admission{Handler: &basemodelwebhook.BaseModelValidator{Decoder: decoder}})
	server.Register("/validate-ome-io-v1beta1-clusterbasemodel",
		&webhook.Admission{Handler: &basemodelwebhook.ClusterBaseModelValidator{Decoder: decoder}})
	return nil
}
