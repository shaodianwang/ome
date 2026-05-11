package components

import (
	"fmt"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	v1beta1 "github.com/sgl-project/ome/pkg/apis/ome/v1beta1"
	"github.com/sgl-project/ome/pkg/runtimeselector"
	basemodelwebhook "github.com/sgl-project/ome/pkg/webhook/admission/basemodel"
	isvcwebhook "github.com/sgl-project/ome/pkg/webhook/admission/isvc"
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

// SetupInferenceServiceWebhook registers the ISVC validating webhook
// on the supplied manager via ctrl.NewWebhookManagedBy — the same
// path cmd/manager/main.go uses, so the registered URL paths match
// what the apiserver dials. Defaulter is intentionally not wired:
// the ISVC defaulter pulls in clientset and ConfigMap dependencies
// the validator suite doesn't need, and tests that care about
// defaults belong in their own suite.
func SetupInferenceServiceWebhook(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr).
		For(&v1beta1.InferenceService{}).
		WithValidator(&isvcwebhook.InferenceServiceValidator{
			Client:          mgr.GetClient(),
			RuntimeSelector: runtimeselector.New(mgr.GetClient()),
		}).
		Complete()
}
