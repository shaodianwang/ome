package components

import (
	"fmt"

	"k8s.io/client-go/kubernetes"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	v1beta1 "github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/quota/tree"
	"github.com/ome-projects/ome/pkg/runtimeselector"
	quotawebhook "github.com/ome-projects/ome/pkg/webhook/admission/acceleratorquota"
	basemodelwebhook "github.com/ome-projects/ome/pkg/webhook/admission/basemodel"
	isvcwebhook "github.com/ome-projects/ome/pkg/webhook/admission/isvc"
	podwebhook "github.com/ome-projects/ome/pkg/webhook/admission/pod"
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

// SetupAcceleratorQuotaWebhook registers the AcceleratorQuota validating
// webhook, wired exactly as cmd/ome-quota-manager does it.
//
// The reader is the manager's APIReader rather than its cache: admission has to
// assemble the tree from live state, and a cache lagging by one write is a tree
// that admits the second half of a racing pair.
//
// maxDepth is a parameter only so a suite can exercise the bound without
// authoring six levels; the root name is not, being a wire-contract identity.
func SetupAcceleratorQuotaWebhook(mgr ctrl.Manager, maxDepth int) error {
	server := mgr.GetWebhookServer()
	if server == nil {
		return fmt.Errorf("SetupAcceleratorQuotaWebhook: manager has no webhook server (set Framework.WebhookPath)")
	}
	server.Register("/validate-ome-io-v1beta1-acceleratorquota", &webhook.Admission{
		Handler: &quotawebhook.Validator{
			Client:  mgr.GetAPIReader(),
			Decoder: admission.NewDecoder(mgr.GetScheme()),
			Options: tree.Options{
				RootName: v1beta1.AcceleratorQuotaRootName,
				MaxDepth: maxDepth,
			},
		},
	})
	return nil
}

// SetupInferenceServiceWebhook registers the ISVC validating webhook
// on the supplied manager via ctrl.NewWebhookManagedBy — the same
// path cmd/manager/main.go uses, so the registered URL paths match
// what the apiserver dials. The defaulter is wired alongside on the
// same builder call (controller-runtime requires them to share a
// builder per type) but is opt-in via a separate testdata fixture:
// suites that don't install the mutating webhook configuration won't
// trigger the defaulter.
func SetupInferenceServiceWebhook(mgr ctrl.Manager) error {
	clientset, err := kubernetes.NewForConfig(mgr.GetConfig())
	if err != nil {
		return fmt.Errorf("SetupInferenceServiceWebhook: build clientset: %w", err)
	}
	return ctrl.NewWebhookManagedBy(mgr, &v1beta1.InferenceService{}).
		WithValidator(&isvcwebhook.InferenceServiceValidator{
			Client:          mgr.GetClient(),
			RuntimeSelector: runtimeselector.New(mgr.GetClient()),
		}).
		WithDefaulter(&isvcwebhook.InferenceServiceDefaulter{
			Client:    mgr.GetClient(),
			ClientSet: clientset,
		}).
		Complete()
}

// SetupPodMutatorWebhook registers the pod mutator on the supplied
// manager's webhook server. Path "/mutate-pods" must match the
// kubebuilder marker on Mutator (mutator.go:19). The pod mutator
// chains 5 injectors at admission time:
// metricsAggregator → modelInit → fineTunedAdapter → servingSidecar
// → rdma. Each injector reads its config from the
// inferenceservice-config ConfigMap in the OME namespace; suites
// that exercise injection paths must seed that CM with the
// per-injector keys their specs need.
//
// Clientset is built from the manager's rest.Config — same pattern
// as SetupInferenceService — because the mutator does a typed Get
// for the inferenceservice-config CM, which goes through the
// clientset rather than the controller-runtime cache.
func SetupPodMutatorWebhook(mgr ctrl.Manager) error {
	server := mgr.GetWebhookServer()
	if server == nil {
		return fmt.Errorf("SetupPodMutatorWebhook: manager has no webhook server (set Framework.WebhookPath)")
	}
	clientset, err := kubernetes.NewForConfig(mgr.GetConfig())
	if err != nil {
		return fmt.Errorf("SetupPodMutatorWebhook: build clientset: %w", err)
	}
	server.Register("/mutate-pods", &webhook.Admission{Handler: &podwebhook.Mutator{
		Client:    mgr.GetClient(),
		Clientset: clientset,
		Decoder:   admission.NewDecoder(mgr.GetScheme()),
	}})
	return nil
}
