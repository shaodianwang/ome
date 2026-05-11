package basemodel

import (
	"context"
	"fmt"
	"net/http"

	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/utils/storage"
)

var log = logf.Log.WithName("basemodel-validation-webhook")

// +kubebuilder:webhook:verbs=create;update,path=/validate-ome-io-v1beta1-basemodel,mutating=false,failurePolicy=fail,groups=ome.io,resources=basemodels,versions=v1beta1,name=basemodel.ome-webhook-server.validator
// +kubebuilder:object:generate=false

// BaseModelValidator denies BaseModels whose PVC URI violates OEP-0004
// shape rules. Pure schema check — no API server reads, hence no Client.
type BaseModelValidator struct {
	Decoder admission.Decoder
}

func (v *BaseModelValidator) Handle(_ context.Context, req admission.Request) admission.Response {
	bm := &v1beta1.BaseModel{}
	if err := v.Decoder.Decode(req, bm); err != nil {
		log.Error(err, "Failed to decode BaseModel", "name", bm.Name, "namespace", bm.Namespace)
		return admission.Errored(http.StatusBadRequest, err)
	}
	if err := validatePVCStorage(&bm.Spec, false); err != nil {
		return admission.Denied(err.Error())
	}
	return admission.Allowed("")
}

// +kubebuilder:webhook:verbs=create;update,path=/validate-ome-io-v1beta1-clusterbasemodel,mutating=false,failurePolicy=fail,groups=ome.io,resources=clusterbasemodels,versions=v1beta1,name=clusterbasemodel.ome-webhook-server.validator
// +kubebuilder:object:generate=false

// ClusterBaseModelValidator is the cluster-scoped sibling of
// BaseModelValidator; same shape rules, no API server reads.
type ClusterBaseModelValidator struct {
	Decoder admission.Decoder
}

func (v *ClusterBaseModelValidator) Handle(_ context.Context, req admission.Request) admission.Response {
	cbm := &v1beta1.ClusterBaseModel{}
	if err := v.Decoder.Decode(req, cbm); err != nil {
		log.Error(err, "Failed to decode ClusterBaseModel", "name", cbm.Name)
		return admission.Errored(http.StatusBadRequest, err)
	}
	if err := validatePVCStorage(&cbm.Spec, true); err != nil {
		return admission.Denied(err.Error())
	}
	return admission.Allowed("")
}

// validatePVCStorage enforces OEP-0004 URI rules:
//
//   - namespaced BaseModel: pvc:// URIs must NOT carry a namespace prefix
//   - ClusterBaseModel: pvc:// URIs MUST carry a namespace prefix
//   - the URI itself must parse
//   - PVC + distribution=Sharded is rejected: the sharded path uses the
//     model-cache provider and runs BEFORE the PVC branch in
//     reconcilePVCStorage, so the combination would silently bypass PVC
//     handling. Force operators to pick one.
//
// Non-PVC URIs are accepted unchanged.
func validatePVCStorage(spec *v1beta1.BaseModelSpec, isClusterScoped bool) error {
	if spec == nil || spec.Storage == nil || spec.Storage.StorageUri == nil {
		return nil
	}
	uri := *spec.Storage.StorageUri
	storageType, err := storage.GetStorageType(uri)
	if err != nil || storageType != storage.StorageTypePVC {
		return nil
	}

	if spec.Distribution != nil && *spec.Distribution == v1beta1.DistributionSharded {
		return fmt.Errorf("PVC storage URIs are not compatible with distribution=Sharded; use distribution=PerNode (or omit) for pvc:// models")
	}

	components, err := storage.ParsePVCStorageURI(uri)
	if err != nil {
		return fmt.Errorf("invalid PVC storage URI %q: %w", uri, err)
	}
	if isClusterScoped && components.Namespace == "" {
		return fmt.Errorf("ClusterBaseModel PVC URI must specify a namespace (format: pvc://{namespace}:{pvc-name}/{sub-path}), got %q", uri)
	}
	if !isClusterScoped && components.Namespace != "" {
		return fmt.Errorf("namespaced BaseModel PVC URI must not specify a namespace; the BaseModel's own namespace is used, got %q", uri)
	}
	return nil
}
