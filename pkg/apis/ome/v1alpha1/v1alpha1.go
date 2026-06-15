package v1alpha1

import (
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/scheme"

	"github.com/ome-projects/ome/pkg/constants"
)

var (
	// APIVersion is the current version for OME management-plane APIs.
	APIVersion = "v1alpha1"

	// SchemeGroupVersion is the group version used to register these objects.
	SchemeGroupVersion = schema.GroupVersion{Group: constants.OMEAPIGroupName, Version: APIVersion}

	// SchemeBuilder is used to add go types to the GroupVersionKind scheme.
	SchemeBuilder = &scheme.Builder{GroupVersion: SchemeGroupVersion}

	// AddToScheme is required by controller-runtime clients.
	AddToScheme = SchemeBuilder.AddToScheme
)

// Resource returns a GroupResource for the named resource.
func Resource(resource string) schema.GroupResource {
	return SchemeGroupVersion.WithResource(resource).GroupResource()
}
