package placement

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
)

// DeriveISVC builds a derived InferenceService from a control-plane source
// ISVC. The derived object is suitable for placement onto a workload cluster:
// - Identity (name/ns) preserved so the worker cluster can address it.
// - Server-side fields cleared (UID, ResourceVersion, generation, etc.).
// - Origin markers added (PlacementOriginLabel, PlacementOriginUIDAnnotation).
// - Kueue gating stamps added to component pod metadata (queue label + serving annotation).
// - Control-plane directives (cluster-selector annotation) removed.
func DeriveISVC(src *v1beta1.InferenceService) *v1beta1.InferenceService {
	d := src.DeepCopy()

	// Clear server-side / control-plane state.
	d.ResourceVersion = ""
	d.UID = ""
	d.Generation = 0
	d.CreationTimestamp = metav1.Time{}
	d.DeletionTimestamp = nil
	d.OwnerReferences = nil
	d.Finalizers = nil
	d.ManagedFields = nil
	d.Status = v1beta1.InferenceServiceStatus{}

	// Resolve queue name: from LocalQueueAnnotation or default.
	queue := src.Annotations[LocalQueueAnnotation]
	if queue == "" {
		queue = DefaultLocalQueue
	}

	// Add origin markers.
	if d.Labels == nil {
		d.Labels = make(map[string]string)
	}
	d.Labels[PlacementOriginLabel] = string(src.UID)
	if d.Annotations == nil {
		d.Annotations = make(map[string]string)
	}
	d.Annotations[PlacementOriginUIDAnnotation] = string(src.UID)
	// Remove control-plane directives.
	delete(d.Annotations, ClusterSelectorAnnotation)

	// Stamp Kueue gating on each component.
	if d.Spec.Engine != nil {
		stampQueue(&d.Spec.Engine.ComponentExtensionSpec, queue)
	}
	if d.Spec.Decoder != nil {
		stampQueue(&d.Spec.Decoder.ComponentExtensionSpec, queue)
	}
	if d.Spec.Router != nil {
		stampQueue(&d.Spec.Router.ComponentExtensionSpec, queue)
	}

	return d
}

// stampQueue adds Kueue gating metadata to a component's pod metadata.
func stampQueue(comp *v1beta1.ComponentExtensionSpec, queue string) {
	if comp.Labels == nil {
		comp.Labels = make(map[string]string)
	}
	comp.Labels[constants.KueueQueueLabelKey] = queue
	if comp.Annotations == nil {
		comp.Annotations = make(map[string]string)
	}
	comp.Annotations[KueueServingAnnotation] = "true"
}
