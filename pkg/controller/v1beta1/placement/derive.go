package placement

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
)

// DeriveISVC builds a derived InferenceService from a control-plane source
// ISVC. The derived object is suitable for placement onto a workload cluster:
//   - Identity (name/ns) preserved so the worker cluster can address it.
//   - Server-side fields cleared (UID, ResourceVersion, generation, etc.).
//   - Origin markers added (PlacementOriginLabel, PlacementOriginUIDAnnotation).
//   - Control-plane identity stamped (PlacementControlPlaneLabel) when controlPlaneID
//     is non-empty, so the GC sweep only reaps deriveds THIS control plane created.
//   - Kueue gating stamped on component pod metadata (the queue-name label).
//   - Control-plane-only directives removed (placement selectors + rollout verbs)
//     so the worker reconciler does not (re)act on the control plane's decisions.
func DeriveISVC(src *v1beta1.InferenceService, controlPlaneID string) *v1beta1.InferenceService {
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
	// Stamp the control-plane identity so GC across shared workload clusters can
	// tell our deriveds apart from another control plane's. Config-driven, no
	// in-code default: an empty id leaves the label unset (single-CP behavior).
	if controlPlaneID != "" {
		d.Labels[PlacementControlPlaneLabel] = controlPlaneID
	}
	if d.Annotations == nil {
		d.Annotations = make(map[string]string)
	}
	d.Annotations[PlacementOriginUIDAnnotation] = string(src.UID)
	// Strip control-plane-only directives so they do not ride along to the
	// worker (placement selectors + rollout operator verbs).
	for _, k := range controlPlaneOnlyAnnotations {
		delete(d.Annotations, k)
	}

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

// setDerivedReplicas pins the apportioned per-cluster replica count on a derived
// ISVC's scalable components for Split mode: MinReplicas becomes the requested
// count (Kueue admits as many as fit), and MaxReplicas is raised to at least
// that so the request is honored while the home can still autoscale UP locally.
// Applied to Engine and, when present, Decoder (a PD pair scales 1:1).
func setDerivedReplicas(d *v1beta1.InferenceService, replicas int32) {
	n := int(replicas)
	apply := func(c *v1beta1.ComponentExtensionSpec) {
		c.MinReplicas = ptr.To(n)
		if c.MaxReplicas < n {
			c.MaxReplicas = n
		}
	}
	if d.Spec.Engine != nil {
		apply(&d.Spec.Engine.ComponentExtensionSpec)
	}
	if d.Spec.Decoder != nil {
		apply(&d.Spec.Decoder.ComponentExtensionSpec)
	}
}

// stampQueue adds the Kueue queue-name label to a component's pod metadata.
// That label is what Kueue's pod integration keys on to gate the pods; it is the
// sole Kueue stamp the derived ISVC needs.
func stampQueue(comp *v1beta1.ComponentExtensionSpec, queue string) {
	if comp.Labels == nil {
		comp.Labels = make(map[string]string)
	}
	comp.Labels[constants.KueueQueueLabelKey] = queue
}
