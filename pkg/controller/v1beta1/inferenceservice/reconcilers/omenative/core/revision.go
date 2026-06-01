package core

import (
	"context"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/revision"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/v1beta1convert"
)

// isvcGVK is the GroupVersionKind every OMENative-on-ISVC adapter
// emits on owner references. Centralized so a future API version bump
// flips this in one place.
var isvcGVK = v1beta1.SchemeGroupVersion.WithKind("InferenceService")

// isvcRevisionKey is the per-Component revision.Key projection for one
// OMENative-on-ISVC reconcile. Mirrors what the ISVC adapter computes
// at the workload boundary; centralized here so the legacy dispatch
// callers don't repeat the build-up.
func isvcRevisionKey(isvc *v1beta1.InferenceService, component v1beta1.ComponentType) revision.Key {
	name := isvc.Name + "-" + string(component)
	return revision.Key{
		Namespace: isvc.Namespace,
		Name:      name,
		Labels: map[string]string{
			constants.InferenceServicePodLabelKey: isvc.Name,
			constants.OMEComponentLabel:           string(component),
			query.LabelManagedBy:                  query.ManagedByOMENative,
		},
	}
}

// EnsureControllerRevisionForISVC is the (isvc, component) shape of
// workload/revision.EnsureControllerRevision used by the legacy
// OMENative-on-ISVC dispatch path. Adapter callers skip this wrapper
// and call workload/revision directly with their own projected Key.
//
// scopeUID is fixed to isvc.UID — both the legacy direct path and the
// IR-managed path partition the revision history by the parent ISVC's
// UID so delete-and-recreate cycles can't silently inherit the
// predecessor's CR identity (see revision.EnsureControllerRevision
// scopeUID rationale).
func EnsureControllerRevisionForISVC(
	ctx context.Context,
	c client.Client,
	reads client.Reader,
	isvc *v1beta1.InferenceService,
	component v1beta1.ComponentType,
	template *corev1.PodSpec,
	templateMeta *metav1.ObjectMeta,
	collisionCount *int32,
) (*appsv1.ControllerRevision, bool, error) {
	return revision.EnsureControllerRevision(ctx, c, reads, isvc, isvcGVK, isvcRevisionKey(isvc, component), template, templateMeta, collisionCount, isvc.UID)
}

// EnsureControllerRevisionForISVCWithWorker is the multi-pod sibling of
// EnsureControllerRevisionForISVC; folds workerSpec into the revision
// hash so worker-only template changes trigger a new CR.
func EnsureControllerRevisionForISVCWithWorker(
	ctx context.Context,
	c client.Client,
	reads client.Reader,
	isvc *v1beta1.InferenceService,
	component v1beta1.ComponentType,
	template *corev1.PodSpec,
	workerSpec *corev1.PodSpec,
	templateMeta *metav1.ObjectMeta,
	collisionCount *int32,
) (*appsv1.ControllerRevision, bool, error) {
	return revision.EnsureControllerRevisionWithWorker(ctx, c, reads, isvc, isvcGVK, isvcRevisionKey(isvc, component), template, workerSpec, templateMeta, collisionCount, isvc.UID)
}

// RetainControllerRevisionsForISVC is the (isvc, component) shape of
// workload/revision.RetainControllerRevisions.
func RetainControllerRevisionsForISVC(
	ctx context.Context,
	c client.Client,
	reads client.Reader,
	isvc *v1beta1.InferenceService,
	component v1beta1.ComponentType,
	maxNonLive int,
	liveNames ...string,
) error {
	return revision.RetainControllerRevisions(ctx, c, reads, isvcRevisionKey(isvc, component), maxNonLive, liveNames...)
}

// CollectLiveRevisionNamesForISVC projects an LifecycleStatus
// to the (current, update, instances) tuple the workload sweeper takes.
// The InstanceStatuses slice is converted at the adapter boundary —
// workload.CollectLiveRevisionNames itself reads only the workload-side
// InstanceStatus type so the revision package stays free of v1beta1
// imports.
func CollectLiveRevisionNamesForISVC(cs *v1beta1.LifecycleStatus) []string {
	if cs == nil {
		return nil
	}
	return revision.CollectLiveRevisionNames(cs.CurrentRevision, cs.UpdateRevision, v1beta1convert.InstanceStatusSliceToWorkload(cs.InstanceStatuses))
}

// RevisionName composes the legacy CR name `<isvc>-<component>-<hash>`.
// Wrapper around workload/revision.Name with the ISVC-form key.
func RevisionName(isvcName string, component v1beta1.ComponentType, hash string) string {
	return revision.Name(revision.Key{Name: isvcName + "-" + string(component)}, hash)
}

// RevisionLabels returns the ISVC-form revision label set.
func RevisionLabels(isvcName string, component v1beta1.ComponentType) map[string]string {
	return revision.Labels(revision.Key{
		Labels: map[string]string{
			constants.InferenceServicePodLabelKey: isvcName,
			constants.OMEComponentLabel:           string(component),
			query.LabelManagedBy:                  query.ManagedByOMENative,
		},
	})
}
