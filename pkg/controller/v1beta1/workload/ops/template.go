package ops

import (
	"context"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/revision"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/status"
	workload "github.com/ome-projects/ome/pkg/controller/v1beta1/workload/types"
)

// podTemplate is what one pod set is rendered from, paired with the
// revision its pods are stamped with. A pod set renders exactly the
// revision it stamps: the label routes it, counts it and drains it as
// that revision, so the content must be that revision's. Only the
// constructors below build the pair, so no caller can stamp one revision
// while rendering another.
type podTemplate struct {
	revision        query.RevisionID
	podSpec         *corev1.PodSpec
	workerPodSpec   *corev1.PodSpec
	meta            *metav1.ObjectMeta
	pairingProtocol string
}

// desiredTemplate is the Component's current template, stamped with rev.
// It is the template of the roll target and of nothing else: a caller
// stamping any other revision renders storedTemplate. A zero rev leaves
// the pods unlabeled, for a row that records no revision at all.
func desiredTemplate(input workload.ReconcileInput, plan workload.ComponentPlan, rev query.RevisionID) podTemplate {
	return podTemplate{
		revision:        rev,
		podSpec:         input.DesiredSpec.PodSpec,
		workerPodSpec:   input.DesiredSpec.WorkerPodSpec,
		meta:            input.DesiredSpec.PodTemplateObjectMeta,
		pairingProtocol: plan.PairingProtocol,
	}
}

// pinnedTemplate is the template an operation pinned to revision name
// renders: the current template when the pin is the roll target, else
// the pinned revision's stored template. found is false when the pinned
// revision has no ControllerRevision left.
func pinnedTemplate(ctx context.Context, reads client.Reader, input workload.ReconcileInput, plan workload.ComponentPlan, target *appsv1.ControllerRevision, name string) (podTemplate, bool, error) {
	if target != nil && name == target.Name {
		return desiredTemplate(input, plan, query.RevisionOf(target)), true, nil
	}
	return storedTemplate(ctx, reads, input, name)
}

// storedTemplate is the template the named ControllerRevision records,
// stamped with that revision. found is false when the revision is gone;
// the caller decides what a rebuild with nothing to render from does.
// The roll target is never read through here: the Component's current
// template IS that revision's, and callers with the target in hand go
// through desiredTemplate.
func storedTemplate(ctx context.Context, reads client.Reader, input workload.ReconcileInput, name string) (podTemplate, bool, error) {
	payload, err := loadRevisionPayload(ctx, reads, input.Key.Namespace, name)
	if err != nil {
		return podTemplate{}, false, err
	}
	if payload == nil {
		return podTemplate{}, false, nil
	}
	tmpl, err := templateFromPayload(input, name, payload)
	if err != nil {
		return podTemplate{}, false, err
	}
	return tmpl, true, nil
}

// templateFromPayload is storedTemplate for a payload the caller already
// holds. The pairing protocol is revision-owned, so a pod rendered from a
// stored revision carries that revision's protocol, not the current
// spec's.
func templateFromPayload(input workload.ReconcileInput, name string, payload *revision.DataPayload) (podTemplate, error) {
	if payload == nil || payload.PodSpec == nil {
		return podTemplate{}, fmt.Errorf("revision %s records no pod template", name)
	}
	tmpl := podTemplate{
		revision:      query.RevisionFromName(name),
		podSpec:       payload.PodSpec,
		workerPodSpec: payload.WorkerPodSpec,
		meta:          revisionMeta(input.DesiredSpec.PodTemplateObjectMeta, payload.PodMeta),
	}
	if payload.PairingProtocol != nil {
		tmpl.pairingProtocol = *payload.PairingProtocol
	}
	return tmpl, nil
}

// revisionMeta composes the labels and annotations a pod of a stored
// revision carries: every key the revision hashed takes the revision's
// value, and a key the revision does not name keeps the current
// template's. The keys no revision hashes — the admission plane's queue
// labels, the annotations inherited from the owner — must keep following
// the live spec, and the renderer cannot tell those apart from a key a
// newer revision added, so the current template is the base and the
// stored revision overrides it.
func revisionMeta(current, stored *metav1.ObjectMeta) *metav1.ObjectMeta {
	if stored == nil || (len(stored.Labels) == 0 && len(stored.Annotations) == 0) {
		return current
	}
	out := &metav1.ObjectMeta{}
	if current != nil {
		out.Labels = cloneStringMap(current.Labels)
		out.Annotations = cloneStringMap(current.Annotations)
	}
	if len(stored.Labels) > 0 && out.Labels == nil {
		out.Labels = make(map[string]string, len(stored.Labels))
	}
	for k, v := range stored.Labels {
		out.Labels[k] = v
	}
	if len(stored.Annotations) > 0 && out.Annotations == nil {
		out.Annotations = make(map[string]string, len(stored.Annotations))
	}
	for k, v := range stored.Annotations {
		out.Annotations[k] = v
	}
	return out
}

func cloneStringMap(in map[string]string) map[string]string {
	if in == nil {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

// announceRevisionGone reports, once per attempt on the row, that a
// rebuild of Instance idx cannot render revision name because its
// ControllerRevision is gone. The rebuild creates nothing: rendering the
// current template under that revision's label would route, count and
// drain one revision while running another. The attempt's own deadline
// bounds the wait.
func announceRevisionGone(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, idx int32, name string) error {
	announced, err := status.Announce(ctx, input, idx, workload.EventReasonRepairRevisionGone)
	if err != nil || !announced {
		return err
	}
	workload.RecordWarning(deps.Recorder, workload.EventTarget(input), workload.EventReasonRepairRevisionGone,
		"OMENative %s cannot rebuild revision %s: its ControllerRevision is gone, so no pod is created from the current template under that revision; the attempt waits for its deadline",
		workload.InstanceKey(input.Key.Component, idx), name)
	return nil
}
