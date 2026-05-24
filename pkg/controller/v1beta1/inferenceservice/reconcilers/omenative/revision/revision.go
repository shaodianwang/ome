// Package revision owns the ControllerRevision history for a
// per-Component OMENative pod template — deterministic hashing,
// collision-aware create, retention.
//
// Boundary: this package is a leaf. It depends on Kubernetes API
// machinery + the project's v1beta1 types + constants for the
// project-wide pod label keys. It owns the labels + OwnerReference
// it stamps on the CRs it creates ("ome.io/managed-by=OMENative" plus
// the InferenceService GroupVersionKind) — the canonical copies of the
// same strings live in query/labels.go, query/names.go, and
// ops/render.go for the Pods + Services those other layers emit. A
// single source of truth would require pushing the OwnerReference
// helper down here too; the stable string-literal duplication is
// acceptable for now.
//
// Imports the audit subpackage solely to share the
// MigrationRequestAnnotationPrefix constant — the controller-written
// lifecycle annotation that must NOT participate in the revision hash.
package revision

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"sort"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/audit"
)

// DataPayload is the canonical content hashed into the
// ControllerRevision name. Only steady-state fields are hashed;
// transient operation overlays (operation-id, drain annotations,
// migration anti-affinity, readiness-gate status) are deliberately
// excluded so an in-flight operation cannot manufacture a fresh
// revision just by stamping a temporary annotation on a pod.
//
// PodMeta carries the pod-template-level labels and annotations the
// user can edit independently of PodSpec. The field is part of the JSON
// shape from day one (no omitempty) so adding non-nil values later
// doesn't invalidate every previously-recorded revision name.
type DataPayload struct {
	PodSpec *corev1.PodSpec    `json:"podSpec"`
	PodMeta *metav1.ObjectMeta `json:"podMeta"`
}

// Hash returns the deterministic hash that becomes the
// ControllerRevision name suffix, plus the canonical serialized bytes
// stored in CR.Data.Raw. collisionCount is folded into the hash so a
// retry after a same-name collision produces a different name.
//
// nil collisionCount is normalized to 0 before folding — the status
// field is *int32 and callers may legitimately pass either nil or
// ptr.To(int32(0)) for "no salt yet"; both must produce the same hash
// or a defaulter that normalizes the pointer would flip every CR hash
// on the next reconcile.
//
// templateMeta carries user-intent labels and annotations from the
// rendered Component metadata so template-meta changes trigger a new
// revision and a rollout. nil hashes the same as empty ObjectMeta —
// both serialize to the same `podMeta` JSON shape.
func Hash(template *corev1.PodSpec, templateMeta *metav1.ObjectMeta, collisionCount *int32) (string, []byte, error) {
	if template == nil {
		return "", nil, fmt.Errorf("nil pod template")
	}
	raw, err := json.Marshal(DataPayload{
		PodSpec: template,
		PodMeta: TemplateMeta(templateMeta),
	})
	if err != nil {
		return "", nil, fmt.Errorf("marshal revision payload: %w", err)
	}
	h := fnv.New32a()
	_, _ = h.Write(raw)
	cc := int32(0)
	if collisionCount != nil {
		cc = *collisionCount
	}
	// Delimit the CollisionCount so it can't concatenate ambiguously with
	// the trailing bytes of `raw` — raw ending in "1" plus cc=0 must not
	// hash the same as raw ending in "10" plus a different cc.
	_, _ = fmt.Fprintf(h, "|cc=%d|", cc)
	return fmt.Sprintf("%08x", h.Sum32()), raw, nil
}

// TemplateMeta strips ObjectMeta to user-intent fields (labels +
// annotations) only. Operational fields (UID, ResourceVersion, etc.)
// are excluded so the hash stays stable when only those change. Returns
// nil when neither labels nor annotations are set — preserves the
// legacy `"podMeta":null` shape so ISVCs that don't yet carry template
// metadata don't see a phantom rollout when this code first deploys.
//
// Filters out lifecycle annotations that the controller writes back to
// the ISVC (migration requests) so adding/removing them does not bump
// the revision hash. Without this, a migration request landing on the
// ISVC would drift every Component's revision hash — including
// Components that aren't the migration target — and force a phantom
// rollout that envtest cannot complete.
func TemplateMeta(src *metav1.ObjectMeta) *metav1.ObjectMeta {
	if src == nil || (len(src.Labels) == 0 && len(src.Annotations) == 0) {
		return nil
	}
	filtered := src.Annotations
	if hasLifecycleAnnotation(src.Annotations) {
		filtered = make(map[string]string, len(src.Annotations))
		for k, v := range src.Annotations {
			if isLifecycleAnnotation(k) {
				continue
			}
			filtered[k] = v
		}
	}
	if len(src.Labels) == 0 && len(filtered) == 0 {
		return nil
	}
	return &metav1.ObjectMeta{
		Labels:      src.Labels,
		Annotations: filtered,
	}
}

// hasLifecycleAnnotation reports whether any key in annotations is a
// controller-written lifecycle annotation. Fast path so the common
// no-lifecycle-annotation case skips the copy in TemplateMeta.
func hasLifecycleAnnotation(annotations map[string]string) bool {
	for k := range annotations {
		if isLifecycleAnnotation(k) {
			return true
		}
	}
	return false
}

// isLifecycleAnnotation reports whether the annotation key is one the
// controller mutates as part of normal reconcile (e.g., migration
// requests) and therefore must not feed into the pod-template revision
// hash. Add new prefixes here when they get introduced.
func isLifecycleAnnotation(key string) bool {
	return strings.HasPrefix(key, audit.MigrationRequestAnnotationPrefix)
}

// Name composes the ControllerRevision object name from the parent
// ISVC name, the Component, and the hash. Deterministic — the same
// inputs always yield the same name, which is what makes
// "reuse-if-exists" work.
func Name(isvcName string, component v1beta1.ComponentType, hash string) string {
	return fmt.Sprintf("%s-%s-%s", isvcName, component, hash)
}

// labelManagedBy / managedByValue are stamped on every CR this package
// creates and used as the selector for List operations. Duplicated
// from render.go's constants — same string, same intent.
const (
	labelManagedBy = "ome.io/managed-by"
	managedByValue = "OMENative"
)

// isvcGroupVersionKind is the GroupVersionKind the OwnerReference on
// every CR points at. Computed from v1beta1.SchemeGroupVersion so a
// future API version bump flips this in one place.
var isvcGroupVersionKind = v1beta1.SchemeGroupVersion.WithKind("InferenceService")

// Labels are stamped on every ControllerRevision so the list selector
// picks them up. Mirror the pod label set so a single label-based query
// can answer "everything OMENative owns for this (isvc, component)."
func Labels(isvcName string, component v1beta1.ComponentType) map[string]string {
	return map[string]string{
		constants.InferenceServicePodLabelKey: isvcName,
		constants.OMEComponentLabel:           string(component),
		labelManagedBy:                        managedByValue,
	}
}

// EnsureControllerRevision creates or reuses a CR matching the
// Component's pod template. `reads` must be the live API reader: a
// stale cache could return NotFound for an object the apiserver
// already holds, causing re-Create over a concurrent writer.
//
// collision=true signals a hash collision (same name, different Data,
// or foreign ownership). Caller bumps CollisionCount and retries with a
// different salt so the next name lands.
//
// New CRs are stamped with Labels() and a controller OwnerReference
// for the parent ISVC; existing CRs keep whatever labels/owners they
// already have.
func EnsureControllerRevision(
	ctx context.Context,
	c client.Client,
	reads client.Reader,
	isvc *v1beta1.InferenceService,
	component v1beta1.ComponentType,
	template *corev1.PodSpec,
	templateMeta *metav1.ObjectMeta,
	collisionCount *int32,
) (cr *appsv1.ControllerRevision, collision bool, err error) {
	if isvc == nil {
		return nil, false, fmt.Errorf("EnsureControllerRevision: nil ISVC")
	}
	if c == nil {
		return nil, false, fmt.Errorf("EnsureControllerRevision: nil client")
	}
	if reads == nil {
		return nil, false, fmt.Errorf("EnsureControllerRevision: nil reader")
	}

	hash, raw, err := Hash(template, templateMeta, collisionCount)
	if err != nil {
		return nil, false, err
	}
	name := Name(isvc.Name, component, hash)
	key := client.ObjectKey{Namespace: isvc.Namespace, Name: name}

	existing := &appsv1.ControllerRevision{}
	if getErr := reads.Get(ctx, key, existing); getErr == nil {
		// Foreign ownership = collision: a CR left over from a
		// previously-deleted-and-recreated ISVC of the same name would
		// otherwise be silently adopted, mixing histories.
		if !controllerOwnedBy(existing.OwnerReferences, isvc.UID) {
			return existing, true, nil
		}
		if !bytes.Equal(existing.Data.Raw, raw) {
			return existing, true, nil
		}
		return existing, false, nil
	} else if !apierrors.IsNotFound(getErr) {
		return nil, false, fmt.Errorf("get ControllerRevision %s: %w", name, getErr)
	}

	nextRev, err := nextRevisionNumber(ctx, reads, isvc, component)
	if err != nil {
		return nil, false, err
	}

	cr = &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{
			Name:            name,
			Namespace:       isvc.Namespace,
			Labels:          Labels(isvc.Name, component),
			OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(isvc, isvcGroupVersionKind)},
		},
		Data:     runtime.RawExtension{Raw: raw},
		Revision: nextRev,
	}
	if cerr := c.Create(ctx, cr); cerr != nil {
		if !apierrors.IsAlreadyExists(cerr) {
			return nil, false, fmt.Errorf("create ControllerRevision %s: %w", name, cerr)
		}
		// Lost the race; re-read via live reader (cache may still show
		// NotFound) and apply the same ownership / match-or-collision
		// gating as the existing-on-Get branch.
		if getErr := reads.Get(ctx, key, cr); getErr != nil {
			return nil, false, fmt.Errorf("re-get after AlreadyExists: %w", getErr)
		}
		if !controllerOwnedBy(cr.OwnerReferences, isvc.UID) {
			return cr, true, nil
		}
		if !bytes.Equal(cr.Data.Raw, raw) {
			return cr, true, nil
		}
	}
	return cr, false, nil
}

// controllerOwnedBy reports whether refs contains a Controller=true
// OwnerReference pointing at uid. Only a true controller ref asserts
// the parent claim we care about.
func controllerOwnedBy(refs []metav1.OwnerReference, uid types.UID) bool {
	for _, ref := range refs {
		if ref.Controller != nil && *ref.Controller && ref.UID == uid {
			return true
		}
	}
	return false
}

// RetainControllerRevisions sweeps the (isvc, component)-scoped
// ControllerRevisions and keeps the most recent maxNonLive non-live
// ones, in addition to any revision named in liveNames. Older non-live
// revisions are deleted. Order is by .Revision (highest = newest).
//
// reads is the live API reader used for the listing step so retention
// decisions aren't made against a stale cache.
//
// liveNames typically holds CurrentRevision and UpdateRevision from
// OMENativeComponentStatus. Empty entries are ignored. The recommended
// default for maxNonLive is 10.
func RetainControllerRevisions(
	ctx context.Context,
	c client.Client,
	reads client.Reader,
	isvc *v1beta1.InferenceService,
	component v1beta1.ComponentType,
	maxNonLive int,
	liveNames ...string,
) error {
	if maxNonLive < 0 {
		return fmt.Errorf("RetainControllerRevisions: maxNonLive must be >= 0")
	}
	existing, err := listControllerRevisions(ctx, reads, isvc, component)
	if err != nil {
		return err
	}

	live := make(map[string]struct{}, len(liveNames))
	for _, n := range liveNames {
		if n != "" {
			live[n] = struct{}{}
		}
	}

	nonLive := make([]appsv1.ControllerRevision, 0, len(existing))
	for _, cr := range existing {
		if _, ok := live[cr.Name]; ok {
			continue
		}
		nonLive = append(nonLive, cr)
	}
	if len(nonLive) <= maxNonLive {
		return nil
	}

	// .Revision is monotonic by construction (nextRevisionNumber returns
	// max+1) so it suffices as the sole sort key. No CreationTimestamp
	// tie-breaker is needed unless we later relax the monotonicity
	// guarantee — e.g., allow CollisionCount resets that could reuse a
	// revision number, which the spec currently does not permit.
	sort.Slice(nonLive, func(i, j int) bool {
		return nonLive[i].Revision > nonLive[j].Revision
	})

	// Don't bail on the first delete failure: a transient API error on
	// the oldest CR would otherwise leave every newer non-live CR behind
	// and starve retention until the failing one is hand-deleted. Collect
	// errors and continue.
	var sweepErr error
	for i := maxNonLive; i < len(nonLive); i++ {
		cr := nonLive[i]
		if derr := c.Delete(ctx, &cr); derr != nil && !apierrors.IsNotFound(derr) {
			sweepErr = errors.Join(sweepErr, fmt.Errorf("delete ControllerRevision %s: %w", cr.Name, derr))
		}
	}
	return sweepErr
}

// listControllerRevisions returns every ControllerRevision owned by
// the (isvc, component) tuple, identified by the labels stamped at
// create time.
func listControllerRevisions(
	ctx context.Context,
	reads client.Reader,
	isvc *v1beta1.InferenceService,
	component v1beta1.ComponentType,
) ([]appsv1.ControllerRevision, error) {
	list := &appsv1.ControllerRevisionList{}
	sel := labels.SelectorFromSet(labels.Set(Labels(isvc.Name, component)))
	if err := reads.List(ctx, list,
		client.InNamespace(isvc.Namespace),
		client.MatchingLabelsSelector{Selector: sel},
	); err != nil {
		return nil, fmt.Errorf("list ControllerRevisions: %w", err)
	}
	return list.Items, nil
}

// nextRevisionNumber returns max(.Revision)+1 across the Component's
// existing CRs, or 1 when none exist. Reads via the supplied reader so
// the caller can force a live read when the monotonicity guarantee
// matters (two concurrent reconciles reading from a stale cache could
// otherwise both compute the same max+1).
func nextRevisionNumber(
	ctx context.Context,
	reads client.Reader,
	isvc *v1beta1.InferenceService,
	component v1beta1.ComponentType,
) (int64, error) {
	existing, err := listControllerRevisions(ctx, reads, isvc, component)
	if err != nil {
		return 0, err
	}
	var maxRev int64
	for _, cr := range existing {
		if cr.Revision > maxRev {
			maxRev = cr.Revision
		}
	}
	return maxRev + 1, nil
}

// CollectLiveRevisionNames is the union retention must protect: the
// Component's Current/UpdateRevision plus every
// InstanceStatus.RunningRevision/TargetRevision. Without the per-instance
// union, an in-flight migration whose source RunningRevision points to
// an older CR could have that CR swept mid-flight, then the migration
// resume path returns nil and the migration bails forever.
func CollectLiveRevisionNames(cs *v1beta1.OMENativeComponentStatus) []string {
	if cs == nil {
		return nil
	}
	seen := map[string]struct{}{}
	add := func(name string) {
		if name == "" {
			return
		}
		seen[name] = struct{}{}
	}
	add(cs.CurrentRevision)
	add(cs.UpdateRevision)
	for _, inst := range cs.InstanceStatuses {
		add(inst.RunningRevision)
		add(inst.TargetRevision)
	}
	if len(seen) == 0 {
		return nil
	}
	out := make([]string, 0, len(seen))
	for n := range seen {
		out = append(out, n)
	}
	return out
}
