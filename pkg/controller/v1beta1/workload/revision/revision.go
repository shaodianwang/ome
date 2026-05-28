// Package revision owns the ControllerRevision history for a
// per-workload pod template — deterministic hashing, collision-aware
// create, retention.
//
// Boundary: this package is a leaf. It depends on Kubernetes API
// machinery + the project's v1beta1 types + constants for the
// project-wide pod label keys. The labels stamped on emitted CRs are
// taken from the caller-supplied workload.WorkloadKey so the same
// labels carry through the pod-selector composition the rest of the
// workload pipeline does.
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
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/audit"
)

// WorkloadKey is the small subset of workload.WorkloadKey this package
// needs. Defining it locally (rather than importing workload) keeps
// this leaf importable by both the workload top-level package (commit
// 9) and by adapters that hold their own composition of these fields.
// Adapters typically build it from the workload.WorkloadKey they
// already have in scope.
type WorkloadKey struct {
	Namespace string
	Name      string
	Labels    map[string]string
}

// DataPayload is the canonical content hashed into the
// ControllerRevision name. Only steady-state fields are hashed;
// transient operation overlays (operation-id, drain annotations,
// migration anti-affinity, readiness-gate status) are deliberately
// excluded so an in-flight operation cannot manufacture a fresh
// revision just by stamping a temporary annotation on a pod.
//
// PodSpec carries the leader / single-pod template.
//
// PodMeta carries the pod-template-level labels and annotations the
// user can edit independently of PodSpec. The field is part of the JSON
// shape from day one (no omitempty) so adding non-nil values later
// doesn't invalidate every previously-recorded revision name.
//
// WorkerPodSpec carries the multi-pod Component's worker template (set
// only when the Component declares both Leader and Worker). It uses
// `omitempty` so single-pod revisions — which have never carried a
// worker — keep their pre-multi-node JSON shape and hash. Without
// omitempty, every single-pod ISVC would see a phantom rollout on the
// first reconcile after the multi-pod field landed.
type DataPayload struct {
	PodSpec       *corev1.PodSpec    `json:"podSpec"`
	PodMeta       *metav1.ObjectMeta `json:"podMeta"`
	WorkerPodSpec *corev1.PodSpec    `json:"workerPodSpec,omitempty"`
}

// Hash returns the deterministic hash that becomes the
// ControllerRevision name suffix, plus the canonical serialized bytes
// stored in CR.Data.Raw. collisionCount is folded into the hash so a
// retry after a same-name collision produces a different name.
//
// Single-pod variant — does not include a worker template. Multi-pod
// Components must use HashWithWorker so worker-only spec changes trigger
// a new revision. See HashWithWorker for the multi-pod path; this
// function is the single-pod / no-worker shorthand.
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
	return HashWithWorker(template, nil, templateMeta, collisionCount)
}

// HashWithWorker is Hash with an explicit worker PodSpec slot. Use for
// multi-pod Components (Leader + Worker); pass workerSpec=nil for
// single-pod / no-worker Components, in which case the canonical bytes
// and hash are identical to Hash(template, templateMeta, collisionCount).
//
// The worker template participates in the revision hash so a worker-only
// image / env / resource bump triggers a new revision and a rollout. The
// alternative — rendering the worker payload at create time without
// hashing it — leaves worker-only spec changes silently un-rolled,
// which is exactly the split-brain failure OEP-0007 §"Update Strategy"
// requires the revision pipeline to prevent.
func HashWithWorker(template, workerSpec *corev1.PodSpec, templateMeta *metav1.ObjectMeta, collisionCount *int32) (string, []byte, error) {
	if template == nil {
		return "", nil, fmt.Errorf("nil pod template")
	}
	raw, err := json.Marshal(DataPayload{
		PodSpec:       template,
		PodMeta:       TemplateMeta(templateMeta),
		WorkerPodSpec: workerSpec,
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
// legacy `"podMeta":null` shape so workloads that don't yet carry
// template metadata don't see a phantom rollout when this code first
// deploys.
//
// Filters out lifecycle annotations that the controller writes back to
// the owner (migration requests) so adding/removing them does not bump
// the revision hash. Without this, a migration request landing on the
// owner would drift every Component's revision hash — including
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

// pausedRolloutAnnotation is the operator-set ISVC annotation that
// freezes coordination groups at their current phase. Duplicated from
// coordination.PausedRolloutAnnotation to keep this package a leaf —
// importing coordination here would close a cycle.
const pausedRolloutAnnotation = "ome.io/rollout-paused"

// isLifecycleAnnotation reports whether the annotation key is one the
// controller reads or writes as part of normal reconcile (migration
// requests, rollout-paused signal) and therefore must not feed into
// the pod-template revision hash. Without this filter, adding or
// removing the annotation would flip the ControllerRevision name and
// drive a phantom rollout on a spec the user never edited.
func isLifecycleAnnotation(key string) bool {
	if strings.HasPrefix(key, audit.MigrationRequestAnnotationPrefix) {
		return true
	}
	return key == pausedRolloutAnnotation
}

// Name composes the ControllerRevision object name from the WorkloadKey
// and the hash — `<key.Name>-<hash>`. Deterministic, so "reuse-if-exists"
// works.
//
// For the ISVC adapter, key.Name = "<isvc>-<component>", producing the
// same name shape (`<isvc>-<component>-<hash>`) that existed pre-refactor.
// IR-adapter callers set key.Name the same way (since one IR is one
// (ISVC, Component) tuple), so cross-adapter CRs share the same name and
// the in-place migration window stays seamless.
func Name(key WorkloadKey, hash string) string {
	return fmt.Sprintf("%s-%s", key.Name, hash)
}

// Labels are stamped on every ControllerRevision so the list selector
// picks them up. Taken directly from the WorkloadKey's seed label set,
// which the adapter populated with the {ome.io/inferenceservice,
// component, managed-by} trio so the existing pod selector also picks
// up the CRs.
func Labels(key WorkloadKey) map[string]string {
	out := make(map[string]string, len(key.Labels))
	for k, v := range key.Labels {
		out[k] = v
	}
	return out
}

// EnsureControllerRevision creates or reuses a CR matching the
// workload's pod template. `reads` must be the live API reader: a
// stale cache could return NotFound for an object the apiserver
// already holds, causing re-Create over a concurrent writer.
//
// collision=true signals a hash collision (same name, different Data,
// or foreign ownership). Caller bumps CollisionCount and retries with a
// different salt so the next name lands.
//
// New CRs are stamped with Labels() and a controller OwnerReference
// for the workload owner; existing CRs keep whatever labels/owners they
// already have.
//
// Single-pod variant — does not hash a worker template. Multi-pod
// callers use EnsureControllerRevisionWithWorker so worker-only spec
// changes trigger a new CR.
func EnsureControllerRevision(
	ctx context.Context,
	c client.Client,
	reads client.Reader,
	owner client.Object,
	ownerGVK schema.GroupVersionKind,
	key WorkloadKey,
	template *corev1.PodSpec,
	templateMeta *metav1.ObjectMeta,
	collisionCount *int32,
) (cr *appsv1.ControllerRevision, collision bool, err error) {
	return EnsureControllerRevisionWithWorker(ctx, c, reads, owner, ownerGVK, key, template, nil, templateMeta, collisionCount)
}

// EnsureControllerRevisionWithWorker is the multi-pod-aware sibling of
// EnsureControllerRevision: it folds workerSpec into the revision hash
// so a worker-only spec change produces a new CR and triggers a rollout.
// Pass workerSpec=nil to get behavior identical to
// EnsureControllerRevision (same name, same Data.Raw).
func EnsureControllerRevisionWithWorker(
	ctx context.Context,
	c client.Client,
	reads client.Reader,
	owner client.Object,
	ownerGVK schema.GroupVersionKind,
	key WorkloadKey,
	template *corev1.PodSpec,
	workerSpec *corev1.PodSpec,
	templateMeta *metav1.ObjectMeta,
	collisionCount *int32,
) (cr *appsv1.ControllerRevision, collision bool, err error) {
	if owner == nil {
		return nil, false, fmt.Errorf("EnsureControllerRevision: nil owner")
	}
	if c == nil {
		return nil, false, fmt.Errorf("EnsureControllerRevision: nil client")
	}
	if reads == nil {
		return nil, false, fmt.Errorf("EnsureControllerRevision: nil reader")
	}

	hash, raw, err := HashWithWorker(template, workerSpec, templateMeta, collisionCount)
	if err != nil {
		return nil, false, err
	}
	name := Name(key, hash)
	objKey := client.ObjectKey{Namespace: key.Namespace, Name: name}

	existing := &appsv1.ControllerRevision{}
	if getErr := reads.Get(ctx, objKey, existing); getErr == nil {
		// Foreign ownership = collision: a CR left over from a
		// previously-deleted-and-recreated owner of the same name would
		// otherwise be silently adopted, mixing histories.
		if !controllerOwnedBy(existing.OwnerReferences, owner.GetUID()) {
			return existing, true, nil
		}
		if !bytes.Equal(existing.Data.Raw, raw) {
			return existing, true, nil
		}
		return existing, false, nil
	} else if !apierrors.IsNotFound(getErr) {
		return nil, false, fmt.Errorf("get ControllerRevision %s: %w", name, getErr)
	}

	nextRev, err := nextRevisionNumber(ctx, reads, key)
	if err != nil {
		return nil, false, err
	}

	cr = &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{
			Name:            name,
			Namespace:       key.Namespace,
			Labels:          Labels(key),
			OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(owner, ownerGVK)},
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
		if getErr := reads.Get(ctx, objKey, cr); getErr != nil {
			return nil, false, fmt.Errorf("re-get after AlreadyExists: %w", getErr)
		}
		if !controllerOwnedBy(cr.OwnerReferences, owner.GetUID()) {
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

// RetainControllerRevisions sweeps the workload-scoped ControllerRevisions
// and keeps the most recent maxNonLive non-live ones, in addition to any
// revision named in liveNames. Older non-live revisions are deleted. Order
// is by .Revision (highest = newest).
//
// reads is the live API reader used for the listing step so retention
// decisions aren't made against a stale cache.
//
// liveNames typically holds CurrentRevision and UpdateRevision plus
// every InstanceStatus.RunningRevision/TargetRevision. Empty entries
// are ignored. The recommended default for maxNonLive is 10.
func RetainControllerRevisions(
	ctx context.Context,
	c client.Client,
	reads client.Reader,
	key WorkloadKey,
	maxNonLive int,
	liveNames ...string,
) error {
	if maxNonLive < 0 {
		return fmt.Errorf("RetainControllerRevisions: maxNonLive must be >= 0")
	}
	existing, err := listControllerRevisions(ctx, reads, key)
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

// listControllerRevisions returns every ControllerRevision matching the
// workload's seed label set.
func listControllerRevisions(
	ctx context.Context,
	reads client.Reader,
	key WorkloadKey,
) ([]appsv1.ControllerRevision, error) {
	list := &appsv1.ControllerRevisionList{}
	sel := labels.SelectorFromSet(labels.Set(Labels(key)))
	if err := reads.List(ctx, list,
		client.InNamespace(key.Namespace),
		client.MatchingLabelsSelector{Selector: sel},
	); err != nil {
		return nil, fmt.Errorf("list ControllerRevisions: %w", err)
	}
	return list.Items, nil
}

// nextRevisionNumber returns max(.Revision)+1 across the workload's
// existing CRs, or 1 when none exist. Reads via the supplied reader so
// the caller can force a live read when the monotonicity guarantee
// matters (two concurrent reconciles reading from a stale cache could
// otherwise both compute the same max+1).
func nextRevisionNumber(
	ctx context.Context,
	reads client.Reader,
	key WorkloadKey,
) (int64, error) {
	existing, err := listControllerRevisions(ctx, reads, key)
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

// CollectLiveRevisionNames is the union retention must protect:
// CurrentRevision + UpdateRevision plus every
// InstanceStatus.RunningRevision/TargetRevision. Without the per-instance
// union, an in-flight migration whose source RunningRevision points to
// an older CR could have that CR swept mid-flight, then the migration
// resume path returns nil and the migration bails forever.
//
// Takes the slice of InstanceStatuses + the two revision pointers
// directly so the call works against either an OMENativeComponentStatus
// (ISVC path) or an InferenceReplicaStatus (IR path) without needing a
// type-specific reader. Either adapter projects its status surface into
// these primitive args.
func CollectLiveRevisionNames(current, update string, instances []v1beta1.OMENativeInstanceStatus) []string {
	seen := map[string]struct{}{}
	add := func(name string) {
		if name == "" {
			return
		}
		seen[name] = struct{}{}
	}
	add(current)
	add(update)
	for _, inst := range instances {
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
