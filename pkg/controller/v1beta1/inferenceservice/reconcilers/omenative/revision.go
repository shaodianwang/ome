package omenative

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"sort"

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
)

// revisionDataPayload is the canonical content hashed into the
// ControllerRevision name. Only steady-state fields are hashed; transient
// operation overlays (operation-id, drain annotations, migration
// anti-affinity, readiness-gate status) are deliberately excluded so an
// in-flight operation cannot manufacture a fresh revision just by stamping
// a temporary annotation on a pod.
//
// PodMeta carries the pod-template-level labels and annotations the user
// can edit independently of PodSpec. The field is part of the JSON shape
// from day one (no omitempty) so adding non-nil values later doesn't
// invalidate every previously-recorded revision name. v1 callers leave
// it nil; the consuming Update PR will populate it from the rendered
// template metadata.
type revisionDataPayload struct {
	PodSpec *corev1.PodSpec    `json:"podSpec"`
	PodMeta *metav1.ObjectMeta `json:"podMeta"`
}

// revisionHash returns the deterministic hash that becomes the
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
// rendered Component metadata. Captured so changes to pod-template
// labels/annotations on the InferenceService spec trigger a new
// revision and a rollout (REV-8). nil is treated as "no metadata"
// and produces the same hash as an empty ObjectMeta — both serialize
// to the same `podMeta` JSON shape.
func revisionHash(template *corev1.PodSpec, templateMeta *metav1.ObjectMeta, collisionCount *int32) (string, []byte, error) {
	if template == nil {
		return "", nil, fmt.Errorf("nil pod template")
	}
	raw, err := json.Marshal(revisionDataPayload{
		PodSpec: template,
		PodMeta: revisionTemplateMeta(templateMeta),
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
	// REV-3: frame the CollisionCount with delimiters so it can't
	// concatenate ambiguously with the trailing bytes of `raw`. Without
	// framing, raw bytes ending in "1" plus cc=0 hash the same input as
	// raw bytes ending in "10" plus a different cc — silent collision.
	//
	// Side effect: every existing CR's hash changes once when this
	// lands. Each existing alpha ISVC will see one forced recreate
	// rollout on the next reconcile (RunningRevision points at the old
	// hash, UpdateRevision computes the new one). Same shape as the
	// REV-8 PodMeta-capture one-time bump.
	_, _ = fmt.Fprintf(h, "|cc=%d|", cc)
	return fmt.Sprintf("%08x", h.Sum32()), raw, nil
}

// revisionTemplateMeta strips the rendered ObjectMeta down to the
// fields that represent user intent — labels and annotations.
// Operational fields (UID, ResourceVersion, CreationTimestamp,
// managed-by-style controller fields) are excluded so the hash
// stays stable across reconciles when only those change.
//
// Returns nil when the source carries no labels AND no annotations
// so the JSON serializes as `"podMeta":null`, preserving the
// legacy hash shape for ISVCs that don't yet carry template metadata.
// This avoids a one-time phantom rollout when this code first
// deploys against existing alpha workloads. ISVCs that DO have
// template labels/annotations will see a single forced revision
// bump on the first reconcile after this lands — those workloads
// have been silently rolling without a real revision history per
// REV-8, so the bump materializes the deferred capture.
func revisionTemplateMeta(src *metav1.ObjectMeta) *metav1.ObjectMeta {
	if src == nil || (len(src.Labels) == 0 && len(src.Annotations) == 0) {
		return nil
	}
	return &metav1.ObjectMeta{
		Labels:      src.Labels,
		Annotations: src.Annotations,
	}
}

// revisionName composes the ControllerRevision object name from the parent
// ISVC name, the Component, and the hash. Deterministic — the same inputs
// always yield the same name, which is what makes "reuse-if-exists" work.
func revisionName(isvcName string, component v1beta1.ComponentType, hash string) string {
	return fmt.Sprintf("%s-%s-%s", isvcName, component, hash)
}

// EnsureControllerRevision creates or reuses a ControllerRevision matching
// the given Component's steady-state pod template.
//
// reads is the live API reader (typically mgr.GetAPIReader()) used for
// the two correctness-critical reads: the initial existence check and
// the re-Get after a Create→AlreadyExists race. Using the cached
// client.Client for these reads would let a stale cache return
// NotFound for an object the API server already holds, causing the
// function to either re-Create over a concurrent writer or treat a
// real conflict as a fresh-create failure. The cached client c is
// still used for the Create itself and for retention bookkeeping.
//
// Return value `collision` reports a hash collision — a CR with the same
// computed name already exists, but its Data payload differs from what we
// would have written. The caller (Update) is expected to bump
// CollisionCount in OMENativeComponentStatus and invoke this function
// again with the new salt; the retry yields a different name and lands.
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

	hash, raw, err := revisionHash(template, templateMeta, collisionCount)
	if err != nil {
		return nil, false, err
	}
	name := revisionName(isvc.Name, component, hash)
	key := client.ObjectKey{Namespace: isvc.Namespace, Name: name}

	existing := &appsv1.ControllerRevision{}
	if getErr := reads.Get(ctx, key, existing); getErr == nil {
		// Same name already exists. REV-4: validate the controller
		// OwnerReference UID matches the current ISVC before reusing —
		// a CR left over from a previously-deleted-and-recreated ISVC
		// of the same name would otherwise be silently adopted, mixing
		// histories. Treat foreign ownership as a collision so the
		// caller bumps CollisionCount and writes a fresh-named CR.
		if !controllerOwnedBy(existing.OwnerReferences, isvc.UID) {
			return existing, true, nil
		}
		// If the Data matches, we reuse; if not, the caller must bump
		// CollisionCount and retry.
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
			Name:      name,
			Namespace: isvc.Namespace,
			Labels:    revisionLabels(isvc.Name, component),
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(isvc, isvcGroupVersionKind),
			},
		},
		Data:     runtime.RawExtension{Raw: raw},
		Revision: nextRev,
	}
	if cerr := c.Create(ctx, cr); cerr != nil {
		if !apierrors.IsAlreadyExists(cerr) {
			return nil, false, fmt.Errorf("create ControllerRevision %s: %w", name, cerr)
		}
		// Lost a race against another reconcile. Re-read via the live
		// reader (the cached client may still show NotFound) and apply
		// the same ownership + match-or-collision logic the existing-
		// on-Get branch uses.
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

// controllerOwnedBy reports whether refs contains a controller
// OwnerReference pointing at uid. Used by EnsureControllerRevision
// (REV-4) to refuse to reuse a CR that belonged to a previously-
// deleted ISVC of the same name. Treats nil-Controller and
// Controller=false the same — only a true controller reference
// asserts the parent claim we care about.
func controllerOwnedBy(refs []metav1.OwnerReference, uid types.UID) bool {
	for _, ref := range refs {
		if ref.Controller != nil && *ref.Controller && ref.UID == uid {
			return true
		}
	}
	return false
}

// RetainControllerRevisions sweeps OMENative-owned ControllerRevisions for
// the Component and keeps the most recent maxNonLive non-live ones, in
// addition to any revision named in liveNames. Older non-live revisions
// are deleted. Order is by .Revision (highest = newest).
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

// listControllerRevisions returns every ControllerRevision owned by the
// given (isvc, component) tuple, identified by labels stamped at create
// time. Takes a client.Reader so the caller can pass either the cached
// client or a live API reader; correctness-critical paths
// (EnsureControllerRevision, RetainControllerRevisions) pass the live
// reader to avoid acting on stale cache contents.
func listControllerRevisions(
	ctx context.Context,
	reads client.Reader,
	isvc *v1beta1.InferenceService,
	component v1beta1.ComponentType,
) ([]appsv1.ControllerRevision, error) {
	list := &appsv1.ControllerRevisionList{}
	sel := labels.SelectorFromSet(labels.Set{
		constants.InferenceServicePodLabelKey: isvc.Name,
		constants.OMEComponentLabel:           string(component),
		labelManagedBy:                        managedByOMENative,
	})
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

// revisionLabels are stamped on every ControllerRevision so the list
// selector picks them up. Mirror the pod label set so a single
// label-based query can answer "everything OMENative owns for this
// (isvc, component)."
func revisionLabels(isvcName string, component v1beta1.ComponentType) map[string]string {
	return map[string]string{
		constants.InferenceServicePodLabelKey: isvcName,
		constants.OMEComponentLabel:           string(component),
		labelManagedBy:                        managedByOMENative,
	}
}
