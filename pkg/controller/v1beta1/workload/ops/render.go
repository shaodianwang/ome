package ops

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	workload "github.com/ome-projects/ome/pkg/controller/v1beta1/workload/types"
)

// Render builds the corev1.Pod for one ordinal of one Runner of one
// Instance, derived from a shared rendered PodSpec template. Equivalent
// to RenderWithRevision with revisionHash="" — kept as a back-compat
// entry point for tests and callers that don't have a target revision
// in scope.
//
// Owner reference comes from owner + ownerGVK; the workload key seeds
// label/selector composition (key.SelectorLabels carries the
// {ome.io/inferenceservice, component, managed-by} trio adapters
// populate at construction time).
func Render(
	owner client.Object,
	ownerGVK schema.GroupVersionKind,
	key workload.Key,
	podSpec *corev1.PodSpec,
	plan workload.ComponentPlan,
	inst workload.InstancePlan,
	runner workload.RunnerPlan,
	ordinal int32,
	hook workload.RenderHook,
) (*corev1.Pod, error) {
	return RenderWithRevision(owner, ownerGVK, key, podSpec, nil, plan, inst, runner, ordinal, "", hook)
}

// RenderWithRevision builds the corev1.Pod for one ordinal of one
// Runner of one Instance, stamping ome.io/revision-hash with
// revisionHash so per-revision Services can select the pod.
//
// The returned Pod carries:
//   - stable name <key.Name>-<inst-idx>-<runner>-<ordinal>
//   - hostname + subdomain so peer DNS resolves under the Component's
//     headless service
//   - the workload labels (ome.io/inferenceservice / component /
//     instance-index / instance-incarnation / runner / managed-by /
//     pod-ordinal), plus ome.io/revision-hash when revisionHash is
//     non-empty
//   - pod-template annotations from podTemplateObjectMeta
//   - controller-owned `ome.io/serving` readiness gate appended after
//     any user-declared gates
//   - OME_* environment variables injected into every container
//     (workload-controlled values override user-supplied ones)
//   - one OwnerReference back to the owner (controller=true)
//
// After composing the canonical pod, the adapter-supplied RenderHook
// (workload.Deps.RenderHook) is invoked once with (pod, runnerName,
// ordinal, revisionHash) so adapters can layer their own overlays
// (e.g., coordination.InjectPeerEnv for the ISVC adapter).
//
// podTemplateObjectMeta may be nil for back-compat tests that don't
// have a fully-built ObjectMeta in scope; user labels / annotations
// are then skipped (workload-owned labels still land).
func RenderWithRevision(
	owner client.Object,
	ownerGVK schema.GroupVersionKind,
	key workload.Key,
	podSpec *corev1.PodSpec,
	podTemplateObjectMeta *metav1.ObjectMeta,
	plan workload.ComponentPlan,
	inst workload.InstancePlan,
	runner workload.RunnerPlan,
	ordinal int32,
	revisionHash string,
	hook workload.RenderHook,
) (*corev1.Pod, error) {
	if owner == nil {
		return nil, fmt.Errorf("nil owner")
	}
	if podSpec == nil {
		return nil, fmt.Errorf("nil PodSpec")
	}

	// Pod-naming inputs derived from the Key + plan. The query.PodName
	// helper takes (isvc-from-OwnerName, component, idx, runner, ordinal);
	// adapters populate Key.OwnerName with the owner's bare name.
	isvcName := key.OwnerName
	name := query.PodName(isvcName, plan.Component, inst.Index, runner.Name, ordinal)
	subdomain := query.HeadlessServiceName(isvcName, plan.Component)

	// Start from the caller's user-intent labels (so user-declared
	// labels land on the pod), then overlay the workload-mandatory
	// labels last so selectors keep working even if the user tried to
	// set the same key. Same merge shape for annotations below.
	lbls := make(map[string]string)
	if podTemplateObjectMeta != nil {
		for k, v := range podTemplateObjectMeta.Labels {
			lbls[k] = v
		}
	}
	for k, v := range podLabels(isvcName, plan.Component, inst.Index, runner.Name, inst.Incarnation, ordinal) {
		lbls[k] = v
	}
	if revisionHash != "" {
		lbls[query.LabelRevisionHash] = revisionHash
	}
	// Multi-pod Instances stamp the scheduler-plugins pod-group label so
	// the gang-aware scheduler co-schedules every pod in the Instance.
	// Single-pod Instances skip the label — no PodGroup, no gang. The
	// PodGroup itself is created by the workload gang reconciler before
	// the first pod is created; this label is the pod-side correlator.
	if inst.TotalPods() > 1 {
		lbls[query.LabelPodGroup] = query.PodGroupName(isvcName, plan.Component, inst.Index)
	}
	var anns map[string]string
	if podTemplateObjectMeta != nil && len(podTemplateObjectMeta.Annotations) > 0 {
		anns = make(map[string]string, len(podTemplateObjectMeta.Annotations))
		for k, v := range podTemplateObjectMeta.Annotations {
			anns[k] = v
		}
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   key.Namespace,
			Labels:      lbls,
			Annotations: anns,
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(owner, ownerGVK),
			},
		},
		Spec: *podSpec.DeepCopy(),
	}

	pod.Spec.Hostname = name
	pod.Spec.Subdomain = subdomain
	// Dedup the ome.io/serving gate when the user template / ServingRuntime
	// already declared it. Kubelet ANDs all gates into PodReady, so two
	// copies would keep PodReady=False forever (controller only writes one
	// status).
	if !hasServingReadinessGate(pod.Spec.ReadinessGates) {
		pod.Spec.ReadinessGates = append(pod.Spec.ReadinessGates, corev1.PodReadinessGate{
			ConditionType: query.ServingConditionType,
		})
	}

	// Last-line container-name backfill: apiserver rejects Pod creates
	// when any container has Name="" with
	// `spec.containers[N].name: Required value`. The upstream merge
	// (utils.MergeRuntimeContainers / utils.restoreRunnerName) already
	// stamps constants.MainContainerName when neither the ServingRuntime
	// nor the ISVC sets a runner name, but a caller that builds a PodSpec
	// directly (back-compat tests, future direct callers) can still hand
	// us an unnamed container. Stamp the canonical default here so the
	// render path is the single source of truth at apply time.
	backfillEmptyContainerNames(&pod.Spec)

	injectOMENativeEnv(pod, plan, inst, runner, ordinal, isvcName)
	applyMigrationOverlay(pod, inst.MigrationOverlay)

	// Adapter-supplied hook: peer-env injection for the ISVC adapter,
	// nil for the IR adapter (peer-env stays on the parent ISVC
	// controller for v1).
	if hook != nil {
		hook(pod, runner.Name, ordinal, revisionHash)
	}

	return pod, nil
}

// backfillEmptyContainerNames stamps constants.MainContainerName on any
// regular or init container whose Name is the empty string. Mirrors the
// final fallback in utils.MergeRuntimeContainers so the apply-time pod
// is always nameable regardless of which upstream path produced its
// PodSpec. Idempotent — already-named containers round-trip unchanged.
func backfillEmptyContainerNames(spec *corev1.PodSpec) {
	if spec == nil {
		return
	}
	for i := range spec.Containers {
		if spec.Containers[i].Name == "" {
			spec.Containers[i].Name = constants.MainContainerName
		}
	}
	for i := range spec.InitContainers {
		if spec.InitContainers[i].Name == "" {
			spec.InitContainers[i].Name = constants.MainContainerName
		}
	}
}

// applyMigrationOverlay layers migration placement onto the rendered pod.
// Hard anti-affinity moves the surge off FromNode; preferred affinity
// nudges toward HintTargetNodes without making placement hard-fail.
// Idempotent: a second Render after a cache-lag retry doesn't
// double-stamp the same NotIn / hint term.
func applyMigrationOverlay(pod *corev1.Pod, overlay *workload.MigrationOverlay) {
	if overlay == nil {
		return
	}
	if pod.Spec.Affinity == nil {
		pod.Spec.Affinity = &corev1.Affinity{}
	}
	if pod.Spec.Affinity.NodeAffinity == nil {
		pod.Spec.Affinity.NodeAffinity = &corev1.NodeAffinity{}
	}
	na := pod.Spec.Affinity.NodeAffinity

	if overlay.FromNode != "" {
		// RequiredDuringSchedulingIgnoredDuringExecution: the surge
		// pod MUST schedule somewhere other than FromNode. NodeSelector
		// terms are OR'd; each term's MatchExpressions are AND'd. We
		// append to each existing term and add a default term when
		// none exist so the require-not-FromNode condition combines
		// with any caller-supplied required selectors.
		req := corev1.NodeSelectorRequirement{
			Key:      "kubernetes.io/hostname",
			Operator: corev1.NodeSelectorOpNotIn,
			Values:   []string{overlay.FromNode},
		}
		if na.RequiredDuringSchedulingIgnoredDuringExecution == nil {
			na.RequiredDuringSchedulingIgnoredDuringExecution = &corev1.NodeSelector{
				NodeSelectorTerms: []corev1.NodeSelectorTerm{{MatchExpressions: []corev1.NodeSelectorRequirement{req}}},
			}
		} else {
			for i := range na.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms {
				term := &na.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms[i]
				if !hasHostnameNotIn(term.MatchExpressions, overlay.FromNode) {
					term.MatchExpressions = append(term.MatchExpressions, req)
				}
			}
		}
	}

	if len(overlay.HintTargetNodes) > 0 && !hasPreferredHintTerm(na.PreferredDuringSchedulingIgnoredDuringExecution, overlay.HintTargetNodes) {
		// PreferredDuringSchedulingIgnoredDuringExecution: nudge the
		// scheduler toward HintTargetNodes but don't block scheduling
		// when none are available.
		na.PreferredDuringSchedulingIgnoredDuringExecution = append(
			na.PreferredDuringSchedulingIgnoredDuringExecution,
			corev1.PreferredSchedulingTerm{
				Weight: overlayPreferredHintWeight,
				Preference: corev1.NodeSelectorTerm{
					MatchExpressions: []corev1.NodeSelectorRequirement{{
						Key:      "kubernetes.io/hostname",
						Operator: corev1.NodeSelectorOpIn,
						Values:   overlay.HintTargetNodes,
					}},
				},
			},
		)
	}
}

// overlayPreferredHintWeight is the scheduler weight on the migration-hint
// preferred affinity. Set mid-range so the nudge stacks with — rather
// than competes against — operator-supplied preferences.
const overlayPreferredHintWeight int32 = 50

// WouldOverlayConflictWithNodeAffinity reports whether the migration
// overlay's NotIn[FromNode] would make the source PodSpec unschedulable.
// Conflict means every existing required NodeSelectorTerm pins
// kubernetes.io/hostname to exactly FromNode — there's no other host
// the surge could land on. A single term that permits other hosts (or
// doesn't pin hostname at all) is enough to defuse the conflict.
//
// Called from migrate.go's up-front validation: a true result means
// reject the migration with a Failed audit row rather than create a
// permanently-pending surge pod.
func WouldOverlayConflictWithNodeAffinity(spec *corev1.PodSpec, overlay *workload.MigrationOverlay) bool {
	if overlay == nil || overlay.FromNode == "" || spec == nil {
		return false
	}
	if spec.Affinity == nil || spec.Affinity.NodeAffinity == nil {
		return false
	}
	req := spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution
	if req == nil || len(req.NodeSelectorTerms) == 0 {
		return false
	}
	for _, term := range req.NodeSelectorTerms {
		if !termPinsHostnameTo(term, overlay.FromNode) {
			return false
		}
	}
	return true
}

// termPinsHostnameTo returns true when term's hostname In-requirement
// permits ONLY hostname. A term without any hostname In-requirement
// returns false — it doesn't pin hostname at all.
func termPinsHostnameTo(term corev1.NodeSelectorTerm, hostname string) bool {
	for _, expr := range term.MatchExpressions {
		if expr.Key != "kubernetes.io/hostname" || expr.Operator != corev1.NodeSelectorOpIn {
			continue
		}
		if len(expr.Values) == 1 && expr.Values[0] == hostname {
			return true
		}
		return false
	}
	return false
}

// hasHostnameNotIn is the idempotency check for the NotIn[hostname]
// stamp; second-Render after a cache-lag retry won't duplicate.
func hasHostnameNotIn(exprs []corev1.NodeSelectorRequirement, hostname string) bool {
	for _, expr := range exprs {
		if expr.Key != "kubernetes.io/hostname" || expr.Operator != corev1.NodeSelectorOpNotIn {
			continue
		}
		for _, v := range expr.Values {
			if v == hostname {
				return true
			}
		}
	}
	return false
}

// hasPreferredHintTerm reports whether existing terms already include
// a preferred affinity for the same hostname In-set the overlay would
// add. Order-insensitive: {A,B,C} matches {C,B,A}.
func hasPreferredHintTerm(existing []corev1.PreferredSchedulingTerm, hints []string) bool {
	want := make(map[string]struct{}, len(hints))
	for _, h := range hints {
		want[h] = struct{}{}
	}
	for _, term := range existing {
		for _, expr := range term.Preference.MatchExpressions {
			if expr.Key != "kubernetes.io/hostname" || expr.Operator != corev1.NodeSelectorOpIn {
				continue
			}
			if len(expr.Values) != len(want) {
				continue
			}
			match := true
			for _, v := range expr.Values {
				if _, ok := want[v]; !ok {
					match = false
					break
				}
			}
			if match {
				return true
			}
		}
	}
	return false
}

// hasServingReadinessGate is the dedup check before Render appends the
// ome.io/serving gate.
func hasServingReadinessGate(gates []corev1.PodReadinessGate) bool {
	for _, g := range gates {
		if g.ConditionType == query.ServingConditionType {
			return true
		}
	}
	return false
}

func podLabels(isvc string, component v1beta1.ComponentType, instanceIdx int32, runner string, incarnation int64, ordinal int32) map[string]string {
	return map[string]string{
		constants.InferenceServicePodLabelKey: isvc,
		constants.OMEComponentLabel:           string(component),
		query.LabelInstanceIdx:                fmt.Sprintf("%d", instanceIdx),
		query.LabelInstanceIncarnation:        fmt.Sprintf("%d", incarnation),
		query.LabelRunner:                     runner,
		query.LabelManagedBy:                  query.ManagedByOMENative,
		query.LabelPodOrdinal:                 fmt.Sprintf("%d", ordinal),
	}
}

// injectOMENativeEnv overlays workload OME_* env vars onto every
// container. If a container declared a variable with the same Name,
// the workload value replaces it (the controller-owned values are the
// authoritative source for these names).
func injectOMENativeEnv(pod *corev1.Pod, plan workload.ComponentPlan, inst workload.InstancePlan, runner workload.RunnerPlan, ordinal int32, isvcName string) {
	env := buildOMENativeEnv(plan, inst, runner, ordinal, isvcName)
	owned := make(map[string]bool, len(env))
	for _, e := range env {
		owned[e.Name] = true
	}
	for i := range pod.Spec.Containers {
		kept := pod.Spec.Containers[i].Env[:0]
		for _, e := range pod.Spec.Containers[i].Env {
			if !owned[e.Name] {
				kept = append(kept, e)
			}
		}
		pod.Spec.Containers[i].Env = append(kept, env...)
	}
}

func buildOMENativeEnv(plan workload.ComponentPlan, inst workload.InstancePlan, runner workload.RunnerPlan, ordinal int32, isvcName string) []corev1.EnvVar {
	// OME_RUNNER_INDEX reports the runner's position within the gang
	// (0..runner.Size-1), NOT the pod-naming ordinal. For single-pod
	// runners (Size=1) it's always 0; SurgeThenDrain alternates the pod
	// name ordinal between 0 and 1 across surges, but the runner is
	// still gang-member 0 of 1 — runtime code that reads this env to
	// pick a shard / tp-rank / etc. must see a stable value across the
	// surge. For multi-pod runners (Size>1) the ordinal IS the gang
	// position; multi-pod surge will need its own naming dimension when
	// it lands.
	gangIndex := ordinal
	if runner.Size <= 1 {
		gangIndex = 0
	}
	env := []corev1.EnvVar{
		{Name: "OME_INFERENCESERVICE_NAME", Value: isvcName},
		{Name: "OME_COMPONENT", Value: string(plan.Component)},
		{Name: "OME_COMPONENT_REPLICAS", Value: fmt.Sprintf("%d", plan.Replicas)},
		{Name: "OME_INSTANCE_INDEX", Value: fmt.Sprintf("%d", inst.Index)},
		{Name: "OME_RUNNER", Value: runner.Name},
		{Name: "OME_RUNNER_SIZE", Value: fmt.Sprintf("%d", runner.Size)},
		{Name: "OME_RUNNER_INDEX", Value: fmt.Sprintf("%d", gangIndex)},
		{Name: "OME_INSTANCE_SUBDOMAIN", Value: query.InstanceSubdomain(isvcName, plan.Component, inst.Index)},
	}
	// OME_LEADER_ADDRESS is emitted only for multi-pod Instances (the
	// Instance has a "leader" Runner). Single-pod Instances have one
	// "default" Runner with no leader, so the variable is omitted —
	// runtimes branch on its presence to detect a multi-pod environment.
	if addr := leaderAddressForInstance(isvcName, plan.Component, inst); addr != "" {
		env = append(env, corev1.EnvVar{Name: "OME_LEADER_ADDRESS", Value: addr})
	}
	return env
}

// leaderAddressForInstance returns the peer-DNS address of the Instance's
// leader pod, or "" when the Instance has no "leader" Runner (single-pod
// "default" layout). The address is the SHORT form
// `<leader-pod-name>.<headless-service-name>` — the pod's DNS search
// path appends `.<ns>.svc.<cluster-domain>` at resolution time, so this
// resolves the same as the fully-qualified
// `<pod>.<headless>.<ns>.svc.cluster.local` form without the controller
// needing to discover the cluster's actual DNS domain.
func leaderAddressForInstance(isvcName string, component v1beta1.ComponentType, inst workload.InstancePlan) string {
	for _, r := range inst.Runners {
		if r.Name == "leader" {
			return fmt.Sprintf("%s.%s",
				query.PodName(isvcName, component, inst.Index, "leader", 0),
				query.HeadlessServiceName(isvcName, component),
			)
		}
	}
	return ""
}
