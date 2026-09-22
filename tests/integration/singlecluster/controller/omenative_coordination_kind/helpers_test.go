package omenative_coordination_kind

import (
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/controllerconfig"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferencereplica"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/irprojector"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	"github.com/ome-projects/ome/tests/integration/kindfix"
	"github.com/ome-projects/ome/tests/integration/kindutil"
	"github.com/ome-projects/ome/tests/integration/util"
)

// =========================================================================
// ISVC + namespace fixtures
// =========================================================================

// applyFixture creates the CBM, CSR, and ISVC in order. It waits for the
// in-process manager's cached reader to observe the CBM + CSR before creating
// the ISVC, so the first reconcile doesn't miss the model + runtime (see
// kindfix.ApplyModelRuntime). Caller chains ginkgo.DeferCleanup to tear down.
func applyFixture(g gomega.Gomega, f *coordFixture) {
	kindfix.ApplyModelRuntime(suiteCtx, g, k8sClient, f.CBM, f.CSR, kindfix.ApplyModelRuntimeOpts{
		CachedReader: kindutil.Suite().Manager.GetClient(),
	})
	g.ExpectWithOffset(1, k8sClient.Create(suiteCtx, f.ISVC)).To(gomega.Succeed(), "create ISVC")
}

// =========================================================================
// Per-revision Service assertions
// =========================================================================

// listPerRevisionServices returns every Service in namespace tagged
// with the per-revision selector key `ome.io/revision-hash`.
func listPerRevisionServices(g gomega.Gomega, namespace string) []corev1.Service {
	svcs := &corev1.ServiceList{}
	g.ExpectWithOffset(1, k8sClient.List(suiteCtx, svcs,
		client.InNamespace(namespace),
	)).To(gomega.Succeed())
	out := make([]corev1.Service, 0, len(svcs.Items))
	for _, s := range svcs.Items {
		if _, ok := s.Spec.Selector[query.LabelRevisionHash]; ok {
			out = append(out, s)
		}
	}
	return out
}

// =========================================================================
// Coordination status assertions
// =========================================================================

// freshISVC re-reads the ISVC named (namespace, name) from the cache.
// Used inside Eventually blocks where the spec needs the latest status.
func freshISVC(g gomega.Gomega, namespace, name string) *v1beta1.InferenceService {
	out := &v1beta1.InferenceService{}
	g.ExpectWithOffset(1, k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: namespace, Name: name}, out)).To(gomega.Succeed())
	return out
}

// freshInferenceReplica reads the projected workload for one ISVC Component.
func freshInferenceReplica(g gomega.Gomega, namespace, isvcName string, component v1beta1.ComponentType) *v1beta1.InferenceReplica {
	out := &v1beta1.InferenceReplica{}
	name := irprojector.InferenceReplicaName(isvcName, component)
	g.ExpectWithOffset(1, k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: namespace, Name: name}, out)).To(gomega.Succeed())
	return out
}

// expectIRStagedAt asserts a canary step held on one Component: the
// InferenceReplica carries the step's partition on the rollout-control
// spec.pacing.partition (the user's lifecycle is not where it travels) and
// reports Ready=True/Staged for that partition rather than an open-ended
// RolloutInProgress.
func expectIRStagedAt(g gomega.Gomega, namespace, isvcName string, component v1beta1.ComponentType, partition int32) {
	ir := freshInferenceReplica(g, namespace, isvcName, component)
	g.ExpectWithOffset(1, ir.Spec.Pacing).NotTo(gomega.BeNil(), "a held canary step must project spec.pacing.partition")
	g.ExpectWithOffset(1, ir.Spec.Pacing.Partition).To(gomega.HaveValue(gomega.Equal(partition)))
	ready := apimeta.FindStatusCondition(ir.Status.Conditions, inferencereplica.InferenceReplicaConditionReady)
	g.ExpectWithOffset(1, ready).NotTo(gomega.BeNil(), "the InferenceReplica must carry a Ready condition")
	g.ExpectWithOffset(1, ready.Status).To(gomega.Equal(metav1.ConditionTrue),
		"a canary held at a step is Ready, not RolloutInProgress: %s", ready.Message)
	g.ExpectWithOffset(1, ready.Reason).To(gomega.Equal(inferencereplica.ReasonStaged))
}

// instanceStatuses returns the authoritative per-instance statuses for one
// ISVC Component from its InferenceReplica.
// Fails g (retryable inside Eventually) when the IR isn't present yet.
func instanceStatuses(g gomega.Gomega, namespace, isvcName string, component v1beta1.ComponentType) []v1beta1.OMENativeInstanceStatus {
	return decodedInstanceStatuses(g, freshInferenceReplica(g, namespace, isvcName, component))
}

// decodedInstanceStatuses returns the logical per-instance rows of a fetched
// InferenceReplica, whichever representation the stored object carries.
// Fails g when the stored representation does not decode.
func decodedInstanceStatuses(g gomega.Gomega, ir *v1beta1.InferenceReplica) []v1beta1.OMENativeInstanceStatus {
	rows, err := util.DecodedInstanceStatuses(ir)
	g.ExpectWithOffset(1, err).NotTo(gomega.HaveOccurred(), "decode instance statuses of InferenceReplica %s/%s", ir.Namespace, ir.Name)
	return rows
}

// migrationEntries reads status.migrations for one ISVC Component from
// its InferenceReplica — the authoritative per-request migration
// records (the audit ConfigMap and events are history, not authority).
// Fails g (retryable inside Eventually) when the IR isn't present yet.
func migrationEntries(g gomega.Gomega, namespace, isvcName string, component v1beta1.ComponentType) []v1beta1.MigrationStatus {
	return freshInferenceReplica(g, namespace, isvcName, component).Status.Migrations
}

// findMigrationEntry returns the entry with requestUUID, or nil.
func findMigrationEntry(entries []v1beta1.MigrationStatus, requestUUID string) *v1beta1.MigrationStatus {
	for i := range entries {
		if entries[i].RequestUUID == requestUUID {
			return &entries[i]
		}
	}
	return nil
}

// podIsReady reports the pod's Ready condition.
func podIsReady(pod *corev1.Pod) bool {
	for _, c := range pod.Status.Conditions {
		if c.Type == corev1.PodReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

// trafficForComponent returns the per-Component Traffic[] from the
// ISVC's component status, or nil when the entry is missing.
func trafficForComponent(isvc *v1beta1.InferenceService, component v1beta1.ComponentType) []v1beta1.ComponentTrafficTarget {
	if isvc.Status.Components == nil {
		return nil
	}
	cs, ok := isvc.Status.Components[component]
	if !ok {
		return nil
	}
	return cs.Traffic
}

// =========================================================================
// RetryBlock helpers
// =========================================================================

// Fast same-target update retry policy for KIND specs. The production
// chart ships minutes-scale windows; specs need seconds-scale ones so an
// exhaust-into-Held cycle fits the suite budget. The numeric consts
// mirror fastUpdateRetryLifecycleJSON — specs assert against them.
const (
	fastRetryMaxAttempts  = int32(2)
	fastRetryInitialDelay = 5 * time.Second
)

const fastUpdateRetryLifecycleJSON = `{"updateRetry":{"maxAttempts":2,"initialDelay":"5s","maxDelay":"20s","multiplier":2.0}}`

// The scoped stuck-pod grace drives RetryBlock's bad-image attempts through
// the failure path without shortening InstanceReadyTimeout for unrelated specs.
const fastUpdateRetryWithStuckPodGraceLifecycleJSON = `{"updateRetry":{"maxAttempts":2,"initialDelay":"5s","maxDelay":"20s","multiplier":2.0},"stuckPodGracePeriod":"60s"}`

// Fast deadline-disposition tuning: the fast updateRetry policy above
// plus a short stuck-pod grace (terminal kubelet waiting states escalate
// in seconds, not the production minutes) and a small relocation budget
// (lifecycle.autoMigrate.maxAttempts=2) so the disposition's relocation
// branch is enabled. The migration capacity caps come along so a
// relocation the disposition starts is admitted.
const fastDispositionLifecycleJSON = `{"updateRetry":{"maxAttempts":2,"initialDelay":"5s","maxDelay":"20s","multiplier":2.0},"stuckPodGracePeriod":"10s","autoMigrate":{"maxAttempts":2},"audit":{"maxInFlightMigrations":3,"maxMigrationsPerWindow":10,"window":"1h"}}`

// configureLifecycleConfig merges value over the suite-level
// inferenceservice-config ConfigMap's lifecycle key and registers a
// DeferCleanup that restores the prior state (removing the key when it
// was absent, which is the fail-safe default every other spec runs
// under: Held on first same-target failure, no fast escalation, no
// relocation budget). Merging — rather than replacing — keeps a spec
// that tunes one window running on the same requeue cadence and
// capacity caps as the rest of the suite; a spec that means to change a
// baseline key simply states it. The
// KIND manager wires the IR reconciler with ConfigCacheTTL=0 (never
// caches — see kindfix.StandardOMESetup), so the edit applies on the very
// next reconcile. The suite runs specs serially, so a per-spec ConfigMap
// patch cannot bleed into a concurrently running spec.
func configureLifecycleConfig(value string) {
	key := client.ObjectKey{Namespace: constants.OMENamespace, Name: constants.InferenceServiceConfigMapName}
	cm := &corev1.ConfigMap{}
	gomega.Expect(k8sClient.Get(suiteCtx, key, cm)).To(gomega.Succeed(), "read inferenceservice-config")
	prev, hadPrev := cm.Data[controllerconfig.LifecycleConfigName]

	setLifecycleKey := func(value string, remove bool) {
		gomega.Eventually(func(g gomega.Gomega) {
			fresh := &corev1.ConfigMap{}
			g.Expect(k8sClient.Get(suiteCtx, key, fresh)).To(gomega.Succeed())
			if fresh.Data == nil {
				fresh.Data = map[string]string{}
			}
			if remove {
				delete(fresh.Data, controllerconfig.LifecycleConfigName)
			} else {
				fresh.Data[controllerconfig.LifecycleConfigName] = value
			}
			g.Expect(k8sClient.Update(suiteCtx, fresh)).To(gomega.Succeed())
		}).WithTimeout(kindShortTimeout).WithPolling(kindPollInterval).Should(gomega.Succeed(),
			"patch inferenceservice-config lifecycle key")
	}

	setLifecycleKey(mergeLifecycleJSON(prev, value), false)
	ginkgo.DeferCleanup(func() {
		setLifecycleKey(prev, !hadPrev)
	})
}

// mergeLifecycleJSON overlays the spec's lifecycle keys on the suite
// baseline, top level only — a block a spec states replaces the
// baseline's block of the same name wholesale.
func mergeLifecycleJSON(base, overlay string) string {
	merged := map[string]json.RawMessage{}
	if base != "" {
		gomega.Expect(json.Unmarshal([]byte(base), &merged)).To(gomega.Succeed(), "parse baseline lifecycle config")
	}
	spec := map[string]json.RawMessage{}
	gomega.Expect(json.Unmarshal([]byte(overlay), &spec)).To(gomega.Succeed(), "parse spec lifecycle config")
	for k, v := range spec {
		merged[k] = v
	}
	out, err := json.Marshal(merged)
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), "render merged lifecycle config")
	return string(out)
}

// configureFastUpdateRetry seeds the seconds-scale updateRetry policy.
func configureFastUpdateRetry() {
	configureLifecycleConfig(fastUpdateRetryLifecycleJSON)
}

// configureFastUpdateRetryWithStuckPodGrace seeds the seconds-scale updateRetry
// policy and a scoped stuck-pod grace for RetryBlock's bad-image failure path.
func configureFastUpdateRetryWithStuckPodGrace() {
	configureLifecycleConfig(fastUpdateRetryWithStuckPodGraceLifecycleJSON)
}

// configureFastDisposition seeds the seconds-scale updateRetry policy
// plus the short stuck-pod grace and the relocation budget the deadline
// disposition consumes.
func configureFastDisposition() {
	configureLifecycleConfig(fastDispositionLifecycleJSON)
}

// retryBlocks reads the per-target-revision RetryBlocks for one ISVC
// Component from its InferenceReplica (the sole status authority).
// Fails g (retryable inside Eventually) when the IR isn't present yet.
func retryBlocks(g gomega.Gomega, namespace, isvcName string, component v1beta1.ComponentType) []v1beta1.RetryBlock {
	return freshInferenceReplica(g, namespace, isvcName, component).Status.RetryBlocks
}

// findRetryBlock returns the block scoped to targetRevision, or nil.
func findRetryBlock(blocks []v1beta1.RetryBlock, targetRevision string) *v1beta1.RetryBlock {
	for i := range blocks {
		if blocks[i].TargetRevision == targetRevision {
			return &blocks[i]
		}
	}
	return nil
}

// =========================================================================
// Node scheduling helpers (deadline-disposition relocation specs)
// =========================================================================

// schedulableWorkerNodes returns the sorted names of every worker node
// (the control-plane node carries the default NoSchedule taint and is
// excluded by its role label). The suite cluster runs 2 workers, which
// relocation specs rely on: one to fail on, one to relocate to.
func schedulableWorkerNodes(g gomega.Gomega) []string {
	nodes := &corev1.NodeList{}
	g.ExpectWithOffset(1, k8sClient.List(suiteCtx, nodes)).To(gomega.Succeed())
	var out []string
	for i := range nodes.Items {
		if _, cp := nodes.Items[i].Labels["node-role.kubernetes.io/control-plane"]; cp {
			continue
		}
		out = append(out, nodes.Items[i].Name)
	}
	sort.Strings(out)
	return out
}

// setNodeUnschedulable flips node.Spec.Unschedulable (kubectl cordon /
// uncordon). Cordoning only affects future scheduling; running pods
// stay put — relocation specs use it to force initial placement onto a
// chosen node, then uncordon so the rebuild has somewhere else to land.
func setNodeUnschedulable(name string, unschedulable bool) {
	gomega.Eventually(func(g gomega.Gomega) {
		node := &corev1.Node{}
		g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{Name: name}, node)).To(gomega.Succeed())
		if node.Spec.Unschedulable == unschedulable {
			return
		}
		node.Spec.Unschedulable = unschedulable
		g.Expect(k8sClient.Update(suiteCtx, node)).To(gomega.Succeed())
	}).WithTimeout(kindShortTimeout).WithPolling(kindPollInterval).Should(gomega.Succeed(),
		"set node %s unschedulable=%v", name, unschedulable)
}

// cordonNode cordons name and registers an uncordon cleanup so a
// failing spec can never leave the shared cluster unschedulable.
func cordonNode(name string) {
	setNodeUnschedulable(name, true)
	ginkgo.DeferCleanup(func() { setNodeUnschedulable(name, false) })
}

// podExcludesHostname reports whether every required NodeAffinity term
// on pod carries a kubernetes.io/hostname NotIn[node] requirement — the
// relocation-directive overlay the renderer materializes from the
// AutoRecover ledger entries.
func podExcludesHostname(pod *corev1.Pod, node string) bool {
	if pod.Spec.Affinity == nil || pod.Spec.Affinity.NodeAffinity == nil ||
		pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution == nil {
		return false
	}
	terms := pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms
	if len(terms) == 0 {
		return false
	}
	for _, term := range terms {
		matched := false
		for _, expr := range term.MatchExpressions {
			if expr.Key != "kubernetes.io/hostname" || expr.Operator != corev1.NodeSelectorOpNotIn {
				continue
			}
			for _, v := range expr.Values {
				if v == node {
					matched = true
					break
				}
			}
		}
		if !matched {
			return false
		}
	}
	return true
}

// withNodeScopedFailure fixtures c to fail readiness only on failNode
// (optionally only in pod failPod): fake-serving's FAIL_ON_NODE mode
// plus the NODE_NAME / POD_NAME fieldRefs it reads, plus the readiness
// probe that surfaces the 503 as ContainersReady=false. The pod then
// parks Running-but-unready on the failing node — no kubelet waiting
// reason, so the deadline disposition classifies it relocatable — and
// the identical revision converges Ready anywhere else.
func withNodeScopedFailure(c *corev1.Container, failNode, failPod string) {
	c.Env = append(c.Env,
		corev1.EnvVar{Name: "NODE_NAME", ValueFrom: &corev1.EnvVarSource{
			FieldRef: &corev1.ObjectFieldSelector{FieldPath: "spec.nodeName"}}},
		corev1.EnvVar{Name: "POD_NAME", ValueFrom: &corev1.EnvVarSource{
			FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"}}},
		corev1.EnvVar{Name: "FAIL_ON_NODE", Value: failNode},
	)
	if failPod != "" {
		c.Env = append(c.Env, corev1.EnvVar{Name: "FAIL_ON_POD", Value: failPod})
	}
	c.ReadinessProbe = &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{
			Path: "/healthz", Port: intstr.FromInt(8080),
		}},
		PeriodSeconds:    1,
		FailureThreshold: 2,
	}
}

// autoRecoverEntries filters the audit ledger to the AutoRecover
// relocation directives for one component.
func autoRecoverEntries(ledger auditLedgerShape, component string) []auditEntryShape {
	var out []auditEntryShape
	for _, e := range ledger.Entries {
		if e.Reason == "AutoRecover" && e.Component == component {
			out = append(out, e)
		}
	}
	return out
}

// =========================================================================
// Pod + ControllerRevision assertions
// =========================================================================

// listOMENativePods returns every OMENative-owned pod in namespace.
// Selector keys mirror what the production code uses (managed-by +
// inferenceservice).
func listOMENativePods(g gomega.Gomega, namespace, isvcName string) []corev1.Pod {
	pods := &corev1.PodList{}
	g.ExpectWithOffset(1, k8sClient.List(suiteCtx, pods,
		client.InNamespace(namespace),
		client.MatchingLabels{
			util.LabelManagedByKey:                util.ManagedByOMENativeValue,
			constants.InferenceServicePodLabelKey: isvcName,
		},
	)).To(gomega.Succeed())
	return pods.Items
}

// listOMENativeCRs returns every ControllerRevision in namespace tagged
// with the OMENative managed-by label. Used by tests that assert on
// revision history alongside per-revision Service materialization.
func listOMENativeCRs(g gomega.Gomega, namespace string) []appsv1.ControllerRevision {
	crs := &appsv1.ControllerRevisionList{}
	g.ExpectWithOffset(1, k8sClient.List(suiteCtx, crs,
		client.InNamespace(namespace),
		client.MatchingLabels{util.LabelManagedByKey: util.ManagedByOMENativeValue},
	)).To(gomega.Succeed())
	return crs.Items
}

// =========================================================================
// Debug formatters
// =========================================================================

// describeTraffic renders a Traffic[] slice in compact form for
// assertion-failure messages.
func describeTraffic(t []v1beta1.ComponentTrafficTarget) string {
	if len(t) == 0 {
		return "(none)"
	}
	out := ""
	for _, e := range t {
		latest := ""
		if e.LatestRevision {
			latest = " (latest)"
		}
		out += fmt.Sprintf("\n  %s = %d%%%s", e.RevisionName, e.Percent, latest)
	}
	return out
}
