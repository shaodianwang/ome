package alfred_kind

import (
	"sort"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"

	"github.com/ome-projects/ome/tests/integration/kindutil"
)

// Signals the specs raise on a worker: planned-maintenance signals (a label,
// a taint and a condition that configured triggers match, and a taint no
// trigger names) and the spot label. resetWorkerSignals clears every one of
// them.
const (
	maintenanceLabelKey   = "maintenance.example.com/state"
	maintenanceLabelValue = "patching"
	maintenanceTaintKey   = "maintenance.example.com/patching"
	maintenanceCondition  = corev1.NodeConditionType("Patching")
	unconfiguredTaintKey  = "other.example.com/reboot"
	// preemptibleLabelKey is one of the spot labels alfred reads by default;
	// present with any value but "false", it marks the node preemptible.
	preemptibleLabelKey = "node.kubernetes.io/preemptible"
)

// schedulableWorkers returns the sorted names of every worker node; the
// control-plane node carries the default NoSchedule taint and is excluded by
// its role label.
func schedulableWorkers() []string {
	nodes := &corev1.NodeList{}
	gomega.Expect(k8sClient.List(suiteCtx, nodes)).To(gomega.Succeed())
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

// resetWorkerSignals returns every worker to a signal-free state: schedulable,
// with none of the conditions, taints or labels the specs raise. The cluster
// is reused across runs, so an interrupted earlier run must not leave inputs
// behind for alfred to act on.
func resetWorkerSignals() {
	conditions := []corev1.NodeConditionType{healthCondition, maintenanceCondition}
	taints := []string{maintenanceTaintKey, unconfiguredTaintKey}
	labels := []string{maintenanceLabelKey, preemptibleLabelKey}
	for _, w := range workers {
		gomega.Expect(kindutil.SetNodeUnschedulable(suiteCtx, k8sClient, w, false)).To(gomega.Succeed(), "uncordon %s", w)
		for _, c := range conditions {
			gomega.Expect(kindutil.ClearNodeCondition(suiteCtx, k8sClient, w, c)).To(gomega.Succeed(), "clear %s on %s", c, w)
		}
		for _, t := range taints {
			gomega.Expect(kindutil.RemoveNodeTaint(suiteCtx, k8sClient, w, t)).To(gomega.Succeed(), "remove taint %s from %s", t, w)
		}
		for _, l := range labels {
			gomega.Expect(kindutil.RemoveNodeLabel(suiteCtx, k8sClient, w, l)).To(gomega.Succeed(), "remove label %s from %s", l, w)
		}
	}
}

// markUnhealthy raises the GpuUnhealthy condition on node and clears it when
// the spec ends.
func markUnhealthy(node string) {
	setNodeCondition(node, healthCondition, corev1.ConditionTrue)
}

// beginUnhealthyEpisode raises GpuUnhealthy on node once the earlier episode's
// record is gone and returns the instant this episode opened. The previous
// episode's drain can land at the spec boundary: the Describe's AfterEach
// deletes the namespace before the DeferCleanup'd condition clear, so its pods
// vanish, and its drain is recorded, a few seconds into the next spec. Drain
// lookups are therefore anchored at this episode's start, not at the spec's.
func beginUnhealthyEpisode(node string) time.Time {
	waitNodeRecordGone(node)
	start := time.Now()
	markUnhealthy(node)
	return start
}

// setNodeCondition upserts a condition and registers its removal.
func setNodeCondition(node string, condType corev1.NodeConditionType, status corev1.ConditionStatus) {
	gomega.Expect(kindutil.SetNodeCondition(suiteCtx, k8sClient, node, condType, status,
		"AlfredKindSuite", "raised by the alfred KIND suite")).To(gomega.Succeed())
	ginkgo.DeferCleanup(func() {
		gomega.Expect(kindutil.ClearNodeCondition(suiteCtx, k8sClient, node, condType)).To(gomega.Succeed())
	})
}

// setHealthCondition sets GpuUnhealthy to status so a spec can drive both
// transitions of a flap itself; the condition is still cleared at spec end.
func setHealthCondition(node string, status corev1.ConditionStatus) error {
	ginkgo.DeferCleanup(func() {
		gomega.Expect(kindutil.ClearNodeCondition(suiteCtx, k8sClient, node, healthCondition)).To(gomega.Succeed())
	})
	return kindutil.SetNodeCondition(suiteCtx, k8sClient, node, healthCondition, status, "AlfredKindSuite", "flap")
}

// labelNodeTemp sets key=value on node and removes the key when the spec ends.
func labelNodeTemp(node, key, value string) {
	gomega.Expect(kindutil.LabelNode(suiteCtx, k8sClient, node, key, value)).To(gomega.Succeed())
	ginkgo.DeferCleanup(func() {
		gomega.Expect(kindutil.RemoveNodeLabel(suiteCtx, k8sClient, node, key)).To(gomega.Succeed())
	})
}

// taintNodeTemp adds taint to node and removes it when the spec ends.
func taintNodeTemp(node string, taint corev1.Taint) {
	gomega.Expect(kindutil.AddNodeTaint(suiteCtx, k8sClient, node, taint)).To(gomega.Succeed())
	ginkgo.DeferCleanup(func() {
		gomega.Expect(kindutil.RemoveNodeTaint(suiteCtx, k8sClient, node, taint.Key)).To(gomega.Succeed())
	})
}

// cordon marks node unschedulable and uncordons it when the spec ends.
func cordon(node string) {
	gomega.Expect(kindutil.SetNodeUnschedulable(suiteCtx, k8sClient, node, true)).To(gomega.Succeed())
	ginkgo.DeferCleanup(func() { uncordon(node) })
}

func uncordon(node string) {
	gomega.Expect(kindutil.SetNodeUnschedulable(suiteCtx, k8sClient, node, false)).To(gomega.Succeed())
}

// pinToNode cordons every other worker so the next pods land on node. The
// returned release uncordons them; it also runs at spec end.
func pinToNode(node string) (release func()) {
	var others []string
	for _, w := range workers {
		if w != node {
			others = append(others, w)
			gomega.Expect(kindutil.SetNodeUnschedulable(suiteCtx, k8sClient, w, true)).To(gomega.Succeed())
		}
	}
	release = func() {
		for _, w := range others {
			uncordon(w)
		}
	}
	ginkgo.DeferCleanup(release)
	return release
}

func otherWorkers(node string) []string {
	var out []string
	for _, w := range workers {
		if w != node {
			out = append(out, w)
		}
	}
	return out
}
