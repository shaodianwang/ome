// Package acceleratorquota_capacity hosts integration tests for deriving a
// cluster's own accelerator capacity onto the reserved root.
//
// The unit tests drive a fake client, so what only a real apiserver can show is
// exactly what is new here: that Nodes and ResourceFlavors really list, and that
// a Node write wakes the controller through a watch rather than a poll.
//
// Separate from the tree-status suite because the two want opposite things from
// one manager, not merely because it was convenient:
//
//   - Resync. That suite compresses it to seconds so a spec can watch a verdict
//     be re-derived with no edit. This one stretches it to minutes, because a
//     spec that sees capacity change within seconds only proves the Node watch
//     is wired if a resync tick cannot explain it.
//   - The reserved root. That suite never deletes it, which is why its specs
//     take unique names instead. This one recreates it per spec, because the
//     high-water mark accumulates in status: a mark left behind by one spec
//     silently changes what the next one measures.
//
// Both suites still pay for their own envtest. Sharing one and restarting the
// manager per Describe is supported (framework/manager.go) and would save its
// startup, at the cost of coupling a fast dependency-free suite to the Kueue CRD
// bundle and to another Describe's teardown.
package acceleratorquota_capacity

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	kueuev1beta2 "sigs.k8s.io/kueue/apis/kueue/v1beta2"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/tests/integration/components"
	"github.com/ome-projects/ome/tests/integration/framework"
	"github.com/ome-projects/ome/tests/integration/util"
)

const (
	tpuResource = "google.com/tpu"
	tpuFlavor   = "tpu7x"
	flavorLabel = "ome.io/test-accelerator"

	// hysteresis is wide enough that a spec can produce a dip on either side of
	// it with whole chips.
	hysteresis = 20

	timeout  = "30s"
	interval = "200ms"
)

func TestAcceleratorQuotaCapacity(t *testing.T) {
	util.RunSuite(t, "AcceleratorQuota Capacity Integration Suite")
}

var (
	suiteCtx    context.Context
	suiteCancel context.CancelFunc
	suiteFwk    *framework.Framework
	k8sClient   client.Client
)

var _ = ginkgo.BeforeSuite(func() {
	framework.SetupTestLogger()

	// The Kueue bundle is what makes the ResourceFlavor read real. Without it
	// the controller would take its no-CRD path and every spec would pass while
	// measuring nothing.
	s, err := framework.NewSuite(&framework.Framework{
		OptionalCRDs: []framework.OptionalCRDBundle{framework.Kueue},
	})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	suiteFwk = s.Fwk
	k8sClient = s.Client
	suiteCtx, suiteCancel = context.WithCancel(s.Ctx)

	gomega.Expect(k8sClient.Create(suiteCtx, &kueuev1beta2.ResourceFlavor{
		ObjectMeta: metav1.ObjectMeta{Name: tpuFlavor},
		Spec: kueuev1beta2.ResourceFlavorSpec{
			NodeLabels: map[string]string{flavorLabel: tpuFlavor},
		},
	})).To(gomega.Succeed())

	mgrSetup := func(ctx context.Context, mgr ctrl.Manager) error {
		return components.SetupAcceleratorQuota(ctx, mgr,
			components.WithQuotaCapacity([]string{tpuResource}, hysteresis),
			// Long enough that a spec observing a change proves the Node watch
			// woke the controller rather than a resync tick happening to fire.
			components.WithQuotaResync(10*time.Minute),
		)
	}
	gomega.Expect(suiteFwk.StartManager(suiteCtx, s.Cfg, mgrSetup)).To(gomega.Succeed())
})

var _ = ginkgo.AfterSuite(func() {
	if suiteCancel != nil {
		suiteCancel()
	}
	if suiteFwk != nil {
		_ = suiteFwk.StopManager(context.Background())
		_ = suiteFwk.Teardown()
	}
})

var nodeSeq int

func uniqueName(prefix string) string {
	nodeSeq++
	return fmt.Sprintf("%s-%d", prefix, nodeSeq)
}

// mustCreateNode registers cleanup, because capacity is cluster-wide: a node
// left behind by one spec is counted by every later one.
func mustCreateNode(name string, chips string, opts ...func(*corev1.Node)) *corev1.Node {
	ginkgo.GinkgoHelper()
	n := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:   name,
			Labels: map[string]string{flavorLabel: tpuFlavor},
		},
	}
	for _, o := range opts {
		o(n)
	}
	gomega.Expect(k8sClient.Create(suiteCtx, n)).To(gomega.Succeed())
	ginkgo.DeferCleanup(func() {
		gomega.Expect(client.IgnoreNotFound(k8sClient.Delete(suiteCtx, n))).To(gomega.Succeed())
	})

	// Allocatable lives in status, which Create does not persist.
	markReady(n, chips)
	return n
}

// markReady sets the status a kubelet would report. envtest runs no kubelet, so
// nothing else ever will.
func markReady(n *corev1.Node, chips string) {
	ginkgo.GinkgoHelper()
	gomega.Eventually(func() error {
		var live corev1.Node
		if err := k8sClient.Get(suiteCtx, client.ObjectKey{Name: n.Name}, &live); err != nil {
			return err
		}
		live.Status.Allocatable = corev1.ResourceList{
			tpuResource: resource.MustParse(chips),
		}
		live.Status.Conditions = []corev1.NodeCondition{{
			Type: corev1.NodeReady, Status: corev1.ConditionTrue,
			LastTransitionTime: metav1.Now(), Reason: "KubeletReady",
		}}
		return k8sClient.Status().Update(suiteCtx, &live)
	}, timeout, interval).Should(gomega.Succeed())
}

// resetRoot gives a spec a root with no capacity recorded.
//
// It does not create or delete one. The controller owns the root here — that is
// what ensureRoot is for — so a spec that created it would race the controller
// doing the same, and a spec that deleted it in cleanup would race the
// controller putting it straight back, which it does in single-digit
// milliseconds. Clearing the status instead leaves nothing to race over, and
// clearing it is what a spec actually needs: the high-water mark accumulates,
// so a mark left behind by one spec silently changes what the next measures.
func resetRoot() {
	ginkgo.GinkgoHelper()
	gomega.Eventually(func() error {
		var root v1beta1.AcceleratorQuota
		if err := k8sClient.Get(suiteCtx,
			client.ObjectKey{Name: v1beta1.AcceleratorQuotaRootName}, &root); err != nil {
			return err
		}
		root.Status.Capacity = nil
		return k8sClient.Status().Update(suiteCtx, &root)
	}, timeout, interval).Should(gomega.Succeed())
}

// rootCapacity reads back the (allocatable, high-water) pair for the TPU
// flavor, or empty strings when the root reports nothing for it.
func rootCapacity() func() [2]string {
	return func() [2]string {
		var root v1beta1.AcceleratorQuota
		if err := k8sClient.Get(suiteCtx,
			client.ObjectKey{Name: v1beta1.AcceleratorQuotaRootName}, &root); err != nil {
			return [2]string{}
		}
		for _, c := range root.Status.Capacity {
			if c.ResourceName == tpuResource && c.ResourceFlavor == tpuFlavor {
				return [2]string{c.Allocatable.String(), c.HighWaterMark.String()}
			}
		}
		return [2]string{}
	}
}
