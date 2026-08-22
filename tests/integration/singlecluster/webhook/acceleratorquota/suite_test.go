// Package acceleratorquota hosts integration tests for the AcceleratorQuota
// validating webhook (OEP-0024).
//
// These drive real admission through a real apiserver, which the unit tests
// cannot: those call Handle directly with a fake client, so they prove the
// handler's logic but not that the apiserver ever reaches it. Everything
// between — the rule set that decides which operations are intercepted, the
// CRD's own CEL rejecting a write before the webhook is consulted, and DELETE
// admission carrying no object body — only exists here.
package acceleratorquota

import (
	"context"
	"fmt"
	"testing"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/tests/integration/components"
	"github.com/ome-projects/ome/tests/integration/framework"
	"github.com/ome-projects/ome/tests/integration/util"
)

// maxDepth is small so a spec can reach it without authoring a long chain. The
// root is 0, so a grouping plus two nested levels is the deepest admitted.
const maxDepth = 3

func TestAcceleratorQuotaWebhook(t *testing.T) {
	util.RunSuite(t, "AcceleratorQuota Webhook Integration Suite")
}

var (
	suiteCtx    context.Context
	suiteCancel context.CancelFunc
	suiteFwk    *framework.Framework
	k8sClient   client.Client
)

var _ = ginkgo.BeforeSuite(func() {
	framework.SetupTestLogger()

	s, err := framework.NewSuite(&framework.Framework{
		WebhookPath: "tests/integration/singlecluster/webhook/acceleratorquota/testdata",
	})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	suiteFwk = s.Fwk
	k8sClient = s.Client
	suiteCtx, suiteCancel = context.WithCancel(s.Ctx)

	mgrSetup := func(_ context.Context, mgr ctrl.Manager) error {
		return components.SetupAcceleratorQuotaWebhook(mgr, maxDepth)
	}
	gomega.Expect(suiteFwk.StartManager(suiteCtx, s.Cfg, mgrSetup)).To(gomega.Succeed())

	// One root for the whole suite. The name is reserved and the tree is
	// single-rooted, so specs cannot each bring their own; they hang uniquely
	// named subtrees off this one instead. Its budget is far larger than any
	// spec needs, so no spec busts containment at the root merely by existing
	// while another spec's subtree is still up.
	gomega.Expect(k8sClient.Create(suiteCtx, cohort(v1beta1.AcceleratorQuotaRootName, "", budget("100000")))).
		To(gomega.Succeed())
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

// uniqueName keeps specs independent. The tree is cluster-scoped and admission
// assembles all of it, so two specs reusing a name would see each other's
// nodes.
func uniqueName(prefix string) string {
	nodeSeq++
	return fmt.Sprintf("%s-%d", prefix, nodeSeq)
}

func budget(nominal string) v1beta1.AcceleratorBudget {
	return v1beta1.AcceleratorBudget{
		ResourceName:   "google.com/tpu",
		ResourceFlavor: "tpu7x",
		Nominal:        resource.MustParse(nominal),
	}
}

func cohort(name, parent string, budgets ...v1beta1.AcceleratorBudget) *v1beta1.AcceleratorQuota {
	q := &v1beta1.AcceleratorQuota{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: v1beta1.AcceleratorQuotaSpec{
			Role:    v1beta1.AcceleratorQuotaRoleCohort,
			Budgets: budgets,
		},
	}
	if parent != "" {
		q.Spec.ParentRef = &v1beta1.AcceleratorQuotaParentRef{Name: parent}
	}
	return q
}

func leaf(name, parent string, budgets ...v1beta1.AcceleratorBudget) *v1beta1.AcceleratorQuota {
	return &v1beta1.AcceleratorQuota{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: v1beta1.AcceleratorQuotaSpec{
			Role:      v1beta1.AcceleratorQuotaRoleClusterQueue,
			ParentRef: &v1beta1.AcceleratorQuotaParentRef{Name: parent},
			Budgets:   budgets,
		},
	}
}

// mustCreate applies nodes parent-first and tears them down child-first.
//
// Order matters in both directions and the webhook is why: a child whose parent
// does not exist yet is rejected as dangling, and a grouping deleted before its
// children is rejected for orphaning them. A cleanup that ignored this would
// leave the tree dirty for every later spec.
func mustCreate(nodes ...*v1beta1.AcceleratorQuota) {
	ginkgo.GinkgoHelper()
	for _, n := range nodes {
		gomega.Expect(k8sClient.Create(suiteCtx, n)).To(gomega.Succeed())
	}
	ginkgo.DeferCleanup(func() {
		for i := len(nodes) - 1; i >= 0; i-- {
			gomega.Expect(client.IgnoreNotFound(k8sClient.Delete(suiteCtx, nodes[i]))).To(gomega.Succeed())
		}
	})
}

// denial returns the apiserver's message for a rejected write, and fails the
// spec if the write was admitted.
func denial(err error) string {
	ginkgo.GinkgoHelper()
	gomega.Expect(err).To(gomega.HaveOccurred(), "the write should have been rejected")
	return err.Error()
}
