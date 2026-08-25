// Package acceleratorquota_materialize checks the quota tree against a live
// Kueue rather than against our reading of it.
//
// Everything the renderer encodes is a Kueue rule, and Kueue enforces almost
// none of them by rejecting a write: a ClusterQueue with no namespaceSelector
// admits nothing, a flavor missing a covered resource is simply never assigned,
// and a queue naming an absent flavor goes inactive. A unit test can pin the
// shape we emit; only Kueue can say whether that shape works.
package acceleratorquota_materialize

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
	gpuResource = "nvidia.com/gpu"
	gpuFlavor   = "a100"
	fieldOwner  = "ome-quota-test"

	timeout  = "30s"
	interval = "200ms"
)

func TestAcceleratorQuotaMaterialize(t *testing.T) {
	util.RunSuite(t, "AcceleratorQuota Materialization Integration Suite")
}

var (
	suiteCtx    context.Context
	suiteCancel context.CancelFunc
	suiteFwk    *framework.Framework
	k8sClient   client.Client
)

var _ = ginkgo.BeforeSuite(func() {
	framework.SetupTestLogger()

	// The write-conflict gate is off because Kueue's own ClusterQueue
	// reconciler loses optimistic-lock races here and logs them at error level.
	// The gate exists to catch OUR reconcilers missing a conflict-aware path,
	// and Kueue's controllers are not ours to fix, so leaving it on would fail
	// this suite for third-party behaviour.
	//
	// The cost is that it would also hide a conflict of our own. That is small
	// by construction rather than by luck: the materializer writes Kueue objects
	// by server-side apply, which carries no resourceVersion and cannot 409, and
	// both the status and finalizer patches already treat a conflict as a
	// requeue.
	s, err := framework.NewSuite(&framework.Framework{
		OptionalCRDs:           []framework.OptionalCRDBundle{framework.Kueue},
		SkipWriteConflictCheck: true,
	})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	suiteFwk = s.Fwk
	k8sClient = s.Client
	suiteCtx, suiteCancel = context.WithCancel(s.Ctx)

	// The flavor deliberately sets NO topologyName. TopologyAwareScheduling is
	// on by default at the Kueue version we target and the Topology CRD is
	// installed by this bundle, so a flavor that set it would be admitted, not
	// rejected — and Kueue would then assign topology domains underneath
	// ome-scheduler. Quota leaves must never reference a TAS flavor.
	gomega.Expect(k8sClient.Create(suiteCtx, &kueuev1beta2.ResourceFlavor{
		ObjectMeta: metav1.ObjectMeta{Name: gpuFlavor},
	})).To(gomega.Succeed())

	mgrSetup := func(ctx context.Context, mgr ctrl.Manager) error {
		if err := components.SetupAcceleratorQuota(ctx, mgr,
			components.WithQuotaMaterialization(mgr, fieldOwner,
				map[corev1.ResourceName]resource.Quantity{
					corev1.ResourceCPU:    resource.MustParse("1k"),
					corev1.ResourceMemory: resource.MustParse("1Ti"),
				}),
			components.WithQuotaResync(10*time.Minute),
		); err != nil {
			return err
		}
		// Kueue's own core controllers, so the objects we write are judged by
		// the thing that judges them in production. Without them a ClusterQueue
		// never gets a status and "did Kueue accept this" is unanswerable.
		//
		// Core rather than the full admission stack: this suite asks whether
		// Kueue accepts the shape we render, not whether a pod gets admitted,
		// and the full stack additionally needs a webhook server and Kueue's
		// webhook manifests installed.
		return framework.StartKueueCore(ctx, mgr)
	}
	gomega.Expect(suiteFwk.StartManager(suiteCtx, s.Cfg, mgrSetup)).To(gomega.Succeed())
})

var _ = ginkgo.AfterSuite(func() {
	if suiteCancel != nil {
		suiteCancel()
	}
	if suiteFwk != nil {
		// Surfaced rather than discarded: a Runnable that errors stops
		// mgr.Start, and the framework stashes that error here. Swallowing it
		// turns "the manager died" into "every informer timed out", which is
		// the symptom rather than the cause.
		if err := suiteFwk.StopManager(context.Background()); err != nil {
			ginkgo.GinkgoWriter.Printf("MANAGER EXITED WITH: %v\n", err)
		}
		_ = suiteFwk.Teardown()
	}
})

var seq int

func uniqueName(prefix string) string {
	seq++
	return fmt.Sprintf("%s-%d", prefix, seq)
}

// mustCreateNamespace makes a bound namespace real. A LocalQueue is namespaced,
// so a leaf binding a namespace that does not exist cannot materialize.
func mustCreateNamespace(name string) {
	ginkgo.GinkgoHelper()
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}}
	gomega.Expect(client.IgnoreAlreadyExists(k8sClient.Create(suiteCtx, ns))).To(gomega.Succeed())
}

// mustCreateQuota creates a node and registers cleanup. Cleanup matters more
// here than in a status-only suite: a leaf left behind keeps a ClusterQueue
// alive, and the next spec's tree would then contain a stranger.
func mustCreateQuota(q *v1beta1.AcceleratorQuota) *v1beta1.AcceleratorQuota {
	ginkgo.GinkgoHelper()
	gomega.Expect(k8sClient.Create(suiteCtx, q)).To(gomega.Succeed())
	ginkgo.DeferCleanup(func() {
		_ = k8sClient.Delete(context.Background(), q)
		// The finalizer holds the object until its Kueue objects are reaped, so
		// waiting for it to go is what keeps one spec's queues out of the next.
		gomega.Eventually(func() bool {
			var got v1beta1.AcceleratorQuota
			err := k8sClient.Get(context.Background(), client.ObjectKeyFromObject(q), &got)
			return client.IgnoreNotFound(err) == nil && err != nil
		}, timeout, interval).Should(gomega.BeTrue(), "quota %s was never reaped", q.Name)
	})
	return q
}

func cohortNode(name, parent string) *v1beta1.AcceleratorQuota {
	q := &v1beta1.AcceleratorQuota{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec:       v1beta1.AcceleratorQuotaSpec{Role: v1beta1.AcceleratorQuotaRoleCohort},
	}
	if parent != "" {
		q.Spec.ParentRef = &v1beta1.AcceleratorQuotaParentRef{Name: parent}
	}
	return q
}

func leafNode(name, parent string, namespaces []string, flavor, nominal string) *v1beta1.AcceleratorQuota {
	return &v1beta1.AcceleratorQuota{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: v1beta1.AcceleratorQuotaSpec{
			Role:       v1beta1.AcceleratorQuotaRoleClusterQueue,
			ParentRef:  &v1beta1.AcceleratorQuotaParentRef{Name: parent},
			Namespaces: namespaces,
			Budgets: []v1beta1.AcceleratorBudget{{
				ResourceName:   gpuResource,
				ResourceFlavor: flavor,
				Nominal:        resource.MustParse(nominal),
			}},
		},
	}
}
