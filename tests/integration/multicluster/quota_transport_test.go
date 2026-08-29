package multicluster

import (
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

// The management plane reaches a member through its OWN transport, wired beside
// the placement plane's rather than sharing it — in production the two are
// separate binaries and cannot share an in-process struct at all.
//
// What only a real registry can show is that a second Manager following the
// same WorkloadCluster objects actually converges: the unit tests drive the
// connector against a fake registry and a fake transport, so they prove the
// decision logic and nothing about whether a real kubeconfig, reconciled by the
// real registry controller, produces a usable client on the other side.
var _ = ginkgo.Describe("Management-plane quota transport", func() {
	var (
		secretNS string
		wcName   string
		secret   string
	)

	ginkgo.BeforeEach(func() {
		secretNS = uniqueName("quota-transport")
		createNamespace(controlCluster, secretNS)
		wcName = uniqueName("quota-wc")
		secret = uniqueName("quota-kc")
	})

	ginkgo.AfterEach(func() {
		deleteIfExists(controlCluster, &v1beta1.WorkloadCluster{ObjectMeta: metav1.ObjectMeta{Name: wcName}})
		deleteIfExists(controlCluster, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: secret, Namespace: secretNS}})
	})

	quotaConnected := func() []string { return controlCluster.quotaClusters.Connected() }

	ginkgo.It("connects to a member the registry reports Ready, on a transport of its own", func() {
		mustCreate(controlCluster, kubeconfigSecretFor(member1Cluster, secretNS, secret))
		mustCreate(controlCluster, WorkloadClusterFor(wcName, secretNS, secret, nil))

		gomega.Eventually(quotaConnected, eventuallyTimeout, eventuallyPoll).
			Should(gomega.ContainElement(wcName), "the quota transport never connected")

		// A usable client, not merely a recorded name: a connection that cannot
		// be handed to a caller would fail the projector rather than this spec.
		cl, ok := controlCluster.quotaClusters.ClientFor(wcName)
		gomega.Expect(ok).To(gomega.BeTrue(), "no client for a connected cluster")
		gomega.Expect(cl).NotTo(gomega.BeNil())

		// Independent of the placement plane's. Both follow the same registry,
		// and neither is the other's.
		gomega.Eventually(func() []string { return controlCluster.clusters.Connected() },
			eventuallyTimeout, eventuallyPoll).Should(gomega.ContainElement(wcName))
		gomega.Expect(controlCluster.quotaClusters).NotTo(gomega.BeIdenticalTo(controlCluster.clusters),
			"the quota plane is sharing the placement plane's transport")
	})

	ginkgo.It("drops a member the registry stops reporting Ready", func() {
		mustCreate(controlCluster, kubeconfigSecretFor(member1Cluster, secretNS, secret))
		mustCreate(controlCluster, WorkloadClusterFor(wcName, secretNS, secret, nil))
		gomega.Eventually(quotaConnected, eventuallyTimeout, eventuallyPoll).
			Should(gomega.ContainElement(wcName))

		// Deleting the credential is what the registry notices; it flips Ready
		// off after its own grace period, and the connector follows that rather
		// than probing on its own.
		deleteIfExists(controlCluster, &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: secret, Namespace: secretNS},
		})

		gomega.Eventually(func(g gomega.Gomega) {
			wc := &v1beta1.WorkloadCluster{}
			g.Expect(controlCluster.client.Get(controlCluster.ctx,
				types.NamespacedName{Name: wcName}, wc)).To(gomega.Succeed())
			g.Expect(wc.Status.Conditions).NotTo(gomega.BeEmpty())
		}, eventuallyTimeout, eventuallyPoll).Should(gomega.Succeed())

		gomega.Eventually(quotaConnected, eventuallyTimeout, eventuallyPoll).
			ShouldNot(gomega.ContainElement(wcName), "the quota transport held a client the registry gave up on")
	})

	ginkgo.It("drops a member that is deregistered", func() {
		mustCreate(controlCluster, kubeconfigSecretFor(member1Cluster, secretNS, secret))
		mustCreate(controlCluster, WorkloadClusterFor(wcName, secretNS, secret, nil))
		gomega.Eventually(quotaConnected, eventuallyTimeout, eventuallyPoll).
			Should(gomega.ContainElement(wcName))

		deleteIfExists(controlCluster, &v1beta1.WorkloadCluster{
			ObjectMeta: metav1.ObjectMeta{Name: wcName},
		})

		gomega.Eventually(quotaConnected, eventuallyTimeout, eventuallyPoll).
			ShouldNot(gomega.ContainElement(wcName), "a deregistered cluster kept its client")
	})
})
