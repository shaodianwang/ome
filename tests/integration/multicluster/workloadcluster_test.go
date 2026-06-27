package multicluster

import (
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	ometesting "github.com/ome-projects/ome/pkg/utils/testing/v1beta1"
)

// These specs exercise the WorkloadCluster registry/transport controller's
// kubeconfig handling against a real member apiserver: the Ready condition's
// reason is the contract a placer (and an operator) reads, so each malformed /
// unsafe / missing kubeconfig must surface the right reason, and a well-formed
// one must connect.
var _ = ginkgo.Describe("WorkloadCluster kubeconfig handling", func() {
	var (
		secretNS string
		wcName   string
		secret   string
	)

	ginkgo.BeforeEach(func() {
		secretNS = uniqueName("wc-secrets")
		createNamespace(controlCluster, secretNS)
		wcName = uniqueName("wc")
		secret = uniqueName("kc")
	})

	ginkgo.AfterEach(func() {
		deleteIfExists(controlCluster, &v1beta1.WorkloadCluster{ObjectMeta: metav1.ObjectMeta{Name: wcName}})
		deleteIfExists(controlCluster, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: secret, Namespace: secretNS}})
	})

	// waitReadyReason asserts the WorkloadCluster's Ready condition settles on the
	// given status/reason.
	waitReadyReason := func(status metav1.ConditionStatus, reason string) {
		gomega.Eventually(func(g gomega.Gomega) {
			wc := &v1beta1.WorkloadCluster{}
			g.Expect(controlCluster.client.Get(controlCluster.ctx, types.NamespacedName{Name: wcName}, wc)).To(gomega.Succeed())
			g.Expect(wc.Status.Conditions).To(ometesting.HaveConditionStatusAndReason(v1beta1.WorkloadClusterReady, status, reason))
		}, eventuallyTimeout, eventuallyPoll).Should(gomega.Succeed())
	}
	// expectReadyReason creates the WorkloadCluster and asserts its Ready condition.
	expectReadyReason := func(status metav1.ConditionStatus, reason string) {
		mustCreate(controlCluster, workloadClusterFor(wcName, secretNS, secret, nil))
		waitReadyReason(status, reason)
	}

	ginkgo.It("reports Ready=True/Reachable for a valid kubeconfig pointing at a live member", func() {
		mustCreate(controlCluster, kubeconfigSecretFor(member1Cluster, secretNS, secret))
		expectReadyReason(metav1.ConditionTrue, "Reachable")
	})

	ginkgo.It("reports Ready=False/SecretNotFound when the kubeconfig Secret is missing", func() {
		// Intentionally do NOT create the Secret.
		expectReadyReason(metav1.ConditionFalse, "SecretNotFound")
	})

	ginkgo.It("reports Ready=False/BadKubeConfig when the kubeconfig uses a CA file path", func() {
		mustCreate(controlCluster, badKubeconfigSecret(member1Cluster, secretNS, secret, caAsPath))
		expectReadyReason(metav1.ConditionFalse, "BadKubeConfig")
	})

	ginkgo.It("reports Ready=False/BadKubeConfig when the kubeconfig disables TLS verification", func() {
		mustCreate(controlCluster, badKubeconfigSecret(member1Cluster, secretNS, secret, insecureSkipTLS))
		expectReadyReason(metav1.ConditionFalse, "BadKubeConfig")
	})

	ginkgo.It("re-validates and reconnects when the kubeconfig Secret is rotated (valid->bad->valid)", func() {
		ginkgo.By("starting from a valid kubeconfig: Ready=True/Reachable")
		mustCreate(controlCluster, kubeconfigSecretFor(member1Cluster, secretNS, secret))
		mustCreate(controlCluster, workloadClusterFor(wcName, secretNS, secret, nil))
		waitReadyReason(metav1.ConditionTrue, "Reachable")

		ginkgo.By("rotating the Secret to an unsafe kubeconfig: Ready flips to False/BadKubeConfig")
		rotateSecret(controlCluster, secretNS, secret, badKubeconfigBytes(member1Cluster, insecureSkipTLS))
		waitReadyReason(metav1.ConditionFalse, "BadKubeConfig")

		ginkgo.By("rotating the Secret back to a valid kubeconfig: Ready returns to True/Reachable")
		rotateSecret(controlCluster, secretNS, secret, kubeConfigBytesFor(member1Cluster))
		waitReadyReason(metav1.ConditionTrue, "Reachable")
	})
})

// badKubeconfigVariant mutates a base kubeconfig into a specific unsafe shape.
type badKubeconfigVariant func(*clientcmdapi.Config)

// caAsPath replaces inline CA data with a (nonexistent) CA file path — the
// validator requires certificate-authority-data, not a path.
func caAsPath(cfg *clientcmdapi.Config) {
	for _, c := range cfg.Clusters {
		c.CertificateAuthorityData = nil
		c.CertificateAuthority = "/etc/does-not-exist/ca.crt"
	}
}

// insecureSkipTLS flips on InsecureSkipTLSVerify, which the validator rejects.
func insecureSkipTLS(cfg *clientcmdapi.Config) {
	for _, c := range cfg.Clusters {
		c.CertificateAuthorityData = nil
		c.InsecureSkipTLSVerify = true
	}
}

// badKubeconfigBytes builds a kubeconfig from a member's rest.Config and applies
// an unsafe mutation. It keeps the real server URL + client credentials so the
// ONLY thing wrong is the property under test.
func badKubeconfigBytes(member cluster, mutate badKubeconfigVariant) []byte {
	const (
		clusterName = "default-cluster"
		userName    = "default-user"
		contextName = "default-context"
	)
	cfg := clientcmdapi.Config{
		Kind:       "Config",
		APIVersion: "v1",
		Clusters: map[string]*clientcmdapi.Cluster{
			clusterName: {
				Server:                   member.cfg.Host,
				CertificateAuthorityData: member.cfg.CAData,
			},
		},
		AuthInfos: map[string]*clientcmdapi.AuthInfo{
			userName: {
				ClientCertificateData: member.cfg.CertData,
				ClientKeyData:         member.cfg.KeyData,
			},
		},
		Contexts:       map[string]*clientcmdapi.Context{contextName: {Cluster: clusterName, AuthInfo: userName}},
		CurrentContext: contextName,
	}
	mutate(&cfg)
	raw, err := clientcmd.Write(cfg)
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), "write bad kubeconfig")
	return raw
}

// badKubeconfigSecret wraps badKubeconfigBytes in a Secret.
func badKubeconfigSecret(member cluster, namespace, name string, mutate badKubeconfigVariant) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Data:       map[string][]byte{kubeConfigSecretKey: badKubeconfigBytes(member, mutate)},
	}
}
