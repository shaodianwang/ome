package util

import (
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
)

// RestConfigToKubeConfig serializes a *rest.Config into kubeconfig bytes a
// WorkloadCluster's kubeConfig Secret can carry. It is the test-side inverse of
// the control plane's clientcmd.RESTConfigFromKubeConfig: the multi-cluster
// integration suite stands up each member cluster as its own envtest instance,
// then writes that member's serving endpoint + client credentials into a Secret
// on the control cluster so a WorkloadCluster CR can connect to it.
//
// Modeled on Kueue's pkg/util/testing/kubeconfig.go. Only the fields the
// WorkloadCluster connection validator accepts are emitted — inline CA data and
// inline client cert/key (plus an optional bearer token) — so the produced
// kubeconfig passes that validator's security checks (no CA file path, no
// insecure-skip, no exec plugin, no on-disk token file). The envtest rest.Config
// carries exactly these (CAData/CertData/KeyData), so this is a faithful
// round-trip for the harness.
func RestConfigToKubeConfig(restConfig *rest.Config) ([]byte, error) {
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
				Server:                   restConfig.Host,
				CertificateAuthorityData: restConfig.CAData,
			},
		},
		AuthInfos: map[string]*clientcmdapi.AuthInfo{
			userName: {
				ClientCertificateData: restConfig.CertData,
				ClientKeyData:         restConfig.KeyData,
				Token:                 restConfig.BearerToken,
			},
		},
		Contexts: map[string]*clientcmdapi.Context{
			contextName: {Cluster: clusterName, AuthInfo: userName},
		},
		CurrentContext: contextName,
	}
	return clientcmd.Write(cfg)
}
