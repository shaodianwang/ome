//go:build meta_internal

package omebot

import (
	"encoding/base64"
	"fmt"
	"os"
	"path/filepath"

	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
)

// Kubeconfig renders the exec-credential kubeconfig for a registry entry.
// The file carries no secret: the exec provider mints a short-lived token
// from the pod's own identity on every use.
func (cl Cluster) Kubeconfig(execCommand string) ([]byte, error) {
	cluster := clientcmdapi.NewCluster()
	cluster.Server = cl.Server
	cluster.InsecureSkipTLSVerify = cl.InsecureSkipTLSVerify
	if cl.CertificateAuthorityData != "" {
		ca, err := base64.StdEncoding.DecodeString(cl.CertificateAuthorityData)
		if err != nil {
			return nil, fmt.Errorf("cluster %s: decode CA: %w", cl.Name, err)
		}
		cluster.CertificateAuthorityData = ca
	}
	auth := clientcmdapi.NewAuthInfo()
	auth.Exec = &clientcmdapi.ExecConfig{
		APIVersion:      "client.authentication.k8s.io/v1",
		Command:         execCommand,
		Args:            append([]string(nil), cl.TokenArgs...),
		InteractiveMode: clientcmdapi.NeverExecInteractiveMode,
	}
	cfg := clientcmdapi.NewConfig()
	cfg.Clusters[cl.Name] = cluster
	cfg.AuthInfos[cl.Name] = auth
	cfg.Contexts[cl.Name] = &clientcmdapi.Context{Cluster: cl.Name, AuthInfo: cl.Name}
	cfg.CurrentContext = cl.Name
	return clientcmd.Write(*cfg)
}

// KubeconfigPaths materialises every rendered kubeconfig under
// cfg.KubeconfigDir and returns the path each cluster uses. Entries that
// point at a mounted file keep that path; in-cluster entries map to "".
func KubeconfigPaths(cfg *Config) (map[string]string, error) {
	paths := make(map[string]string, len(cfg.Clusters))
	for _, cl := range cfg.Clusters {
		switch {
		case cl.InCluster:
			paths[cl.Name] = ""
		case cl.KubeconfigPath != "":
			paths[cl.Name] = cl.KubeconfigPath
		default:
			if err := os.MkdirAll(cfg.KubeconfigDir, 0o700); err != nil {
				return nil, fmt.Errorf("create kubeconfig dir: %w", err)
			}
			raw, err := cl.Kubeconfig(cfg.ExecCommand)
			if err != nil {
				return nil, err
			}
			path := filepath.Join(cfg.KubeconfigDir, cl.Name+".yaml")
			if err := os.WriteFile(path, raw, 0o600); err != nil {
				return nil, fmt.Errorf("write kubeconfig for %s: %w", cl.Name, err)
			}
			paths[cl.Name] = path
		}
	}
	return paths, nil
}
