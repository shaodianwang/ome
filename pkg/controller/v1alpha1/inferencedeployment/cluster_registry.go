package inferencedeployment

import (
	"context"
	"encoding/base64"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	ClusterCredentialLabel = "ome.io/cluster-credential"
	ClusterNameLabel       = "ome.io/cluster-name"

	SecretKeyClusterName              = "clusterName"
	SecretKeyServer                   = "server"
	SecretKeyBearerToken              = "bearerToken"
	SecretKeyCertificateAuthorityData = "certificateAuthorityData"
)

// RemoteClientFactory builds a client for a workload cluster by target name.
type RemoteClientFactory func(ctx context.Context, targetName string, scheme *runtime.Scheme) (client.Client, error)

// ClusterRegistry is the startup-loaded workload cluster credential registry.
type ClusterRegistry struct {
	clients map[string]client.Client
	configs map[string]*rest.Config
}

// ClusterCredential contains the minimum data needed to authenticate to a
// workload cluster API server with a bearer token.
type ClusterCredential struct {
	Name string

	Server string

	BearerToken string

	CertificateAuthorityData string

	InsecureSkipTLSVerify bool
}

// LoadClusterRegistryFromSecrets loads workload cluster credentials from
// management-cluster Secrets once at manager startup.
func LoadClusterRegistryFromSecrets(ctx context.Context, c client.Client, namespace string, scheme *runtime.Scheme) (*ClusterRegistry, error) {
	if namespace == "" {
		return nil, fmt.Errorf("cluster credential namespace is required")
	}

	secrets := &corev1.SecretList{}
	if err := c.List(ctx, secrets,
		client.InNamespace(namespace),
		client.MatchingLabels{ClusterCredentialLabel: "true"},
	); err != nil {
		return nil, fmt.Errorf("list workload cluster credential secrets in namespace %q: %w", namespace, err)
	}

	clusters := make([]ClusterCredential, 0, len(secrets.Items))
	for i := range secrets.Items {
		credential, err := clusterCredentialFromSecret(&secrets.Items[i])
		if err != nil {
			return nil, err
		}
		clusters = append(clusters, credential)
	}
	return newClusterRegistry(clusters, scheme, fmt.Sprintf("cluster credential secrets in namespace %q", namespace))
}

func newClusterRegistry(clusters []ClusterCredential, scheme *runtime.Scheme, source string) (*ClusterRegistry, error) {
	registry := &ClusterRegistry{
		clients: make(map[string]client.Client, len(clusters)),
		configs: make(map[string]*rest.Config, len(clusters)),
	}
	for _, cluster := range clusters {
		if err := cluster.validate(); err != nil {
			return nil, fmt.Errorf("invalid %s entry: %w", source, err)
		}
		if _, exists := registry.clients[cluster.Name]; exists {
			return nil, fmt.Errorf("duplicate %s entry %q", source, cluster.Name)
		}
		cfg, err := cluster.restConfig()
		if err != nil {
			return nil, err
		}
		remoteClient, err := client.New(cfg, client.Options{Scheme: scheme})
		if err != nil {
			return nil, fmt.Errorf("create remote client for target cluster %q: %w", cluster.Name, err)
		}
		registry.clients[cluster.Name] = remoteClient
		registry.configs[cluster.Name] = rest.CopyConfig(cfg)
	}
	if len(registry.clients) == 0 {
		return nil, fmt.Errorf("%s contains no clusters", source)
	}
	return registry, nil
}

// ClientFor returns the prebuilt controller-runtime client for a named workload
// cluster.
func (r *ClusterRegistry) ClientFor(_ context.Context, targetName string, scheme *runtime.Scheme) (client.Client, error) {
	_ = scheme
	remoteClient, ok := r.clients[targetName]
	if !ok {
		return nil, fmt.Errorf("target cluster %q not found in startup registry", targetName)
	}
	return remoteClient, nil
}

// RestConfigs returns copies of the startup-loaded workload cluster REST
// configs. Callers can use these to build watch clients without mutating the
// registry's shared configs.
func (r *ClusterRegistry) RestConfigs() map[string]*rest.Config {
	out := make(map[string]*rest.Config, len(r.configs))
	for name, cfg := range r.configs {
		out[name] = rest.CopyConfig(cfg)
	}
	return out
}

func (c ClusterCredential) validate() error {
	if c.Name == "" {
		return fmt.Errorf("name is required")
	}
	if c.Server == "" {
		return fmt.Errorf("server is required for cluster %q", c.Name)
	}
	if c.BearerToken == "" {
		return fmt.Errorf("bearerToken is required for cluster %q", c.Name)
	}
	if c.CertificateAuthorityData == "" && !c.InsecureSkipTLSVerify {
		return fmt.Errorf("certificateAuthorityData or insecureSkipTLSVerify is required for cluster %q", c.Name)
	}
	return nil
}

func (c ClusterCredential) restConfig() (*rest.Config, error) {
	caData, err := decodeCAData(c.CertificateAuthorityData)
	if err != nil {
		return nil, fmt.Errorf("decode certificateAuthorityData for cluster %q: %w", c.Name, err)
	}
	return &rest.Config{
		Host:        c.Server,
		BearerToken: c.BearerToken,
		TLSClientConfig: rest.TLSClientConfig{
			CAData:   caData,
			Insecure: c.InsecureSkipTLSVerify,
		},
	}, nil
}

func decodeCAData(in string) ([]byte, error) {
	if in == "" {
		return nil, nil
	}
	trimmed := strings.TrimSpace(in)
	if strings.HasPrefix(trimmed, "-----BEGIN ") {
		return []byte(trimmed), nil
	}
	return base64.StdEncoding.DecodeString(trimmed)
}

func clusterCredentialFromSecret(secret *corev1.Secret) (ClusterCredential, error) {
	ref := fmt.Sprintf("Secret %s/%s", secret.Namespace, secret.Name)
	credential := ClusterCredential{
		Name:                     secretString(secret, SecretKeyClusterName),
		Server:                   secretString(secret, SecretKeyServer),
		BearerToken:              secretString(secret, SecretKeyBearerToken),
		CertificateAuthorityData: secretString(secret, SecretKeyCertificateAuthorityData),
	}
	if credential.Name == "" {
		return ClusterCredential{}, fmt.Errorf("%s is missing data key %q", ref, SecretKeyClusterName)
	}
	if secret.Labels[ClusterNameLabel] != "" && secret.Labels[ClusterNameLabel] != credential.Name {
		return ClusterCredential{}, fmt.Errorf("%s label %q=%q does not match data key %q=%q", ref, ClusterNameLabel, secret.Labels[ClusterNameLabel], SecretKeyClusterName, credential.Name)
	}
	return credential, nil
}

func secretString(secret *corev1.Secret, key string) string {
	return strings.TrimSpace(string(secret.Data[key]))
}
