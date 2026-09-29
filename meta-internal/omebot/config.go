//go:build meta_internal

package omebot

import (
	"encoding/base64"
	"fmt"
	"os"
	"regexp"
	"strings"

	"sigs.k8s.io/yaml"
)

// Config is the whole runtime configuration. Everything behavioural lives
// here and is supplied by the chart; the binary bakes in no defaults for
// ports, paths, timeouts or cluster identities.
type Config struct {
	// Listen is the address the HTTP server binds, for example ":8080".
	Listen string `json:"listen"`
	// KubectlOmePath is the kubectl-ome binary every CLI command runs.
	KubectlOmePath string `json:"kubectlOmePath"`
	// KubeconfigDir receives one rendered kubeconfig per registry cluster.
	KubeconfigDir string `json:"kubeconfigDir"`
	// ExecCommand is the exec-credential provider named in rendered
	// kubeconfigs (the cloud CLI injected into the pod).
	ExecCommand string `json:"execCommand"`
	// Env lists extra environment variables (KEY=VALUE) exported to
	// kubectl-ome and, through it, to the exec-credential provider.
	Env      []string `json:"env,omitempty"`
	Timeouts Timeouts `json:"timeouts"`
	// OutputLimitBytes caps the CLI output kept per cluster per command.
	OutputLimitBytes int        `json:"outputLimitBytes"`
	Auth             AuthConfig `json:"auth"`
	OME              OMEConfig  `json:"ome"`
	Clusters         []Cluster  `json:"clusters"`
}

// Timeouts bound the three kinds of work the service performs.
type Timeouts struct {
	// CommandSeconds bounds one kubectl-ome invocation.
	CommandSeconds int `json:"commandSeconds"`
	// ProbeSeconds bounds one cluster reachability probe.
	ProbeSeconds int `json:"probeSeconds"`
	// FanoutSeconds bounds a command that runs on every cluster.
	FanoutSeconds int `json:"fanoutSeconds"`
}

// AuthConfig selects how callers of the HTTP API are authenticated.
type AuthConfig struct {
	// Mode is "none" or "hmac".
	Mode string `json:"mode"`
	// SecretFile holds the shared HMAC key when Mode is "hmac".
	SecretFile string `json:"secretFile,omitempty"`
	// MaxSkewSeconds is the accepted age of a signed request.
	MaxSkewSeconds int `json:"maxSkewSeconds,omitempty"`
}

// OMEConfig names the controller Deployment whose image tag is reported as
// the installed OME version. Both fields empty disables the lookup.
type OMEConfig struct {
	Namespace         string `json:"namespace,omitempty"`
	ManagerDeployment string `json:"managerDeployment,omitempty"`
}

// Cluster is one registry entry. Exactly one credential source applies:
// Server plus TokenArgs renders an exec-credential kubeconfig, KubeconfigPath
// points at a mounted one, and InCluster uses the pod's ServiceAccount.
type Cluster struct {
	Name       string `json:"name"`
	Provider   string `json:"provider"`
	Role       string `json:"role,omitempty"`
	Hub        string `json:"hub,omitempty"`
	KargoStage string `json:"kargoStage,omitempty"`

	Server                   string   `json:"server,omitempty"`
	InsecureSkipTLSVerify    bool     `json:"insecureSkipTLSVerify,omitempty"`
	CertificateAuthorityData string   `json:"certificateAuthorityData,omitempty"`
	TokenArgs                []string `json:"tokenArgs,omitempty"`
	KubeconfigPath           string   `json:"kubeconfigPath,omitempty"`
	InCluster                bool     `json:"inCluster,omitempty"`
}

const (
	AuthModeNone = "none"
	AuthModeHMAC = "hmac"
)

var clusterNameRE = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]*[a-z0-9])?$`)

// Load reads and validates a YAML config file.
func Load(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	return Parse(raw)
}

// Parse decodes and validates YAML config bytes.
func Parse(raw []byte) (*Config, error) {
	var cfg Config
	if err := yaml.UnmarshalStrict(raw, &cfg); err != nil {
		return nil, fmt.Errorf("decode config: %w", err)
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

// Validate rejects a configuration that would fail later in a less obvious
// place.
func (c *Config) Validate() error {
	var problems []string
	add := func(format string, args ...any) { problems = append(problems, fmt.Sprintf(format, args...)) }

	if c.Listen == "" {
		add("listen is required")
	}
	if c.KubectlOmePath == "" {
		add("kubectlOmePath is required")
	}
	if c.Timeouts.CommandSeconds <= 0 {
		add("timeouts.commandSeconds must be > 0")
	}
	if c.Timeouts.ProbeSeconds <= 0 {
		add("timeouts.probeSeconds must be > 0")
	}
	if c.Timeouts.FanoutSeconds <= 0 {
		add("timeouts.fanoutSeconds must be > 0")
	}
	if c.OutputLimitBytes <= 0 {
		add("outputLimitBytes must be > 0")
	}
	switch c.Auth.Mode {
	case AuthModeNone:
	case AuthModeHMAC:
		if c.Auth.SecretFile == "" {
			add("auth.secretFile is required when auth.mode is hmac")
		}
		if c.Auth.MaxSkewSeconds <= 0 {
			add("auth.maxSkewSeconds must be > 0 when auth.mode is hmac")
		}
	default:
		add("auth.mode must be %q or %q", AuthModeNone, AuthModeHMAC)
	}
	if (c.OME.Namespace == "") != (c.OME.ManagerDeployment == "") {
		add("ome.namespace and ome.managerDeployment must be set together")
	}
	for _, kv := range c.Env {
		if !strings.Contains(kv, "=") {
			add("env entry %q is not KEY=VALUE", kv)
		}
	}
	if len(c.Clusters) == 0 {
		add("at least one cluster is required")
	}
	seen := map[string]bool{}
	needsRender, needsExec := false, false
	for i, cl := range c.Clusters {
		where := fmt.Sprintf("clusters[%d]", i)
		if cl.Name == "" {
			add("%s: name is required", where)
		} else {
			where = "cluster " + cl.Name
			if !clusterNameRE.MatchString(cl.Name) {
				add("%s: name must be a lowercase DNS label", where)
			}
			if seen[cl.Name] {
				add("%s: duplicate name", where)
			}
			seen[cl.Name] = true
		}
		if cl.Provider == "" {
			add("%s: provider is required", where)
		}
		sources := 0
		if cl.Server != "" || len(cl.TokenArgs) > 0 {
			sources++
			needsRender = true
			if cl.Server == "" || len(cl.TokenArgs) == 0 {
				add("%s: server and tokenArgs must be set together", where)
			}
			needsExec = true
		}
		if cl.KubeconfigPath != "" {
			sources++
		}
		if cl.InCluster {
			sources++
		}
		if sources != 1 {
			add("%s: exactly one of server+tokenArgs, kubeconfigPath, inCluster is required", where)
		}
		if cl.InsecureSkipTLSVerify && cl.CertificateAuthorityData != "" {
			add("%s: insecureSkipTLSVerify and certificateAuthorityData are mutually exclusive", where)
		}
		if cl.CertificateAuthorityData != "" {
			if _, err := base64.StdEncoding.DecodeString(cl.CertificateAuthorityData); err != nil {
				add("%s: certificateAuthorityData is not base64", where)
			}
		}
	}
	if needsRender && c.KubeconfigDir == "" {
		add("kubeconfigDir is required when a cluster uses server+tokenArgs")
	}
	if needsExec && c.ExecCommand == "" {
		add("execCommand is required when a cluster uses tokenArgs")
	}
	if len(problems) > 0 {
		return fmt.Errorf("invalid config: %s", strings.Join(problems, "; "))
	}
	return nil
}

// Cluster returns the registry entry with the given name.
func (c *Config) Cluster(name string) (Cluster, bool) {
	for _, cl := range c.Clusters {
		if cl.Name == name {
			return cl, true
		}
	}
	return Cluster{}, false
}

// ClusterNames lists registry names in configuration order.
func (c *Config) ClusterNames() []string {
	names := make([]string, 0, len(c.Clusters))
	for _, cl := range c.Clusters {
		names = append(names, cl.Name)
	}
	return names
}
