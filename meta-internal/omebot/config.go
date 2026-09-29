//go:build meta_internal

package omebot

import (
	"encoding/base64"
	"fmt"
	"os"
	"path/filepath"
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
	OutputLimitBytes int `json:"outputLimitBytes"`
	// MaxRequestBytes caps an HTTP request body.
	MaxRequestBytes int `json:"maxRequestBytes"`
	// MaxInFlight bounds concurrent requests that run a process or probe
	// clusters; excess requests are refused as busy.
	MaxInFlight int        `json:"maxInFlight"`
	Auth        AuthConfig `json:"auth"`
	OME         OMEConfig  `json:"ome"`
	Clusters    []Cluster  `json:"clusters"`
	// Tools are the non-kubectl-ome commands POST /v1/tool may run.
	Tools []ToolConfig `json:"tools,omitempty"`
	// Docs enables the built-in docs tool; nil disables it.
	Docs *DocsConfig `json:"docs,omitempty"`
	// Kargo enables GET /v1/release; nil disables it.
	Kargo *KargoConfig `json:"kargo,omitempty"`
}

// ToolConfig is one entry of the tool registry. A call runs Command with
// ArgsPrefix followed by the caller's arguments, never through a shell.
type ToolConfig struct {
	Name    string `json:"name"`
	Command string `json:"command"`
	// ArgsPrefix precedes every caller argument, typically a script path.
	ArgsPrefix []string `json:"argsPrefix,omitempty"`
	// Subcommands is the closed set the first caller argument must match.
	Subcommands []string `json:"subcommands"`
	// ForbiddenFlags are refused as an argument or as a --flag=value prefix.
	ForbiddenFlags []string `json:"forbiddenFlags,omitempty"`
	TimeoutSeconds int      `json:"timeoutSeconds"`
	// EnvFromFiles maps an environment variable to a file whose trimmed
	// content is its value; files are read per call so rotation needs no
	// restart.
	EnvFromFiles map[string]string `json:"envFromFiles,omitempty"`
	// Env lists extra KEY=VALUE pairs for this tool.
	Env []string `json:"env,omitempty"`
}

// DocsConfig configures the built-in docs tool.
type DocsConfig struct {
	Roots []DocsRoot `json:"roots"`
	// MaxMatches caps the lines one search returns.
	MaxMatches int `json:"maxMatches"`
	// MaxReadBytes caps the bytes one read returns.
	MaxReadBytes int `json:"maxReadBytes"`
}

// DocsRoot is a named directory the docs tool may read; callers address
// files as <name>/<relative path>.
type DocsRoot struct {
	Name string `json:"name"`
	Path string `json:"path"`
}

// KargoConfig locates the Kargo project the release endpoint reads.
type KargoConfig struct {
	// Cluster is the registry entry whose credentials read Kargo.
	Cluster string `json:"cluster,omitempty"`
	// InCluster reads Kargo with the pod's own ServiceAccount, for a Kargo
	// that runs where the bot runs. Exactly one of Cluster and InCluster.
	InCluster bool   `json:"inCluster,omitempty"`
	Namespace string `json:"namespace"`
	// RecentFreight is how many of the newest Freight objects to report.
	RecentFreight int `json:"recentFreight"`
}

// Timeouts bound the three kinds of work the service performs.
type Timeouts struct {
	// CommandSeconds bounds one kubectl-ome invocation.
	CommandSeconds int `json:"commandSeconds"`
	// ProbeSeconds bounds one cluster reachability probe.
	ProbeSeconds int `json:"probeSeconds"`
	// FanoutSeconds bounds a command that runs on every cluster.
	FanoutSeconds int `json:"fanoutSeconds"`
	// ReadHeaderSeconds bounds reading an HTTP request's headers.
	ReadHeaderSeconds int `json:"readHeaderSeconds"`
	// ShutdownSeconds is the grace period for in-flight requests on SIGTERM.
	ShutdownSeconds int `json:"shutdownSeconds"`
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

var (
	clusterNameRE = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]*[a-z0-9])?$`)
	envNameRE     = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
)

// DocsToolName is the tool name the built-in docs tool answers to.
const DocsToolName = "docs"

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
	if c.Timeouts.ReadHeaderSeconds <= 0 {
		add("timeouts.readHeaderSeconds must be > 0")
	}
	if c.Timeouts.ShutdownSeconds <= 0 {
		add("timeouts.shutdownSeconds must be > 0")
	}
	if c.OutputLimitBytes <= 0 {
		add("outputLimitBytes must be > 0")
	}
	if c.MaxRequestBytes <= 0 {
		add("maxRequestBytes must be > 0")
	}
	if c.MaxInFlight <= 0 {
		add("maxInFlight must be > 0")
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
	c.validateTools(add)
	c.validateDocs(add)
	if k := c.Kargo; k != nil {
		switch {
		case k.InCluster && k.Cluster != "":
			add("kargo: set exactly one of cluster and inCluster")
		case k.InCluster:
		default:
			if _, ok := c.Cluster(k.Cluster); !ok {
				add("kargo.cluster %q is not a registry cluster", k.Cluster)
			}
		}
		if k.Namespace == "" {
			add("kargo.namespace is required")
		}
		if k.RecentFreight <= 0 {
			add("kargo.recentFreight must be > 0")
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

func (c *Config) validateTools(add func(string, ...any)) {
	seen := map[string]bool{}
	for i, t := range c.Tools {
		where := fmt.Sprintf("tools[%d]", i)
		if !clusterNameRE.MatchString(t.Name) {
			add("%s: name must be a lowercase DNS label", where)
		} else {
			where = "tool " + t.Name
		}
		if seen[t.Name] {
			add("%s: duplicate name", where)
		}
		seen[t.Name] = true
		if t.Name == DocsToolName && c.Docs != nil {
			add("%s: name is reserved for the built-in docs tool", where)
		}
		if t.Command == "" {
			add("%s: command is required", where)
		}
		if len(t.Subcommands) == 0 {
			add("%s: at least one subcommand is required", where)
		}
		if t.TimeoutSeconds <= 0 {
			add("%s: timeoutSeconds must be > 0", where)
		}
		for k, path := range t.EnvFromFiles {
			if !envNameRE.MatchString(k) {
				add("%s: envFromFiles key %q is not an environment variable name", where, k)
			}
			if !filepath.IsAbs(path) {
				add("%s: envFromFiles path for %s must be absolute", where, k)
			}
		}
		for _, kv := range t.Env {
			if !strings.Contains(kv, "=") {
				add("%s: env entry %q is not KEY=VALUE", where, kv)
			}
		}
	}
}

func (c *Config) validateDocs(add func(string, ...any)) {
	d := c.Docs
	if d == nil {
		return
	}
	if len(d.Roots) == 0 {
		add("docs.roots needs at least one root")
	}
	if d.MaxMatches <= 0 {
		add("docs.maxMatches must be > 0")
	}
	if d.MaxReadBytes <= 0 {
		add("docs.maxReadBytes must be > 0")
	}
	seen := map[string]bool{}
	for i, r := range d.Roots {
		if !clusterNameRE.MatchString(r.Name) {
			add("docs.roots[%d]: name must be a lowercase DNS label", i)
		}
		if seen[r.Name] {
			add("docs root %s: duplicate name", r.Name)
		}
		seen[r.Name] = true
		if st, err := os.Stat(r.Path); err != nil || !st.IsDir() {
			add("docs root %s: %q is not a directory", r.Name, r.Path)
		}
	}
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
