//go:build meta_internal

package omebot

import (
	"strings"
	"testing"
)

const validConfig = `
listen: ":8080"
kubectlOmePath: /usr/local/bin/kubectl-ome
kubeconfigDir: /tmp/kc
execCommand: /opt/cf/cloud_oss/cloud_oss
timeouts: {commandSeconds: 25, probeSeconds: 10, fanoutSeconds: 60, readHeaderSeconds: 10, shutdownSeconds: 10}
outputLimitBytes: 12000
maxRequestBytes: 65536
maxInFlight: 4
auth: {mode: none}
clusters:
  - name: member-a
    provider: gke
    role: member
    hub: hub-a
    kargoStage: stage-1
    server: https://member-a.example.com
    insecureSkipTLSVerify: true
    tokenArgs: [k8s, get-token, project-a, member-a, --provider, gcp, --output, exec-credential]
  - name: hub-a
    provider: mks
    role: hub
    kubeconfigPath: /etc/kubeconfigs/hub-a
  - name: local
    provider: mks
    inCluster: true
`

func TestParseValid(t *testing.T) {
	cfg, err := Parse([]byte(validConfig))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if got := cfg.ClusterNames(); strings.Join(got, ",") != "member-a,hub-a,local" {
		t.Fatalf("cluster names = %v", got)
	}
	if _, ok := cfg.Cluster("hub-a"); !ok {
		t.Fatal("hub-a not found")
	}
}

func TestParseWritableAndMutations(t *testing.T) {
	raw := strings.Replace(validConfig, "    role: member\n", "    role: member\n    writable: true\n", 1) + "mutations: {enabled: true}\n"
	cfg, err := Parse([]byte(raw))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if !cfg.Mutations.Enabled || strings.Join(cfg.WritableClusterNames(), ",") != "member-a" {
		t.Fatalf("mutations %+v writable %v", cfg.Mutations, cfg.WritableClusterNames())
	}
	if cfg, err := Parse([]byte(validConfig)); err != nil || cfg.Mutations.Enabled || len(cfg.WritableClusterNames()) != 0 {
		t.Fatalf("defaults: cfg %+v err %v", cfg, err)
	}
}

func TestParseRejects(t *testing.T) {
	cases := map[string]string{
		"mutations without a writable cluster": validConfig + "mutations: {enabled: true}\n",
		"unknown field":                        strings.Replace(validConfig, "listen:", "listne:", 1),
		"missing exec command":                 strings.Replace(validConfig, "execCommand: /opt/cf/cloud_oss/cloud_oss\n", "", 1),
		"two credential sources":               strings.Replace(validConfig, "    kubeconfigPath: /etc/kubeconfigs/hub-a\n", "    kubeconfigPath: /etc/kubeconfigs/hub-a\n    inCluster: true\n", 1),
		"hmac without secret":                  strings.Replace(validConfig, "auth: {mode: none}", "auth: {mode: hmac, maxSkewSeconds: 300}", 1),
		"bad auth mode":                        strings.Replace(validConfig, "auth: {mode: none}", "auth: {mode: basic}", 1),
		"ca with insecure":                     strings.Replace(validConfig, "    insecureSkipTLSVerify: true\n", "    insecureSkipTLSVerify: true\n    certificateAuthorityData: YWJj\n", 1),
		"uppercase cluster name":               strings.Replace(validConfig, "name: member-a", "name: Member-A", 1),
		"duplicate cluster names":              strings.Replace(validConfig, "name: hub-a", "name: member-a", 1),
		"zero timeout":                         strings.Replace(validConfig, "commandSeconds: 25", "commandSeconds: 0", 1),
		"env not key=value":                    strings.Replace(validConfig, "listen: \":8080\"", "listen: \":8080\"\nenv: [HOME]", 1),
		"zero maxInFlight":                     strings.Replace(validConfig, "maxInFlight: 4", "maxInFlight: 0", 1),
		"zero maxRequestBytes":                 strings.Replace(validConfig, "maxRequestBytes: 65536", "maxRequestBytes: 0", 1),
		"zero readHeaderSeconds":               strings.Replace(validConfig, "readHeaderSeconds: 10", "readHeaderSeconds: 0", 1),
		"zero shutdownSeconds":                 strings.Replace(validConfig, "shutdownSeconds: 10", "shutdownSeconds: 0", 1),
	}
	for name, raw := range cases {
		if _, err := Parse([]byte(raw)); err == nil {
			t.Errorf("%s: expected an error", name)
		}
	}
}

// extrasConfig appends a tool registry, docs roots under dir and a Kargo
// block to validConfig.
func extrasConfig(dir string) string {
	return validConfig + `
tools:
  - name: builds
    command: /usr/bin/python3
    argsPrefix: [/opt/ome/skills/ci-triage/scripts/main_builds.py]
    subcommands: [list, diff]
    timeoutSeconds: 60
    envFromFiles: {BUILDKITE_API_TOKEN: /etc/ome-bot/secrets/buildkite-token}
    env: [OME_CITRIAGE_SCRATCH=/tmp/scratch]
docs:
  roots:
    - {name: docs, path: ` + dir + `}
  maxMatches: 50
  maxReadBytes: 24000
kargo: {cluster: local, namespace: kargo-proj-a, recentFreight: 5}
`
}

func TestParseExtras(t *testing.T) {
	dir := t.TempDir()
	cfg, err := Parse([]byte(extrasConfig(dir)))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(cfg.Tools) != 1 || cfg.Docs == nil || cfg.Kargo == nil || cfg.MaxInFlight != 4 {
		t.Fatalf("extras not decoded: %+v", cfg)
	}
	inCluster := strings.Replace(extrasConfig(dir), "cluster: local,", "inCluster: true,", 1)
	if cfg, err := Parse([]byte(inCluster)); err != nil || !cfg.Kargo.InCluster {
		t.Fatalf("in-cluster kargo: cfg %+v err %v", cfg, err)
	}
}

func TestParseRejectsExtras(t *testing.T) {
	dir := t.TempDir()
	good := extrasConfig(dir)
	cases := map[string]string{
		"tool without subcommands":    strings.Replace(good, "    subcommands: [list, diff]\n", "", 1),
		"tool zero timeout":           strings.Replace(good, "timeoutSeconds: 60", "timeoutSeconds: 0", 1),
		"tool without command":        strings.Replace(good, "    command: /usr/bin/python3\n", "", 1),
		"tool named docs":             strings.Replace(good, "  - name: builds", "  - name: docs", 1),
		"tool bad name":               strings.Replace(good, "  - name: builds", "  - name: Builds!", 1),
		"duplicate tool":              strings.Replace(good, "docs:\n", "  - {name: builds, command: /bin/true, subcommands: [x], timeoutSeconds: 1}\ndocs:\n", 1),
		"env file key not env name":   strings.Replace(good, "{BUILDKITE_API_TOKEN:", "{bad-name:", 1),
		"env file path relative":      strings.Replace(good, ": /etc/ome-bot/secrets/buildkite-token", ": secrets/buildkite-token", 1),
		"tool env not key=value":      strings.Replace(good, "env: [OME_CITRIAGE_SCRATCH=/tmp/scratch]", "env: [OME_CITRIAGE_SCRATCH]", 1),
		"docs root missing":           strings.Replace(good, "path: "+dir, "path: "+dir+"/nope", 1),
		"docs duplicate root":         strings.Replace(good, "  maxMatches: 50", "    - {name: docs, path: "+dir+"}\n  maxMatches: 50", 1),
		"docs zero maxMatches":        strings.Replace(good, "maxMatches: 50", "maxMatches: 0", 1),
		"docs zero maxReadBytes":      strings.Replace(good, "maxReadBytes: 24000", "maxReadBytes: 0", 1),
		"kargo unknown cluster":       strings.Replace(good, "cluster: local", "cluster: nope", 1),
		"kargo no namespace":          strings.Replace(good, "namespace: kargo-proj-a, ", "", 1),
		"kargo zero recentFreight":    strings.Replace(good, "recentFreight: 5", "recentFreight: 0", 1),
		"kargo cluster and inCluster": strings.Replace(good, "cluster: local,", "cluster: local, inCluster: true,", 1),
		"kargo neither source":        strings.Replace(good, "cluster: local, ", "", 1),
	}
	for name, raw := range cases {
		if raw == good {
			t.Errorf("%s: replacement did not apply", name)
			continue
		}
		if _, err := Parse([]byte(raw)); err == nil {
			t.Errorf("%s: expected an error", name)
		}
	}
}
