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
timeouts: {commandSeconds: 25, probeSeconds: 10, fanoutSeconds: 60}
outputLimitBytes: 12000
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

func TestParseRejects(t *testing.T) {
	cases := map[string]string{
		"unknown field":           strings.Replace(validConfig, "listen:", "listne:", 1),
		"missing exec command":    strings.Replace(validConfig, "execCommand: /opt/cf/cloud_oss/cloud_oss\n", "", 1),
		"two credential sources":  strings.Replace(validConfig, "    kubeconfigPath: /etc/kubeconfigs/hub-a\n", "    kubeconfigPath: /etc/kubeconfigs/hub-a\n    inCluster: true\n", 1),
		"hmac without secret":     strings.Replace(validConfig, "auth: {mode: none}", "auth: {mode: hmac, maxSkewSeconds: 300}", 1),
		"bad auth mode":           strings.Replace(validConfig, "auth: {mode: none}", "auth: {mode: basic}", 1),
		"ca with insecure":        strings.Replace(validConfig, "    insecureSkipTLSVerify: true\n", "    insecureSkipTLSVerify: true\n    certificateAuthorityData: YWJj\n", 1),
		"uppercase cluster name":  strings.Replace(validConfig, "name: member-a", "name: Member-A", 1),
		"duplicate cluster names": strings.Replace(validConfig, "name: hub-a", "name: member-a", 1),
		"zero timeout":            strings.Replace(validConfig, "commandSeconds: 25", "commandSeconds: 0", 1),
		"env not key=value":       strings.Replace(validConfig, "listen: \":8080\"", "listen: \":8080\"\nenv: [HOME]", 1),
	}
	for name, raw := range cases {
		if _, err := Parse([]byte(raw)); err == nil {
			t.Errorf("%s: expected an error", name)
		}
	}
}
