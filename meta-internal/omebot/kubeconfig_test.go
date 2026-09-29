//go:build meta_internal

package omebot

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestKubeconfigPathsRendersExecCredential(t *testing.T) {
	cfg, err := Parse([]byte(validConfig))
	if err != nil {
		t.Fatal(err)
	}
	cfg.KubeconfigDir = t.TempDir()
	paths, err := KubeconfigPaths(cfg)
	if err != nil {
		t.Fatalf("KubeconfigPaths: %v", err)
	}
	if paths["local"] != "" || paths["hub-a"] != "/etc/kubeconfigs/hub-a" {
		t.Fatalf("unexpected paths: %v", paths)
	}
	want := filepath.Join(cfg.KubeconfigDir, "member-a.yaml")
	if paths["member-a"] != want {
		t.Fatalf("member-a path = %q, want %q", paths["member-a"], want)
	}
	raw, err := os.ReadFile(want)
	if err != nil {
		t.Fatal(err)
	}
	for _, needle := range []string{
		"server: https://member-a.example.com",
		"insecure-skip-tls-verify: true",
		"command: /opt/cf/cloud_oss/cloud_oss",
		"- exec-credential",
		"interactiveMode: Never",
		"current-context: member-a",
	} {
		if !strings.Contains(string(raw), needle) {
			t.Errorf("rendered kubeconfig lacks %q:\n%s", needle, raw)
		}
	}
	if strings.Contains(string(raw), "token:") {
		t.Error("rendered kubeconfig must not carry a static token")
	}
	info, _ := os.Stat(want)
	if info.Mode().Perm() != 0o600 {
		t.Errorf("kubeconfig mode = %v, want 0600", info.Mode().Perm())
	}
}
