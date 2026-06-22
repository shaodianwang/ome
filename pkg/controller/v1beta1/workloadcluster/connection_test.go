package workloadcluster

import (
	"testing"

	"k8s.io/client-go/rest"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
)

func TestValidateRESTConfig(t *testing.T) {
	cases := []struct {
		name    string
		cfg     *rest.Config
		wantErr bool
	}{
		{"clean", &rest.Config{Host: "https://h", BearerToken: "t", TLSClientConfig: rest.TLSClientConfig{CAData: []byte("ca")}}, false},
		{"clean ip host", &rest.Config{Host: "https://10.0.0.1:6443", BearerToken: "t"}, false},
		{"exec provider", &rest.Config{ExecProvider: &clientcmdapi.ExecConfig{Command: "aws"}}, true},
		{"bearer token file", &rest.Config{BearerTokenFile: "/var/run/token"}, true},
		{"insecure TLS", &rest.Config{TLSClientConfig: rest.TLSClientConfig{Insecure: true}}, true},
		{"basic auth", &rest.Config{Host: "https://h", Username: "u", Password: "p"}, true},
		{"ca file path", &rest.Config{Host: "https://h", TLSClientConfig: rest.TLSClientConfig{CAFile: "/etc/ca.crt"}}, true},
		{"untrusted host", &rest.Config{Host: "https://bad_host"}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateRESTConfig(tc.cfg)
			if (err != nil) != tc.wantErr {
				t.Errorf("validateRESTConfig() err=%v wantErr=%v", err, tc.wantErr)
			}
		})
	}
}

// TestValidateKubeConfigBytes pins the raw-kubeconfig-level checks (run across
// ALL clusters/users, before flattening): per-user token files, per-cluster
// insecure TLS, and CA file paths are rejected; a server URL is required.
func TestValidateKubeConfigBytes(t *testing.T) {
	const clean = `apiVersion: v1
kind: Config
clusters:
- name: c
  cluster:
    server: https://example.com
    certificate-authority-data: ZHVtbXk=
contexts:
- name: ctx
  context: {cluster: c, user: u}
current-context: ctx
users:
- name: u
  user:
    token: abc
`
	const tokenFile = `apiVersion: v1
kind: Config
clusters:
- name: c
  cluster: {server: https://example.com, certificate-authority-data: ZHVtbXk=}
users:
- name: u
  user:
    tokenFile: /var/run/secrets/token
`
	const insecure = `apiVersion: v1
kind: Config
clusters:
- name: c
  cluster:
    server: https://example.com
    insecure-skip-tls-verify: true
`
	const caPath = `apiVersion: v1
kind: Config
clusters:
- name: c
  cluster:
    server: https://example.com
    certificate-authority: /etc/ca.crt
`
	const noServer = `apiVersion: v1
kind: Config
clusters:
- name: c
  cluster:
    certificate-authority-data: ZHVtbXk=
`
	cases := []struct {
		name    string
		raw     string
		wantErr bool
	}{
		{"clean", clean, false},
		{"per-user token file", tokenFile, true},
		{"per-cluster insecure", insecure, true},
		{"ca file path", caPath, true},
		{"missing server", noServer, true},
		{"malformed", "this is not a kubeconfig", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateKubeConfigBytes([]byte(tc.raw))
			if (err != nil) != tc.wantErr {
				t.Errorf("validateKubeConfigBytes() err=%v wantErr=%v", err, tc.wantErr)
			}
		})
	}
}

func TestRESTConfigFromKubeConfig_BadBytes(t *testing.T) {
	if _, err := RESTConfigFromKubeConfig([]byte("not-a-kubeconfig")); err == nil {
		t.Errorf("expected error for malformed kubeconfig, got nil")
	}
}
