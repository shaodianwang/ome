package inferencedeployment

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestClusterCredentialFromSecret(t *testing.T) {
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "ido",
			Name:      "cluster-a",
			Labels: map[string]string{
				ClusterCredentialLabel: "true",
				ClusterNameLabel:       "cluster-a",
			},
		},
		Data: map[string][]byte{
			SecretKeyClusterName:              []byte("cluster-a"),
			SecretKeyServer:                   []byte("https://cluster-a.example"),
			SecretKeyBearerToken:              []byte("token"),
			SecretKeyCertificateAuthorityData: []byte("-----BEGIN CERTIFICATE-----\n...\n-----END CERTIFICATE-----"),
		},
	}

	credential, err := clusterCredentialFromSecret(secret)
	if err != nil {
		t.Fatalf("clusterCredentialFromSecret() error = %v", err)
	}
	if credential.Name != "cluster-a" || credential.Server != "https://cluster-a.example" || credential.BearerToken != "token" {
		t.Fatalf("credential = %#v", credential)
	}
}

func TestClusterCredentialFromSecretRejectsMismatchedLabel(t *testing.T) {
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "ido",
			Name:      "cluster-a",
			Labels: map[string]string{
				ClusterNameLabel: "different",
			},
		},
		Data: map[string][]byte{
			SecretKeyClusterName: []byte("cluster-a"),
		},
	}

	if _, err := clusterCredentialFromSecret(secret); err == nil {
		t.Fatalf("clusterCredentialFromSecret() error = nil, want mismatch error")
	}
}
