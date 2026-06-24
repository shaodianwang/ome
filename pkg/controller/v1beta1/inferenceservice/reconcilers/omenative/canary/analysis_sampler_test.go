package canary

import (
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestResolveBearerToken(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "prom-auth", Namespace: "ns"},
		Data:       map[string][]byte{"token": []byte("s3cr3t")},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(secret).Build()
	ref := func(name, key string) *corev1.SecretKeySelector {
		return &corev1.SecretKeySelector{
			LocalObjectReference: corev1.LocalObjectReference{Name: name},
			Key:                  key,
		}
	}

	t.Run("success", func(t *testing.T) {
		got, err := resolveBearerToken(context.Background(), c, "ns", ref("prom-auth", "token"))
		if err != nil || got != "s3cr3t" {
			t.Fatalf("got (%q,%v), want (s3cr3t,nil)", got, err)
		}
	})
	t.Run("secret not found", func(t *testing.T) {
		_, err := resolveBearerToken(context.Background(), c, "ns", ref("missing", "token"))
		if err == nil || !strings.Contains(err.Error(), "not found") {
			t.Fatalf("err = %v, want a not-found error", err)
		}
	})
	t.Run("missing key", func(t *testing.T) {
		_, err := resolveBearerToken(context.Background(), c, "ns", ref("prom-auth", "nope"))
		if err == nil || !strings.Contains(err.Error(), "no key") {
			t.Fatalf("err = %v, want a missing-key error", err)
		}
	})
	t.Run("nil client", func(t *testing.T) {
		if _, err := resolveBearerToken(context.Background(), nil, "ns", ref("prom-auth", "token")); err == nil {
			t.Fatal("want an error for a nil client")
		}
	})
}
