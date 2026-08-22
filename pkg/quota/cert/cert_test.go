package cert

import (
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr/testr"
)

func complete() Options {
	return Options{
		Namespace:         "ome",
		SecretName:        "quota-webhook-cert",
		ServiceName:       "quota-webhook",
		WebhookConfigName: "quota.ome.io",
		CertDir:           "/tmp/k8s-webhook-server/serving-certs",
		CAName:            "quota-ca",
		CAOrganization:    "ome",
	}
}

// Every name is supplied by the chart, so a missing one is a wiring bug that has
// to surface at startup: the rotator would otherwise generate a cert nobody can
// find, or inject a caBundle into nothing, and the webhook would just hang
// unready with no explanation.
func TestOptionsValidate(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*Options)
		wantErr string
	}{
		{
			name:   "a fully specified set is accepted",
			mutate: func(*Options) {},
		},
		{
			name:    "namespace is required",
			mutate:  func(o *Options) { o.Namespace = "" },
			wantErr: "namespace is required",
		},
		{
			name:    "secret name is required",
			mutate:  func(o *Options) { o.SecretName = "" },
			wantErr: "secret name is required",
		},
		{
			name:    "service name is required",
			mutate:  func(o *Options) { o.ServiceName = "" },
			wantErr: "service name is required",
		},
		{
			name:    "webhook config name is required",
			mutate:  func(o *Options) { o.WebhookConfigName = "" },
			wantErr: "webhook config name is required",
		},
		{
			name:    "cert directory is required",
			mutate:  func(o *Options) { o.CertDir = "" },
			wantErr: "cert directory is required",
		},
		{
			name:    "CA name is required",
			mutate:  func(o *Options) { o.CAName = "" },
			wantErr: "CA name is required",
		},
		{
			name:    "CA organization is required",
			mutate:  func(o *Options) { o.CAOrganization = "" },
			wantErr: "CA organization is required",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			opts := complete()
			tc.mutate(&opts)

			err := opts.validate()
			switch {
			case tc.wantErr == "" && err != nil:
				t.Fatalf("validate() = %v, want no error", err)
			case tc.wantErr != "" && err == nil:
				t.Fatalf("validate() = nil, want an error naming %q", tc.wantErr)
			case tc.wantErr != "" && !strings.Contains(err.Error(), tc.wantErr):
				t.Errorf("validate() = %q, want it to name %q", err, tc.wantErr)
			}
		})
	}
}

// Manage must reject before it registers anything. Half-registering a rotator
// would leave the manager with a Runnable that can never succeed.
func TestManageRejectsIncompleteOptions(t *testing.T) {
	opts := complete()
	opts.SecretName = ""

	// A nil manager is safe precisely because validation runs first; if that
	// ordering ever changes, this panics rather than passing quietly.
	if err := Manage(nil, opts, make(chan struct{})); err == nil {
		t.Fatal("Manage() = nil, want an error for the missing secret name")
	}
}

// WaitForReady gates webhook registration, so it must actually block until the
// cert exists — returning early would register a handler the apiserver cannot
// complete a TLS handshake with, turning every quota write into a connection
// error under failurePolicy=Fail.
func TestWaitForReady(t *testing.T) {
	certsReady := make(chan struct{})
	returned := make(chan struct{})
	go func() {
		defer close(returned)
		WaitForReady(testr.New(t), certsReady)
	}()

	select {
	case <-returned:
		t.Fatal("WaitForReady() returned before the certificate was ready")
	case <-time.After(50 * time.Millisecond):
	}

	close(certsReady)
	select {
	case <-returned:
	case <-time.After(5 * time.Second):
		t.Fatal("WaitForReady() did not return once the certificate was ready")
	}
}
