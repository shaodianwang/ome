//go:build meta_internal

package xdspublisher

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ome-projects/ome/pkg/controller/v1beta1/placement/routing"
)

func validOptions() map[string]string {
	return map[string]string{
		optTargetTemplate: "{{.EndpointHostname}}",
		optResyncInterval: "1m",
		optURLTemplate:    "https://automation.example.com/api/v1/clusters/{cluster}/weight",
		optRequestTimeout: "10s",
		optTLSCertFile:    "/certs/client.pem",
		optTLSKeyFile:     "/certs/client-key.pem",
		optTLSCAFile:      "/certs/ca.pem",
		optOncall:         "ome",
		optACL:            "ome-xds-publisher",
	}
}

func TestParseConfig(t *testing.T) {
	cfg, err := parseConfig(validOptions())
	require.NoError(t, err)
	assert.Equal(t, "{{.EndpointHostname}}", cfg.targetTemplate)
	assert.Equal(t, time.Minute, cfg.resyncInterval)
	assert.Equal(t, 10*time.Second, cfg.requestTimeout)
	assert.Equal(t, "https://automation.example.com/api/v1/clusters/{cluster}/weight", cfg.urlTemplate)
	assert.Equal(t, "/certs/client.pem", cfg.tlsCertFile)
	assert.Equal(t, "/certs/client-key.pem", cfg.tlsKeyFile)
	assert.Equal(t, "/certs/ca.pem", cfg.tlsCAFile)
	assert.Equal(t, "ome", cfg.oncall)
	require.NotNil(t, cfg.acl)
	assert.Equal(t, "ome-xds-publisher", *cfg.acl)

	tests := []struct {
		name   string
		mutate func(map[string]string)
		want   string
	}{
		{"unknown option", func(o map[string]string) { o["typo"] = "value" }, "not accepted"},
		{"missing oncall", func(o map[string]string) { delete(o, optOncall) }, optOncall},
		{"empty ACL", func(o map[string]string) { o[optACL] = " " }, "must not be empty"},
		{"missing template", func(o map[string]string) { delete(o, optTargetTemplate) }, optTargetTemplate},
		{"bad template", func(o map[string]string) { o[optTargetTemplate] = "{{" }, optTargetTemplate},
		{"missing resync", func(o map[string]string) { delete(o, optResyncInterval) }, optResyncInterval},
		{"bad resync", func(o map[string]string) { o[optResyncInterval] = "five" }, "not a duration"},
		{"zero resync", func(o map[string]string) { o[optResyncInterval] = "0s" }, "must be positive"},
		{"missing URL", func(o map[string]string) { delete(o, optURLTemplate) }, optURLTemplate},
		{"HTTP URL", func(o map[string]string) { o[optURLTemplate] = "http://automation.example.com/{cluster}" }, "absolute HTTPS"},
		{"missing URL placeholder", func(o map[string]string) { o[optURLTemplate] = "https://automation.example.com/weight" }, "exactly one"},
		{"URL placeholder outside path", func(o map[string]string) { o[optURLTemplate] = "https://{cluster}.example.com/weight" }, "URL path"},
		{"missing request timeout", func(o map[string]string) { delete(o, optRequestTimeout) }, optRequestTimeout},
		{"bad request timeout", func(o map[string]string) { o[optRequestTimeout] = "later" }, "not a duration"},
		{"missing TLS cert", func(o map[string]string) { delete(o, optTLSCertFile) }, optTLSCertFile},
		{"missing TLS key", func(o map[string]string) { delete(o, optTLSKeyFile) }, optTLSKeyFile},
		{"missing TLS CA", func(o map[string]string) { delete(o, optTLSCAFile) }, optTLSCAFile},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			options := validOptions()
			tt.mutate(options)
			_, err := parseConfig(options)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.want)
		})
	}
}

func TestParseConfigAllowsOmittedACL(t *testing.T) {
	options := validOptions()
	delete(options, optACL)
	cfg, err := parseConfig(options)
	require.NoError(t, err)
	assert.Nil(t, cfg.acl)
}

func TestNewPublisherBuildsHTTPClient(t *testing.T) {
	httpPublisher := &recordingPublisher{}
	var gotConfig config
	factory := func(cfg config) (ClusterWeightPublisher, error) {
		gotConfig = cfg
		return httpPublisher, nil
	}

	got, err := newTrafficMapPublisherWithFactory(
		routing.PublisherConfig{Options: validOptions()},
		factory,
	)
	require.NoError(t, err)
	assert.Same(t, httpPublisher, got.Publisher.(*Publisher).weights)
	assert.Equal(t, time.Minute, got.ResyncInterval)
	assert.Equal(t, map[string]string{
		optTargetTemplate: "{{.EndpointHostname}}",
	}, got.GlobalOptions)
	assert.Equal(t, "ome", gotConfig.oncall)
	require.NotNil(t, gotConfig.acl)
	assert.Equal(t, "ome-xds-publisher", *gotConfig.acl)
}

func TestNewPublisherReportsHTTPClientFailure(t *testing.T) {
	factory := func(config) (ClusterWeightPublisher, error) {
		return nil, errors.New("TLS unavailable")
	}

	_, err := newPublisher(
		routing.PublisherConfig{Options: validOptions()},
		factory,
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "TLS unavailable")
}

func TestNewPublisherRejectsNilFactory(t *testing.T) {
	_, err := newPublisher(
		routing.PublisherConfig{Options: validOptions()},
		nil,
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "must not be nil")
}

func TestHTTPPublisherIsRegistered(t *testing.T) {
	assert.Contains(t, routing.RegisteredTrafficMapPublishers(), Name)
}
