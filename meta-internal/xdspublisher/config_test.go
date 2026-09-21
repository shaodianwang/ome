//go:build meta_internal

package xdspublisher

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/labels"

	"github.com/ome-projects/ome/pkg/controller/v1beta1/placement/routing"
)

func validOptions() map[string]string {
	return map[string]string{
		optTargetTemplate:  "{{.EndpointHostname}}",
		optURLTemplate:     "https://automation.example.com/api/v1/clusters/{cluster}/weight",
		optRequestTimeout:  "10s",
		optTLSCertFile:     "/certs/client.pem",
		optTLSKeyFile:      "/certs/client-key.pem",
		optTLSCAFile:       "/certs/ca.pem",
		optOncall:          "ome",
		optACL:             "ome-xds-publisher",
		optWeightMode:      weightModeNormalized,
		optNormalizedTotal: "100",
	}
}

func TestParseConfig(t *testing.T) {
	cfg, err := parseConfig(validOptions())
	require.NoError(t, err)
	assert.Equal(t, "{{.EndpointHostname}}", cfg.targetTemplate)
	assert.Equal(t, 10*time.Second, cfg.requestTimeout)
	assert.Equal(t, "https://automation.example.com/api/v1/clusters/{cluster}/weight", cfg.urlTemplate)
	assert.Equal(t, "/certs/client.pem", cfg.tlsCertFile)
	assert.Equal(t, "/certs/client-key.pem", cfg.tlsKeyFile)
	assert.Equal(t, "/certs/ca.pem", cfg.tlsCAFile)
	assert.Equal(t, "ome", cfg.oncall)
	require.NotNil(t, cfg.acl)
	assert.Equal(t, "ome-xds-publisher", *cfg.acl)
	assert.Empty(t, cfg.sourceSelector)
	assert.Equal(t, weightModeNormalized, cfg.weightMode)
	assert.Equal(t, "100", cfg.normalizedTotal)

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
		{"bad source selector", func(o map[string]string) { o[optSourceSelector] = "publish in (" }, optSourceSelector},
		{"missing weight mode", func(o map[string]string) { delete(o, optWeightMode) }, optWeightMode},
		{"bad weight mode", func(o map[string]string) { o[optWeightMode] = "percent" }, optWeightMode},
		{"missing normalized total", func(o map[string]string) { delete(o, optNormalizedTotal) }, optNormalizedTotal},
		{"empty normalized total", func(o map[string]string) { o[optNormalizedTotal] = " " }, optNormalizedTotal},
		{"zero normalized total", func(o map[string]string) { o[optNormalizedTotal] = "0" }, "finite positive"},
		{"negative normalized total", func(o map[string]string) { o[optNormalizedTotal] = "-1" }, "finite positive"},
		{"NaN normalized total", func(o map[string]string) { o[optNormalizedTotal] = "NaN" }, "finite positive"},
		{"infinite normalized total", func(o map[string]string) { o[optNormalizedTotal] = "+Inf" }, "finite positive"},
		{"invalid normalized total", func(o map[string]string) { o[optNormalizedTotal] = "all" }, "finite positive"},
		{"publisher resync is not an opaque option", func(o map[string]string) { o["resyncInterval"] = "1m" }, "not accepted"},
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

func TestParseConfigCanonicalizesSourceSelector(t *testing.T) {
	options := validOptions()
	options[optSourceSelector] = " publish=true, environment in (prod,canary) "

	cfg, err := parseConfig(options)
	require.NoError(t, err)
	want, err := labels.Parse(options[optSourceSelector])
	require.NoError(t, err)
	assert.Equal(t, want.String(), cfg.sourceSelector)
}

func TestParseConfigCanonicalizesRelativeWeightMode(t *testing.T) {
	options := validOptions()
	options[optWeightMode] = "  relative  "
	delete(options, optNormalizedTotal)

	cfg, err := parseConfig(options)
	require.NoError(t, err)
	assert.Equal(t, weightModeRelative, cfg.weightMode)
	assert.Empty(t, cfg.normalizedTotal)
}

func TestParseConfigCanonicalizesNormalizedTotal(t *testing.T) {
	options := validOptions()
	options[optNormalizedTotal] = "  1e2  "

	cfg, err := parseConfig(options)
	require.NoError(t, err)
	assert.Equal(t, "100", cfg.normalizedTotal)
}

func TestParseConfigRejectsNormalizedTotalInRelativeMode(t *testing.T) {
	options := validOptions()
	options[optWeightMode] = weightModeRelative

	_, err := parseConfig(options)
	require.Error(t, err)
	assert.Contains(t, err.Error(), optNormalizedTotal)
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
	assert.Zero(t, got.ResyncInterval, "the common publisher config owns the wrapper cadence")
	assert.Equal(t, map[string]string{
		optTargetTemplate:  "{{.EndpointHostname}}",
		optWeightMode:      weightModeNormalized,
		optNormalizedTotal: "100",
	}, got.GlobalOptions)
	assert.Equal(t, "ome", gotConfig.oncall)
	require.NotNil(t, gotConfig.acl)
	assert.Equal(t, "ome-xds-publisher", *gotConfig.acl)
}

func TestNewPublisherCarriesSourceSelectorAsGlobalBehavior(t *testing.T) {
	options := validOptions()
	options[optSourceSelector] = "publish=true, environment in (prod,canary)"
	wantSelector, err := labels.Parse(options[optSourceSelector])
	require.NoError(t, err)

	var gotConfig config
	got, err := newTrafficMapPublisherWithFactory(
		routing.PublisherConfig{Options: options},
		func(cfg config) (ClusterWeightPublisher, error) {
			gotConfig = cfg
			return &recordingPublisher{}, nil
		},
	)
	require.NoError(t, err)
	assert.Equal(t, wantSelector.String(), gotConfig.sourceSelector)
	assert.Equal(t, map[string]string{
		optTargetTemplate:  "{{.EndpointHostname}}",
		optSourceSelector:  wantSelector.String(),
		optWeightMode:      weightModeNormalized,
		optNormalizedTotal: "100",
	}, got.GlobalOptions)
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
