//go:build meta_internal

package xdspublisher

import (
	"errors"
	"go/parser"
	"go/token"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/ome-projects/ome/pkg/controller/v1beta1/placement/routing"
)

func validOptions() map[string]string {
	return map[string]string{
		optTargetTemplate: "{{.EndpointHostname}}",
		optResyncInterval: "1m",
		optOncall:         "ome",
		optACL:            "ome-xds-publisher",
	}
}

func TestParseConfig(t *testing.T) {
	cfg, err := parseConfig(validOptions())
	require.NoError(t, err)
	assert.Equal(t, time.Minute, cfg.resyncInterval)
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

func TestNewPublisherBuildsNativeClient(t *testing.T) {
	native := &recordingPublisher{}
	var gotOncall string
	var gotACL *string
	factory := func(oncall string, acl *string) (ClusterWeightPublisher, error) {
		gotOncall, gotACL = oncall, acl
		return native, nil
	}

	kubeClient := fake.NewClientBuilder().Build()
	got, err := newPublisher(
		routing.PublisherConfig{Options: validOptions()},
		kubeClient,
		kubeClient,
		factory,
	)
	require.NoError(t, err)
	assert.Same(t, native, got.weights)
	assert.Equal(t, "ome", gotOncall)
	require.NotNil(t, gotACL)
	assert.Equal(t, "ome-xds-publisher", *gotACL)
}

func TestNewPublisherReportsNativeClientFailure(t *testing.T) {
	factory := func(string, *string) (ClusterWeightPublisher, error) {
		return nil, errors.New("binding unavailable")
	}

	kubeClient := fake.NewClientBuilder().Build()
	_, err := newPublisher(
		routing.PublisherConfig{Options: validOptions()},
		kubeClient,
		kubeClient,
		factory,
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "binding unavailable")
}

func TestRegisterClusterWeightPublisherRejectsNilFactory(t *testing.T) {
	err := RegisterClusterWeightPublisher(nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "must not be nil")
}

func TestNativeBindingTemplateIsValidGo(t *testing.T) {
	source, err := os.ReadFile("native_meta_xds.go.in")
	require.NoError(t, err)
	_, err = parser.ParseFile(token.NewFileSet(), "native_meta_xds_generated.go", source, parser.AllErrors)
	require.NoError(t, err)
}
