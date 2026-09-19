package routing

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

func TestResolveConfigEnabled(t *testing.T) {
	trueValue, falseValue := true, false
	tests := []struct {
		name          string
		globalEnabled bool
		spec          *v1beta1.InferenceServiceSpec
		wantEnabled   bool
	}{
		{name: "nil spec inherits enabled", globalEnabled: true, wantEnabled: true},
		{name: "nil enabled inherits enabled", globalEnabled: true, spec: routingISVCSpec(&v1beta1.RoutingSpec{}), wantEnabled: true},
		{name: "false opts out", globalEnabled: true, spec: routingISVCSpec(&v1beta1.RoutingSpec{Enabled: &falseValue})},
		{name: "true preserves enabled", globalEnabled: true, spec: routingISVCSpec(&v1beta1.RoutingSpec{Enabled: &trueValue}), wantEnabled: true},
		{name: "nil spec inherits disabled", globalEnabled: false},
		{name: "nil enabled inherits disabled", globalEnabled: false, spec: routingISVCSpec(&v1beta1.RoutingSpec{})},
		{name: "true cannot bypass global gate", globalEnabled: false, spec: routingISVCSpec(&v1beta1.RoutingSpec{Enabled: &trueValue})},
		{name: "false remains disabled", globalEnabled: false, spec: routingISVCSpec(&v1beta1.RoutingSpec{Enabled: &falseValue})},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ResolveConfig(Config{Enabled: tt.globalEnabled}, tt.spec, nil)
			require.NoError(t, err)
			assert.Equal(t, tt.wantEnabled, got.Enabled)
		})
	}
}

func TestResolveConfigValidationFollowsEffectiveEnablement(t *testing.T) {
	trueValue, falseValue := true, false
	stagedCapacity := func() *v1beta1.RoutingCapacitySpec {
		capacity := validRoutingCapacitySpec()
		capacity.Format = "Unavailable"
		return capacity
	}
	stagedPublisher := &v1beta1.RoutingPublisherSpec{Options: map[string]string{
		"routingClass": "staged",
	}}

	tests := []struct {
		name            string
		global          Config
		routing         *v1beta1.RoutingSpec
		wantEnabled     bool
		wantErrContains string
		wantResolver    bool
	}{
		{
			name: "disabled installation skips invalid global observer plugin",
			global: func() Config {
				cfg := testGlobalRoutingConfig()
				cfg.Enabled = false
				cfg.Capacity.Format = CapacityFormat("Unavailable")
				return cfg
			}(),
		},
		{
			name: "enabled installation validates inherited global observer plugin",
			global: func() Config {
				cfg := testGlobalRoutingConfig()
				cfg.Capacity.Format = CapacityFormat("Unavailable")
				return cfg
			}(),
			wantEnabled:     true,
			wantErrContains: "routing.capacity.format \"Unavailable\" is not registered",
		},
		{
			name:        "per-ISVC opt-out skips staged observer and publisher semantics",
			global:      testGlobalRoutingConfig(),
			routing:     &v1beta1.RoutingSpec{Enabled: &falseValue, Capacity: stagedCapacity(), Publisher: stagedPublisher},
			wantEnabled: false,
		},
		{
			name: "inline true cannot bypass disabled installation",
			global: func() Config {
				cfg := testGlobalRoutingConfig()
				cfg.Enabled = false
				return cfg
			}(),
			routing:     &v1beta1.RoutingSpec{Enabled: &trueValue, Capacity: stagedCapacity(), Publisher: stagedPublisher},
			wantEnabled: false,
		},
		{
			name:            "enabled policy validates observer before publisher semantics",
			global:          testGlobalRoutingConfig(),
			routing:         &v1beta1.RoutingSpec{Enabled: &trueValue, Capacity: stagedCapacity(), Publisher: stagedPublisher},
			wantEnabled:     true,
			wantErrContains: "spec.routing.capacity.format \"Unavailable\" is not registered",
		},
		{
			name:         "enabled valid policy resolves publisher semantics",
			global:       testGlobalRoutingConfig(),
			routing:      &v1beta1.RoutingSpec{Enabled: &trueValue, Publisher: stagedPublisher},
			wantEnabled:  true,
			wantResolver: true,
		},
		{
			name:            "generic API shape remains mandatory while opted out",
			global:          testGlobalRoutingConfig(),
			routing:         &v1beta1.RoutingSpec{Enabled: &falseValue, Capacity: &v1beta1.RoutingCapacitySpec{Path: "/capacity"}, Publisher: stagedPublisher},
			wantErrContains: "method is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resolverCalled := false
			got, err := ResolveConfig(tt.global, routingISVCSpec(tt.routing),
				func(globalOptions, inlineOptions map[string]string) (map[string]string, error) {
					resolverCalled = true
					for key, value := range inlineOptions {
						globalOptions[key] = value
					}
					return globalOptions, nil
				})
			if tt.wantErrContains != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErrContains)
				assert.False(t, resolverCalled)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantEnabled, got.Enabled)
			assert.Equal(t, tt.wantResolver, resolverCalled)
			if tt.wantResolver {
				assert.Equal(t, "staged", got.Publisher.Options["routingClass"])
			} else {
				assert.Equal(t, tt.global.Publisher.Options, got.Publisher.Options,
					"disabled routing must retain global publisher semantics without applying staged options")
			}
		})
	}
}

func TestResolveConfigObserverBlocks(t *testing.T) {
	global := testGlobalRoutingConfig()

	t.Run("nil blocks inherit", func(t *testing.T) {
		got, err := ResolveConfig(global, routingISVCSpec(&v1beta1.RoutingSpec{}), nil)
		require.NoError(t, err)
		assert.Equal(t, global.Probe, got.Probe)
		assert.Equal(t, global.Capacity, got.Capacity)
	})

	t.Run("disabled blocks clear", func(t *testing.T) {
		got, err := ResolveConfig(global, routingISVCSpec(&v1beta1.RoutingSpec{
			Probe:    &v1beta1.RoutingProbeSpec{Disabled: true},
			Capacity: &v1beta1.RoutingCapacitySpec{Disabled: true},
		}), nil)
		require.NoError(t, err)
		assert.Equal(t, ProbeConfig{}, got.Probe)
		assert.Equal(t, CapacityConfig{}, got.Capacity)
	})

	t.Run("inline blocks replace atomically", func(t *testing.T) {
		got, err := ResolveConfig(global, routingISVCSpec(&v1beta1.RoutingSpec{
			Probe: &v1beta1.RoutingProbeSpec{
				Path:             "/ready",
				Method:           "HEAD",
				AcceptStatuses:   []int32{204},
				GateStatuses:     []int32{500, 503},
				Period:           metav1.Duration{Duration: 2 * time.Minute},
				Timeout:          metav1.Duration{Duration: 7 * time.Second},
				FailureThreshold: 4,
				SuccessThreshold: 5,
				AllFailedPolicy:  v1beta1.RoutingAllFailedPolicyDrain,
			},
			Capacity: &v1beta1.RoutingCapacitySpec{
				Path:    "/capacity",
				Method:  "POST",
				Format:  string(FormatReport),
				Period:  metav1.Duration{Duration: 3 * time.Minute},
				Timeout: metav1.Duration{Duration: 8 * time.Second},
				Samples: 7,
				Quorum:  3,
				MaxAge:  metav1.Duration{Duration: 10 * time.Minute},
			},
		}), nil)
		require.NoError(t, err)

		assert.Equal(t, ProbeConfig{
			Path:             "/ready",
			Method:           "HEAD",
			AcceptStatuses:   []int{204},
			GateStatuses:     []int{500, 503},
			Period:           2 * time.Minute,
			Timeout:          7 * time.Second,
			FailureThreshold: 4,
			SuccessThreshold: 5,
			AllFailedPolicy:  AllFailedPolicyDrain,
		}, got.Probe)
		assert.Equal(t, CapacityConfig{
			Path:    "/capacity",
			Method:  "POST",
			Format:  FormatReport,
			Period:  3 * time.Minute,
			Timeout: 8 * time.Second,
			Samples: 7,
			Quorum:  3,
			MaxAge:  10 * time.Minute,
		}, got.Capacity)
	})

	t.Run("partial inline block does not merge global fields", func(t *testing.T) {
		_, err := ResolveConfig(global, routingISVCSpec(&v1beta1.RoutingSpec{
			Probe: &v1beta1.RoutingProbeSpec{Path: "/replacement"},
		}), nil)
		require.EqualError(t, err,
			"spec.routing.probe.method is required")

		_, err = ResolveConfig(global, routingISVCSpec(&v1beta1.RoutingSpec{
			Capacity: &v1beta1.RoutingCapacitySpec{Path: "/replacement"},
		}), nil)
		require.EqualError(t, err,
			"spec.routing.capacity.method is required")
	})
}

func TestResolveConfigPublisherOptions(t *testing.T) {
	global := testGlobalRoutingConfig()
	routingSpec := &v1beta1.RoutingSpec{
		Publisher: &v1beta1.RoutingPublisherSpec{Options: map[string]string{
			"routingClass": "latency-sensitive",
			"serviceAlias": "service.example.com",
		}},
	}
	var callbackResult map[string]string
	resolver := func(globalOptions, inlineOptions map[string]string) (map[string]string, error) {
		assert.Equal(t, global.Publisher.Options, globalOptions)
		assert.Equal(t, routingSpec.Publisher.Options, inlineOptions)
		for key, value := range inlineOptions {
			globalOptions[key] = value
		}
		inlineOptions["routingClass"] = "callback-mutated"
		callbackResult = globalOptions
		return globalOptions, nil
	}

	got, err := ResolveConfig(global, routingISVCSpec(routingSpec), resolver)
	require.NoError(t, err)
	assert.Equal(t, global.Publisher.Name, got.Publisher.Name)
	assert.Equal(t, map[string]string{
		"globalOnly":   "retained",
		"routingClass": "latency-sensitive",
		"serviceAlias": "service.example.com",
	}, got.Publisher.Options)
	got.Publisher.Options["routingClass"] = "caller-mutated"
	assert.Equal(t, "latency-sensitive", callbackResult["routingClass"],
		"effective config must not alias the publisher's returned map")
	assert.Equal(t, "global", global.Publisher.Options["routingClass"],
		"publisher callback must receive a copy of global options")
	assert.Equal(t, "latency-sensitive", routingSpec.Publisher.Options["routingClass"],
		"publisher callback must receive a copy of inline options")

	_, err = ResolveConfig(global, routingISVCSpec(&v1beta1.RoutingSpec{
		Publisher: &v1beta1.RoutingPublisherSpec{Options: map[string]string{
			"routingClass": "latency-sensitive",
		}},
	}), nil)
	require.EqualError(t, err,
		"spec.routing.publisher.options cannot be set because the selected publisher does not support per-InferenceService options")

	rejectOutOfScope := func(_, _ map[string]string) (map[string]string, error) {
		return nil, errors.New("serviceAlias is outside the permitted scope")
	}
	_, err = ResolveConfig(global, routingISVCSpec(&v1beta1.RoutingSpec{
		Publisher: &v1beta1.RoutingPublisherSpec{Options: map[string]string{
			"serviceAlias": "other.example.com",
		}},
	}), rejectOutOfScope)
	require.EqualError(t, err,
		"spec.routing.publisher.options: serviceAlias is outside the permitted scope")
}

func TestResolveConfigRunsSharedRoutingValidation(t *testing.T) {
	validProbe := func() *v1beta1.RoutingProbeSpec {
		return &v1beta1.RoutingProbeSpec{
			Path:             "/ready",
			Method:           "GET",
			AcceptStatuses:   []int32{200},
			GateStatuses:     []int32{500},
			Period:           metav1.Duration{Duration: time.Minute},
			Timeout:          metav1.Duration{Duration: time.Second},
			FailureThreshold: 2,
			SuccessThreshold: 2,
			AllFailedPolicy:  v1beta1.RoutingAllFailedPolicyPreserveTraffic,
		}
	}
	tests := []struct {
		name        string
		spec        *v1beta1.InferenceServiceSpec
		wantContain string
	}{
		{
			name: "disabled probe is exclusive",
			spec: routingISVCSpec(&v1beta1.RoutingSpec{
				Probe: &v1beta1.RoutingProbeSpec{Disabled: true, Path: "/ready"},
			}),
			wantContain: "disabled must not be combined",
		},
		{
			name: "HTTP status is bounded",
			spec: func() *v1beta1.InferenceServiceSpec {
				probe := validProbe()
				probe.AcceptStatuses = []int32{99}
				return routingISVCSpec(&v1beta1.RoutingSpec{Probe: probe})
			}(),
			wantContain: "invalid HTTP status",
		},
		{
			name: "HTTP method is restricted",
			spec: func() *v1beta1.InferenceServiceSpec {
				probe := validProbe()
				probe.Method = "DELETE"
				return routingISVCSpec(&v1beta1.RoutingSpec{Probe: probe})
			}(),
			wantContain: "method",
		},
		{
			name: "publisher option size is bounded",
			spec: routingISVCSpec(&v1beta1.RoutingSpec{
				Publisher: &v1beta1.RoutingPublisherSpec{Options: map[string]string{
					"routingClass": strings.Repeat("x", v1beta1.MaxRoutingPublisherOptionValueBytes+1),
				}},
			}),
			wantContain: "byte value limit",
		},
		{
			name: "new and deprecated capacity factors conflict",
			spec: &v1beta1.InferenceServiceSpec{
				Routing: &v1beta1.RoutingSpec{
					CapacityFactors: map[string]resource.Quantity{},
				},
				Placement: &v1beta1.PlacementSpec{
					//nolint:staticcheck // compatibility coverage for deprecated spec.placement.capacityFactors
					CapacityFactors: map[string]resource.Quantity{},
				},
			},
			wantContain: "must not both be set",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			publisherCalled := false
			_, err := ResolveConfig(testGlobalRoutingConfig(), tt.spec,
				func(_, _ map[string]string) (map[string]string, error) {
					publisherCalled = true
					return nil, nil
				})
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantContain)
			assert.False(t, publisherCalled)
		})
	}
}

func TestResolveConfigMutationIsolation(t *testing.T) {
	global := testGlobalRoutingConfig()
	global.Capacity = CapacityConfig{Options: map[string]string{"source": "global"}}
	routingSpec := &v1beta1.RoutingSpec{
		Publisher: &v1beta1.RoutingPublisherSpec{
			Options: map[string]string{"routingClass": "inline"},
		},
	}
	resolver := func(globalOptions, inlineOptions map[string]string) (map[string]string, error) {
		for key, value := range inlineOptions {
			globalOptions[key] = value
		}
		return globalOptions, nil
	}

	first, err := ResolveConfig(global, routingISVCSpec(routingSpec), resolver)
	require.NoError(t, err)
	second, err := ResolveConfig(global, routingISVCSpec(routingSpec), resolver)
	require.NoError(t, err)

	first.Probe.AcceptStatuses[0] = 299
	first.Probe.GateStatuses[0] = 599
	first.Capacity.Options["source"] = "first"
	first.Publisher.Options["routingClass"] = "first"
	first.Publisher.Options["globalOnly"] = "first"
	routingSpec.Publisher.Options["routingClass"] = "source-mutated"

	assert.Equal(t, []int{200, 204}, global.Probe.AcceptStatuses)
	assert.Equal(t, []int{500, 503}, global.Probe.GateStatuses)
	assert.Equal(t, map[string]string{"source": "global"}, global.Capacity.Options)
	assert.Equal(t, map[string]string{
		"globalOnly":   "retained",
		"routingClass": "global",
	}, global.Publisher.Options)
	assert.Equal(t, map[string]string{"source": "global"}, second.Capacity.Options)
	assert.Equal(t, map[string]string{
		"globalOnly":   "retained",
		"routingClass": "inline",
	}, second.Publisher.Options)
}

func routingISVCSpec(routing *v1beta1.RoutingSpec) *v1beta1.InferenceServiceSpec {
	return &v1beta1.InferenceServiceSpec{Routing: routing}
}

func testGlobalRoutingConfig() Config {
	return Config{
		Enabled: true,
		Probe: ProbeConfig{
			Path:             "/health",
			Method:           "GET",
			AcceptStatuses:   []int{200, 204},
			GateStatuses:     []int{500, 503},
			Period:           time.Minute,
			Timeout:          5 * time.Second,
			FailureThreshold: 2,
			SuccessThreshold: 3,
			AllFailedPolicy:  AllFailedPolicyPreserveTraffic,
		},
		Capacity: CapacityConfig{
			Path:    "/capacity",
			Method:  "GET",
			Format:  FormatReport,
			Period:  2 * time.Minute,
			Timeout: 5 * time.Second,
			Samples: 6,
			Quorum:  2,
			MaxAge:  5 * time.Minute,
		},
		Publisher: PublisherConfig{
			Name: "publisher",
			Options: map[string]string{
				"globalOnly":   "retained",
				"routingClass": "global",
			},
		},
	}
}

func validRoutingCapacitySpec() *v1beta1.RoutingCapacitySpec {
	return &v1beta1.RoutingCapacitySpec{
		Path:    "/capacity",
		Method:  "GET",
		Format:  string(FormatReport),
		Period:  metav1.Duration{Duration: 30 * time.Second},
		Timeout: metav1.Duration{Duration: 3 * time.Second},
		Samples: 10,
		Quorum:  2,
		MaxAge:  metav1.Duration{Duration: time.Minute},
	}
}
