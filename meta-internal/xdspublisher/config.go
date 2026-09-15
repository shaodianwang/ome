//go:build meta_internal

// Package xdspublisher contains the deployment-specific TrafficMap adapter for
// the internal native xDS publisher. A Buck/Ligen composition supplies the
// generated client through RegisterClusterWeightPublisher.
package xdspublisher

import (
	"fmt"
	"strings"
	"text/template"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client"

	placementendpoint "github.com/ome-projects/ome/pkg/controller/v1beta1/placement/endpoint"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/placement/routing"
)

const (
	// Name is the value selected at routing.publisher.name.
	Name = "xds"

	optTargetTemplate = "targetTemplate"
	optResyncInterval = "resyncInterval"
	optOncall         = "oncall"
	optACL            = "acl"
)

var acceptedOptions = map[string]struct{}{
	optTargetTemplate: {},
	optResyncInterval: {},
	optOncall:         {},
	optACL:            {},
}

type config struct {
	targetTemplate *template.Template
	resyncInterval time.Duration
	oncall         string
	acl            *string
}

// Publisher writes TrafficMap weights through the internal xDS API.
type Publisher struct {
	client.Client
	APIReader client.Reader
	config    config
	weights   ClusterWeightPublisher
}

var _ placementendpoint.EndpointPublisher = (*Publisher)(nil)

// ClusterWeightPublisher is the narrow native surface used by the adapter.
type ClusterWeightPublisher interface {
	PublishWeight(cluster string, weight uint32) error
	ClearWeight(cluster string) error
}

// ClusterWeightPublisherFactory constructs the native client.
type ClusterWeightPublisherFactory func(oncall string, acl *string) (ClusterWeightPublisher, error)

// RegisterClusterWeightPublisher installs the xDS adapter only in a binary
// whose composition can provide the generated internal client.
func RegisterClusterWeightPublisher(factory ClusterWeightPublisherFactory) error {
	if factory == nil {
		return fmt.Errorf("xDS cluster-weight publisher factory must not be nil")
	}
	return routing.RegisterTrafficMapPublisher(Name, func(
		config routing.PublisherConfig,
		kubeClient client.Client,
		apiReader client.Reader,
	) (*routing.TrafficMapPublisher, error) {
		publisher, err := newPublisher(config, kubeClient, apiReader, factory)
		if err != nil {
			return nil, err
		}
		return &routing.TrafficMapPublisher{
			Publisher:      publisher,
			ResyncInterval: publisher.config.resyncInterval,
		}, nil
	})
}

func newPublisher(
	publisherConfig routing.PublisherConfig,
	kubeClient client.Client,
	apiReader client.Reader,
	factory ClusterWeightPublisherFactory,
) (*Publisher, error) {
	if kubeClient == nil {
		return nil, fmt.Errorf("Kubernetes client must not be nil")
	}
	cfg, err := parseConfig(publisherConfig.Options)
	if err != nil {
		return nil, err
	}
	weights, err := factory(cfg.oncall, cfg.acl)
	if err != nil {
		return nil, err
	}
	if weights == nil {
		return nil, fmt.Errorf("xDS cluster-weight publisher factory returned nil")
	}
	return &Publisher{
		Client:    kubeClient,
		APIReader: apiReader,
		config:    cfg,
		weights:   weights,
	}, nil
}

// Name implements endpoint.EndpointPublisher.
func (*Publisher) Name() string { return Name }

func parseConfig(options map[string]string) (config, error) {
	for key := range options {
		if _, ok := acceptedOptions[key]; !ok {
			return config{}, fmt.Errorf("routing.publisher.options[%q] is not accepted by %q", key, Name)
		}
	}
	required := func(key string) (string, error) {
		value := strings.TrimSpace(options[key])
		if value == "" {
			return "", fmt.Errorf("routing.publisher.options[%q] is required by %q", key, Name)
		}
		return value, nil
	}

	oncall, err := required(optOncall)
	if err != nil {
		return config{}, err
	}
	var acl *string
	if value, ok := options[optACL]; ok {
		value = strings.TrimSpace(value)
		if value == "" {
			return config{}, fmt.Errorf("routing.publisher.options[%q] must not be empty when supplied", optACL)
		}
		acl = &value
	}

	templateText, err := required(optTargetTemplate)
	if err != nil {
		return config{}, err
	}
	targetTemplate, err := template.New("xds-target").Option("missingkey=error").Parse(templateText)
	if err != nil {
		return config{}, fmt.Errorf("routing.publisher.options[%q]: %w", optTargetTemplate, err)
	}

	resyncInterval, err := parsePositiveDuration(options, optResyncInterval)
	if err != nil {
		return config{}, err
	}

	return config{
		targetTemplate: targetTemplate,
		resyncInterval: resyncInterval,
		oncall:         oncall,
		acl:            acl,
	}, nil
}

func parsePositiveDuration(options map[string]string, key string) (time.Duration, error) {
	raw := strings.TrimSpace(options[key])
	if raw == "" {
		return 0, fmt.Errorf("routing.publisher.options[%q] is required by %q", key, Name)
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("routing.publisher.options[%q]: %q is not a duration", key, raw)
	}
	if d <= 0 {
		return 0, fmt.Errorf("routing.publisher.options[%q] must be positive", key)
	}
	return d, nil
}
