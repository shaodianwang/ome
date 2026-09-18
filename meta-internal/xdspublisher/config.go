//go:build meta_internal

// Package xdspublisher contains the deployment-specific TrafficMap adapter for
// the internal xDS automation service.
package xdspublisher

import (
	"context"
	"fmt"
	"net/url"
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

	optTargetTemplate     = "targetTemplate"
	optResyncInterval     = "resyncInterval"
	optURLTemplate        = "urlTemplate"
	optRequestTimeout     = "requestTimeout"
	optTLSCertFile        = "tlsCertFile"
	optTLSKeyFile         = "tlsKeyFile"
	optTLSCAFile          = "tlsCAFile"
	optOncall             = "oncall"
	optACL                = "acl"
	clusterURLPlaceholder = "{cluster}"
)

var acceptedOptions = map[string]struct{}{
	optTargetTemplate: {},
	optResyncInterval: {},
	optURLTemplate:    {},
	optRequestTimeout: {},
	optTLSCertFile:    {},
	optTLSKeyFile:     {},
	optTLSCAFile:      {},
	optOncall:         {},
	optACL:            {},
}

type config struct {
	targetTemplate *template.Template
	resyncInterval time.Duration
	urlTemplate    string
	requestTimeout time.Duration
	tlsCertFile    string
	tlsKeyFile     string
	tlsCAFile      string
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

// ClusterWeightPublisher is the narrow service surface used by the adapter.
type ClusterWeightPublisher interface {
	PublishWeight(ctx context.Context, cluster string, weight uint32) error
	ClearWeight(ctx context.Context, cluster string) error
}

type clusterWeightPublisherFactory func(config) (ClusterWeightPublisher, error)

func init() {
	if err := routing.RegisterTrafficMapPublisher(Name, newTrafficMapPublisher); err != nil {
		panic(fmt.Sprintf("register %s TrafficMap publisher: %v", Name, err))
	}
}

func newTrafficMapPublisher(
	publisherConfig routing.PublisherConfig,
	kubeClient client.Client,
	apiReader client.Reader,
) (*routing.TrafficMapPublisher, error) {
	publisher, err := newPublisher(
		publisherConfig,
		kubeClient,
		apiReader,
		newHTTPClusterWeightPublisher,
	)
	if err != nil {
		return nil, err
	}
	return &routing.TrafficMapPublisher{
		Publisher:      publisher,
		ResyncInterval: publisher.config.resyncInterval,
	}, nil
}

func newPublisher(
	publisherConfig routing.PublisherConfig,
	kubeClient client.Client,
	apiReader client.Reader,
	factory clusterWeightPublisherFactory,
) (*Publisher, error) {
	if kubeClient == nil {
		return nil, fmt.Errorf("Kubernetes client must not be nil")
	}
	if factory == nil {
		return nil, fmt.Errorf("xDS cluster-weight publisher factory must not be nil")
	}
	cfg, err := parseConfig(publisherConfig.Options)
	if err != nil {
		return nil, err
	}
	weights, err := factory(cfg)
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
	requestTimeout, err := parsePositiveDuration(options, optRequestTimeout)
	if err != nil {
		return config{}, err
	}

	urlTemplate, err := required(optURLTemplate)
	if err != nil {
		return config{}, err
	}
	if err := validateURLTemplate(urlTemplate); err != nil {
		return config{}, fmt.Errorf("routing.publisher.options[%q]: %w", optURLTemplate, err)
	}
	tlsCertFile, err := required(optTLSCertFile)
	if err != nil {
		return config{}, err
	}
	tlsKeyFile, err := required(optTLSKeyFile)
	if err != nil {
		return config{}, err
	}
	tlsCAFile, err := required(optTLSCAFile)
	if err != nil {
		return config{}, err
	}

	return config{
		targetTemplate: targetTemplate,
		resyncInterval: resyncInterval,
		urlTemplate:    urlTemplate,
		requestTimeout: requestTimeout,
		tlsCertFile:    tlsCertFile,
		tlsKeyFile:     tlsKeyFile,
		tlsCAFile:      tlsCAFile,
		oncall:         oncall,
		acl:            acl,
	}, nil
}

func validateURLTemplate(raw string) error {
	if strings.Count(raw, clusterURLPlaceholder) != 1 {
		return fmt.Errorf("must contain exactly one %q placeholder", clusterURLPlaceholder)
	}
	const probeCluster = "ome-cluster-placeholder"
	parsed, err := url.Parse(strings.Replace(raw, clusterURLPlaceholder, probeCluster, 1))
	if err != nil {
		return fmt.Errorf("must be a valid URL: %w", err)
	}
	if parsed.Scheme != "https" || parsed.Host == "" {
		return fmt.Errorf("must be an absolute HTTPS URL")
	}
	if parsed.User != nil {
		return fmt.Errorf("must not contain user information")
	}
	if parsed.Fragment != "" {
		return fmt.Errorf("must not contain a fragment")
	}
	if !strings.Contains(parsed.Path, probeCluster) {
		return fmt.Errorf("%q must occur in the URL path", clusterURLPlaceholder)
	}
	return nil
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
