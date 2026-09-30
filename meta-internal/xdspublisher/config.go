//go:build meta_internal

// Package xdspublisher contains the deployment-specific TrafficMap adapter for
// the internal xDS automation service.
package xdspublisher

import (
	"context"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"text/template"
	"time"

	"k8s.io/apimachinery/pkg/labels"
	"sigs.k8s.io/controller-runtime/pkg/client"

	placementendpoint "github.com/ome-projects/ome/pkg/controller/v1beta1/placement/endpoint"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/placement/routing"
)

const (
	// Name is the value selected at routing.publisher.name.
	Name = "xds"

	optTargetTemplate       = "targetTemplate"
	optURLTemplate          = "urlTemplate"
	optRequestTimeout       = "requestTimeout"
	optTLSCertFile          = "tlsCertFile"
	optTLSKeyFile           = "tlsKeyFile"
	optTLSCAFile            = "tlsCAFile"
	optOncall               = "oncall"
	optACL                  = "acl"
	optSourceSelector       = "sourceSelector"
	optWeightMode           = "weightMode"
	optNormalizedTotal      = "normalizedTotal"
	optNoPositivePlanPolicy = "noPositivePlanPolicy"
	clusterURLPlaceholder   = "{cluster}"

	weightModeAbsolute                            = "absolute"
	weightModeNormalized                          = "normalized"
	weightModeRelative                            = "relative"
	defaultWeightMode                             = weightModeAbsolute
	noPositivePlanPolicyApplyCurrent              = string(placementendpoint.TrafficMapNoPositivePlanPolicyApplyCurrent)
	noPositivePlanPolicyRetainLastPositive        = string(placementendpoint.TrafficMapNoPositivePlanPolicyRetainLastPositive)
	maxXDSWeight                           uint64 = 1<<32 - 1
	maxNormalizedTotal                            = maxXDSWeight
)

var acceptedOptions = map[string]struct{}{
	optTargetTemplate:       {},
	optURLTemplate:          {},
	optRequestTimeout:       {},
	optTLSCertFile:          {},
	optTLSKeyFile:           {},
	optTLSCAFile:            {},
	optOncall:               {},
	optACL:                  {},
	optSourceSelector:       {},
	optWeightMode:           {},
	optNormalizedTotal:      {},
	optNoPositivePlanPolicy: {},
}

type config struct {
	targetTemplate       string
	urlTemplate          string
	requestTimeout       time.Duration
	tlsCertFile          string
	tlsKeyFile           string
	tlsCAFile            string
	oncall               string
	acl                  *string
	sourceSelector       string
	weightMode           string
	normalizedTotal      string
	noPositivePlanPolicy string
}

// Publisher writes TrafficMap weights through the internal xDS API.
type Publisher struct {
	config  config
	weights ClusterWeightPublisher
}

var _ placementendpoint.TrafficMapPublisher = (*Publisher)(nil)

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
	_ client.Client,
	_ client.Reader,
) (*routing.TrafficMapPublisher, error) {
	return newTrafficMapPublisherWithFactory(publisherConfig, newHTTPClusterWeightPublisher)
}

func newTrafficMapPublisherWithFactory(
	publisherConfig routing.PublisherConfig,
	factory clusterWeightPublisherFactory,
) (*routing.TrafficMapPublisher, error) {
	publisher, err := newPublisher(publisherConfig, factory)
	if err != nil {
		return nil, err
	}
	globalOptions := map[string]string{
		optTargetTemplate:       publisher.config.targetTemplate,
		optWeightMode:           publisher.config.weightMode,
		optNoPositivePlanPolicy: publisher.config.noPositivePlanPolicy,
	}
	if publisher.config.normalizedTotal != "" {
		globalOptions[optNormalizedTotal] = publisher.config.normalizedTotal
	}
	if publisher.config.sourceSelector != "" {
		globalOptions[optSourceSelector] = publisher.config.sourceSelector
	}
	return &routing.TrafficMapPublisher{Publisher: publisher, GlobalOptions: globalOptions}, nil
}

func newPublisher(
	publisherConfig routing.PublisherConfig,
	factory clusterWeightPublisherFactory,
) (*Publisher, error) {
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
		config:  cfg,
		weights: weights,
	}, nil
}

// Name implements endpoint.TrafficMapPublisher.
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
	if _, err := parseTargetTemplate(templateText); err != nil {
		return config{}, err
	}
	_, sourceSelector, err := parseSourceSelector(options[optSourceSelector])
	if err != nil {
		return config{}, err
	}
	weightMode, err := parseWeightMode(options[optWeightMode])
	if err != nil {
		return config{}, err
	}
	_, normalizedTotal, err := parseNormalizedTotal(options, weightMode)
	if err != nil {
		return config{}, err
	}
	noPositivePlanPolicy, err := parseNoPositivePlanPolicy(options[optNoPositivePlanPolicy])
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
		targetTemplate:       templateText,
		urlTemplate:          urlTemplate,
		requestTimeout:       requestTimeout,
		tlsCertFile:          tlsCertFile,
		tlsKeyFile:           tlsKeyFile,
		tlsCAFile:            tlsCAFile,
		oncall:               oncall,
		acl:                  acl,
		sourceSelector:       sourceSelector,
		weightMode:           weightMode,
		normalizedTotal:      normalizedTotal,
		noPositivePlanPolicy: noPositivePlanPolicy,
	}, nil
}

func parseTargetTemplate(templateText string) (*template.Template, error) {
	targetTemplate, err := template.New("xds-target").
		Funcs(targetTemplateFuncs).
		Option("missingkey=error").
		Parse(templateText)
	if err != nil {
		return nil, fmt.Errorf("routing.publisher.options[%q]: %w", optTargetTemplate, err)
	}
	return targetTemplate, nil
}

func parseSourceSelector(raw string) (labels.Selector, string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return labels.Everything(), "", nil
	}
	selector, err := labels.Parse(raw)
	if err != nil {
		return nil, "", fmt.Errorf("routing.publisher.options[%q] must be a valid Kubernetes label selector: %w",
			optSourceSelector, err)
	}
	return selector, selector.String(), nil
}

func parseWeightMode(raw string) (string, error) {
	mode := strings.TrimSpace(raw)
	if mode == "" {
		return defaultWeightMode, nil
	}
	switch mode {
	case weightModeAbsolute, weightModeNormalized, weightModeRelative:
		return mode, nil
	default:
		return "", fmt.Errorf(
			"routing.publisher.options[%q] must be %q, %q, or %q",
			optWeightMode, weightModeAbsolute, weightModeNormalized, weightModeRelative,
		)
	}
}

func parseNormalizedTotal(options map[string]string, weightMode string) (uint32, string, error) {
	raw, found := options[optNormalizedTotal]
	switch weightMode {
	case weightModeAbsolute, weightModeRelative:
		if found {
			return 0, "", fmt.Errorf(
				"routing.publisher.options[%q] is not accepted when %q is %q",
				optNormalizedTotal, optWeightMode, weightMode,
			)
		}
		return 0, "", nil
	case weightModeNormalized:
	default:
		return 0, "", fmt.Errorf("cannot parse %q for unsupported weight mode %q", optNormalizedTotal, weightMode)
	}
	raw = strings.TrimSpace(raw)
	if !found || raw == "" {
		return 0, "", fmt.Errorf(
			"routing.publisher.options[%q] is required when %q is %q",
			optNormalizedTotal, optWeightMode, weightModeNormalized,
		)
	}
	total, err := strconv.ParseUint(raw, 10, 32)
	if err != nil || total == 0 {
		return 0, "", fmt.Errorf("routing.publisher.options[%q] must be a positive integer no greater than %d",
			optNormalizedTotal, maxNormalizedTotal)
	}
	return uint32(total), strconv.FormatUint(total, 10), nil
}

func parseNoPositivePlanPolicy(raw string) (string, error) {
	policy := strings.TrimSpace(raw)
	if policy == "" {
		return "", fmt.Errorf("routing.publisher.options[%q] is required by %q", optNoPositivePlanPolicy, Name)
	}
	switch policy {
	case noPositivePlanPolicyApplyCurrent, noPositivePlanPolicyRetainLastPositive:
		return policy, nil
	default:
		return "", fmt.Errorf(
			"routing.publisher.options[%q] must be %q or %q",
			optNoPositivePlanPolicy,
			noPositivePlanPolicyApplyCurrent,
			noPositivePlanPolicyRetainLastPositive,
		)
	}
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
