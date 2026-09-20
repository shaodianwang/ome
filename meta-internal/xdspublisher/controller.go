//go:build meta_internal

package xdspublisher

import (
	"bytes"
	"context"
	"fmt"
	"net/url"
	"slices"
	"sort"
	"strings"
	"text/template"

	"k8s.io/apimachinery/pkg/types"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	placementendpoint "github.com/ome-projects/ome/pkg/controller/v1beta1/placement/endpoint"
)

// legacyTargetsAnnotation belongs to the retired InferenceService-owned
// publisher lifecycle. Its presence requires that lifecycle to finish cleanup.
const legacyTargetsAnnotation = "routing.ome.io/xds-published-targets"

type weightedTarget struct {
	name   string
	weight uint32
}

type publishPlan struct {
	targets []weightedTarget
	claims  []string
}

func (p *publishPlan) Claims() []string {
	if p == nil {
		return nil
	}
	return slices.Clone(p.claims)
}

type targetTemplateData struct {
	Namespace        string
	Service          string
	Cluster          string
	Endpoint         string
	EndpointHost     string
	EndpointHostname string
	EndpointPort     string
}

// Stateful implements endpoint.TrafficMapPublisher.
func (*Publisher) Stateful() bool { return true }

// ResolveOptions implements endpoint.TrafficMapPublisher. Transport and
// credentials are installation-wide and never enter an InferenceService or its
// status digest. Target selection is also installation-wide because changing it
// per object could redirect writes outside the operator-owned target scope.
func (*Publisher) ResolveOptions(global, inline map[string]string) (map[string]string, error) {
	if len(inline) != 0 {
		keys := make([]string, 0, len(inline))
		for key := range inline {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		return nil, fmt.Errorf("xDS publisher does not accept per-InferenceService options: %s",
			strings.Join(keys, ", "))
	}
	for key := range global {
		if key != optTargetTemplate {
			return nil, fmt.Errorf("xDS publisher received unsupported global option %q", key)
		}
	}
	templateText := strings.TrimSpace(global[optTargetTemplate])
	if templateText == "" {
		return nil, fmt.Errorf("xDS publisher global option %q is required", optTargetTemplate)
	}
	if _, err := parseTargetTemplate(templateText); err != nil {
		return nil, err
	}
	return map[string]string{optTargetTemplate: templateText}, nil
}

// Plan implements endpoint.TrafficMapPublisher. It rejects objects that still
// carry state owned by the retired InferenceService lifecycle so two lifecycle
// owners can never write or clear the same external target.
func (*Publisher) Plan(
	owner *v1beta1.InferenceService,
	trafficMap *v1beta1.TrafficMap,
	effectiveOptions map[string]string,
) (placementendpoint.TrafficMapPublishPlan, error) {
	if owner == nil {
		return nil, fmt.Errorf("InferenceService owner must not be nil")
	}
	if trafficMap == nil {
		return nil, fmt.Errorf("TrafficMap must not be nil")
	}
	if slices.Contains(owner.Finalizers, placementendpoint.EndpointFinalizer) {
		return nil, fmt.Errorf(
			"InferenceService %s/%s still has legacy endpoint finalizer %q; drain it with the previous publisher lifecycle before upgrading",
			owner.Namespace, owner.Name, placementendpoint.EndpointFinalizer,
		)
	}
	if _, found := owner.Annotations[legacyTargetsAnnotation]; found {
		return nil, fmt.Errorf(
			"InferenceService %s/%s still has legacy xDS target annotation %q; drain it with the previous publisher lifecycle before upgrading",
			owner.Namespace, owner.Name, legacyTargetsAnnotation,
		)
	}

	targetTemplate, err := effectiveTargetTemplate(effectiveOptions)
	if err != nil {
		return nil, err
	}
	targets, err := desiredTargets(owner, trafficMap, targetTemplate)
	if err != nil {
		return nil, err
	}
	claims := make([]string, len(targets))
	for i := range targets {
		claims[i] = targets[i].name
	}
	sort.Strings(claims)
	return &publishPlan{targets: targets, claims: claims}, nil
}

// Drain implements endpoint.TrafficMapPublisher. A stale override remains
// explicitly held at zero until the TrafficMap journal is finalized.
func (p *Publisher) Drain(ctx context.Context, _ types.NamespacedName, claims []string) error {
	validated, err := validateClaims(claims)
	if err != nil {
		return fmt.Errorf("validate stale xDS target claims: %w", err)
	}
	for _, claim := range validated {
		if err := p.weights.PublishWeight(ctx, claim, 0); err != nil {
			return fmt.Errorf("publish zero weight for stale xDS target %q: %w", claim, err)
		}
	}
	return nil
}

// Apply implements endpoint.TrafficMapPublisher.
func (p *Publisher) Apply(
	ctx context.Context,
	plan placementendpoint.TrafficMapPublishPlan,
) (placementendpoint.TrafficMapPublishResult, error) {
	xdsPlan, ok := plan.(*publishPlan)
	if !ok || xdsPlan == nil {
		return placementendpoint.TrafficMapPublishResult{}, fmt.Errorf("xDS publisher received plan of type %T", plan)
	}
	for _, target := range xdsPlan.targets {
		if err := p.weights.PublishWeight(ctx, target.name, target.weight); err != nil {
			return placementendpoint.TrafficMapPublishResult{}, fmt.Errorf(
				"publish weight %d for xDS target %q: %w", target.weight, target.name, err,
			)
		}
	}
	return placementendpoint.TrafficMapPublishResult{}, nil
}

// Unpublish implements endpoint.TrafficMapPublisher. The durable TrafficMap
// journal is the complete cleanup authority; owner and TrafficMap reads are not
// needed for deletion recovery.
func (p *Publisher) Unpublish(
	ctx context.Context,
	_ types.NamespacedName,
	journal v1beta1.TrafficMapPublisherStatus,
) error {
	if journal.PublisherName != "" && journal.PublisherName != Name {
		return fmt.Errorf("xDS publisher cannot clean journal owned by %q", journal.PublisherName)
	}
	targets, err := validateClaims(journal.ClaimedTargets)
	if err != nil {
		return fmt.Errorf("validate xDS cleanup journal: %w", err)
	}
	for _, target := range targets {
		if err := p.weights.ClearWeight(ctx, target); err != nil {
			return fmt.Errorf("clear weight for xDS target %q: %w", target, err)
		}
	}
	return nil
}

func effectiveTargetTemplate(options map[string]string) (*template.Template, error) {
	for key := range options {
		if key != optTargetTemplate {
			return nil, fmt.Errorf("xDS publisher effective options contain unsupported key %q", key)
		}
	}
	templateText := strings.TrimSpace(options[optTargetTemplate])
	if templateText == "" {
		return nil, fmt.Errorf("xDS publisher effective option %q is required", optTargetTemplate)
	}
	return parseTargetTemplate(templateText)
}

func desiredTargets(
	owner *v1beta1.InferenceService,
	trafficMap *v1beta1.TrafficMap,
	targetTemplate *template.Template,
) ([]weightedTarget, error) {
	if trafficMap.Spec.Service == "" {
		return nil, fmt.Errorf("TrafficMap service must not be empty")
	}
	targets := make([]weightedTarget, 0, len(trafficMap.Spec.Entries))
	seen := make(map[string]struct{}, len(trafficMap.Spec.Entries))
	for _, entry := range trafficMap.Spec.Entries {
		if entry.Endpoint == nil {
			return nil, fmt.Errorf("TrafficMap entry %q has no valid endpoint", entry.Cluster)
		}
		endpointURL, err := url.Parse(entry.Endpoint.String())
		if err != nil || endpointURL.Host == "" {
			return nil, fmt.Errorf("TrafficMap entry %q has invalid endpoint %q", entry.Cluster, entry.Endpoint.String())
		}
		if entry.Weight < 0 {
			return nil, fmt.Errorf("TrafficMap entry %q has negative weight %d", entry.Cluster, entry.Weight)
		}
		data := targetTemplateData{
			Namespace:        owner.Namespace,
			Service:          trafficMap.Spec.Service,
			Cluster:          entry.Cluster,
			Endpoint:         entry.Endpoint.String(),
			EndpointHost:     endpointURL.Host,
			EndpointHostname: endpointURL.Hostname(),
			EndpointPort:     endpointURL.Port(),
		}
		var rendered bytes.Buffer
		if err := targetTemplate.Execute(&rendered, data); err != nil {
			return nil, fmt.Errorf("render xDS target for cluster %q: %w", entry.Cluster, err)
		}
		name, err := canonicalTarget(strings.TrimSpace(rendered.String()))
		if err != nil {
			return nil, fmt.Errorf("render xDS target for cluster %q: %w", entry.Cluster, err)
		}
		if _, duplicate := seen[name]; duplicate {
			return nil, fmt.Errorf("xDS target template maps more than one entry to %q", name)
		}
		seen[name] = struct{}{}
		targets = append(targets, weightedTarget{name: name, weight: uint32(entry.Weight)})
	}
	sort.Slice(targets, func(i, j int) bool {
		if (targets[i].weight == 0) != (targets[j].weight == 0) {
			return targets[i].weight == 0
		}
		return targets[i].name < targets[j].name
	})
	return targets, nil
}

func canonicalTarget(target string) (string, error) {
	if target == "" {
		return "", fmt.Errorf("target is empty")
	}
	if strings.Contains(target, "/") {
		return "", fmt.Errorf("target %q must be a bare cluster name without a scope prefix", target)
	}
	primary, _, _ := strings.Cut(target, ";fallback=")
	if primary == "" {
		return "", fmt.Errorf("target must not be empty before the fallback suffix")
	}
	return primary, nil
}

func validateClaims(claims []string) ([]string, error) {
	if len(claims) > v1beta1.MaxTrafficMapPublisherTargets {
		return nil, fmt.Errorf("journal has %d targets, maximum is %d",
			len(claims), v1beta1.MaxTrafficMapPublisherTargets)
	}
	validated := slices.Clone(claims)
	for _, claim := range validated {
		if len(claim) > v1beta1.MaxTrafficMapPublisherTargetLength {
			return nil, fmt.Errorf("target %q has %d bytes, maximum is %d",
				claim, len(claim), v1beta1.MaxTrafficMapPublisherTargetLength)
		}
		canonical, err := canonicalTarget(strings.TrimSpace(claim))
		if err != nil {
			return nil, err
		}
		if canonical != claim {
			return nil, fmt.Errorf("target %q is not canonical", claim)
		}
	}
	sort.Strings(validated)
	for i := 1; i < len(validated); i++ {
		if validated[i] == validated[i-1] {
			return nil, fmt.Errorf("target %q appears more than once", validated[i])
		}
	}
	return validated, nil
}
