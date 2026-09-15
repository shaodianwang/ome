//go:build meta_internal

package xdspublisher

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"slices"
	"sort"
	"strings"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	placementendpoint "github.com/ome-projects/ome/pkg/controller/v1beta1/placement/endpoint"
)

const targetsAnnotation = "routing.ome.io/xds-published-targets"

type weightedTarget struct {
	name   string
	weight uint32
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

// Publish implements endpoint.EndpointPublisher. The shared endpoint
// reconciler owns the watch and finalizer; this adapter owns only the external
// SMC state that cannot be discovered through Kubernetes.
func (p *Publisher) Publish(
	ctx context.Context,
	isvc *v1beta1.InferenceService,
	target placementendpoint.Target,
) error {
	desired, err := p.desiredTargets(isvc, target)
	if err != nil {
		return err
	}
	key := client.ObjectKeyFromObject(isvc)
	journal, err := p.readTargets(ctx, key)
	if err != nil {
		return err
	}
	current := targetNames(desired)

	owners := map[string]types.NamespacedName{}
	if len(current) != 0 || len(journal) != 0 {
		owners, err = p.otherTargetOwners(ctx, key)
		if err != nil {
			return err
		}
	}
	if collisionErr := targetCollision(current, owners); collisionErr != nil {
		zeroed, err := p.zeroUnclaimedTargets(difference(journal, current), owners)
		if err != nil {
			return fmt.Errorf("zero stale xDS targets while rejecting collision: %w", err)
		}
		retained := union(uncontestedIntersection(journal, current, owners), zeroed)
		if err := p.rememberTargets(ctx, key, retained); err != nil {
			return fmt.Errorf("compact xDS target journal while rejecting collision: %w", err)
		}
		return collisionErr
	}

	// Removing an active target means weight zero. Clearing the property would
	// restore the statically authored route weight and could resume traffic.
	zeroed, err := p.zeroUnclaimedTargets(difference(journal, current), owners)
	if err != nil {
		return fmt.Errorf("zero stale xDS targets: %w", err)
	}

	// Record ownership before each external mutation so a partial publish or
	// process restart still has enough state to clean up.
	for _, desiredTarget := range desired {
		journal = union(journal, []string{desiredTarget.name})
		if err := p.rememberTargets(ctx, key, journal); err != nil {
			return fmt.Errorf("journal xDS target %q: %w", desiredTarget.name, err)
		}
		if err := p.weights.PublishWeight(desiredTarget.name, desiredTarget.weight); err != nil {
			return fmt.Errorf("publish weight %d for xDS target %q: %w",
				desiredTarget.weight, desiredTarget.name, err)
		}
	}
	if err := p.rememberTargets(ctx, key, union(current, zeroed)); err != nil {
		return fmt.Errorf("compact xDS target journal: %w", err)
	}
	return nil
}

// Unpublish implements endpoint.EndpointPublisher. It restores static routing
// by clearing every override recorded for this InferenceService.
func (p *Publisher) Unpublish(ctx context.Context, isvc *v1beta1.InferenceService) error {
	key := client.ObjectKeyFromObject(isvc)
	journal, err := p.readTargets(ctx, key)
	if err != nil {
		return err
	}
	if len(journal) == 0 {
		return p.rememberTargets(ctx, key, nil)
	}
	owners, err := p.otherTargetOwners(ctx, key)
	if err != nil {
		return err
	}
	for _, target := range journal {
		if _, shared := owners[target]; shared {
			continue
		}
		if err := p.weights.ClearWeight(target); err != nil {
			return fmt.Errorf("clear weight for xDS target %q: %w", target, err)
		}
	}
	return p.rememberTargets(ctx, key, nil)
}

func (p *Publisher) desiredTargets(
	isvc *v1beta1.InferenceService,
	target placementendpoint.Target,
) ([]weightedTarget, error) {
	service := target.Service
	if service == "" {
		service = isvc.Name
	}
	targets := make([]weightedTarget, 0, len(target.Homes))
	seen := make(map[string]struct{}, len(target.Homes))
	for _, home := range target.Homes {
		if home.Endpoint == "" {
			return nil, fmt.Errorf("TrafficMap entry %q has no endpoint", home.Cluster)
		}
		endpointURL, err := url.Parse(home.Endpoint)
		if err != nil || endpointURL.Host == "" {
			return nil, fmt.Errorf("TrafficMap entry %q has invalid endpoint %q", home.Cluster, home.Endpoint)
		}
		data := targetTemplateData{
			Namespace:        isvc.Namespace,
			Service:          service,
			Cluster:          home.Cluster,
			Endpoint:         home.Endpoint,
			EndpointHost:     endpointURL.Host,
			EndpointHostname: endpointURL.Hostname(),
			EndpointPort:     endpointURL.Port(),
		}
		var rendered bytes.Buffer
		if err := p.config.targetTemplate.Execute(&rendered, data); err != nil {
			return nil, fmt.Errorf("render xDS target for cluster %q: %w", home.Cluster, err)
		}
		name, err := canonicalTarget(strings.TrimSpace(rendered.String()))
		if err != nil {
			return nil, fmt.Errorf("render xDS target for cluster %q: %w", home.Cluster, err)
		}
		if _, duplicate := seen[name]; duplicate {
			return nil, fmt.Errorf("xDS target template maps more than one entry to %q", name)
		}
		if home.Weight < 0 {
			return nil, fmt.Errorf("TrafficMap entry %q has negative weight %d", home.Cluster, home.Weight)
		}
		seen[name] = struct{}{}
		targets = append(targets, weightedTarget{name: name, weight: uint32(home.Weight)})
	}
	sort.Slice(targets, func(i, j int) bool {
		if (targets[i].weight == 0) != (targets[j].weight == 0) {
			return targets[i].weight == 0
		}
		return targets[i].name < targets[j].name
	})
	return targets, nil
}

func (p *Publisher) zeroUnclaimedTargets(
	targets []string,
	owners map[string]types.NamespacedName,
) ([]string, error) {
	retained := make([]string, 0, len(targets))
	for _, target := range targets {
		if _, shared := owners[target]; shared {
			continue
		}
		if err := p.weights.PublishWeight(target, 0); err != nil {
			return nil, fmt.Errorf("publish zero weight for stale xDS target %q: %w", target, err)
		}
		retained = append(retained, target)
	}
	return retained, nil
}

func (p *Publisher) readTargets(ctx context.Context, key types.NamespacedName) ([]string, error) {
	latest := &v1beta1.InferenceService{}
	if err := p.reader().Get(ctx, key, latest); err != nil {
		return nil, err
	}
	return publishedTargets(latest)
}

func (p *Publisher) rememberTargets(ctx context.Context, key types.NamespacedName, targets []string) error {
	targets = union(nil, targets)
	var encoded []byte
	var err error
	if len(targets) != 0 {
		encoded, err = json.Marshal(targets)
		if err != nil {
			return fmt.Errorf("encode xDS published targets: %w", err)
		}
	}
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &v1beta1.InferenceService{}
		if err := p.reader().Get(ctx, key, latest); err != nil {
			return client.IgnoreNotFound(err)
		}
		if len(encoded) == 0 {
			if _, ok := latest.Annotations[targetsAnnotation]; !ok {
				return nil
			}
			delete(latest.Annotations, targetsAnnotation)
			return p.Update(ctx, latest)
		}
		if latest.Annotations[targetsAnnotation] == string(encoded) {
			return nil
		}
		if latest.Annotations == nil {
			latest.Annotations = map[string]string{}
		}
		latest.Annotations[targetsAnnotation] = string(encoded)
		return p.Update(ctx, latest)
	})
}

func (p *Publisher) otherTargetOwners(
	ctx context.Context,
	self types.NamespacedName,
) (map[string]types.NamespacedName, error) {
	services := &v1beta1.InferenceServiceList{}
	if err := p.reader().List(ctx, services); err != nil {
		return nil, fmt.Errorf("list InferenceServices for xDS target ownership: %w", err)
	}
	owners := make(map[string]types.NamespacedName)
	for i := range services.Items {
		other := &services.Items[i]
		key := client.ObjectKeyFromObject(other)
		if key == self {
			continue
		}
		targets, err := publishedTargets(other)
		if err != nil {
			return nil, fmt.Errorf("read xDS target claims from InferenceService %s: %w", key.String(), err)
		}
		for _, target := range targets {
			if _, exists := owners[target]; !exists {
				owners[target] = key
			}
		}
	}
	return owners, nil
}

func (p *Publisher) reader() client.Reader {
	if p.APIReader != nil {
		return p.APIReader
	}
	return p.Client
}

func publishedTargets(isvc *v1beta1.InferenceService) ([]string, error) {
	raw := isvc.Annotations[targetsAnnotation]
	if raw == "" {
		return nil, nil
	}
	var targets []string
	if err := json.Unmarshal([]byte(raw), &targets); err != nil {
		return nil, fmt.Errorf("decode %s annotation: %w", targetsAnnotation, err)
	}
	for i, target := range targets {
		canonical, err := canonicalTarget(target)
		if err != nil {
			return nil, fmt.Errorf("invalid target in %s annotation: %w", targetsAnnotation, err)
		}
		targets[i] = canonical
	}
	sort.Strings(targets)
	return slices.Compact(targets), nil
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

func targetNames(targets []weightedTarget) []string {
	names := make([]string, len(targets))
	for i := range targets {
		names[i] = targets[i].name
	}
	return names
}

func targetCollision(targets []string, owners map[string]types.NamespacedName) error {
	for _, target := range targets {
		if owner, collision := owners[target]; collision {
			return fmt.Errorf("xDS target %q is already claimed by InferenceService %s", target, owner.String())
		}
	}
	return nil
}

func uncontestedIntersection(
	journal, current []string,
	owners map[string]types.NamespacedName,
) []string {
	currentSet := make(map[string]struct{}, len(current))
	for _, target := range current {
		currentSet[target] = struct{}{}
	}
	var retained []string
	for _, target := range journal {
		if _, desired := currentSet[target]; !desired {
			continue
		}
		if _, contested := owners[target]; contested {
			continue
		}
		retained = append(retained, target)
	}
	return union(nil, retained)
}

func difference(previous, current []string) []string {
	currentSet := make(map[string]struct{}, len(current))
	for _, target := range current {
		currentSet[target] = struct{}{}
	}
	var out []string
	for _, target := range previous {
		if _, keep := currentSet[target]; !keep {
			out = append(out, target)
		}
	}
	return union(nil, out)
}

func union(a, b []string) []string {
	out := append(append(make([]string, 0, len(a)+len(b)), a...), b...)
	sort.Strings(out)
	return slices.Compact(out)
}
