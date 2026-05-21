package omenative

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// IsPodDrained reports whether pod is no longer routable through serviceName.
// It returns true when, across every EndpointSlice for serviceName in the
// pod's namespace, either:
//   - no endpoint targets pod, or
//   - the endpoint that targets pod has Conditions.Ready != true.
//
// The returned bool is the EndpointSlice-convergence signal used during
// drain: once true, kube-proxy will have removed (or marked not-ready)
// the pod's address and new connections to serviceName stop landing on
// this pod. Existing connections and runtime-level shutdown are out of
// scope — the caller layers gracePeriodSeconds on top of this signal.
//
// reader is taken as client.Reader (not client.Client) so callers may
// pass a live API reader to bypass the cached controller-runtime read
// when cache lag would make a drain falsely declare done. The cached
// client works as well and is what tests use.
func IsPodDrained(ctx context.Context, reader client.Reader, namespace, serviceName string, pod *corev1.Pod) (bool, error) {
	if pod == nil {
		return false, fmt.Errorf("IsPodDrained: nil pod")
	}
	if serviceName == "" {
		return false, fmt.Errorf("IsPodDrained: empty serviceName")
	}

	slices, err := endpointSlicesForService(ctx, reader, namespace, serviceName)
	if err != nil {
		return false, err
	}
	// No slice at all means kube-proxy has nothing to route to — drained
	// by definition. Also covers the case where the Service hasn't been
	// created yet, which is benign for callers checking drain completion.
	if len(slices) == 0 {
		return true, nil
	}

	for _, slice := range slices {
		for _, ep := range slice.Endpoints {
			if !endpointTargetsPod(ep, pod) {
				continue
			}
			if endpointIsReady(ep) {
				return false, nil
			}
		}
	}
	return true, nil
}

// IsPodInRotation is the inverse of IsPodDrained: it reports whether
// pod's endpoint exists in serviceName's EndpointSlices with
// Conditions.Ready != false. Used by Migrate to confirm the surge is
// receiving traffic before initiating the source-drain, so there is
// no zero-routable-endpoint window during the swap.
//
// Returns false when no EndpointSlice for serviceName lists the pod
// at all — kube-proxy can't possibly be routing to it yet.
func IsPodInRotation(ctx context.Context, reader client.Reader, namespace, serviceName string, pod *corev1.Pod) (bool, error) {
	if pod == nil {
		return false, fmt.Errorf("IsPodInRotation: nil pod")
	}
	if serviceName == "" {
		return false, fmt.Errorf("IsPodInRotation: empty serviceName")
	}
	slices, err := endpointSlicesForService(ctx, reader, namespace, serviceName)
	if err != nil {
		return false, err
	}
	for _, slice := range slices {
		for _, ep := range slice.Endpoints {
			if !endpointTargetsPod(ep, pod) {
				continue
			}
			if endpointIsReady(ep) {
				return true, nil
			}
		}
	}
	return false, nil
}

// endpointSlicesForService lists EndpointSlices in namespace owned by
// serviceName via the standard kubernetes.io/service-name label that the
// in-tree endpointslice controller stamps on every slice it manages.
func endpointSlicesForService(ctx context.Context, reader client.Reader, namespace, serviceName string) ([]discoveryv1.EndpointSlice, error) {
	list := &discoveryv1.EndpointSliceList{}
	if err := reader.List(ctx, list,
		client.InNamespace(namespace),
		client.MatchingLabels{discoveryv1.LabelServiceName: serviceName},
	); err != nil {
		return nil, fmt.Errorf("list EndpointSlices for service %s/%s: %w", namespace, serviceName, err)
	}
	return list.Items, nil
}

// endpointTargetsPod matches an EndpointSlice endpoint to pod by TargetRef.
// TargetRef is preferred over Addresses[] because IP addresses can be
// reused across pods, while TargetRef carries the stable Pod identity.
func endpointTargetsPod(ep discoveryv1.Endpoint, pod *corev1.Pod) bool {
	ref := ep.TargetRef
	if ref == nil {
		return false
	}
	if ref.Kind != "" && ref.Kind != "Pod" {
		return false
	}
	if ref.Namespace != "" && ref.Namespace != pod.Namespace {
		return false
	}
	return ref.Name == pod.Name
}

// endpointIsReady reports whether the endpoint is eligible for new Service
// traffic. A nil Ready pointer is treated as ready per the discovery/v1
// contract: producers SHOULD set Conditions.Ready, but if a slice omits
// it the endpoint is presumed routable, matching kube-proxy's behavior.
func endpointIsReady(ep discoveryv1.Endpoint) bool {
	if ep.Conditions.Ready == nil {
		return true
	}
	return *ep.Conditions.Ready
}
