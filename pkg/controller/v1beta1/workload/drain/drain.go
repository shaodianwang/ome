// Package drain owns the EndpointSlice-convergence signals OMENative
// uses to gate destructive pod operations: IsPodDrained (safe to
// delete?) and IsPodInRotation (surge eligible for new traffic?).
//
// Boundary: pure leaf. Only Kubernetes API machinery. Knows nothing
// about ReconcileParams or any OMENative-specific shape — every entry
// point takes a plain client.Reader + (namespace, serviceName, pod).
package drain

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
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

	slices, err := EndpointSlicesForService(ctx, reader, namespace, serviceName)
	if err != nil {
		return false, err
	}
	if len(slices) == 0 {
		// Distinguish "no Service at all" (drain trivially complete —
		// nothing routes to anything) from "Service exists but slices
		// haven't propagated yet" (cache cold-start, kube-controller-
		// manager lag, no matching pods yet). The latter looks the
		// same as the former without this disambiguation, and we'd
		// previously fail-open and report drain done while a serving
		// pod was about to get an image patch or delete.
		return drainedWhenSliceListEmpty(ctx, reader, namespace, serviceName)
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

// drainedWhenSliceListEmpty resolves the ambiguous "no slices"
// observation by checking whether the Service itself exists. Service
// absent → drained (no traffic path). Service present → slices
// haven't been materialized yet → conservative: NOT drained, caller
// requeues.
func drainedWhenSliceListEmpty(ctx context.Context, reader client.Reader, namespace, serviceName string) (bool, error) {
	svc := &corev1.Service{}
	err := reader.Get(ctx, client.ObjectKey{Namespace: namespace, Name: serviceName}, svc)
	if apierrors.IsNotFound(err) {
		// No Service → no kube-proxy routing → drain is trivially
		// complete. With the services reconciler in place this branch
		// fires only when the Service hasn't been created yet on this
		// reconcile pass, which is a controller bug — the service
		// reconciliation runs before any op that needs drain.
		return true, nil
	}
	if err != nil {
		return false, fmt.Errorf("drainedWhenSliceListEmpty: get service %s/%s: %w", namespace, serviceName, err)
	}
	// Service exists, slice list empty. Could mean (a) no pods match
	// the selector yet, (b) kube-controller-manager hasn't created
	// slices yet, (c) informer cache cold-start. Any of those is a
	// transient state where we should not declare drain complete.
	return false, nil
}

// IsPodInRotation reports whether pod has at least one endpoint Ready
// AND non-terminating — eligible for NEW traffic. Migrate uses this to
// confirm the surge is receiving traffic before draining the source so
// the swap has no zero-routable-endpoint window. Terminating endpoints
// don't count: kube-proxy keeps them for in-flight requests but won't
// send new traffic, and swapping onto a terminating surge would be
// pointless.
func IsPodInRotation(ctx context.Context, reader client.Reader, namespace, serviceName string, pod *corev1.Pod) (bool, error) {
	if pod == nil {
		return false, fmt.Errorf("IsPodInRotation: nil pod")
	}
	if serviceName == "" {
		return false, fmt.Errorf("IsPodInRotation: empty serviceName")
	}
	slices, err := EndpointSlicesForService(ctx, reader, namespace, serviceName)
	if err != nil {
		return false, err
	}
	for _, slice := range slices {
		for _, ep := range slice.Endpoints {
			if !endpointTargetsPod(ep, pod) {
				continue
			}
			if EndpointAvailable(ep) {
				return true, nil
			}
		}
	}
	return false, nil
}

// EndpointSlicesForService lists EndpointSlices in namespace owned by
// serviceName via the standard kubernetes.io/service-name label that the
// in-tree endpointslice controller stamps on every slice it manages.
//
// Exported so status_aggregate's availablePodSet helper (which folds
// rotation across every pod into a single map) can reuse the same
// label query and slice walk.
func EndpointSlicesForService(ctx context.Context, reader client.Reader, namespace, serviceName string) ([]discoveryv1.EndpointSlice, error) {
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

// endpointIsReady reports whether the endpoint is currently receiving
// new Service traffic. Drives the drain wait: a pod is considered
// drained once every slice entry targeting it reports Ready=false.
// Terminating endpoints are deliberately considered "still receiving"
// when Ready=true so the controller doesn't declare drain complete
// while kube-proxy is still routing in-flight connections.
//
// A nil Ready pointer is treated as ready per the discovery/v1
// contract: producers SHOULD set Conditions.Ready, but if a slice
// omits it the endpoint is presumed routable, matching kube-proxy's
// behavior.
func endpointIsReady(ep discoveryv1.Endpoint) bool {
	if ep.Conditions.Ready == nil {
		return true
	}
	return *ep.Conditions.Ready
}

// EndpointAvailable reports whether the endpoint is eligible for NEW
// traffic — Ready=true AND Terminating!=true. Drives the status
// `AvailablePodCount` counter and `IsPodInRotation`'s surge check.
//
// Distinct from endpointIsReady because of the EndpointSlice
// tri-state contract: a terminating pod whose probes still pass
// reports Ready=true + Serving=true + Terminating=true. kube-proxy
// keeps it in rotation for in-flight requests but won't send NEW
// requests. Counting it as Available would inflate AvailableReplicas;
// treating it as in-rotation would let Migrate swap onto a pod that's
// about to disappear.
//
// A nil Terminating pointer is treated as not-terminating (the
// common steady-state shape).
func EndpointAvailable(ep discoveryv1.Endpoint) bool {
	if !endpointIsReady(ep) {
		return false
	}
	if ep.Conditions.Terminating != nil && *ep.Conditions.Terminating {
		return false
	}
	return true
}
