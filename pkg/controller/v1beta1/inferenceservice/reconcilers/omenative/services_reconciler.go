package omenative

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

// reconcileHeadlessService ensures the per-Component headless Service
// (`<isvc>-<component>-headless`) exists and matches the spec
// BuildHeadlessService produces. Idempotent and drift-correcting: on
// every reconcile it patches Selector / PublishNotReadyAddresses /
// Type / Labels / OwnerReferences back to the controller-desired
// shape, but never touches ClusterIP after Create (immutable
// post-create).
//
// Called at the top of ReconcileComponent before any per-Instance
// work so the drain path's EndpointSlice reads have a Service to
// look at on the first reconcile. Without this, IsPodDrained would
// fall through its "no slices == drained" branch on every drain
// check and skip the wait entirely — see drain.go's revised
// fail-loud behavior.
func reconcileHeadlessService(ctx context.Context, p ReconcileParams) error {
	if p.Client == nil {
		return fmt.Errorf("reconcileHeadlessService: nil client")
	}
	if p.ISVC == nil {
		return fmt.Errorf("reconcileHeadlessService: nil ISVC")
	}

	desired, err := BuildHeadlessService(p.ISVC, p.Component)
	if err != nil {
		return fmt.Errorf("reconcileHeadlessService: build desired: %w", err)
	}

	target := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      desired.Name,
			Namespace: desired.Namespace,
		},
	}

	_, err = controllerutil.CreateOrUpdate(ctx, p.Client, target, func() error {
		// ClusterIP is immutable after create. Only stamp it on the
		// fresh-create path; existing Services keep whatever the
		// apiserver assigned (or kept from previous reconciles, since
		// we always declare ClusterIPNone).
		if target.CreationTimestamp.IsZero() {
			target.Spec.ClusterIP = desired.Spec.ClusterIP
		}
		target.Spec.PublishNotReadyAddresses = desired.Spec.PublishNotReadyAddresses
		target.Spec.Selector = desired.Spec.Selector
		target.Spec.Type = desired.Spec.Type
		// Ports stay nil — see BuildHeadlessService comment for why
		// (peer-DNS only, no kube-proxy routing).
		target.Labels = desired.Labels
		target.OwnerReferences = desired.OwnerReferences
		return nil
	})
	if err != nil {
		return fmt.Errorf("reconcileHeadlessService: CreateOrUpdate %s/%s: %w",
			target.Namespace, target.Name, err)
	}
	return nil
}

// ensureHeadlessService is the entry point ReconcileComponent calls.
// Wrapped for symmetry with future per-Component service emissions
// (routing Service, ingress Service) that will join this top-of-
// reconcile block as separate small helpers under one umbrella.
func ensureHeadlessService(ctx context.Context, p ReconcileParams) error {
	if err := reconcileHeadlessService(ctx, p); err != nil {
		return fmt.Errorf("OMENative reconciler: ensure headless service (component=%s): %w", p.Component, err)
	}
	return nil
}
