package omenative

import (
	"context"
	"fmt"

	monitoringv1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/internalsource"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
)

// BuildPodMonitor produces the per-Component Prometheus PodMonitor —
// `<isvc>-<comp>` — that scrapes /metrics off OMENative pods. Same
// shape RawDeployment / MultiNode emit (see
// reconcilers/podmonitor/createPodMonitor) but with an OMENative-scoped
// pod selector — no `app=<isvc>-<comp>` dependency, since OMENative
// pods don't carry that label.
//
// Metrics port is derived the same way Raw does it: the first container
// port named "metrics" wins; otherwise the first declared port; default
// "http". Runtimes that listen elsewhere should declare the port in the
// runner spec.
//
// Pure compute, no I/O.
func BuildPodMonitor(isvc *v1beta1.InferenceService, component v1beta1.ComponentType, podSpec *corev1.PodSpec) (*monitoringv1.PodMonitor, error) {
	if isvc == nil {
		return nil, fmt.Errorf("nil InferenceService")
	}
	name := query.StableServiceName(isvc.Name, internalsource.ComponentTypeToWorkload(component))
	selector := map[string]string{
		constants.InferenceServicePodLabelKey: isvc.Name,
		constants.OMEComponentLabel:           string(component),
		query.LabelManagedBy:                  query.ManagedByOMENative,
	}
	portName := podMonitorMetricsPort(podSpec)
	return &monitoringv1.PodMonitor{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: isvc.Namespace,
			Labels:    selector,
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(isvc, isvcGroupVersionKind),
			},
		},
		Spec: monitoringv1.PodMonitorSpec{
			NamespaceSelector: monitoringv1.NamespaceSelector{
				MatchNames: []string{isvc.Namespace},
			},
			Selector: metav1.LabelSelector{
				MatchLabels: selector,
			},
			PodMetricsEndpoints: []monitoringv1.PodMetricsEndpoint{{
				Port:     &portName,
				Path:     "/metrics",
				Interval: "10s",
			}},
		},
	}, nil
}

// podMonitorMetricsPort picks the scrape port from the rendered
// PodSpec. Prefers a port named "metrics"; falls back to the first
// declared port; defaults to "http" when the runner declares no ports
// (matches the RawDeployment fallback in reconcilers/podmonitor).
func podMonitorMetricsPort(podSpec *corev1.PodSpec) string {
	if podSpec == nil || len(podSpec.Containers) == 0 {
		return "http"
	}
	ports := podSpec.Containers[0].Ports
	for _, p := range ports {
		if p.Name == "metrics" {
			return "metrics"
		}
	}
	if len(ports) > 0 && ports[0].Name != "" {
		return ports[0].Name
	}
	return "http"
}

// reconcilePodMonitor ensures the per-Component PodMonitor exists and
// matches BuildPodMonitor's output. Idempotent + drift-correcting on
// Selector / NamespaceSelector / PodMetricsEndpoints / Labels /
// OwnerReferences. CreateOrUpdate's no-op semantics keep
// ResourceVersion stable across reconciles when nothing changed.
func reconcilePodMonitor(ctx context.Context, p ReconcileParams) error {
	if p.Client == nil {
		return fmt.Errorf("reconcilePodMonitor: nil client")
	}
	if p.ISVC == nil {
		return fmt.Errorf("reconcilePodMonitor: nil ISVC")
	}
	desired, err := BuildPodMonitor(p.ISVC, p.Component, p.PodSpec)
	if err != nil {
		return fmt.Errorf("reconcilePodMonitor: build desired: %w", err)
	}
	target := &monitoringv1.PodMonitor{
		ObjectMeta: metav1.ObjectMeta{
			Name:      desired.Name,
			Namespace: desired.Namespace,
		},
	}
	_, err = controllerutil.CreateOrUpdate(ctx, p.Client, target, func() error {
		target.Spec.NamespaceSelector = desired.Spec.NamespaceSelector
		target.Spec.Selector = desired.Spec.Selector
		target.Spec.PodMetricsEndpoints = desired.Spec.PodMetricsEndpoints
		target.Labels = desired.Labels
		target.OwnerReferences = desired.OwnerReferences
		return nil
	})
	if err != nil {
		return fmt.Errorf("reconcilePodMonitor: CreateOrUpdate %s/%s: %w",
			target.Namespace, target.Name, err)
	}
	return nil
}

// EnsurePodMonitor is the entry point ReconcileComponent calls after
// the headless + stable Services land. Skipped when PodSpec is nil
// (Components with MinReplicas=0 leave the rendered spec empty —
// nothing to scrape).
func EnsurePodMonitor(ctx context.Context, p ReconcileParams) error {
	if p.PodSpec == nil {
		return nil
	}
	if err := reconcilePodMonitor(ctx, p); err != nil {
		return fmt.Errorf("OMENative reconciler: ensure pod monitor (component=%s): %w", p.Component, err)
	}
	return nil
}
