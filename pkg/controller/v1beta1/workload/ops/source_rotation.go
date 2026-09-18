// source_rotation.go — the surge's report on its own source.
//
// A SurgeThenDrain cycle keeps the source in rotation until it reaches
// the drain step; that ordering is the no-downtime guarantee. So a
// source already out of rotation while the step is still Surge was taken
// out by something outside this rollout, and the Instance is serving
// nothing while its replacement comes up. The roll continues — the
// replacement is the only way back to capacity — but the state is named
// on the operation, and it stops the escalation pass from reading the
// pod set as healthy capacity.
package ops

import (
	"context"
	"fmt"
	"sort"

	corev1 "k8s.io/api/core/v1"

	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/drain"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/podreadiness"
	workload "github.com/ome-projects/ome/pkg/controller/v1beta1/workload/types"
)

// sourceRouted reports whether pod is still taking traffic, read through
// BOTH views the rollout owns: the lifecycle serving gate the controller
// writes, and the pod's endpoint in the per-revision routed Service,
// which is the same convergence signal the drain step waits on. Either
// view saying "out" is out — a gate still True with no routable endpoint
// is not capacity, and neither is the reverse.
//
// An absent Service, or a Service whose slices have not propagated, is
// NOT a reading: it is a fact about the Service and says nothing about
// whether the pod stopped serving. The endpoint view abstains there and
// the serving gate is the only one that speaks, so a cold start cannot
// be misreported as a source someone took out of rotation.
func sourceRouted(ctx context.Context, drainer *drain.Batcher, input workload.ReconcileInput, plan workload.ComponentPlan, pod *corev1.Pod) (bool, error) {
	if pod == nil || pod.DeletionTimestamp != nil || !podreadiness.IsServing(pod) {
		return false, nil
	}
	serviceName := drainServiceForPod(input, plan, pod)
	if serviceName == "" {
		// No routed Service to read the pod's membership from; the gate is
		// the only view there is.
		return true, nil
	}
	return drainer.IsPodRouted(ctx, serviceName, pod)
}

// reconcileSourceRotationReport records or releases the SourceUnrouted
// token for one surging Instance. Edge-triggered through the shared
// external-hold writer, so a source out of rotation for minutes writes
// status once; released by this same authority as soon as the source is
// routed again or the step advances past Surge, which is where the surge
// takes the source out itself.
func reconcileSourceRotationReport(ctx context.Context, drainer *drain.Batcher, input workload.ReconcileInput, plan workload.ComponentPlan, idx int32, status *workload.InstanceStatus, sources []*corev1.Pod) error {
	unrouted, err := unroutedSource(ctx, drainer, input, plan, status, sources)
	if err != nil {
		return fmt.Errorf("check source rotation (instance=%d): %w", idx, err)
	}
	if unrouted == nil {
		return workload.ClearExternalHold(ctx, input, idx, workload.WaitingReasonSourceUnrouted)
	}
	if _, _, err := workload.MarkExternalHold(ctx, input, idx, workload.WaitingReasonSourceUnrouted,
		workload.SourceUnroutedTermination(unrouted), sourceUnroutedWritable); err != nil {
		return fmt.Errorf("report unrouted source (instance=%d): %w", idx, err)
	}
	return nil
}

// unroutedSource returns the source pod to name in the report, or nil
// when there is nothing to report: the step has moved past Surge, the
// source is already gone, or at least one source is still in rotation.
// One routed source is enough — the Instance is taking traffic.
//
// Chosen by NAME rather than by bucket order, which comes from an
// informer map iteration and differs between passes; naming a different
// pod every pass is exactly what the edge trigger exists to prevent.
func unroutedSource(ctx context.Context, drainer *drain.Batcher, input workload.ReconcileInput, plan workload.ComponentPlan, status *workload.InstanceStatus, sources []*corev1.Pod) (*corev1.Pod, error) {
	if status == nil || status.Operation == nil || len(sources) == 0 ||
		status.Operation.Type != workload.InstanceOperationUpdate ||
		status.Operation.Step != updateStepSurge {
		return nil, nil
	}
	ordered := append([]*corev1.Pod(nil), sources...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].Name < ordered[j].Name })
	var out *corev1.Pod
	for _, pod := range ordered {
		routed, err := sourceRouted(ctx, drainer, input, plan, pod)
		if err != nil {
			return nil, err
		}
		if routed {
			return nil, nil
		}
		if out == nil {
			out = pod
		}
	}
	return out, nil
}

// sourceUnroutedWritable reports whether this authority may own the
// operation's Waiting token. An operation parked on a DIFFERENT wait
// keeps it: that token's writer owns what the row reports it is queued
// behind, and unlike this report those waits also park the deadline.
func sourceUnroutedWritable(op *workload.InstanceOperation) bool {
	if op == nil || op.Type != workload.InstanceOperationUpdate {
		return false
	}
	return op.Waiting == "" || workload.OperationSourceUnrouted(op)
}
