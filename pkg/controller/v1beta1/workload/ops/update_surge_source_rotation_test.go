package ops

import (
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	workload "github.com/ome-projects/ome/pkg/controller/v1beta1/workload/types"
)

// routedServiceForPod creates the per-revision routed Service whose
// EndpointSlices carry the rotation view of a fixture pod. The slices
// themselves are the reading; the Service only gives them a name to hang
// off, so a test that creates one without slices still expresses "the
// endpoint view has nothing to say".
func routedServiceForPod(t *testing.T, f *midSurgeFixture, pod *corev1.Pod) string {
	t.Helper()
	name := query.PerRevisionServiceName(f.isvc.Name, workload.ComponentEngine, pod.Labels[query.LabelRevisionHash])
	svc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Namespace: f.isvc.Namespace, Name: name}}
	if err := f.client.Create(context.Background(), svc); err != nil {
		t.Fatalf("create routed service: %v", err)
	}
	return name
}

// waitingTokenOnInstance returns the Instance's Operation.Waiting token.
func waitingTokenOnInstance(t *testing.T, f *midSurgeFixture) (string, *v1beta1.InstanceTermination) {
	t.Helper()
	s := legacyInstanceStatusesOnIR(f.client, f.isvc, workload.ComponentEngine)[0]
	if s.Operation == nil {
		return "", s.LastFailure
	}
	return s.Operation.Waiting, s.LastFailure
}

// TestUpdateSurge_SourceServingGateOffIsReported: the surge does not
// touch its source's serving gate before the drain step, so a gate
// dropped at Step=Surge was dropped by something else and the Instance
// is serving nothing while its replacement comes up. The roll carries
// on — the replacement is the only route back to capacity — but the
// operation names the state.
//
// Cells: instance/Update/Surge/pod.servingOff[source]/before
func TestUpdateSurge_SourceServingGateOffIsReported(t *testing.T) {
	f := newMidSurgeFixture(t, updateStepSurge, false /* surgeReady */, false /* sourceServing */)

	f.run(t, workload.UpdateStrategySurgeThenDrain)

	token, evidence := waitingTokenOnInstance(t, f)
	if token != workload.WaitingReasonSourceUnrouted {
		t.Fatalf("Operation.Waiting: got %q want %s", token, workload.WaitingReasonSourceUnrouted)
	}
	if evidence == nil || evidence.PodName != f.sourcePod.Name ||
		evidence.Reason != workload.WaitingReasonSourceUnrouted {
		t.Fatalf("LastFailure must name the unrouted source, got %+v", evidence)
	}
	if _, alive := f.livePod(t, f.sourcePod); !alive {
		t.Errorf("the report must not delete the source; the drain step owns that")
	}
}

// TestUpdateSurge_SourceOutOfTheRoutedServiceIsReported: the same state
// arrived at through the other view. The serving gate is still True but
// the source has no ready endpoint in its per-revision routed Service,
// so nothing is actually taking traffic. Both views must agree before a
// source counts as capacity.
//
// Cells: instance/Update/Surge/endpoint.rotationOut[source]/before
func TestUpdateSurge_SourceOutOfTheRoutedServiceIsReported(t *testing.T) {
	f := newMidSurgeFixture(t, updateStepSurge, false /* surgeReady */, true /* sourceServing */)
	service := routedServiceForPod(t, f, f.sourcePod)
	slice := legacySliceWithEndpoint(f.isvc.Namespace, service+"-abc", service, f.sourcePod, false /* not ready */)
	if err := f.client.Create(context.Background(), slice); err != nil {
		t.Fatalf("create endpoint slice: %v", err)
	}

	f.run(t, workload.UpdateStrategySurgeThenDrain)

	token, evidence := waitingTokenOnInstance(t, f)
	if token != workload.WaitingReasonSourceUnrouted {
		t.Fatalf("Operation.Waiting: got %q want %s", token, workload.WaitingReasonSourceUnrouted)
	}
	if evidence == nil || !strings.Contains(evidence.Message, f.sourcePod.Name) {
		t.Fatalf("LastFailure must name the unrouted source, got %+v", evidence)
	}
}

// TestUpdateSurge_RoutedSourceIsNotReported pins both halves of the
// negative: a source whose gate is True AND whose endpoint is ready is
// capacity, and a report left over from an earlier pass is released by
// the same authority that set it.
//
// Cells: instance/Update/Surge/pod.servingOff[source]/before, instance/Update/Surge/endpoint.rotationOut[source]/before
func TestUpdateSurge_RoutedSourceIsNotReported(t *testing.T) {
	f := newMidSurgeFixture(t, updateStepSurge, false /* surgeReady */, true /* sourceServing */)
	service := routedServiceForPod(t, f, f.sourcePod)
	slice := legacySliceWithEndpoint(f.isvc.Namespace, service+"-abc", service, f.sourcePod, true /* ready */)
	if err := f.client.Create(context.Background(), slice); err != nil {
		t.Fatalf("create endpoint slice: %v", err)
	}
	seedWaitingToken(t, f, workload.WaitingReasonSourceUnrouted)

	f.run(t, workload.UpdateStrategySurgeThenDrain)

	if token, _ := waitingTokenOnInstance(t, f); token != "" {
		t.Fatalf("Operation.Waiting: got %q want it released once the source is routed again", token)
	}
}

// TestUpdateSurge_SourceRotationReportEndsWithTheSurgeStep: past
// Step=Surge the surge takes the source out of rotation itself, so the
// report has nothing left to say and is released rather than left to
// describe the rollout's own effect.
//
// Cells: instance/Update/SurgeDrain/pod.servingOff[source]/before
func TestUpdateSurge_SourceRotationReportEndsWithTheSurgeStep(t *testing.T) {
	f := newMidSurgeFixture(t, updateStepSurgeDrain, true /* surgeReady */, false /* sourceServing */)
	seedWaitingToken(t, f, workload.WaitingReasonSourceUnrouted)

	f.run(t, workload.UpdateStrategySurgeThenDrain)

	if token, _ := waitingTokenOnInstance(t, f); token != "" {
		t.Fatalf("Operation.Waiting: got %q want it released past the surge step", token)
	}
}

// TestUpdateSurge_SourceWithNoRoutedServiceYetIsNotReported: an absent
// Service, or one whose slices have not propagated, is a fact about the
// Service and not evidence that the source stopped serving. The endpoint
// view abstains there, so a cold start is not misreported as a source
// someone took out of rotation.
//
// Cells: instance/Update/Surge/endpoint.rotationOut[source]/before
func TestUpdateSurge_SourceWithNoRoutedServiceYetIsNotReported(t *testing.T) {
	t.Run("no service", func(t *testing.T) {
		f := newMidSurgeFixture(t, updateStepSurge, false /* surgeReady */, true /* sourceServing */)
		f.run(t, workload.UpdateStrategySurgeThenDrain)
		if token, _ := waitingTokenOnInstance(t, f); token != "" {
			t.Fatalf("Operation.Waiting: got %q want none (no Service to read a membership from)", token)
		}
	})
	t.Run("service without slices", func(t *testing.T) {
		f := newMidSurgeFixture(t, updateStepSurge, false /* surgeReady */, true /* sourceServing */)
		routedServiceForPod(t, f, f.sourcePod)
		f.run(t, workload.UpdateStrategySurgeThenDrain)
		if token, _ := waitingTokenOnInstance(t, f); token != "" {
			t.Fatalf("Operation.Waiting: got %q want none (slices have not propagated)", token)
		}
	})
}

// TestSourceUnroutedDoesNotParkTheDeadline pins the design choice behind
// the token: every other Waiting token names a wait an operator or
// another controller owns and parks InstanceReadyTimeout, while this one
// names a workload that is getting worse. The clock must keep running.
func TestSourceUnroutedDoesNotParkTheDeadline(t *testing.T) {
	op := &workload.InstanceOperation{
		Type:    workload.InstanceOperationUpdate,
		Step:    updateStepSurge,
		Waiting: workload.WaitingReasonSourceUnrouted,
	}
	if workload.OperationExternallyHeld(op) {
		t.Fatalf("a SourceUnrouted report must not park the attempt deadline")
	}
	if !workload.OperationSourceUnrouted(op) {
		t.Fatalf("OperationSourceUnrouted must recognize its own token")
	}
}

// seedWaitingToken puts token on the Instance's in-flight operation, the
// state a previous pass would have left behind.
func seedWaitingToken(t *testing.T, f *midSurgeFixture, token string) {
	t.Helper()
	ir := &v1beta1.InferenceReplica{}
	key := client.ObjectKey{Namespace: f.isvc.Namespace, Name: legacyIRName(f.isvc, workload.ComponentEngine)}
	if err := f.client.Get(context.Background(), key, ir); err != nil {
		t.Fatalf("re-read IR: %v", err)
	}
	ir.Status.InstanceStatuses[0].Operation.Waiting = token
	if err := f.client.Status().Update(context.Background(), ir); err != nil {
		t.Fatalf("seed waiting token: %v", err)
	}
}
