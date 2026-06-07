package canary

import (
	"strings"
	"testing"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"k8s.io/client-go/tools/record"
)

func TestRecordDispatch_StepCounterAndEvent(t *testing.T) {
	isvc := &v1beta1.InferenceService{}
	isvc.Name = "metrics-svc"
	isvc.Status.Canary = &v1beta1.CanaryStatus{CurrentStep: 1, ObservedTrafficWeight: 50}
	rec := record.NewFakeRecorder(8)

	before := testutil.ToFloat64(canaryStepTotal.WithLabelValues(isvc.Name, "engine"))
	recordDispatch(rec, isvc, v1beta1.EngineComponent, &Result{Active: true, Stepped: true})
	if got := testutil.ToFloat64(canaryStepTotal.WithLabelValues(isvc.Name, "engine")) - before; got != 1 {
		t.Fatalf("step counter should increment by 1 on Stepped, got %v", got)
	}
	if got := testutil.ToFloat64(canaryCurrentStep.WithLabelValues(isvc.Name, "engine")); got != 1 {
		t.Fatalf("current-step gauge should be 1, got %v", got)
	}
	select {
	case ev := <-rec.Events:
		if !strings.Contains(ev, EventReasonCanaryStepAdvanced) {
			t.Fatalf("expected a %s event, got %q", EventReasonCanaryStepAdvanced, ev)
		}
	default:
		t.Fatal("expected a step-advanced event")
	}

	// A non-stepped pass must NOT increment the step counter.
	after := testutil.ToFloat64(canaryStepTotal.WithLabelValues(isvc.Name, "engine"))
	recordDispatch(rec, isvc, v1beta1.EngineComponent, &Result{Active: true})
	if got := testutil.ToFloat64(canaryStepTotal.WithLabelValues(isvc.Name, "engine")); got != after {
		t.Fatalf("non-stepped pass must not increment the step counter, got %v want %v", got, after)
	}
}

func TestRecordDispatch_Completion(t *testing.T) {
	isvc := &v1beta1.InferenceService{}
	isvc.Name = "complete-svc"
	// Done sentinel: completion keeps Status.Canary (CurrentStep past last step).
	isvc.Status.Canary = &v1beta1.CanaryStatus{CurrentStep: 2, ObservedTrafficWeight: 100}
	rec := record.NewFakeRecorder(8)

	before := testutil.ToFloat64(canaryCompleteTotal.WithLabelValues(isvc.Name))
	recordDispatch(rec, isvc, v1beta1.EngineComponent, &Result{Active: true, Complete: true})
	if got := testutil.ToFloat64(canaryCompleteTotal.WithLabelValues(isvc.Name)) - before; got != 1 {
		t.Fatalf("complete counter should increment by 1, got %v", got)
	}
	if got := testutil.ToFloat64(canaryCurrentStep.WithLabelValues(isvc.Name, "engine")); got != 0 {
		t.Fatalf("current-step gauge should reset to 0 on completion, got %v", got)
	}
	if got := testutil.ToFloat64(canaryTrafficWeight.WithLabelValues(isvc.Name, "engine")); got != 0 {
		t.Fatalf("traffic-weight gauge should reset to 0 on completion, got %v", got)
	}
	select {
	case ev := <-rec.Events:
		if !strings.Contains(ev, EventReasonCanaryCompleted) {
			t.Fatalf("expected a %s event, got %q", EventReasonCanaryCompleted, ev)
		}
	default:
		t.Fatal("expected a completed event")
	}
}
