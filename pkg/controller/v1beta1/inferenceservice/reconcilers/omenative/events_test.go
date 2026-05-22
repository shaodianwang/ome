package omenative

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

func TestRecordEventf_NilRecorderIsNoOp(t *testing.T) {
	// Tests that construct ReconcileParams{} without a Recorder
	// must not panic when ops call recordEventf.
	p := ReconcileParams{
		ISVC:     &v1beta1.InferenceService{},
		Recorder: nil,
	}
	recordEventf(p, corev1.EventTypeNormal, "TestReason", "no panic for %s", "nil recorder")
	recordNormal(p, "X", "no panic %d", 1)
	recordWarning(p, "X", "no panic %s", "ok")
}

func TestRecordEventf_NilISVCIsNoOp(t *testing.T) {
	rec := record.NewFakeRecorder(1)
	p := ReconcileParams{
		ISVC:     nil,
		Recorder: rec,
	}
	recordNormal(p, "X", "should not fire")
	if len(rec.Events) != 0 {
		t.Errorf("expected no events when ISVC is nil, got %d", len(rec.Events))
	}
}

func TestRecordNormal_EmitsAgainstISVC(t *testing.T) {
	rec := record.NewFakeRecorder(2)
	isvc := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "x", Namespace: "ns"}}
	p := ReconcileParams{ISVC: isvc, Recorder: rec}

	recordNormal(p, EventReasonInstanceReady, "Instance %d ready", 0)

	if len(rec.Events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(rec.Events))
	}
	msg := <-rec.Events
	// fake recorder formats as "<type> <reason> <message>".
	want := "Normal InstanceReady Instance 0 ready"
	if msg != want {
		t.Errorf("event format mismatch: got %q want %q", msg, want)
	}
}

func TestRecordWarning_EventTypeWarning(t *testing.T) {
	rec := record.NewFakeRecorder(1)
	isvc := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "x", Namespace: "ns"}}
	p := ReconcileParams{ISVC: isvc, Recorder: rec}

	recordWarning(p, EventReasonOperationDeadlineExceeded, "stuck for %s", "30m")

	msg := <-rec.Events
	want := "Warning OperationDeadlineExceeded stuck for 30m"
	if msg != want {
		t.Errorf("event format mismatch: got %q want %q", msg, want)
	}
}

func TestInstanceKeyFormat(t *testing.T) {
	got := instanceKey(v1beta1.EngineComponent, 3)
	want := "component=engine instance=3"
	if got != want {
		t.Errorf("instanceKey format: got %q want %q", got, want)
	}
}
