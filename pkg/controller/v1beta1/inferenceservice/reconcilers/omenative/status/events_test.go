package status

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

func TestRecordEventf_NilRecorderIsNoOp(t *testing.T) {
	// Callers that wire a nil recorder (test fixtures) must not panic.
	isvc := &v1beta1.InferenceService{}
	RecordEventf(nil, isvc, corev1.EventTypeNormal, "TestReason", "no panic for %s", "nil recorder")
	RecordNormal(nil, isvc, "X", "no panic %d", 1)
	RecordWarning(nil, isvc, "X", "no panic %s", "ok")
}

func TestRecordEventf_NilISVCIsNoOp(t *testing.T) {
	rec := record.NewFakeRecorder(1)
	RecordNormal(rec, nil, "X", "should not fire")
	if len(rec.Events) != 0 {
		t.Errorf("expected no events when ISVC is nil, got %d", len(rec.Events))
	}
}

func TestRecordNormal_EmitsAgainstISVC(t *testing.T) {
	rec := record.NewFakeRecorder(2)
	isvc := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "x", Namespace: "ns"}}

	RecordNormal(rec, isvc, EventReasonInstanceReady, "Instance %d ready", 0)

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

	RecordWarning(rec, isvc, EventReasonOperationDeadlineExceeded, "stuck for %s", "30m")

	msg := <-rec.Events
	want := "Warning OperationDeadlineExceeded stuck for 30m"
	if msg != want {
		t.Errorf("event format mismatch: got %q want %q", msg, want)
	}
}

func TestInstanceKeyFormat(t *testing.T) {
	got := InstanceKey(v1beta1.EngineComponent, 3)
	want := "component=engine instance=3"
	if got != want {
		t.Errorf("InstanceKey format: got %q want %q", got, want)
	}
}
