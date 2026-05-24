package ops

import (
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

func TestRevisionHashFromTarget_NilReturnsEmpty(t *testing.T) {
	if got := revisionHashFromTarget(nil); got != "" {
		t.Errorf("nil target: got %q want empty", got)
	}
}

func TestRevisionHashFromTarget_ExtractsHashFromName(t *testing.T) {
	target := &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{Name: "llama-engine-abcd1234"},
	}
	if got := revisionHashFromTarget(target); got != "abcd1234" {
		t.Errorf("hash: got %q want abcd1234", got)
	}
}

func TestRevisionHashForInstance_TargetWins(t *testing.T) {
	target := &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{Name: "llama-engine-newhash"},
	}
	isvc := &v1beta1.InferenceService{
		Status: v1beta1.InferenceServiceStatus{
			Components: map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
				v1beta1.EngineComponent: {
					OMENative: &v1beta1.OMENativeComponentStatus{
						InstanceStatuses: []v1beta1.OMENativeInstanceStatus{
							{Index: 0, RunningRevision: "llama-engine-oldhash"},
						},
					},
				},
			},
		},
	}
	if got := revisionHashForInstance(isvc, v1beta1.EngineComponent, 0, target); got != "newhash" {
		t.Errorf("target precedence: got %q want newhash", got)
	}
}

func TestRevisionHashForInstance_FallsBackToInstanceRunningRevision(t *testing.T) {
	isvc := &v1beta1.InferenceService{
		Status: v1beta1.InferenceServiceStatus{
			Components: map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
				v1beta1.EngineComponent: {
					OMENative: &v1beta1.OMENativeComponentStatus{
						InstanceStatuses: []v1beta1.OMENativeInstanceStatus{
							{Index: 0, RunningRevision: "llama-engine-runninghash"},
						},
					},
				},
			},
		},
	}
	if got := revisionHashForInstance(isvc, v1beta1.EngineComponent, 0, nil); got != "runninghash" {
		t.Errorf("running revision fallback: got %q want runninghash", got)
	}
}

func TestRevisionHashForInstance_FallsBackToComponentUpdateRevision(t *testing.T) {
	isvc := &v1beta1.InferenceService{
		Status: v1beta1.InferenceServiceStatus{
			Components: map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
				v1beta1.EngineComponent: {
					OMENative: &v1beta1.OMENativeComponentStatus{
						UpdateRevision: "llama-engine-updatehash",
					},
				},
			},
		},
	}
	if got := revisionHashForInstance(isvc, v1beta1.EngineComponent, 0, nil); got != "updatehash" {
		t.Errorf("component update revision fallback: got %q want updatehash", got)
	}
}

func TestRevisionHashForInstance_NilISVCReturnsEmpty(t *testing.T) {
	if got := revisionHashForInstance(nil, v1beta1.EngineComponent, 0, nil); got != "" {
		t.Errorf("nil isvc: got %q want empty", got)
	}
}

func TestRevisionHashForInstance_MissingStatusReturnsEmpty(t *testing.T) {
	isvc := &v1beta1.InferenceService{}
	if got := revisionHashForInstance(isvc, v1beta1.EngineComponent, 0, nil); got != "" {
		t.Errorf("missing status: got %q want empty", got)
	}
}
