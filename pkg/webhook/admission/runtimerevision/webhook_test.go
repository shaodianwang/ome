package runtimerevision

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/onsi/gomega"
	admissionv1 "k8s.io/api/admission/v1"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"github.com/ome-projects/ome/pkg/constants"
)

func newDecoder(t *testing.T) admission.Decoder {
	t.Helper()
	s := runtime.NewScheme()
	if err := appsv1.AddToScheme(s); err != nil {
		t.Fatalf("scheme: %v", err)
	}
	return admission.NewDecoder(s)
}

func encode(t *testing.T, obj *appsv1.ControllerRevision) []byte {
	t.Helper()
	raw, err := json.Marshal(obj)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return raw
}

func TestHandle_NonUpdate_PassesThrough(t *testing.T) {
	g := gomega.NewWithT(t)
	v := &ImmutabilityValidator{Decoder: newDecoder(t)}
	req := admission.Request{AdmissionRequest: admissionv1.AdmissionRequest{
		Operation: admissionv1.Create,
	}}
	g.Expect(v.Handle(context.Background(), req).Allowed).To(gomega.BeTrue())
}

func TestHandle_UpdateOnNonOMERevision_PassesThrough(t *testing.T) {
	g := gomega.NewWithT(t)
	old := &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{Name: "sts-foo-1", Namespace: "team-a"},
	}
	raw := encode(t, old)
	req := admission.Request{AdmissionRequest: admissionv1.AdmissionRequest{
		Operation: admissionv1.Update,
		OldObject: runtime.RawExtension{Raw: raw},
	}}
	v := &ImmutabilityValidator{Decoder: newDecoder(t)}
	g.Expect(v.Handle(context.Background(), req).Allowed).To(gomega.BeTrue(),
		"revisions not annotated as OME-created should be ignored")
}

func TestHandle_UpdateOnOMERevision_Denied(t *testing.T) {
	g := gomega.NewWithT(t)
	old := &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cr-srt-llama-pd-abc12345",
			Namespace: "ome",
			Annotations: map[string]string{
				constants.RuntimeRevisionCreatedByKey: constants.RuntimeRevisionCreatedByOMEValue,
			},
		},
	}
	raw := encode(t, old)
	req := admission.Request{AdmissionRequest: admissionv1.AdmissionRequest{
		Operation: admissionv1.Update,
		OldObject: runtime.RawExtension{Raw: raw},
	}}
	v := &ImmutabilityValidator{Decoder: newDecoder(t)}
	resp := v.Handle(context.Background(), req)
	g.Expect(resp.Allowed).To(gomega.BeFalse())
	g.Expect(resp.Result.Message).To(gomega.ContainSubstring("immutable"))
}
