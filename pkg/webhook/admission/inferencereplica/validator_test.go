package inferencereplica

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/onsi/gomega"
	admissionv1 "k8s.io/api/admission/v1"
	authenticationv1 "k8s.io/api/authentication/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
)

func newDecoder(t *testing.T) admission.Decoder {
	t.Helper()
	s := runtime.NewScheme()
	if err := v1beta1.AddToScheme(s); err != nil {
		t.Fatalf("v1beta1.AddToScheme: %v", err)
	}
	return admission.NewDecoder(s)
}

func encode(t *testing.T, obj *v1beta1.InferenceReplica) []byte {
	t.Helper()
	raw, err := json.Marshal(obj)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return raw
}

// baselineIR returns a syntactically-valid InferenceReplica suitable for
// cloning into Create / Update fixtures.
func baselineIR(annotations map[string]string) *v1beta1.InferenceReplica {
	return &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "llama-engine",
			Namespace:   "prod-models",
			Annotations: annotations,
		},
		Spec: v1beta1.InferenceReplicaSpec{
			ParentRef: v1beta1.ParentReference{
				Name: "llama",
				UID:  "00000000-0000-0000-0000-000000000001",
			},
			Component: v1beta1.EngineComponent,
			Runners: []v1beta1.Runner{
				{
					Name: v1beta1.RunnerNameDefault,
					Size: 1,
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{{
								Name:  "ome-container",
								Image: "sgl:1.0",
							}},
						},
					},
				},
			},
		},
	}
}

// withControllerWrite stamps the controller-write annotation, matching
// what the ISVC controller's client does on every Create/Update.
func withControllerWrite() map[string]string {
	return map[string]string{
		constants.InferenceReplicaControllerWriteAnnotationKey: constants.InferenceReplicaControllerWriteAnnotationVal,
	}
}

func createReq(t *testing.T, obj *v1beta1.InferenceReplica) admission.Request {
	t.Helper()
	return admission.Request{AdmissionRequest: admissionv1.AdmissionRequest{
		Operation: admissionv1.Create,
		Namespace: obj.Namespace,
		Name:      obj.Name,
		UserInfo:  authenticationv1.UserInfo{Username: "alice"},
		Object:    runtime.RawExtension{Raw: encode(t, obj)},
	}}
}

func updateReq(t *testing.T, oldObj, newObj *v1beta1.InferenceReplica) admission.Request {
	t.Helper()
	return admission.Request{AdmissionRequest: admissionv1.AdmissionRequest{
		Operation: admissionv1.Update,
		Namespace: newObj.Namespace,
		Name:      newObj.Name,
		UserInfo:  authenticationv1.UserInfo{Username: "alice"},
		OldObject: runtime.RawExtension{Raw: encode(t, oldObj)},
		Object:    runtime.RawExtension{Raw: encode(t, newObj)},
	}}
}

func TestHandle_CreateWithoutAnnotation_Denied(t *testing.T) {
	g := gomega.NewWithT(t)
	v := &Validator{Decoder: newDecoder(t)}
	resp := v.Handle(context.Background(), createReq(t, baselineIR(nil)))

	g.Expect(resp.Allowed).To(gomega.BeFalse(),
		"users should not be able to create InferenceReplicas without the controller-write annotation")
	g.Expect(resp.Result.Message).To(gomega.ContainSubstring("controller-only resource"))
	g.Expect(resp.Result.Message).To(gomega.ContainSubstring(constants.InferenceReplicaControllerWriteAnnotationKey))
}

func TestHandle_CreateWithEmptyAnnotationMap_Denied(t *testing.T) {
	g := gomega.NewWithT(t)
	v := &Validator{Decoder: newDecoder(t)}
	resp := v.Handle(context.Background(), createReq(t, baselineIR(map[string]string{})))

	g.Expect(resp.Allowed).To(gomega.BeFalse(),
		"empty annotation map should be treated the same as nil")
}

func TestHandle_CreateWithWrongAnnotationValue_Denied(t *testing.T) {
	g := gomega.NewWithT(t)
	v := &Validator{Decoder: newDecoder(t)}
	// Only the literal "true" is accepted — guards against truthy variants
	// like "True" / "yes" that hand editors might use.
	resp := v.Handle(context.Background(), createReq(t, baselineIR(
		map[string]string{constants.InferenceReplicaControllerWriteAnnotationKey: "yes"},
	)))

	g.Expect(resp.Allowed).To(gomega.BeFalse(),
		`annotation value must be the literal "true"; truthy variants reject`)
}

func TestHandle_CreateWithControllerWriteAnnotation_Allowed(t *testing.T) {
	g := gomega.NewWithT(t)
	v := &Validator{Decoder: newDecoder(t)}
	resp := v.Handle(context.Background(), createReq(t, baselineIR(withControllerWrite())))

	g.Expect(resp.Allowed).To(gomega.BeTrue(),
		"ISVC controller writes carrying the annotation must pass")
}

func TestHandle_UpdateWithoutAnnotation_Denied(t *testing.T) {
	g := gomega.NewWithT(t)
	v := &Validator{Decoder: newDecoder(t)}
	old := baselineIR(withControllerWrite())
	// New object drops the annotation — simulates a hand edit that
	// preserved the resource but dropped the controller marker.
	newObj := baselineIR(nil)
	resp := v.Handle(context.Background(), updateReq(t, old, newObj))

	g.Expect(resp.Allowed).To(gomega.BeFalse(),
		"removing the controller-write annotation on update should reject")
}

func TestHandle_UpdateWithSpecChangeNoAnnotation_Denied(t *testing.T) {
	g := gomega.NewWithT(t)
	v := &Validator{Decoder: newDecoder(t)}
	old := baselineIR(withControllerWrite())
	// Hand-edit bumping replicas + dropping the annotation: this is the
	// most dangerous path (a user trying to override the controller).
	newObj := baselineIR(nil)
	replicas := int32(7)
	newObj.Spec.Replicas = &replicas
	resp := v.Handle(context.Background(), updateReq(t, old, newObj))

	g.Expect(resp.Allowed).To(gomega.BeFalse(),
		"spec edits without the annotation must reject")
}

func TestHandle_UpdateControllerWriteWithSpecEdit_Allowed(t *testing.T) {
	g := gomega.NewWithT(t)
	v := &Validator{Decoder: newDecoder(t)}
	old := baselineIR(withControllerWrite())
	newObj := baselineIR(withControllerWrite())
	// Controller's normal flow: bumps replicas as part of an HPA-driven
	// scale-up patch. Annotation stays in place.
	replicas := int32(3)
	newObj.Spec.Replicas = &replicas
	resp := v.Handle(context.Background(), updateReq(t, old, newObj))

	g.Expect(resp.Allowed).To(gomega.BeTrue(),
		"controller updates (annotation present) should pass even when spec.replicas changes")
}

func TestHandle_UpdateChangesParentRef_Denied(t *testing.T) {
	g := gomega.NewWithT(t)
	v := &Validator{Decoder: newDecoder(t)}
	old := baselineIR(withControllerWrite())
	newObj := baselineIR(withControllerWrite())
	newObj.Spec.ParentRef.Name = "different-isvc"
	resp := v.Handle(context.Background(), updateReq(t, old, newObj))

	g.Expect(resp.Allowed).To(gomega.BeFalse(),
		"spec.parentRef is immutable even from the controller")
	g.Expect(resp.Result.Message).To(gomega.ContainSubstring("spec.parentRef is immutable"))
}

func TestHandle_UpdateChangesComponent_Denied(t *testing.T) {
	g := gomega.NewWithT(t)
	v := &Validator{Decoder: newDecoder(t)}
	old := baselineIR(withControllerWrite())
	newObj := baselineIR(withControllerWrite())
	newObj.Spec.Component = v1beta1.DecoderComponent
	resp := v.Handle(context.Background(), updateReq(t, old, newObj))

	g.Expect(resp.Allowed).To(gomega.BeFalse(),
		"spec.component is immutable; moving a workload between slots requires recreation")
	g.Expect(resp.Result.Message).To(gomega.ContainSubstring("spec.component is immutable"))
}

func TestHandle_UpdateChangesRunners_Allowed_WithAnnotation(t *testing.T) {
	g := gomega.NewWithT(t)
	v := &Validator{Decoder: newDecoder(t)}
	old := baselineIR(withControllerWrite())
	newObj := baselineIR(withControllerWrite())
	// The controller rerenders Runners on every reconcile (image bumps,
	// env injection, etc.). This MUST be allowed when the annotation is
	// present.
	newObj.Spec.Runners[0].Template.Spec.Containers[0].Image = "sgl:2.0"
	resp := v.Handle(context.Background(), updateReq(t, old, newObj))

	g.Expect(resp.Allowed).To(gomega.BeTrue(),
		"controller rerenders Runners on every reconcile; must be allowed with annotation")
}

func TestHandle_DeleteOperation_Allowed(t *testing.T) {
	g := gomega.NewWithT(t)
	v := &Validator{Decoder: newDecoder(t)}
	// Delete is not in the kubebuilder verbs list, but defensive: if a
	// future webhook config widens scope, deletes should pass (operators
	// may need to recover from a stuck CR).
	req := admission.Request{AdmissionRequest: admissionv1.AdmissionRequest{
		Operation: admissionv1.Delete,
		Namespace: "prod-models",
		Name:      "llama-engine",
	}}
	resp := v.Handle(context.Background(), req)

	g.Expect(resp.Allowed).To(gomega.BeTrue(),
		"delete operations are not gated by this webhook")
}

func TestHandle_MalformedObject_Errored(t *testing.T) {
	g := gomega.NewWithT(t)
	v := &Validator{Decoder: newDecoder(t)}
	req := admission.Request{AdmissionRequest: admissionv1.AdmissionRequest{
		Operation: admissionv1.Create,
		Namespace: "prod-models",
		Name:      "llama-engine",
		Object:    runtime.RawExtension{Raw: []byte(`{not json`)},
	}}
	resp := v.Handle(context.Background(), req)

	g.Expect(resp.Allowed).To(gomega.BeFalse(),
		"malformed object body should surface as an Errored response, not Denied")
}
