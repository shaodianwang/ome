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

// withReplicas returns a clone of `ir` with Replicas set; convenience
// for the Update-path table cases that need a spec diff.
func withReplicas(ir *v1beta1.InferenceReplica, n int32) *v1beta1.InferenceReplica {
	out := ir.DeepCopy()
	out.Spec.Replicas = &n
	return out
}

// withComponent returns a clone with the Component slot changed.
func withComponent(ir *v1beta1.InferenceReplica, c v1beta1.ComponentType) *v1beta1.InferenceReplica {
	out := ir.DeepCopy()
	out.Spec.Component = c
	return out
}

// withParent returns a clone whose parentRef points at a different ISVC.
func withParent(ir *v1beta1.InferenceReplica, name string) *v1beta1.InferenceReplica {
	out := ir.DeepCopy()
	out.Spec.ParentRef.Name = name
	return out
}

// withImage returns a clone whose first runner uses a different image.
func withImage(ir *v1beta1.InferenceReplica, image string) *v1beta1.InferenceReplica {
	out := ir.DeepCopy()
	out.Spec.Runners[0].Template.Spec.Containers[0].Image = image
	return out
}

// TestHandle covers every gate in pkg/webhook/admission/inferencereplica.
//
// Each row builds an admission request from `req(t)`, runs Handle, and
// asserts Allowed plus optional rejection substring(s). Cases not built
// via createReq/updateReq use the inline `req` builder; that's how
// Delete and the malformed-body cases stay in the same table.
func TestHandle(t *testing.T) {
	wcw := withControllerWrite() // baseline annotation; "controller write"

	tests := []struct {
		name         string
		req          func(t *testing.T) admission.Request
		wantAllowed  bool
		wantContains []string // optional substring matches against resp.Result.Message
	}{
		{
			name:         "create without annotation → denied",
			req:          func(t *testing.T) admission.Request { return createReq(t, baselineIR(nil)) },
			wantAllowed:  false,
			wantContains: []string{"controller-only resource", constants.InferenceReplicaControllerWriteAnnotationKey},
		},
		{
			name:        "create with empty annotation map → denied (same as nil)",
			req:         func(t *testing.T) admission.Request { return createReq(t, baselineIR(map[string]string{})) },
			wantAllowed: false,
		},
		{
			name: `create with annotation value other than literal "true" → denied`,
			req: func(t *testing.T) admission.Request {
				return createReq(t, baselineIR(map[string]string{
					constants.InferenceReplicaControllerWriteAnnotationKey: "yes",
				}))
			},
			wantAllowed: false,
		},
		{
			name:        "create with controller-write annotation → allowed",
			req:         func(t *testing.T) admission.Request { return createReq(t, baselineIR(wcw)) },
			wantAllowed: true,
		},
		{
			name: "update dropping annotation → denied",
			req: func(t *testing.T) admission.Request {
				return updateReq(t, baselineIR(wcw), baselineIR(nil))
			},
			wantAllowed: false,
		},
		{
			name: "update with spec change AND dropped annotation → denied",
			req: func(t *testing.T) admission.Request {
				return updateReq(t, baselineIR(wcw), withReplicas(baselineIR(nil), 7))
			},
			wantAllowed: false,
		},
		{
			name: "update with spec change (controller write) → allowed",
			req: func(t *testing.T) admission.Request {
				return updateReq(t, baselineIR(wcw), withReplicas(baselineIR(wcw), 3))
			},
			wantAllowed: true,
		},
		{
			name: "update changing spec.parentRef → denied",
			req: func(t *testing.T) admission.Request {
				return updateReq(t, baselineIR(wcw), withParent(baselineIR(wcw), "different-isvc"))
			},
			wantAllowed:  false,
			wantContains: []string{"spec.parentRef is immutable"},
		},
		{
			name: "update changing spec.component → denied",
			req: func(t *testing.T) admission.Request {
				return updateReq(t, baselineIR(wcw), withComponent(baselineIR(wcw), v1beta1.DecoderComponent))
			},
			wantAllowed:  false,
			wantContains: []string{"spec.component is immutable"},
		},
		{
			name: "update rerendering runners (controller write) → allowed",
			req: func(t *testing.T) admission.Request {
				return updateReq(t, baselineIR(wcw), withImage(baselineIR(wcw), "sgl:2.0"))
			},
			wantAllowed: true,
		},
		{
			name: "delete (not registered for this webhook) → allowed",
			req: func(t *testing.T) admission.Request {
				return admission.Request{AdmissionRequest: admissionv1.AdmissionRequest{
					Operation: admissionv1.Delete,
					Namespace: "prod-models",
					Name:      "llama-engine",
				}}
			},
			wantAllowed: true,
		},
		{
			name: "malformed body → errored (Allowed=false)",
			req: func(t *testing.T) admission.Request {
				return admission.Request{AdmissionRequest: admissionv1.AdmissionRequest{
					Operation: admissionv1.Create,
					Namespace: "prod-models",
					Name:      "llama-engine",
					Object:    runtime.RawExtension{Raw: []byte(`{not json`)},
				}}
			},
			wantAllowed: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			g := gomega.NewWithT(t)
			v := &Validator{Decoder: newDecoder(t)}
			resp := v.Handle(context.Background(), tc.req(t))
			g.Expect(resp.Allowed).To(gomega.Equal(tc.wantAllowed))
			for _, sub := range tc.wantContains {
				g.Expect(resp.Result.Message).To(gomega.ContainSubstring(sub))
			}
		})
	}
}
