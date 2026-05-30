package inferencereplica

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/onsi/gomega"
	admissionv1 "k8s.io/api/admission/v1"
	authenticationv1 "k8s.io/api/authentication/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	kedav1 "github.com/kedacore/keda/v2/apis/keda/v1alpha1"

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

// withParentUID returns a clone whose parentRef.UID is rewritten — same
// name, different UID. This is the exact failure mode the immutability
// gate exists for: a stale ISVC controller cache stamping the wrong UID
// onto a live IR after the parent was recreated under the same name.
func withParentUID(ir *v1beta1.InferenceReplica, uid types.UID) *v1beta1.InferenceReplica {
	out := ir.DeepCopy()
	out.Spec.ParentRef.UID = uid
	return out
}

// withPacingPartition returns a clone with spec.pacing.partition set
// (creating the Pacing block if nil).
func withPacingPartition(ir *v1beta1.InferenceReplica, partition int32) *v1beta1.InferenceReplica {
	out := ir.DeepCopy()
	if out.Spec.Pacing == nil {
		out.Spec.Pacing = &v1beta1.InferenceReplicaPacing{}
	}
	out.Spec.Pacing.Partition = &partition
	return out
}

// withReplicasAndPacingPartition returns a clone with both spec.replicas
// and spec.pacing.partition set. Used by the over-Partition table cases.
func withReplicasAndPacingPartition(ir *v1beta1.InferenceReplica, replicas, partition int32) *v1beta1.InferenceReplica {
	out := withReplicas(ir, replicas)
	if out.Spec.Pacing == nil {
		out.Spec.Pacing = &v1beta1.InferenceReplicaPacing{}
	}
	out.Spec.Pacing.Partition = &partition
	return out
}

// withImage returns a clone whose first runner uses a different image.
func withImage(ir *v1beta1.InferenceReplica, image string) *v1beta1.InferenceReplica {
	out := ir.DeepCopy()
	out.Spec.Runners[0].Template.Spec.Containers[0].Image = image
	return out
}

// withAutoscaler returns a clone with the Autoscaler block set on
// spec.autoscaler. Used by the OEP-0013 IR-side webhook shape tests.
func withAutoscaler(ir *v1beta1.InferenceReplica, as *v1beta1.ComponentAutoscaler) *v1beta1.InferenceReplica {
	out := ir.DeepCopy()
	out.Spec.Autoscaler = as
	return out
}

// withReplicasAndAutoscaler returns a clone with both spec.replicas
// and spec.autoscaler set. Used to test the KEDA
// IdleReplicaCount-vs-Replicas (IR-side floor) check.
func withReplicasAndAutoscaler(ir *v1beta1.InferenceReplica, n int32, as *v1beta1.ComponentAutoscaler) *v1beta1.InferenceReplica {
	out := ir.DeepCopy()
	out.Spec.Replicas = &n
	out.Spec.Autoscaler = as
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
		// OEP-0013: IR-side defense-in-depth on spec.autoscaler shape.
		// Per Decision Q4 the IR webhook does NOT block external writes
		// to spec.autoscaler (the /scale subresource only mutates
		// spec.replicas), but any successful write must carry a
		// shape-valid Autoscaler block.
		{
			name: "create with valid hpa autoscaler → allowed",
			req: func(t *testing.T) admission.Request {
				return createReq(t, withAutoscaler(baselineIR(wcw),
					&v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerHPA}))
			},
			wantAllowed: true,
		},
		{
			name: "create with valid keda autoscaler (1 trigger) → allowed",
			req: func(t *testing.T) admission.Request {
				return createReq(t, withAutoscaler(baselineIR(wcw),
					&v1beta1.ComponentAutoscaler{
						Class: v1beta1.AutoscalerKEDA,
						Keda: &v1beta1.KedaAutoscaler{Triggers: []kedav1.ScaleTriggers{{
							Type:     "prometheus",
							Metadata: map[string]string{"query": "x", "threshold": "1", "serverAddress": "http://p:9090"},
						}}},
					}))
			},
			wantAllowed: true,
		},
		{
			name: "create with keda + empty Triggers → denied",
			req: func(t *testing.T) admission.Request {
				return createReq(t, withAutoscaler(baselineIR(wcw),
					&v1beta1.ComponentAutoscaler{
						Class: v1beta1.AutoscalerKEDA,
						Keda:  &v1beta1.KedaAutoscaler{Triggers: nil},
					}))
			},
			wantAllowed:  false,
			wantContains: []string{"class=keda requires at least 1 trigger"},
		},
		{
			name: "create with hpa Type=Resource, Resource=nil → denied",
			req: func(t *testing.T) admission.Request {
				return createReq(t, withAutoscaler(baselineIR(wcw),
					&v1beta1.ComponentAutoscaler{
						Class: v1beta1.AutoscalerHPA,
						HPA: &v1beta1.HPAAutoscaler{Metrics: []autoscalingv2.MetricSpec{{
							Type: autoscalingv2.ResourceMetricSourceType,
						}}},
					}))
			},
			wantAllowed:  false,
			wantContains: []string{"type=Resource but"},
		},
		{
			name: "create with keda IdleReplicaCount >= IR replicas floor → denied",
			req: func(t *testing.T) admission.Request {
				return createReq(t, withReplicasAndAutoscaler(baselineIR(wcw), 2,
					&v1beta1.ComponentAutoscaler{
						Class: v1beta1.AutoscalerKEDA,
						Keda: &v1beta1.KedaAutoscaler{
							Triggers: []kedav1.ScaleTriggers{{
								Type:     "prometheus",
								Metadata: map[string]string{"query": "x", "threshold": "1", "serverAddress": "http://p:9090"},
							}},
							IdleReplicaCount: ptr.To[int32](2),
						},
					}))
			},
			wantAllowed:  false,
			wantContains: []string{"keda.idleReplicaCount must be < minReplicas"},
		},
		{
			name: "create with bogus class value → denied (defense-in-depth)",
			req: func(t *testing.T) admission.Request {
				return createReq(t, withAutoscaler(baselineIR(wcw),
					&v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerClass("knative")}))
			},
			wantAllowed:  false,
			wantContains: []string{"is not one of hpa|keda|external|none"},
		},
		{
			name: "create with valid external class → allowed",
			req: func(t *testing.T) admission.Request {
				return createReq(t, withAutoscaler(baselineIR(wcw),
					&v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerExternal}))
			},
			wantAllowed: true,
		},
		{
			name: "create with valid none class → allowed",
			req: func(t *testing.T) admission.Request {
				return createReq(t, withAutoscaler(baselineIR(wcw),
					&v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerNone}))
			},
			wantAllowed: true,
		},
		// Review §6 (fourth bullet): immutability gate runs BEFORE the
		// controller-write annotation gate. A misbehaving controller
		// (stale ISVC cache) that stamps the annotation MUST NOT also be
		// able to flip spec.parentRef.UID to the wrong parent or move
		// the IR between Component slots. Pre-fix the annotation gate
		// ran first, so an annotation-true + UID-rewrite payload slipped
		// through; post-fix the immutability error wins.
		{
			name: "update with annotation, same parentRef → allowed (baseline)",
			req: func(t *testing.T) admission.Request {
				return updateReq(t, baselineIR(wcw), baselineIR(wcw))
			},
			wantAllowed: true,
		},
		{
			name: "update with annotation, different parentRef.UID → denied (regression: was allowed pre-fix)",
			req: func(t *testing.T) admission.Request {
				return updateReq(t, baselineIR(wcw),
					withParentUID(baselineIR(wcw), types.UID("ffffffff-ffff-ffff-ffff-ffffffffffff")))
			},
			wantAllowed:  false,
			wantContains: []string{"spec.parentRef is immutable"},
		},
		{
			name: "update with annotation, different parentRef.Name → denied (regression: was allowed pre-fix)",
			req: func(t *testing.T) admission.Request {
				return updateReq(t, baselineIR(wcw), withParent(baselineIR(wcw), "different-isvc"))
			},
			wantAllowed:  false,
			wantContains: []string{"spec.parentRef is immutable"},
		},
		{
			name: "update with annotation, different component → denied (regression: was allowed pre-fix)",
			req: func(t *testing.T) admission.Request {
				return updateReq(t, baselineIR(wcw),
					withComponent(baselineIR(wcw), v1beta1.DecoderComponent))
			},
			wantAllowed:  false,
			wantContains: []string{"spec.component is immutable"},
		},
		{
			name: "update without annotation, same parentRef → denied (annotation gate)",
			req: func(t *testing.T) admission.Request {
				return updateReq(t, baselineIR(wcw), baselineIR(nil))
			},
			wantAllowed:  false,
			wantContains: []string{"controller-only resource"},
		},
		{
			name: "update without annotation, different parentRef.UID → denied (immutability fires first)",
			req: func(t *testing.T) admission.Request {
				return updateReq(t, baselineIR(wcw),
					withParentUID(baselineIR(nil), types.UID("ffffffff-ffff-ffff-ffff-ffffffffffff")))
			},
			wantAllowed:  false,
			wantContains: []string{"spec.parentRef is immutable"},
		},
		// Review §6 (second bullet): Pacing.Partition must be <=
		// effective Replicas. Over-Partition silently freezes the
		// whole replica set forever (rollout engine treats Partition
		// as a per-index threshold).
		{
			name: "create with pacing nil → allowed (no partition to validate)",
			req: func(t *testing.T) admission.Request {
				return createReq(t, baselineIR(wcw))
			},
			wantAllowed: true,
		},
		{
			name: "create with pacing.partition nil → allowed",
			req: func(t *testing.T) admission.Request {
				ir := baselineIR(wcw)
				ir.Spec.Pacing = &v1beta1.InferenceReplicaPacing{}
				return createReq(t, ir)
			},
			wantAllowed: true,
		},
		{
			name: "create with partition == replicas → allowed (boundary)",
			req: func(t *testing.T) admission.Request {
				return createReq(t, withReplicasAndPacingPartition(baselineIR(wcw), 3, 3))
			},
			wantAllowed: true,
		},
		{
			name: "create with partition < replicas → allowed",
			req: func(t *testing.T) admission.Request {
				return createReq(t, withReplicasAndPacingPartition(baselineIR(wcw), 5, 2))
			},
			wantAllowed: true,
		},
		{
			name: "create with partition > replicas → denied",
			req: func(t *testing.T) admission.Request {
				return createReq(t, withReplicasAndPacingPartition(baselineIR(wcw), 3, 5))
			},
			wantAllowed:  false,
			wantContains: []string{"spec.pacing.partition", "must be <= spec.replicas"},
		},
		{
			name: "create with partition=2 and replicas nil → denied (defaults to 1)",
			req: func(t *testing.T) admission.Request {
				return createReq(t, withPacingPartition(baselineIR(wcw), 2))
			},
			wantAllowed:  false,
			wantContains: []string{"spec.pacing.partition (2)", "spec.replicas (1)"},
		},
		{
			name: "create with partition=0 (default canary) and replicas=5 → allowed",
			req: func(t *testing.T) admission.Request {
				return createReq(t, withReplicasAndPacingPartition(baselineIR(wcw), 5, 0))
			},
			wantAllowed: true,
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
