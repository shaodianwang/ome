package inferenceservice

import (
	kedav1 "github.com/kedacore/keda/v2/apis/keda/v1alpha1"
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	autoscalingv1 "k8s.io/api/autoscaling/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	isvcutils "github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/utils"
	v1beta1testing "github.com/ome-projects/ome/pkg/utils/testing/v1beta1"

	"github.com/ome-projects/ome/tests/integration/util"
)

// The OMENative path dispatches per-Component autoscalers against the
// InferenceReplica: the HPA / ScaledObject is named after the IR, owner-refed
// to the IR, and targets the IR's /scale subresource. These specs drive the
// full ISVC reconcile loop against a real apiserver, so they cover the CRD
// scale subresource, owner-ref GC wiring, and status mirroring that the
// fake-client unit tests cannot.
var _ = ginkgo.Describe("InferenceService OMENative autoscaler dispatch", func() {
	var ns *corev1.Namespace

	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "autoscal-ir-")
		Expect(err).NotTo(HaveOccurred())
	})

	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("typed HPA targets the InferenceReplica scale subresource with the ISVC bounds", func() {
		minReplicas := 2
		fixture := createAutoscalIRFixture(ns.Name, "autoscal-hpa", &minReplicas, 5,
			&v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerHPA}, nil)

		ir := expectAutoscalIR(fixture)
		hpa := expectAutoscalIRHPA(fixture)
		Expect(hpa.Spec.ScaleTargetRef.APIVersion).To(Equal(v1beta1.SchemeGroupVersion.String()))
		Expect(hpa.Spec.ScaleTargetRef.Kind).To(Equal("InferenceReplica"))
		Expect(hpa.Spec.ScaleTargetRef.Name).To(Equal(fixture.irName))
		Expect(hpa.Spec.MinReplicas).NotTo(BeNil())
		Expect(*hpa.Spec.MinReplicas).To(Equal(int32(2)))
		Expect(hpa.Spec.MaxReplicas).To(Equal(int32(5)))
		expectAutoscalIRControllerOwner(hpa, ir)

		expectAutoscalIRScaledObjectAbsent(fixture)
		waitForAutoscalIRStatus(fixture, v1beta1.AutoscalerHPA, v1beta1.AutoscalerManagedByOME, "isvc")
	})

	ginkgo.It("typed KEDA creates an InferenceReplica-target ScaledObject and no HPA", func() {
		minReplicas := 1
		fixture := createAutoscalIRFixture(ns.Name, "autoscal-keda", &minReplicas, 3,
			autoscalIRKEDAAutoscaler(), nil)

		ir := expectAutoscalIR(fixture)
		so := expectAutoscalIRScaledObject(fixture)
		Expect(so.Spec.ScaleTargetRef).NotTo(BeNil())
		Expect(so.Spec.ScaleTargetRef.APIVersion).To(Equal(v1beta1.SchemeGroupVersion.String()))
		Expect(so.Spec.ScaleTargetRef.Kind).To(Equal("InferenceReplica"))
		Expect(so.Spec.ScaleTargetRef.Name).To(Equal(fixture.irName))
		Expect(so.Spec.MinReplicaCount).NotTo(BeNil())
		Expect(*so.Spec.MinReplicaCount).To(Equal(int32(1)))
		Expect(so.Spec.MaxReplicaCount).NotTo(BeNil())
		Expect(*so.Spec.MaxReplicaCount).To(Equal(int32(3)))
		expectAutoscalIRControllerOwner(so, ir)

		expectAutoscalIRHPAAbsent(fixture)
		waitForAutoscalIRStatus(fixture, v1beta1.AutoscalerKEDA, v1beta1.AutoscalerManagedByOME, "isvc")
	})

	for _, tt := range []struct {
		name      string
		prefix    string
		class     v1beta1.AutoscalerClass
		managedBy v1beta1.AutoscalerManagedBy
	}{
		{
			name:      "typed None leaves both OME autoscaler resources absent",
			prefix:    "autoscal-none",
			class:     v1beta1.AutoscalerNone,
			managedBy: v1beta1.AutoscalerManagedByNone,
		},
		{
			name:      "typed External leaves both OME autoscaler resources absent",
			prefix:    "autoscal-ext",
			class:     v1beta1.AutoscalerExternal,
			managedBy: v1beta1.AutoscalerManagedByExternal,
		},
	} {
		tt := tt
		ginkgo.It(tt.name, func() {
			minReplicas := 1
			fixture := createAutoscalIRFixture(ns.Name, tt.prefix, &minReplicas, 3,
				&v1beta1.ComponentAutoscaler{Class: tt.class}, nil)

			waitForAutoscalIRStatus(fixture, tt.class, tt.managedBy, "isvc")
			expectAutoscalIRHPAAbsent(fixture)
			expectAutoscalIRScaledObjectAbsent(fixture)
		})
	}

	ginkgo.It("runtime-inherited KEDA dispatches for the IR and reports the runtime spec source", func() {
		minReplicas := 1
		fixture := createAutoscalIRFixture(ns.Name, "autoscal-rt", &minReplicas, 3,
			nil, autoscalIRKEDAAutoscaler())

		ir := expectAutoscalIR(fixture)
		so := expectAutoscalIRScaledObject(fixture)
		Expect(so.Spec.ScaleTargetRef.Kind).To(Equal("InferenceReplica"))
		Expect(so.Spec.ScaleTargetRef.Name).To(Equal(fixture.irName))
		expectAutoscalIRControllerOwner(so, ir)
		expectAutoscalIRHPAAbsent(fixture)
		waitForAutoscalIRStatus(fixture, v1beta1.AutoscalerKEDA, v1beta1.AutoscalerManagedByOME, "runtime")
	})

	ginkgo.It("omitted replica bounds resolve to the configured defaults on the IR and its HPA", func() {
		// The stored spec carries no bounds; the reconciler applies the
		// seeded deploy.replicas (min=1, engine max=3) to the merged spec.
		fixture := createAutoscalIRFixture(ns.Name, "autoscal-floor", nil, 0,
			&v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerHPA}, nil)

		ir := expectAutoscalIR(fixture)
		Expect(ir.Spec.Replicas).NotTo(BeNil())
		Expect(*ir.Spec.Replicas).To(Equal(int32(1)))

		hpa := expectAutoscalIRHPA(fixture)
		Expect(hpa.Spec.MinReplicas).NotTo(BeNil())
		Expect(*hpa.Spec.MinReplicas).To(Equal(int32(1)))
		Expect(hpa.Spec.MaxReplicas).To(Equal(int32(3)))
	})

	ginkgo.It("class transition HPA to KEDA to None removes the stale scaler at each step", func() {
		minReplicas := 1
		fixture := createAutoscalIRFixture(ns.Name, "autoscal-switch", &minReplicas, 3,
			&v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerHPA}, nil)

		ir := expectAutoscalIR(fixture)
		hpa := expectAutoscalIRHPA(fixture)
		expectAutoscalIRControllerOwner(hpa, ir)

		updateAutoscalIRAutoscaler(fixture, autoscalIRKEDAAutoscaler())
		so := expectAutoscalIRScaledObject(fixture)
		expectAutoscalIRControllerOwner(so, ir)
		expectAutoscalIRHPAAbsent(fixture)
		waitForAutoscalIRStatus(fixture, v1beta1.AutoscalerKEDA, v1beta1.AutoscalerManagedByOME, "isvc")

		updateAutoscalIRAutoscaler(fixture, &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerNone})
		expectAutoscalIRHPAAbsent(fixture)
		expectAutoscalIRScaledObjectAbsent(fixture)
		waitForAutoscalIRStatus(fixture, v1beta1.AutoscalerNone, v1beta1.AutoscalerManagedByNone, "isvc")
	})

	ginkgo.It("scale subresource writes survive reconciles for a managed class and revert on None", func() {
		minReplicas := 2
		fixture := createAutoscalIRFixture(ns.Name, "autoscal-rt-trip", &minReplicas, 5,
			&v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerHPA}, nil)

		ir := expectAutoscalIR(fixture)
		Expect(ir.Spec.Replicas).NotTo(BeNil())
		Expect(*ir.Spec.Replicas).To(Equal(int32(2)))
		expectAutoscalIRHPA(fixture)

		ginkgo.By("scaling the InferenceReplica to 4 through /scale, as an autoscaler would")
		Eventually(func(g Gomega) {
			live := &v1beta1.InferenceReplica{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{
				Namespace: fixture.isvc.Namespace,
				Name:      fixture.irName,
			}, live)).To(Succeed())
			scale := &autoscalingv1.Scale{}
			g.Expect(k8sClient.SubResource("scale").Get(suiteCtx, live, scale)).To(Succeed())
			g.Expect(scale.Spec.Replicas).To(Equal(int32(2)))
			scale.Spec.Replicas = 4
			g.Expect(k8sClient.SubResource("scale").Update(suiteCtx, live, client.WithSubResourceBody(scale))).To(Succeed())
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("forcing a full ISVC reprojection and confirming it lands on the IR")
		nudgeKey := "example.com/autoscal-nudge"
		Eventually(func() error {
			latest := &v1beta1.InferenceService{}
			if err := k8sClient.Get(suiteCtx, types.NamespacedName{
				Namespace: fixture.isvc.Namespace,
				Name:      fixture.isvc.Name,
			}, latest); err != nil {
				return err
			}
			if latest.Annotations == nil {
				latest.Annotations = map[string]string{}
			}
			latest.Annotations[nudgeKey] = "reprojected"
			return k8sClient.Update(suiteCtx, latest)
		}).WithTimeout(util.ShortTimeout).WithPolling(util.PollInterval).Should(Succeed())
		Eventually(func(g Gomega) {
			live := &v1beta1.InferenceReplica{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{
				Namespace: fixture.isvc.Namespace,
				Name:      fixture.irName,
			}, live)).To(Succeed())
			g.Expect(live.Spec.Runners).NotTo(BeEmpty())
			g.Expect(live.Spec.Runners[0].Template.Annotations).To(HaveKeyWithValue(nudgeKey, "reprojected"))
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("asserting the reprojection preserved the autoscaler-written count")
		Consistently(func(g Gomega) {
			live := &v1beta1.InferenceReplica{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{
				Namespace: fixture.isvc.Namespace,
				Name:      fixture.irName,
			}, live)).To(Succeed())
			g.Expect(live.Spec.Replicas).NotTo(BeNil())
			g.Expect(*live.Spec.Replicas).To(Equal(int32(4)))
		}).WithTimeout(util.ShortTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("switching to None so the ISVC controller reclaims the replica count")
		updateAutoscalIRAutoscaler(fixture, &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerNone})
		expectAutoscalIRHPAAbsent(fixture)
		Eventually(func(g Gomega) {
			live := &v1beta1.InferenceReplica{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{
				Namespace: fixture.isvc.Namespace,
				Name:      fixture.irName,
			}, live)).To(Succeed())
			g.Expect(live.Spec.Replicas).NotTo(BeNil())
			g.Expect(*live.Spec.Replicas).To(Equal(int32(2)))
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})
})

type autoscalIRFixture struct {
	isvc   *v1beta1.InferenceService
	irName string
}

// createAutoscalIRFixture creates a ClusterBaseModel + ClusterServingRuntime +
// OMENative InferenceService triple whose engine carries the given bounds and
// typed autoscaler; runtimeAutoscaler lands on the runtime's engineConfig so
// inheritance specs can leave the ISVC block nil.
func createAutoscalIRFixture(
	namespace string,
	prefix string,
	minReplicas *int,
	maxReplicas int,
	typedAutoscaler *v1beta1.ComponentAutoscaler,
	runtimeAutoscaler *v1beta1.ComponentAutoscaler,
) autoscalIRFixture {
	modelName := prefix + "-model"
	runtimeName := prefix + "-runtime"
	isvcName := prefix + "-isvc"

	model := v1beta1testing.MakeClusterBaseModel(modelName).
		StorageURI("oci://n/example/b/models/o/" + prefix).
		ModelFormat("safetensors").
		ModelArchitecture("LlamaForCausalLM").
		ModelFramework("Transformers").Obj()
	Expect(k8sClient.Create(suiteCtx, model)).To(Succeed())
	ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, model) })

	// The runtime deliberately carries no replica bounds: ISVC-omitted
	// bounds would otherwise inherit the runtime's, masking the floor
	// behavior the bounds specs pin.
	runtimeEngine := &v1beta1.EngineSpec{
		ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
			Autoscaler: copyRawTypedAutoscaler(runtimeAutoscaler),
		},
		Runner: &v1beta1.RunnerSpec{
			Container: corev1.Container{
				Name:  constants.MainContainerName,
				Image: "example.com/runtime:test",
				Ports: []corev1.ContainerPort{{Name: "http", ContainerPort: 8080}},
			},
		},
	}
	runtime := v1beta1testing.MakeClusterServingRuntime(runtimeName).
		SupportsModelFormat("safetensors", true, 10).
		WithLastFormatArchitecture("LlamaForCausalLM").
		EngineConfig(runtimeEngine).Obj()
	Expect(k8sClient.Create(suiteCtx, runtime)).To(Succeed())
	ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, runtime) })

	isvc := v1beta1testing.MakeInferenceService(isvcName, namespace).
		Model(modelName).
		Runtime(runtimeName).
		Engine(&v1beta1.EngineSpec{
			ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
				MinReplicas: minReplicas,
				MaxReplicas: maxReplicas,
				Autoscaler:  copyRawTypedAutoscaler(typedAutoscaler),
			},
		}).Obj()
	omeNativeMode := constants.OMENative
	isvc.Spec.DeploymentMode = &omeNativeMode
	Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())

	return autoscalIRFixture{
		isvc:   isvc,
		irName: isvcName + "-engine",
	}
}

func autoscalIRKEDAAutoscaler() *v1beta1.ComponentAutoscaler {
	return &v1beta1.ComponentAutoscaler{
		Class: v1beta1.AutoscalerKEDA,
		Keda: &v1beta1.KedaAutoscaler{
			Triggers: []kedav1.ScaleTriggers{{
				Type: "prometheus",
				Metadata: map[string]string{
					"serverAddress": "https://metrics.example.com",
					"query":         "requests_in_flight",
					"threshold":     "5",
				},
			}},
		},
	}
}

func expectAutoscalIR(fixture autoscalIRFixture) *v1beta1.InferenceReplica {
	ir := &v1beta1.InferenceReplica{}
	Eventually(func() error {
		return k8sClient.Get(suiteCtx, types.NamespacedName{
			Namespace: fixture.isvc.Namespace,
			Name:      fixture.irName,
		}, ir)
	}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	return ir
}

func expectAutoscalIRHPA(fixture autoscalIRFixture) *autoscalingv2.HorizontalPodAutoscaler {
	hpa := &autoscalingv2.HorizontalPodAutoscaler{}
	Eventually(func() error {
		return k8sClient.Get(suiteCtx, types.NamespacedName{
			Namespace: fixture.isvc.Namespace,
			Name:      fixture.irName,
		}, hpa)
	}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	return hpa
}

func expectAutoscalIRHPAAbsent(fixture autoscalIRFixture) {
	Eventually(func() bool {
		err := k8sClient.Get(suiteCtx, types.NamespacedName{
			Namespace: fixture.isvc.Namespace,
			Name:      fixture.irName,
		}, &autoscalingv2.HorizontalPodAutoscaler{})
		return apierrors.IsNotFound(err)
	}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(BeTrue())
}

func expectAutoscalIRScaledObject(fixture autoscalIRFixture) *kedav1.ScaledObject {
	so := &kedav1.ScaledObject{}
	Eventually(func() error {
		return k8sClient.Get(suiteCtx, types.NamespacedName{
			Namespace: fixture.isvc.Namespace,
			Name:      isvcutils.GetScaledObjectName(fixture.irName),
		}, so)
	}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	return so
}

func expectAutoscalIRScaledObjectAbsent(fixture autoscalIRFixture) {
	Eventually(func() bool {
		err := k8sClient.Get(suiteCtx, types.NamespacedName{
			Namespace: fixture.isvc.Namespace,
			Name:      isvcutils.GetScaledObjectName(fixture.irName),
		}, &kedav1.ScaledObject{})
		return apierrors.IsNotFound(err)
	}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(BeTrue())
}

// expectAutoscalIRControllerOwner asserts the scaler is controller-owned by
// the InferenceReplica so GC cascades the scaler with the IR.
func expectAutoscalIRControllerOwner(object metav1.Object, ir *v1beta1.InferenceReplica) {
	controller := metav1.GetControllerOf(object)
	Expect(controller).NotTo(BeNil())
	Expect(controller.APIVersion).To(Equal(v1beta1.SchemeGroupVersion.String()))
	Expect(controller.Kind).To(Equal("InferenceReplica"))
	Expect(controller.Name).To(Equal(ir.Name))
	Expect(controller.UID).To(Equal(ir.UID))
}

func waitForAutoscalIRStatus(
	fixture autoscalIRFixture,
	class v1beta1.AutoscalerClass,
	managedBy v1beta1.AutoscalerManagedBy,
	source string,
) {
	Eventually(func(g Gomega) {
		latest := &v1beta1.InferenceService{}
		g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{
			Namespace: fixture.isvc.Namespace,
			Name:      fixture.isvc.Name,
		}, latest)).To(Succeed())
		status, ok := latest.Status.Components[v1beta1.EngineComponent]
		g.Expect(ok).To(BeTrue())
		g.Expect(status.Autoscaler).NotTo(BeNil())
		g.Expect(status.Autoscaler.Class).To(Equal(class))
		g.Expect(status.Autoscaler.ManagedBy).To(Equal(managedBy))
		g.Expect(status.Autoscaler.SpecSource).To(Equal(source))
		g.Expect(status.ScaleTargetRef).NotTo(BeNil())
		g.Expect(status.ScaleTargetRef.APIVersion).To(Equal(v1beta1.SchemeGroupVersion.String()))
		g.Expect(status.ScaleTargetRef.Kind).To(Equal("InferenceReplica"))
		g.Expect(status.ScaleTargetRef.Name).To(Equal(fixture.irName))
	}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
}

func updateAutoscalIRAutoscaler(fixture autoscalIRFixture, autoscaler *v1beta1.ComponentAutoscaler) {
	Eventually(func() error {
		latest := &v1beta1.InferenceService{}
		if err := k8sClient.Get(suiteCtx, types.NamespacedName{
			Namespace: fixture.isvc.Namespace,
			Name:      fixture.isvc.Name,
		}, latest); err != nil {
			return err
		}
		latest.Spec.Engine.ComponentExtensionSpec.Autoscaler = copyRawTypedAutoscaler(autoscaler)
		return k8sClient.Update(suiteCtx, latest)
	}).WithTimeout(util.ShortTimeout).WithPolling(util.PollInterval).Should(Succeed())
}
