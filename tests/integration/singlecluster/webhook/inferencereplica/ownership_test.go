package inferencereplica

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	v1beta1testing "github.com/ome-projects/ome/pkg/utils/testing/v1beta1"
	"github.com/ome-projects/ome/tests/integration/util"
)

// bareIR is a standalone replica: rendered runners, no parent, no owner.
func bareIR(namespace, name string, component v1beta1.ComponentType) *v1beta1.InferenceReplica {
	return &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec: v1beta1.InferenceReplicaSpec{
			Component: component,
			Runners: []v1beta1.Runner{{
				Name: v1beta1.RunnerNameDefault,
				Size: 1,
				Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{
					Containers: []corev1.Container{{Name: "server", Image: "example.com/serving:1.0"}},
				}},
			}},
		},
	}
}

// refsIR is a standalone replica that renders from the named
// ClusterServingRuntime instead of stored runners.
func refsIR(namespace, name string, component v1beta1.ComponentType, runtime string) *v1beta1.InferenceReplica {
	return &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec: v1beta1.InferenceReplicaSpec{
			Component:  component,
			RuntimeRef: &v1beta1.ServingRuntimeRef{Name: runtime},
		},
	}
}

// projectedIR is a replica as the InferenceService controller projects it:
// controller owner reference and parentRef naming the parent.
func projectedIR(namespace, parent string) *v1beta1.InferenceReplica {
	ir := bareIR(namespace, parent+"-engine", v1beta1.EngineComponent)
	ir.Spec.ParentRef = &v1beta1.ParentReference{Name: parent}
	ir.OwnerReferences = []metav1.OwnerReference{{
		APIVersion: v1beta1.SchemeGroupVersion.String(), Kind: "InferenceService",
		Name: parent, UID: "00000000-0000-4000-8000-000000000001", Controller: ptr.To(true),
	}}
	return ir
}

var _ = ginkgo.Describe("InferenceReplica ownership admission", func() {
	var ns *corev1.Namespace

	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "ir-admission-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("lets a user create and edit a standalone replica but not its controller-only fields", func() {
		ir := bareIR(ns.Name, "pool-a", v1beta1.EngineComponent)
		Expect(userClient.Create(suiteCtx, ir)).To(Succeed())

		fresh := &v1beta1.InferenceReplica{}
		Expect(userClient.Get(suiteCtx, client.ObjectKeyFromObject(ir), fresh)).To(Succeed())
		fresh.Spec.Runners[0].Template.Spec.Containers[0].Image = "example.com/serving:1.1"
		Expect(userClient.Update(suiteCtx, fresh)).To(Succeed())

		Expect(userClient.Get(suiteCtx, client.ObjectKeyFromObject(ir), fresh)).To(Succeed())
		fresh.Spec.PlacementReplicaLimit = ptr.To(int32(2))
		err := userClient.Update(suiteCtx, fresh)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("written only by the InferenceService controller"))
	})

	ginkgo.It("rejects a standalone replica that carries parentRef", func() {
		ir := bareIR(ns.Name, "pool-b", v1beta1.EngineComponent)
		ir.Spec.ParentRef = &v1beta1.ParentReference{Name: "someone-else"}
		err := userClient.Create(suiteCtx, ir)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("omit it on a standalone"))
	})

	ginkgo.It("admits projected replicas only from the controller identity", func() {
		ir := projectedIR(ns.Name, "svc")
		err := userClient.Create(suiteCtx, ir)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("projected by InferenceService"))

		Expect(k8sClient.Create(suiteCtx, ir)).To(Succeed())

		fresh := &v1beta1.InferenceReplica{}
		Expect(userClient.Get(suiteCtx, client.ObjectKeyFromObject(ir), fresh)).To(Succeed())
		fresh.Spec.Replicas = ptr.To(int32(3))
		err = userClient.Update(suiteCtx, fresh)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("projected by InferenceService"))

		Expect(userClient.Get(suiteCtx, client.ObjectKeyFromObject(ir), fresh)).To(Succeed())
		fresh.Annotations = map[string]string{"ome.io/reset-instances": "all"}
		Expect(userClient.Update(suiteCtx, fresh)).To(Succeed())

		Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(ir), fresh)).To(Succeed())
		fresh.Spec.Replicas = ptr.To(int32(3))
		Expect(k8sClient.Update(suiteCtx, fresh)).To(Succeed())
	})

	ginkgo.It("admits a finalizer add and remove on a projected replica from a plain user", func() {
		ir := projectedIR(ns.Name, "svc")
		Expect(k8sClient.Create(suiteCtx, ir)).To(Succeed())

		fresh := &v1beta1.InferenceReplica{}
		Expect(userClient.Get(suiteCtx, client.ObjectKeyFromObject(ir), fresh)).To(Succeed())
		fresh.Finalizers = append(fresh.Finalizers, "example.com/hold")
		Expect(userClient.Update(suiteCtx, fresh)).To(Succeed())
		Expect(userClient.Get(suiteCtx, client.ObjectKeyFromObject(ir), fresh)).To(Succeed())
		fresh.Finalizers = nil
		Expect(userClient.Update(suiteCtx, fresh)).To(Succeed())
	})

	ginkgo.It("rejects a standalone replica named like an InferenceService", func() {
		isvc := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "shared", Namespace: ns.Name}}
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())
		err := userClient.Create(suiteCtx, bareIR(ns.Name, "shared", v1beta1.EngineComponent))
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("would share pod and Service names"))
	})

	ginkgo.It("rejects runners together with refs on a standalone replica", func() {
		ir := bareIR(ns.Name, "pool-a", v1beta1.EngineComponent)
		ir.Spec.ModelRef = &v1beta1.ModelRef{Name: "model-a"}
		err := userClient.Create(suiteCtx, ir)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("exclusive"))
	})

	ginkgo.It("rejects a runtime that lacks the replica's piece", func() {
		runtime := v1beta1testing.MakeClusterServingRuntime(ns.Name + "-runtime").
			EngineRunner(corev1.Container{Name: "ome-container", Image: "example.com/serving:1.0"}).Obj()
		Expect(k8sClient.Create(suiteCtx, runtime)).To(Succeed())
		ginkgo.DeferCleanup(func() {
			Expect(client.IgnoreNotFound(k8sClient.Delete(suiteCtx, runtime))).To(Succeed())
		})

		err := userClient.Create(suiteCtx, refsIR(ns.Name, "pool-a", v1beta1.DecoderComponent, runtime.Name))
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring(`ClusterServingRuntime "` + runtime.Name + `" has no decoder piece`))

		Expect(userClient.Create(suiteCtx, refsIR(ns.Name, "pool-b", v1beta1.EngineComponent, runtime.Name))).To(Succeed())
	})

	ginkgo.It("admits a standalone replica whose runtime is not found", func() {
		Expect(userClient.Create(suiteCtx, refsIR(ns.Name, "pool-a", v1beta1.DecoderComponent, ns.Name+"-missing"))).To(Succeed())
	})
})
