package inferenceservice

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	v1beta1testing "github.com/ome-projects/ome/pkg/utils/testing/v1beta1"
	"github.com/ome-projects/ome/tests/integration/util"
)

// Admission of the referenced form (spec.replicaRefs). Only the
// InferenceService validator runs in this suite, so the replicas are created
// as plain objects and never reconciled.

// standaloneReplica is a user-created replica of one component with a single
// rendered runner and no parent.
func standaloneReplica(namespace, name string, c v1beta1.ComponentType) *v1beta1.InferenceReplica {
	return &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec: v1beta1.InferenceReplicaSpec{
			Component: c,
			Runners: []v1beta1.Runner{{Name: v1beta1.RunnerNameDefault, Size: 1, Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{
				Containers: []corev1.Container{{Name: "server", Image: "example.com/serving:1.0"}},
			}}}},
		},
	}
}

// referencingService is a service with no model and no runtime whose roles
// are the named replicas.
func referencingService(namespace, name string, refs *v1beta1.ReplicaRefs) *v1beta1.InferenceService {
	isvc := v1beta1testing.MakeInferenceService(name, namespace).Obj()
	isvc.Spec.ReplicaRefs = refs
	return isvc
}

var _ = ginkgo.Describe("InferenceService referenced form admission", ginkgo.Ordered, func() {
	var ns string

	ginkgo.BeforeAll(func() {
		namespace, err := util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "isvc-refs-")
		Expect(err).NotTo(HaveOccurred())
		ns = namespace.Name
		for _, r := range []struct {
			name string
			c    v1beta1.ComponentType
		}{
			{"pool-a", v1beta1.EngineComponent}, {"pool-b", v1beta1.EngineComponent}, {"pool-c", v1beta1.EngineComponent},
			{"pool-d", v1beta1.DecoderComponent}, {"router-a", v1beta1.RouterComponent}, {"router-c", v1beta1.RouterComponent},
		} {
			Expect(k8sClient.Create(suiteCtx, standaloneReplica(ns, r.name, r.c))).To(Succeed())
		}
		projected := standaloneReplica(ns, "svc-owned-engine", v1beta1.EngineComponent)
		projected.Spec.ParentRef = &v1beta1.ParentReference{Name: "svc-owned"}
		Expect(k8sClient.Create(suiteCtx, projected)).To(Succeed())
	})
	ginkgo.AfterAll(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns)).To(Succeed())
	})

	ginkgo.It("admits an engine, a router with engine and decoder, and a router with engine, without model or runtime", func() {
		Expect(k8sClient.Create(suiteCtx, referencingService(ns, "one", &v1beta1.ReplicaRefs{Engine: []string{"pool-b"}}))).To(Succeed())
		Expect(k8sClient.Create(suiteCtx, referencingService(ns, "three", &v1beta1.ReplicaRefs{Router: []string{"router-a"}, Engine: []string{"pool-a"}, Decoder: []string{"pool-d"}}))).To(Succeed())
		Expect(k8sClient.Create(suiteCtx, referencingService(ns, "two", &v1beta1.ReplicaRefs{Router: []string{"router-c"}, Engine: []string{"pool-c"}}))).To(Succeed())
	})

	ginkgo.It("denies references the replicas cannot satisfy", func() {
		for _, tc := range []struct {
			name string
			refs *v1beta1.ReplicaRefs
			want string
		}{
			{"missing", &v1beta1.ReplicaRefs{Engine: []string{"pool-z"}}, "does not exist"},
			{"projected", &v1beta1.ReplicaRefs{Engine: []string{"svc-owned-engine"}}, "InferenceService svc-owned projects"},
			{"wrong-component", &v1beta1.ReplicaRefs{Engine: []string{"router-a"}}, "whose component is router"},
			{"second-service", &v1beta1.ReplicaRefs{Engine: []string{"pool-b"}}, "InferenceService one already fronts"},
		} {
			err := k8sClient.Create(suiteCtx, referencingService(ns, tc.name, tc.refs))
			Expect(err).To(HaveOccurred(), tc.name)
			Expect(err.Error()).To(ContainSubstring(tc.want), tc.name)
		}
	})

	ginkgo.It("denies an inline role, a rollout group and a model beside references", func() {
		withEngine := referencingService(ns, "mixed", &v1beta1.ReplicaRefs{Engine: []string{"pool-a"}})
		withEngine.Spec.Engine = &v1beta1.EngineSpec{}
		err := k8sClient.Create(suiteCtx, withEngine)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("spec.engine and spec.replicaRefs cannot both be set"))

		withRollout := referencingService(ns, "rolling", &v1beta1.ReplicaRefs{Engine: []string{"pool-a"}})
		withRollout.Spec.Rollout = &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{{Components: []v1beta1.ComponentType{v1beta1.EngineComponent}}}}
		err = k8sClient.Create(suiteCtx, withRollout)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("spec.rollout is not accepted with spec.replicaRefs"))

		withModel := referencingService(ns, "modeled", &v1beta1.ReplicaRefs{Engine: []string{"pool-a"}})
		withModel.Spec.Model = &v1beta1.ModelRef{Name: "example-model"}
		err = k8sClient.Create(suiteCtx, withModel)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("spec.model is not accepted with spec.replicaRefs"))
	})

	ginkgo.It("denies changing or dropping a reference on update", func() {
		key := client.ObjectKey{Namespace: ns, Name: "one"}
		for _, mutate := range []func(*v1beta1.InferenceService){
			func(isvc *v1beta1.InferenceService) { isvc.Spec.ReplicaRefs.Engine = []string{"pool-a"} },
			func(isvc *v1beta1.InferenceService) { isvc.Spec.ReplicaRefs.Router = []string{"router-a"} },
			func(isvc *v1beta1.InferenceService) { isvc.Spec.ReplicaRefs = nil },
		} {
			latest := &v1beta1.InferenceService{}
			Expect(k8sClient.Get(suiteCtx, key, latest)).To(Succeed())
			mutate(latest)
			err := k8sClient.Update(suiteCtx, latest)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("spec.replicaRefs is fixed at create"))
		}
	})
})
