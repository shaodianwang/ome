package servingruntime

import (
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/tests/integration/util"
)

// minimalRuntimeSpec passes the unrelated priority validation so
// inheritance-focused negatives don't trip over other webhook rules.
func minimalRuntimeSpec() v1beta1.ServingRuntimeSpec {
	return v1beta1.ServingRuntimeSpec{
		SupportedModelFormats: []v1beta1.SupportedModelFormat{{
			ModelFormat:    &v1beta1.ModelFormat{Name: "safetensors"},
			ModelFramework: &v1beta1.ModelFrameworkSpec{Name: "transformers"},
		}},
	}
}

var _ = ginkgo.Describe("ClusterServingRuntime inheritance webhook", ginkgo.Ordered, func() {
	var ns string

	ginkgo.BeforeAll(func() {
		nsObj, err := util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "csr-webhook-")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ns = nsObj.Name
		ginkgo.DeferCleanup(func() {
			_ = util.DeleteNamespace(suiteCtx, k8sClient, ns)
		})
	})

	ginkgo.It("admits a cluster runtime with no inherit-from annotation", func() {
		csr := &v1beta1.ClusterServingRuntime{
			ObjectMeta: metav1.ObjectMeta{GenerateName: "solo-"},
			Spec:       minimalRuntimeSpec(),
		}
		gomega.Expect(k8sClient.Create(suiteCtx, csr)).To(gomega.Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, csr) })
	})

	ginkgo.It("admits an inheriting runtime when the parent profile exists", func() {
		profile := &v1beta1.ClusterServingRuntime{
			ObjectMeta: metav1.ObjectMeta{
				GenerateName: "profile-infra-",
				Annotations:  map[string]string{constants.RuntimeProfileAnnotationKey: "true"},
			},
			Spec: v1beta1.ServingRuntimeSpec{Disabled: ptr.To(true)},
		}
		gomega.Expect(k8sClient.Create(suiteCtx, profile)).To(gomega.Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, profile) })

		child := &v1beta1.ClusterServingRuntime{
			ObjectMeta: metav1.ObjectMeta{
				GenerateName: "rt-",
				Annotations:  map[string]string{constants.RuntimeInheritFromAnnotationKey: profile.Name},
			},
			Spec: minimalRuntimeSpec(),
		}
		gomega.Expect(k8sClient.Create(suiteCtx, child)).To(gomega.Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, child) })
	})

	ginkgo.It("rejects an inheriting runtime when the parent profile does not exist", func() {
		child := &v1beta1.ClusterServingRuntime{
			ObjectMeta: metav1.ObjectMeta{
				GenerateName: "rt-",
				Annotations:  map[string]string{constants.RuntimeInheritFromAnnotationKey: "ghost-parent"},
			},
			Spec: minimalRuntimeSpec(),
		}
		err := k8sClient.Create(suiteCtx, child)
		gomega.Expect(err).To(gomega.HaveOccurred())
		gomega.Expect(apierrors.IsInvalid(err) || apierrors.IsForbidden(err)).To(gomega.BeTrue(), "expected admission rejection, got %v", err)
		gomega.Expect(err.Error()).To(gomega.ContainSubstring("ghost-parent"))
	})

	ginkgo.It("rejects a profile that is not also disabled", func() {
		profile := &v1beta1.ClusterServingRuntime{
			ObjectMeta: metav1.ObjectMeta{
				GenerateName: "bad-profile-",
				Annotations:  map[string]string{constants.RuntimeProfileAnnotationKey: "true"},
			},
			Spec: v1beta1.ServingRuntimeSpec{}, // disabled not set → false
		}
		err := k8sClient.Create(suiteCtx, profile)
		gomega.Expect(err).To(gomega.HaveOccurred())
		gomega.Expect(err.Error()).To(gomega.ContainSubstring("disabled: true"))
	})

	ginkgo.It("rejects a direct two-node inheritance cycle on update", func() {
		// Create A → B and B → unrelated, then patch unrelated → A
		// to close the cycle through B. Admission must reject the
		// patch.
		unrelated := &v1beta1.ClusterServingRuntime{
			ObjectMeta: metav1.ObjectMeta{GenerateName: "cycle-leaf-"},
			Spec:       minimalRuntimeSpec(),
		}
		gomega.Expect(k8sClient.Create(suiteCtx, unrelated)).To(gomega.Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, unrelated) })

		mid := &v1beta1.ClusterServingRuntime{
			ObjectMeta: metav1.ObjectMeta{
				GenerateName: "cycle-mid-",
				Annotations:  map[string]string{constants.RuntimeInheritFromAnnotationKey: unrelated.Name},
			},
			Spec: minimalRuntimeSpec(),
		}
		gomega.Expect(k8sClient.Create(suiteCtx, mid)).To(gomega.Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, mid) })

		top := &v1beta1.ClusterServingRuntime{
			ObjectMeta: metav1.ObjectMeta{
				GenerateName: "cycle-top-",
				Annotations:  map[string]string{constants.RuntimeInheritFromAnnotationKey: mid.Name},
			},
			Spec: minimalRuntimeSpec(),
		}
		gomega.Expect(k8sClient.Create(suiteCtx, top)).To(gomega.Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, top) })

		// Close the cycle: unrelated → top, so unrelated → top → mid → unrelated.
		unrelated.Annotations = map[string]string{constants.RuntimeInheritFromAnnotationKey: top.Name}
		err := k8sClient.Update(suiteCtx, unrelated)
		gomega.Expect(err).To(gomega.HaveOccurred())
		gomega.Expect(err.Error()).To(gomega.ContainSubstring("cycle"))
	})

	ginkgo.It("rejects a cluster runtime that names a namespaced ServingRuntime as its parent", func() {
		// The cluster fetcher only looks at ClusterServingRuntime; a
		// namespaced SR with the same name is invisible. Encodes the
		// scope rule "cluster CAN'T inherit from namespaced — no
		// namespace data leaks into cluster scope."
		teamNS, err := util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "csr-rejects-ns-")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.DeferCleanup(func() {
			_ = util.DeleteNamespace(suiteCtx, k8sClient, teamNS.Name)
		})
		nsOnly := &v1beta1.ServingRuntime{
			ObjectMeta: metav1.ObjectMeta{GenerateName: "ns-only-", Namespace: teamNS.Name},
			Spec:       minimalRuntimeSpec(),
		}
		gomega.Expect(k8sClient.Create(suiteCtx, nsOnly)).To(gomega.Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, nsOnly) })

		csr := &v1beta1.ClusterServingRuntime{
			ObjectMeta: metav1.ObjectMeta{
				GenerateName: "csr-cant-see-ns-",
				Annotations:  map[string]string{constants.RuntimeInheritFromAnnotationKey: nsOnly.Name},
			},
			Spec: minimalRuntimeSpec(),
		}
		err = k8sClient.Create(suiteCtx, csr)
		gomega.Expect(err).To(gomega.HaveOccurred())
		gomega.Expect(err.Error()).To(gomega.ContainSubstring("not found"))
	})

	ginkgo.It("flipping disabled true→false on a profile-marked cluster runtime is rejected on UPDATE", func() {
		profile := &v1beta1.ClusterServingRuntime{
			ObjectMeta: metav1.ObjectMeta{
				GenerateName: "cluster-profile-update-",
				Annotations:  map[string]string{constants.RuntimeProfileAnnotationKey: "true"},
			},
			Spec: v1beta1.ServingRuntimeSpec{Disabled: ptr.To(true)},
		}
		gomega.Expect(k8sClient.Create(suiteCtx, profile)).To(gomega.Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, profile) })

		profile.Spec.Disabled = ptr.To(false)
		err := k8sClient.Update(suiteCtx, profile)
		gomega.Expect(err).To(gomega.HaveOccurred())
		gomega.Expect(err.Error()).To(gomega.ContainSubstring("disabled: true"))
	})

	ginkgo.It("rejects a chain that exceeds the depth limit", func() {
		// Build a 6-deep chain (n0..n5). The 7th create (the runtime
		// that points to n5) walks 7 links including itself, exceeding
		// the default max of 5.
		previous := ""
		created := []*v1beta1.ClusterServingRuntime{}
		for i := 0; i < 6; i++ {
			ann := map[string]string{}
			if previous != "" {
				ann[constants.RuntimeInheritFromAnnotationKey] = previous
			}
			r := &v1beta1.ClusterServingRuntime{
				ObjectMeta: metav1.ObjectMeta{GenerateName: "depth-", Annotations: ann},
				Spec:       minimalRuntimeSpec(),
			}
			err := k8sClient.Create(suiteCtx, r)
			if i < 5 {
				gomega.Expect(err).NotTo(gomega.HaveOccurred(), "creating link %d should succeed", i)
				created = append(created, r)
			} else {
				// The 6th create has depth-from-root=6 > 5; admission must reject.
				gomega.Expect(err).To(gomega.HaveOccurred(), "creating link 6 should be rejected")
				gomega.Expect(err.Error()).To(gomega.ContainSubstring("max depth"))
			}
			previous = r.Name
		}
		for _, r := range created {
			ginkgo.DeferCleanup(func(target *v1beta1.ClusterServingRuntime) func() {
				return func() { _ = k8sClient.Delete(suiteCtx, target) }
			}(r))
		}
	})
})

var _ = ginkgo.Describe("ServingRuntime inheritance webhook (namespaced scope)", ginkgo.Ordered, func() {
	var ns string

	ginkgo.BeforeAll(func() {
		nsObj, err := util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "sr-webhook-")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ns = nsObj.Name
		ginkgo.DeferCleanup(func() {
			_ = util.DeleteNamespace(suiteCtx, k8sClient, ns)
		})
	})

	ginkgo.It("admits a namespaced runtime inheriting from a same-namespace parent", func() {
		parent := &v1beta1.ServingRuntime{
			ObjectMeta: metav1.ObjectMeta{GenerateName: "ns-parent-", Namespace: ns},
			Spec:       minimalRuntimeSpec(),
		}
		gomega.Expect(k8sClient.Create(suiteCtx, parent)).To(gomega.Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, parent) })

		child := &v1beta1.ServingRuntime{
			ObjectMeta: metav1.ObjectMeta{
				GenerateName: "ns-child-",
				Namespace:    ns,
				Annotations:  map[string]string{constants.RuntimeInheritFromAnnotationKey: parent.Name},
			},
			Spec: minimalRuntimeSpec(),
		}
		gomega.Expect(k8sClient.Create(suiteCtx, child)).To(gomega.Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, child) })
	})

	ginkgo.It("admits a namespaced runtime inheriting from a cluster-scoped parent", func() {
		clusterParent := &v1beta1.ClusterServingRuntime{
			ObjectMeta: metav1.ObjectMeta{GenerateName: "cluster-parent-"},
			Spec:       minimalRuntimeSpec(),
		}
		gomega.Expect(k8sClient.Create(suiteCtx, clusterParent)).To(gomega.Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, clusterParent) })

		child := &v1beta1.ServingRuntime{
			ObjectMeta: metav1.ObjectMeta{
				GenerateName: "fallback-",
				Namespace:    ns,
				Annotations:  map[string]string{constants.RuntimeInheritFromAnnotationKey: clusterParent.Name},
			},
			Spec: minimalRuntimeSpec(),
		}
		gomega.Expect(k8sClient.Create(suiteCtx, child)).To(gomega.Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, child) })
	})

	ginkgo.It("flipping disabled true→false on a profile-marked runtime is rejected on UPDATE", func() {
		// Profile marker invariant is checked on every admission (CREATE
		// and UPDATE). A profile that becomes "selectable" by clearing
		// disabled would be a footgun — the runtime selector could
		// suddenly pick it up.
		profile := &v1beta1.ServingRuntime{
			ObjectMeta: metav1.ObjectMeta{
				GenerateName: "profile-update-",
				Namespace:    ns,
				Annotations:  map[string]string{constants.RuntimeProfileAnnotationKey: "true"},
			},
			Spec: v1beta1.ServingRuntimeSpec{Disabled: ptr.To(true)},
		}
		gomega.Expect(k8sClient.Create(suiteCtx, profile)).To(gomega.Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, profile) })

		profile.Spec.Disabled = ptr.To(false)
		err := k8sClient.Update(suiteCtx, profile)
		gomega.Expect(err).To(gomega.HaveOccurred())
		gomega.Expect(err.Error()).To(gomega.ContainSubstring("disabled: true"))
	})

	ginkgo.It("rejects cross-namespace inheritance (same name only exists in another namespace)", func() {
		otherNS, err := util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "sr-other-")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		ginkgo.DeferCleanup(func() {
			_ = util.DeleteNamespace(suiteCtx, k8sClient, otherNS.Name)
		})
		alien := &v1beta1.ServingRuntime{
			ObjectMeta: metav1.ObjectMeta{GenerateName: "alien-", Namespace: otherNS.Name},
			Spec:       minimalRuntimeSpec(),
		}
		gomega.Expect(k8sClient.Create(suiteCtx, alien)).To(gomega.Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, alien) })

		child := &v1beta1.ServingRuntime{
			ObjectMeta: metav1.ObjectMeta{
				GenerateName: "cross-ns-",
				Namespace:    ns,
				Annotations:  map[string]string{constants.RuntimeInheritFromAnnotationKey: alien.Name},
			},
			Spec: minimalRuntimeSpec(),
		}
		err = k8sClient.Create(suiteCtx, child)
		gomega.Expect(err).To(gomega.HaveOccurred())
		gomega.Expect(err.Error()).To(gomega.ContainSubstring("not found"))
	})
})
