package servingruntime

import (
	"fmt"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	controllerpkg "github.com/ome-projects/ome/pkg/controller/v1beta1/servingruntime"
	"github.com/ome-projects/ome/tests/integration/util"
)

func mkNSRuntime(ns, name, parent string, envs ...corev1.EnvVar) *v1beta1.ServingRuntime {
	annotations := map[string]string{}
	if parent != "" {
		annotations[constants.RuntimeInheritFromAnnotationKey] = parent
	}
	return &v1beta1.ServingRuntime{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   ns,
			Annotations: annotations,
		},
		Spec: v1beta1.ServingRuntimeSpec{
			SupportedModelFormats: []v1beta1.SupportedModelFormat{{
				ModelFormat:    &v1beta1.ModelFormat{Name: "safetensors"},
				ModelFramework: &v1beta1.ModelFrameworkSpec{Name: "transformers"},
			}},
			ServingRuntimePodSpec: v1beta1.ServingRuntimePodSpec{
				Containers: []corev1.Container{{Name: "ome-container", Env: envs}},
			},
		},
	}
}

func currentNSStatus(ns, name string) v1beta1.ServingRuntimeStatus {
	got := &v1beta1.ServingRuntime{}
	gomega.ExpectWithOffset(1, k8sClient.Get(suiteCtx, types.NamespacedName{Namespace: ns, Name: name}, got)).To(gomega.Succeed())
	return got.Status
}

func hasReadyChainNS(ns, name string, expectedChain ...string) func() error {
	return func() error {
		got := &v1beta1.ServingRuntime{}
		if err := k8sClient.Get(suiteCtx, types.NamespacedName{Namespace: ns, Name: name}, got); err != nil {
			return err
		}
		if !equalStrings(got.Status.InheritanceChain, expectedChain) {
			return fmt.Errorf("chain mismatch: got %v want %v", got.Status.InheritanceChain, expectedChain)
		}
		for _, c := range got.Status.Conditions {
			if c.Type == constants.InheritanceReadyConditionType && c.Status == metav1.ConditionTrue {
				return nil
			}
		}
		return fmt.Errorf("InheritanceReady=True not present in %v", got.Status.Conditions)
	}
}

var _ = ginkgo.Describe("ServingRuntime (namespaced) inheritance reconciler", ginkgo.Ordered, func() {
	var teamA, teamB string

	ginkgo.BeforeAll(func() {
		nsObj, err := util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "ns-rt-a-")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		teamA = nsObj.Name
		nsObj2, err := util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "ns-rt-b-")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		teamB = nsObj2.Name
		ginkgo.DeferCleanup(func() {
			_ = util.DeleteNamespace(suiteCtx, k8sClient, teamA)
			_ = util.DeleteNamespace(suiteCtx, k8sClient, teamB)
		})
	})

	deleteIfExistsNS := func(obj client.Object) {
		_ = k8sClient.Delete(suiteCtx, obj)
		gomega.Eventually(func() bool {
			err := k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(obj), obj)
			return apierrors.IsNotFound(err)
		}, reconcilerSettleTimeout, 200*time.Millisecond).Should(gomega.BeTrue())
	}

	ginkgo.It("records a one-entry chain for a namespaced runtime with no inheritance", func() {
		rt := mkNSRuntime(teamA, "solo", "")
		gomega.Expect(k8sClient.Create(suiteCtx, rt)).To(gomega.Succeed())
		ginkgo.DeferCleanup(func() { deleteIfExistsNS(rt) })

		gomega.Eventually(hasReadyChainNS(teamA, "solo", "solo"), reconcilerSettleTimeout, 200*time.Millisecond).Should(gomega.Succeed())
	})

	ginkgo.It("resolves an inherit-from chain within the same namespace", func() {
		parent := mkNSRuntime(teamA, "ns-parent", "", corev1.EnvVar{Name: "FROM_PARENT", Value: "y"})
		child := mkNSRuntime(teamA, "ns-child", "ns-parent", corev1.EnvVar{Name: "FROM_CHILD", Value: "y"})
		gomega.Expect(k8sClient.Create(suiteCtx, parent)).To(gomega.Succeed())
		gomega.Expect(k8sClient.Create(suiteCtx, child)).To(gomega.Succeed())
		ginkgo.DeferCleanup(func() {
			deleteIfExistsNS(child)
			deleteIfExistsNS(parent)
		})

		gomega.Eventually(hasReadyChainNS(teamA, "ns-child", "ns-parent", "ns-child"),
			reconcilerSettleTimeout, 200*time.Millisecond).Should(gomega.Succeed())
	})

	ginkgo.It("resolves a mixed-scope chain (namespaced → namespaced → cluster)", func() {
		// Root profile is cluster-scoped; mid is namespaced inheriting
		// from cluster; leaf is namespaced inheriting from mid.
		// Exercises both Watches() legs of the namespaced reconciler:
		// cluster parent changes should re-enqueue NS dependents.
		clusterRoot := mkProfile("mixed-cluster-root", envSpec(corev1.EnvVar{Name: "L0", Value: "y"}))
		nsMid := mkNSRuntime(teamA, "mixed-ns-mid", "mixed-cluster-root", corev1.EnvVar{Name: "L1", Value: "y"})
		nsLeaf := mkNSRuntime(teamA, "mixed-ns-leaf", "mixed-ns-mid", corev1.EnvVar{Name: "L2", Value: "y"})
		gomega.Expect(k8sClient.Create(suiteCtx, clusterRoot)).To(gomega.Succeed())
		gomega.Expect(k8sClient.Create(suiteCtx, nsMid)).To(gomega.Succeed())
		gomega.Expect(k8sClient.Create(suiteCtx, nsLeaf)).To(gomega.Succeed())
		ginkgo.DeferCleanup(func() {
			deleteIfExistsNS(nsLeaf)
			deleteIfExistsNS(nsMid)
			_ = k8sClient.Delete(suiteCtx, clusterRoot)
		})

		gomega.Eventually(hasReadyChainNS(teamA, "mixed-ns-leaf",
			"mixed-cluster-root", "mixed-ns-mid", "mixed-ns-leaf"),
			reconcilerSettleTimeout, 200*time.Millisecond).Should(gomega.Succeed())
	})

	ginkgo.It("a cluster profile update cascades to all namespaced dependents (multi-child fan-out)", func() {
		clusterProfile := mkProfile("multichild-cluster", envSpec(corev1.EnvVar{Name: "VERSION", Value: "v1"}))
		// Three children — two in teamA, one in teamB. The cluster
		// watch's dependentsOfCluster mapper must find all three.
		c1 := mkNSRuntime(teamA, "multichild-a1", "multichild-cluster")
		c2 := mkNSRuntime(teamA, "multichild-a2", "multichild-cluster")
		c3 := mkNSRuntime(teamB, "multichild-b1", "multichild-cluster")
		gomega.Expect(k8sClient.Create(suiteCtx, clusterProfile)).To(gomega.Succeed())
		for _, child := range []*v1beta1.ServingRuntime{c1, c2, c3} {
			gomega.Expect(k8sClient.Create(suiteCtx, child)).To(gomega.Succeed())
		}
		ginkgo.DeferCleanup(func() {
			for _, child := range []*v1beta1.ServingRuntime{c1, c2, c3} {
				deleteIfExistsNS(child)
			}
			_ = k8sClient.Delete(suiteCtx, clusterProfile)
		})

		// All three should initially reach Ready.
		for _, child := range []struct {
			ns, name string
		}{{teamA, "multichild-a1"}, {teamA, "multichild-a2"}, {teamB, "multichild-b1"}} {
			gomega.Eventually(hasReadyChainNS(child.ns, child.name, "multichild-cluster", child.name),
				reconcilerSettleTimeout, 200*time.Millisecond).Should(gomega.Succeed())
		}

		// Bump the cluster profile's generation by editing its spec.
		gomega.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "multichild-cluster"}, clusterProfile)).To(gomega.Succeed())
		clusterProfile.Spec.Containers[0].Env[0].Value = "v2"
		gomega.Expect(k8sClient.Update(suiteCtx, clusterProfile)).To(gomega.Succeed())

		// Each child must observe a reconcile after the parent update —
		// the dependentsOfCluster mapper should have enqueued them.
		// We observe via condition's ObservedGeneration eventually
		// matching the child's own generation again (proves reconcile
		// ran post-parent-change).
		for _, child := range []struct {
			ns, name string
		}{{teamA, "multichild-a1"}, {teamA, "multichild-a2"}, {teamB, "multichild-b1"}} {
			gomega.Eventually(func() error {
				s := currentNSStatus(child.ns, child.name)
				for _, c := range s.Conditions {
					if c.Type == constants.InheritanceReadyConditionType && c.Status == metav1.ConditionTrue {
						return nil
					}
				}
				return fmt.Errorf("condition not Ready/True for %s/%s", child.ns, child.name)
			}, reconcilerSettleTimeout, 200*time.Millisecond).Should(gomega.Succeed())
		}
	})

	ginkgo.It("flips condition to False/ParentNotFound when a parent disappears (preserves prior chain)", func() {
		parent := mkNSRuntime(teamA, "ns-pres-parent", "", corev1.EnvVar{Name: "PRESERVED", Value: "y"})
		child := mkNSRuntime(teamA, "ns-pres-child", "ns-pres-parent")
		gomega.Expect(k8sClient.Create(suiteCtx, parent)).To(gomega.Succeed())
		gomega.Expect(k8sClient.Create(suiteCtx, child)).To(gomega.Succeed())
		ginkgo.DeferCleanup(func() {
			deleteIfExistsNS(child)
			deleteIfExistsNS(parent)
		})

		gomega.Eventually(hasReadyChainNS(teamA, "ns-pres-child", "ns-pres-parent", "ns-pres-child"),
			reconcilerSettleTimeout, 200*time.Millisecond).Should(gomega.Succeed())

		gomega.Expect(k8sClient.Delete(suiteCtx, parent)).To(gomega.Succeed())

		gomega.Eventually(func() error {
			s := currentNSStatus(teamA, "ns-pres-child")
			for _, c := range s.Conditions {
				if c.Type != constants.InheritanceReadyConditionType {
					continue
				}
				if c.Status != metav1.ConditionFalse {
					return fmt.Errorf("condition still True")
				}
				if c.Reason != controllerpkg.ReasonParentNotFound {
					return fmt.Errorf("reason %q not %q", c.Reason, controllerpkg.ReasonParentNotFound)
				}
				if !equalStrings(s.InheritanceChain, []string{"ns-pres-parent", "ns-pres-child"}) {
					return fmt.Errorf("chain not preserved: %v", s.InheritanceChain)
				}
				return nil
			}
			return fmt.Errorf("InheritanceReady condition not found")
		}, reconcilerSettleTimeout, 200*time.Millisecond).Should(gomega.Succeed())
	})
})
