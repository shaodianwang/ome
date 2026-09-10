package acceleratorquota

import (
	"fmt"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

const (
	timeout  = "30s"
	interval = "200ms"
)

var nodeSeq int

// uniqueName keeps specs independent without deleting the reserved root between
// them: the tree is cluster-scoped and whole-tree reconciled, so two specs using
// the same leaf name would see each other's violations.
func uniqueName(prefix string) string {
	nodeSeq++
	return fmt.Sprintf("%s-%d", prefix, nodeSeq)
}

func budget(nominal string) v1beta1.AcceleratorBudget {
	return v1beta1.AcceleratorBudget{
		ResourceName:   "google.com/tpu",
		ResourceFlavor: "tpu7x",
		Nominal:        resource.MustParse(nominal),
	}
}

func cohort(name, parent string, budgets ...v1beta1.AcceleratorBudget) *v1beta1.AcceleratorQuota {
	q := &v1beta1.AcceleratorQuota{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: v1beta1.AcceleratorQuotaSpec{
			Role:    v1beta1.AcceleratorQuotaRoleCohort,
			Budgets: budgets,
		},
	}
	if parent != "" {
		q.Spec.ParentRef = &v1beta1.AcceleratorQuotaParentRef{Name: parent}
	}
	return q
}

func leaf(name, parent string, budgets ...v1beta1.AcceleratorBudget) *v1beta1.AcceleratorQuota {
	return &v1beta1.AcceleratorQuota{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: v1beta1.AcceleratorQuotaSpec{
			Role:      v1beta1.AcceleratorQuotaRoleClusterQueue,
			ParentRef: &v1beta1.AcceleratorQuotaParentRef{Name: parent},
			Budgets:   budgets,
		},
	}
}

func mustCreate(objs ...*v1beta1.AcceleratorQuota) {
	ginkgo.GinkgoHelper()
	for _, o := range objs {
		gomega.Expect(k8sClient.Create(suiteCtx, o)).To(gomega.Succeed())
		ginkgo.DeferCleanup(func(name string) func() {
			return func() {
				_ = k8sClient.Delete(suiteCtx, &v1beta1.AcceleratorQuota{
					ObjectMeta: metav1.ObjectMeta{Name: name},
				})
			}
		}(o.Name))
	}
}

// conditionOf polls a node's condition, which is how every assertion here waits:
// the controller is whole-tree, so a spec's own create is not the only event that
// can move a verdict.
func conditionOf(name, condType string) func() *metav1.Condition {
	return func() *metav1.Condition {
		var q v1beta1.AcceleratorQuota
		if err := k8sClient.Get(suiteCtx, client.ObjectKey{Name: name}, &q); err != nil {
			return nil
		}
		return apimeta.FindStatusCondition(q.Status.Conditions, condType)
	}
}

func parentOf(name string) func() string {
	return func() string {
		var q v1beta1.AcceleratorQuota
		if err := k8sClient.Get(suiteCtx, client.ObjectKey{Name: name}, &q); err != nil {
			return ""
		}
		return q.Status.Parent
	}
}

var _ = ginkgo.Describe("AcceleratorQuota tree status", func() {
	ginkgo.It("reports position and Ready for a well-formed tree", func() {
		root := v1beta1.AcceleratorQuotaRootName
		org := uniqueName("org")
		team := uniqueName("team")

		mustCreate(
			cohort(root, "", budget("128")),
			cohort(org, root, budget("100")),
			leaf(team, org, budget("60")),
		)

		// The resolved edge, which is the whole of what status reports about a
		// node's position: ancestry is a walk over it, and the node's own name
		// already spells the path out.
		gomega.Eventually(parentOf(team), timeout, interval).Should(gomega.Equal(org))
		gomega.Eventually(func() metav1.ConditionStatus {
			c := conditionOf(team, v1beta1.AcceleratorQuotaReady)()
			if c == nil {
				return metav1.ConditionUnknown
			}
			return c.Status
		}, timeout, interval).Should(gomega.Equal(metav1.ConditionTrue))

		var q v1beta1.AcceleratorQuota
		gomega.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{Name: team}, &q)).To(gomega.Succeed())
		gomega.Expect(q.Status.Parent).To(gomega.Equal(org))
		gomega.Expect(q.Status.ObservedGeneration).To(gomega.Equal(q.Generation))
	})

	// The parent is blamed and the whole subtree freezes. This is the behavior
	// the materializer will depend on, so it is worth pinning against a real
	// apiserver and not only a fake client.
	ginkgo.It("blames the parent on a containment bust and freezes its descendants", func() {
		root := v1beta1.AcceleratorQuotaRootName
		org := uniqueName("org")
		big := uniqueName("big")

		mustCreate(
			cohort(root, "", budget("128")),
			cohort(org, root, budget("50")),
			leaf(big, org, budget("60")),
		)

		gomega.Eventually(func() string {
			c := conditionOf(org, v1beta1.AcceleratorQuotaDegraded)()
			if c == nil || c.Status != metav1.ConditionTrue {
				return ""
			}
			return c.Reason
		}, timeout, interval).Should(gomega.Equal(v1beta1.AcceleratorQuotaReasonContainmentViolated))

		// The descendant inherits the ancestor's cause verbatim, so an operator
		// is pointed at the node that actually needs the edit.
		gomega.Eventually(func() string {
			c := conditionOf(big, v1beta1.AcceleratorQuotaDegraded)()
			if c == nil || c.Status != metav1.ConditionTrue {
				return ""
			}
			return c.Reason
		}, timeout, interval).Should(gomega.Equal(v1beta1.AcceleratorQuotaReasonContainmentViolated))

		// Repairing it clears both, rather than leaving a stale Degraded.
		var child v1beta1.AcceleratorQuota
		gomega.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{Name: big}, &child)).To(gomega.Succeed())
		child.Spec.Budgets[0].Nominal = resource.MustParse("40")
		gomega.Expect(k8sClient.Update(suiteCtx, &child)).To(gomega.Succeed())

		for _, n := range []string{org, big} {
			gomega.Eventually(func() metav1.ConditionStatus {
				c := conditionOf(n, v1beta1.AcceleratorQuotaDegraded)()
				if c == nil {
					return metav1.ConditionUnknown
				}
				return c.Status
			}, timeout, interval).Should(gomega.Equal(metav1.ConditionFalse), "node %s should recover", n)
		}
	})

	// A node beneath a break must not be left Ready: it would otherwise
	// materialize a ClusterQueue under a Cohort that was never created, which
	// Kueue silently rehomes onto an implicit default cohort.
	ginkgo.It("marks descendants of an unresolved parent Unreachable", func() {
		orphan := uniqueName("orphan")
		child := uniqueName("child")

		mustCreate(
			cohort(v1beta1.AcceleratorQuotaRootName, "", budget("128")),
			cohort(orphan, "ghost-does-not-exist"),
			leaf(child, orphan, budget("1")),
		)

		gomega.Eventually(func() string {
			c := conditionOf(orphan, v1beta1.AcceleratorQuotaDegraded)()
			if c == nil || c.Status != metav1.ConditionTrue {
				return ""
			}
			return c.Reason
		}, timeout, interval).Should(gomega.Equal(v1beta1.AcceleratorQuotaReasonParentMissing))

		gomega.Eventually(func() string {
			c := conditionOf(child, v1beta1.AcceleratorQuotaDegraded)()
			if c == nil || c.Status != metav1.ConditionTrue {
				return ""
			}
			return c.Reason
		}, timeout, interval).Should(gomega.Equal(v1beta1.AcceleratorQuotaReasonUnreachable))

		// status.parent still reports the edge the author declared and the tree
		// resolved -- that is the diagnosis, not a claim of position. What must
		// never happen is the node reading Ready, which is what a materializer
		// acts on.
		gomega.Expect(parentOf(child)()).To(gomega.Equal(orphan))
		gomega.Consistently(func() metav1.ConditionStatus {
			c := conditionOf(child, v1beta1.AcceleratorQuotaReady)()
			if c == nil {
				return metav1.ConditionUnknown
			}
			return c.Status
		}, "2s", interval).ShouldNot(gomega.Equal(metav1.ConditionTrue),
			"an unreachable node must never read Ready")
	})

	ginkgo.It("enforces depth against the configured bound", func() {
		root := v1beta1.AcceleratorQuotaRootName
		l1, l2, l3 := uniqueName("l1"), uniqueName("l2"), uniqueName("l3")
		deep := uniqueName("deep")

		// The suite configures MaxDepth 3, so this leaf at depth 4 is over.
		mustCreate(
			cohort(root, "", budget("128")),
			cohort(l1, root), cohort(l2, l1), cohort(l3, l2),
			leaf(deep, l3, budget("1")),
		)

		gomega.Eventually(func() string {
			c := conditionOf(deep, v1beta1.AcceleratorQuotaDegraded)()
			if c == nil || c.Status != metav1.ConditionTrue {
				return ""
			}
			return c.Reason
		}, timeout, interval).Should(gomega.Equal(v1beta1.AcceleratorQuotaReasonDepthExceeded))
	})
})

// These assert the CRD's own schema, which a fake client cannot: the unit tests
// construct Go structs directly and never go through apiserver validation, so a
// broken CEL rule would be invisible there.
var _ = ginkgo.Describe("AcceleratorQuota admission", func() {
	ginkgo.It("rejects a Cohort carrying a leaf-only field", func() {
		c := cohort(uniqueName("bad-cohort"), v1beta1.AcceleratorQuotaRootName)
		c.Spec.PriorityTier = "nope"
		err := k8sClient.Create(suiteCtx, c)
		gomega.Expect(err).To(gomega.HaveOccurred())
		gomega.Expect(err.Error()).To(gomega.ContainSubstring("must not set priorityTier"))
	})

	ginkgo.It("rejects a leaf with no budget", func() {
		l := leaf(uniqueName("no-budget"), v1beta1.AcceleratorQuotaRootName)
		err := k8sClient.Create(suiteCtx, l)
		gomega.Expect(err).To(gomega.HaveOccurred())
		gomega.Expect(err.Error()).To(gomega.ContainSubstring("at least one budget"))
	})

	ginkgo.It("rejects a budget with no resourceName", func() {
		l := leaf(uniqueName("no-resource"), v1beta1.AcceleratorQuotaRootName,
			v1beta1.AcceleratorBudget{ResourceFlavor: "tpu7x", Nominal: resource.MustParse("1")})
		err := k8sClient.Create(suiteCtx, l)
		gomega.Expect(err).To(gomega.HaveOccurred())
		gomega.Expect(err.Error()).To(gomega.ContainSubstring("resourceName"))
	})

	// The name is copied into a label value on every Kueue object the node
	// materializes, and a label value may not exceed 63 characters.
	ginkgo.It("rejects a name longer than 63 characters", func() {
		long := ""
		for i := 0; i < 64; i++ {
			long += "a"
		}
		err := k8sClient.Create(suiteCtx, leaf(long, v1beta1.AcceleratorQuotaRootName, budget("1")))
		gomega.Expect(err).To(gomega.HaveOccurred())
		gomega.Expect(err.Error()).To(gomega.ContainSubstring("may not exceed 63 characters"))
	})
})
