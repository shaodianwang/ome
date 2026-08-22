package acceleratorquota

import (
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

const root = v1beta1.AcceleratorQuotaRootName

var _ = ginkgo.Describe("AcceleratorQuota admission", func() {
	ginkgo.It("admits a sound subtree", func() {
		org, teamA := uniqueName("org"), uniqueName("team")
		mustCreate(
			cohort(org, root, budget("100")),
			leaf(teamA, org, budget("60")),
		)
	})

	ginkgo.It("rejects a parentRef that does not resolve", func() {
		orphan := uniqueName("orphan")
		msg := denial(k8sClient.Create(suiteCtx, leaf(orphan, "no-such-cohort", budget("1"))))
		gomega.Expect(msg).To(gomega.ContainSubstring(
			`parentRef "no-such-cohort" does not resolve to an existing AcceleratorQuota`))
	})

	// The violation lands on the parent, whose name is nowhere in the request.
	// Filtering violations by the submitted object would miss this entirely,
	// which is the whole reason admission assembles and diffs the tree.
	ginkgo.It("rejects a child that busts a parent it does not name", func() {
		org, teamA, teamB := uniqueName("org"), uniqueName("team"), uniqueName("team")
		mustCreate(
			cohort(org, root, budget("100")),
			leaf(teamA, org, budget("60")),
		)

		msg := denial(k8sClient.Create(suiteCtx, leaf(teamB, org, budget("50"))))
		gomega.Expect(msg).To(gomega.ContainSubstring(
			org + ": budget google.com/tpu on tpu7x is 100 but its children total 110"))
	})

	ginkgo.It("rejects a second parent-less node", func() {
		rogue := uniqueName("rogue")
		msg := denial(k8sClient.Create(suiteCtx, cohort(rogue, "", budget("64"))))
		gomega.Expect(msg).To(gomega.ContainSubstring(
			`only the reserved root "root" may omit parentRef`))
	})

	ginkgo.It("rejects a node parented to itself", func() {
		loop := uniqueName("loop")
		msg := denial(k8sClient.Create(suiteCtx, cohort(loop, loop, budget("8"))))
		gomega.Expect(msg).To(gomega.ContainSubstring("parentRef points at the node itself"))
	})

	ginkgo.It("rejects a child attached under a leaf", func() {
		org, teamA, sub := uniqueName("org"), uniqueName("team"), uniqueName("sub")
		mustCreate(
			cohort(org, root, budget("100")),
			leaf(teamA, org, budget("60")),
		)

		msg := denial(k8sClient.Create(suiteCtx, leaf(sub, teamA, budget("10"))))
		gomega.Expect(msg).To(gomega.ContainSubstring("is a ClusterQueue node, which takes no children"))
	})

	ginkgo.It("rejects a namespace already bound by another leaf", func() {
		org, incumbent, poacher := uniqueName("org"), uniqueName("team"), uniqueName("team")
		ns := uniqueName("ns")

		first := leaf(incumbent, org, budget("60"))
		first.Spec.Namespaces = []string{ns}
		mustCreate(cohort(org, root, budget("100")), first)

		second := leaf(poacher, org, budget("10"))
		second.Spec.Namespaces = []string{ns}
		msg := denial(k8sClient.Create(suiteCtx, second))
		gomega.Expect(msg).To(gomega.ContainSubstring(
			"is already bound by leaf " + `"` + incumbent + `"`))
	})

	// A DELETE admission review carries no object body, so the handler has to
	// work from the request name against live state. Nothing about that is
	// exercised by a CREATE.
	ginkgo.It("rejects deleting a grouping that still has children", func() {
		org, teamA := uniqueName("org"), uniqueName("team")
		orgNode := cohort(org, root, budget("100"))
		mustCreate(orgNode, leaf(teamA, org, budget("60")))

		msg := denial(k8sClient.Delete(suiteCtx, orgNode))
		gomega.Expect(msg).To(gomega.ContainSubstring(
			teamA + `: parentRef "` + org + `" does not resolve`))
	})

	ginkgo.It("admits deleting a leaf", func() {
		org, teamA := uniqueName("org"), uniqueName("team")
		teamNode := leaf(teamA, org, budget("60"))
		mustCreate(cohort(org, root, budget("100")), teamNode)

		gomega.Expect(k8sClient.Delete(suiteCtx, teamNode)).To(gomega.Succeed())
	})

	// UPDATE is judged on what it adds, so the direction of a change decides it:
	// shrinking the parent under its children is new breakage, while lowering a
	// child is not. The repair case proper — a write that leaves a tree still
	// violating being admitted — cannot be staged here, because admission is
	// what prevents the tree reaching that state in the first place; it is
	// reachable only by concurrent writes, and is covered by unit tests against
	// an arbitrary starting set.
	ginkgo.It("judges an update by the violations it adds, not the ones it leaves", func() {
		org, teamA, teamB := uniqueName("org"), uniqueName("team"), uniqueName("team")
		mustCreate(
			cohort(org, root, budget("100")),
			leaf(teamA, org, budget("60")),
			leaf(teamB, org, budget("40")),
		)

		shrunk := &v1beta1.AcceleratorQuota{}
		gomega.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{Name: org}, shrunk)).To(gomega.Succeed())
		shrunk.Spec.Budgets = []v1beta1.AcceleratorBudget{budget("50")}
		msg := denial(k8sClient.Update(suiteCtx, shrunk))
		gomega.Expect(msg).To(gomega.ContainSubstring(
			org + ": budget google.com/tpu on tpu7x is 50 but its children total 100"))

		lowered := &v1beta1.AcceleratorQuota{}
		gomega.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{Name: teamB}, lowered)).To(gomega.Succeed())
		lowered.Spec.Budgets = []v1beta1.AcceleratorBudget{budget("20")}
		gomega.Expect(k8sClient.Update(suiteCtx, lowered)).To(gomega.Succeed())
	})

	// The CRD's own CEL runs before the webhook is consulted. Both tiers reject,
	// but only the webhook's message is node-prefixed — worth pinning, because a
	// rule silently migrating between tiers changes what an operator sees.
	ginkgo.It("rejects a leaf-only field on a grouping before the webhook is reached", func() {
		grouping := cohort(uniqueName("org"), root, budget("8"))
		grouping.Spec.Namespaces = []string{"somewhere"}

		msg := denial(k8sClient.Create(suiteCtx, grouping))
		gomega.Expect(msg).To(gomega.ContainSubstring(
			"a Cohort node must not set namespaces, priorityTier, or distribution"))
		gomega.Expect(msg).NotTo(gomega.ContainSubstring("admission webhook"),
			"this is a CEL rejection; the webhook should never see it")
	})

	// The bound is on distance from the root, so it is a property of where a
	// node lands rather than of the node itself — the same write is fine one
	// level up. Nothing in the submitted object says how deep it is.
	ginkgo.It("rejects a node past the configured depth", func() {
		l1, l2, l3 := uniqueName("l1"), uniqueName("l2"), uniqueName("l3")
		mustCreate(
			cohort(l1, root, budget("100")),
			cohort(l2, l1, budget("100")),
			cohort(l3, l2, budget("100")), // depth 3, the deepest admitted
		)

		tooDeep := leaf(uniqueName("deep"), l3, budget("10"))
		msg := denial(k8sClient.Create(suiteCtx, tooDeep))
		gomega.Expect(msg).To(gomega.ContainSubstring(
			"is 4 levels below the root, exceeding the configured maximum of 3"))
	})

	ginkgo.It("rejects a name too long to survive as a label value", func() {
		long := &v1beta1.AcceleratorQuota{
			ObjectMeta: metav1.ObjectMeta{Name: longName()},
			Spec: v1beta1.AcceleratorQuotaSpec{
				Role:      v1beta1.AcceleratorQuotaRoleClusterQueue,
				ParentRef: &v1beta1.AcceleratorQuotaParentRef{Name: root},
				Budgets: []v1beta1.AcceleratorBudget{{
					ResourceName:   "google.com/tpu",
					ResourceFlavor: "tpu7x",
					Nominal:        resource.MustParse("1"),
				}},
			},
		}
		msg := denial(k8sClient.Create(suiteCtx, long))
		gomega.Expect(msg).To(gomega.ContainSubstring("may not exceed 63 characters"))
	})
})

// longName is 64 characters: one past the cap the name is held to so it stays
// usable as a label value on every Kueue object the node materializes.
func longName() string {
	const n = 64
	b := make([]byte, n)
	for i := range b {
		b[i] = 'a'
	}
	return string(b)
}
