package multicluster

import (
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

// The management plane against real members: one fleet budget authored on the
// hub becomes a per-cluster number on each member's own apiserver, written
// across a real connection with a real server-side apply.
//
// The unit tests drive fake members, so they prove the arithmetic and the
// bookkeeping and nothing about whether an apply actually lands, whether the
// marks survive the round trip, or whether a delete reaches another cluster.
// That is what only real apiservers can show.
var _ = ginkgo.Describe("Management-plane quota projection", ginkgo.Ordered, func() {
	const (
		tpu    = "google.com/tpu"
		flavor = "tpu7x"
	)

	var (
		secretNS string
		wcA      string
		wcB      string
		secretA  string
		secretB  string
		org      string
		team     string
	)

	// capacityOn gives a member the root its own controller would have created,
	// carrying the capacity it derived from its hardware. That object is the
	// member's half of the contract; the hub only reads it.
	capacityOn := func(c cluster, chips string) {
		root := &v1beta1.AcceleratorQuota{
			ObjectMeta: metav1.ObjectMeta{Name: v1beta1.AcceleratorQuotaRootName},
			Spec:       v1beta1.AcceleratorQuotaSpec{Role: v1beta1.AcceleratorQuotaRoleCohort},
		}
		mustCreate(c, root)
		gomega.Eventually(func(g gomega.Gomega) {
			var live v1beta1.AcceleratorQuota
			g.Expect(c.client.Get(c.ctx, types.NamespacedName{Name: root.Name}, &live)).To(gomega.Succeed())
			live.Status.Capacity = []v1beta1.AcceleratorCapacityStatus{{
				ResourceName:   tpu,
				ResourceFlavor: flavor,
				Allocatable:    resource.MustParse(chips),
			}}
			g.Expect(c.client.Status().Update(c.ctx, &live)).To(gomega.Succeed())
		}, eventuallyTimeout, eventuallyPoll).Should(gomega.Succeed())
	}

	// nominalOn reads back the share a member was given.
	nominalOn := func(c cluster, name string) (string, error) {
		var q v1beta1.AcceleratorQuota
		if err := c.client.Get(c.ctx, types.NamespacedName{Name: name}, &q); err != nil {
			return "", err
		}
		if len(q.Spec.Budgets) == 0 {
			return "", nil
		}
		return q.Spec.Budgets[0].Nominal.String(), nil
	}

	// absentOn reports whether a copy is gone from a member. Deletion is the
	// assertion, so "not found" is the success and any other error is a failure
	// to distinguish from it.
	absentOn := func(c cluster, name string) bool {
		var q v1beta1.AcceleratorQuota
		err := c.client.Get(c.ctx, types.NamespacedName{Name: name}, &q)
		return err != nil && client.IgnoreNotFound(err) == nil
	}

	registerMember := func(name, secret string, member cluster) {
		mustCreate(controlCluster, kubeconfigSecretFor(member, secretNS, secret))
		mustCreate(controlCluster, WorkloadClusterFor(name, secretNS, secret, nil))
	}

	// Registered once for the whole Describe, not per spec. The projector holds
	// every projection while any registered member is unconnected -- correct in
	// production, where that means a partition -- so a registry that churned
	// between specs would keep the fleet permanently held and nothing would ever
	// land.
	ginkgo.BeforeAll(func() {
		secretNS = uniqueName("quota-proj")
		createNamespace(controlCluster, secretNS)
		wcA, wcB = uniqueName("qwc-a"), uniqueName("qwc-b")
		secretA, secretB = uniqueName("qkc-a"), uniqueName("qkc-b")

		registerMember(wcA, secretA, member1Cluster)
		registerMember(wcB, secretB, member2Cluster)
		capacityOn(member1Cluster, "3")
		capacityOn(member2Cluster, "1")

		// The hub's own root. On a management plane it is admin-authored -- the
		// member's local controller creates the member's, and the hub has no
		// controller doing that for itself. Without it the authored tiers have
		// no parent to hang from and the tree never assembles.
		mustCreate(controlCluster, &v1beta1.AcceleratorQuota{
			ObjectMeta: metav1.ObjectMeta{Name: v1beta1.AcceleratorQuotaRootName},
			Spec:       v1beta1.AcceleratorQuotaSpec{Role: v1beta1.AcceleratorQuotaRoleCohort},
		})

		// Both connected before anything is authored, so a spec that sees
		// nothing projected is reporting a real failure rather than a fleet that
		// had not finished coming up.
		gomega.Eventually(func() []string { return controlCluster.quotaClusters.Connected() },
			eventuallyTimeout, eventuallyPoll).Should(gomega.ConsistOf(wcA, wcB))
	})

	ginkgo.BeforeEach(func() {
		org, team = uniqueName("org"), uniqueName("team")
	})

	// Only this spec's tree. Deleting the source is what reaps the copies, and
	// the members are left registered for the next spec.
	ginkgo.AfterEach(func() {
		for _, name := range []string{team, org} {
			deleteIfExists(controlCluster, &v1beta1.AcceleratorQuota{ObjectMeta: metav1.ObjectMeta{Name: name}})
		}
	})

	ginkgo.AfterAll(func() {
		deleteIfExists(controlCluster, &v1beta1.AcceleratorQuota{
			ObjectMeta: metav1.ObjectMeta{Name: v1beta1.AcceleratorQuotaRootName}})
		for _, wc := range []string{wcA, wcB} {
			deleteIfExists(controlCluster, &v1beta1.WorkloadCluster{ObjectMeta: metav1.ObjectMeta{Name: wc}})
		}
		for _, sec := range []string{secretA, secretB} {
			deleteIfExists(controlCluster, &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: sec, Namespace: secretNS}})
		}
		for _, m := range []cluster{member1Cluster, member2Cluster} {
			deleteIfExists(m, &v1beta1.AcceleratorQuota{
				ObjectMeta: metav1.ObjectMeta{Name: v1beta1.AcceleratorQuotaRootName}})
		}
	})

	// authorTree writes a fleet budget on the hub. The reserved root is not
	// authored: each member creates its own, which is what capacityOn stands in
	// for above.
	authorTree := func(nominal string) {
		mustCreate(controlCluster, &v1beta1.AcceleratorQuota{
			ObjectMeta: metav1.ObjectMeta{Name: org},
			Spec: v1beta1.AcceleratorQuotaSpec{
				Role:      v1beta1.AcceleratorQuotaRoleCohort,
				ParentRef: &v1beta1.AcceleratorQuotaParentRef{Name: v1beta1.AcceleratorQuotaRootName},
			},
		})
		mustCreate(controlCluster, &v1beta1.AcceleratorQuota{
			ObjectMeta: metav1.ObjectMeta{Name: team},
			Spec: v1beta1.AcceleratorQuotaSpec{
				Role:       v1beta1.AcceleratorQuotaRoleClusterQueue,
				ParentRef:  &v1beta1.AcceleratorQuotaParentRef{Name: org},
				Namespaces: []string{"tenant-ns"},
				Budgets: []v1beta1.AcceleratorBudget{{
					ResourceName:   tpu,
					ResourceFlavor: flavor,
					Nominal:        resource.MustParse(nominal),
				}},
			},
		})
	}

	ginkgo.It("splits one fleet budget across two real members", func() {
		authorTree("120")

		// 3:1 reported capacity, so 90 and 30 — and 120 in total, never more.
		gomega.Eventually(func() (string, error) { return nominalOn(member1Cluster, team) },
			eventuallyTimeout, eventuallyPoll).Should(gomega.Equal("90"))
		gomega.Eventually(func() (string, error) { return nominalOn(member2Cluster, team) },
			eventuallyTimeout, eventuallyPoll).Should(gomega.Equal("30"))

		ginkgo.By("carrying the marks that make a copy recognisable as one")
		var got v1beta1.AcceleratorQuota
		gomega.Expect(member1Cluster.client.Get(member1Cluster.ctx,
			types.NamespacedName{Name: team}, &got)).To(gomega.Succeed())
		gomega.Expect(got.Labels).To(gomega.HaveKeyWithValue(v1beta1.AcceleratorQuotaOriginLabel, quotaOrigin))
		gomega.Expect(got.Annotations).To(gomega.HaveKeyWithValue(v1beta1.AcceleratorQuotaClusterAnnotation, wcA))
		// A copy carries a resolved number and never the split that produced it.
		gomega.Expect(got.Spec.Budgets[0].PerCluster).To(gomega.BeEmpty())

		ginkgo.By("bringing the ancestor across as topology, so parentRef resolves")
		var ancestor v1beta1.AcceleratorQuota
		gomega.Expect(member1Cluster.client.Get(member1Cluster.ctx,
			types.NamespacedName{Name: org}, &ancestor)).To(gomega.Succeed())
		gomega.Expect(ancestor.Spec.Budgets).To(gomega.BeEmpty())
		gomega.Expect(ancestor.Spec.ParentRef.Name).To(gomega.Equal(v1beta1.AcceleratorQuotaRootName))
	})

	ginkgo.It("takes a tenant's budget back from every member when it is deleted", func() {
		authorTree("120")
		gomega.Eventually(func() (string, error) { return nominalOn(member1Cluster, team) },
			eventuallyTimeout, eventuallyPoll).Should(gomega.Equal("90"))
		gomega.Eventually(func() (string, error) { return nominalOn(member2Cluster, team) },
			eventuallyTimeout, eventuallyPoll).Should(gomega.Equal("30"))

		var source v1beta1.AcceleratorQuota
		gomega.Expect(controlCluster.client.Get(controlCluster.ctx,
			types.NamespacedName{Name: team}, &source)).To(gomega.Succeed())
		gomega.Expect(source.Finalizers).NotTo(gomega.BeEmpty(),
			"an unclaimed source would strand its budgets on every member")

		gomega.Expect(controlCluster.client.Delete(controlCluster.ctx, &source)).To(gomega.Succeed())

		// Gone from the members, and only then gone from the hub.
		for _, m := range []cluster{member1Cluster, member2Cluster} {
			gomega.Eventually(func() bool {
				var q v1beta1.AcceleratorQuota
				err := m.client.Get(m.ctx, types.NamespacedName{Name: team}, &q)
				return client.IgnoreNotFound(err) == nil && err != nil
			}, eventuallyTimeout, eventuallyPoll).Should(gomega.BeTrue(),
				"the tenant was deleted on the hub but its budget is still live on a member")
		}
		gomega.Eventually(func() bool {
			var q v1beta1.AcceleratorQuota
			err := controlCluster.client.Get(controlCluster.ctx, types.NamespacedName{Name: team}, &q)
			return client.IgnoreNotFound(err) == nil && err != nil
		}, eventuallyTimeout, eventuallyPoll).Should(gomega.BeTrue(), "the source never released")
	})

	ginkgo.It("takes the whole tier off a member apportioned nothing", func() {
		authorTree("120")
		gomega.Eventually(func() (string, error) { return nominalOn(member1Cluster, team) },
			eventuallyTimeout, eventuallyPoll).Should(gomega.Equal("90"))
		gomega.Eventually(func() (string, error) { return nominalOn(member2Cluster, team) },
			eventuallyTimeout, eventuallyPoll).Should(gomega.Equal("30"))

		// An explicit split is the only way to say "none here" on purpose. Under
		// Proportional a member reporting capacity always draws something, so a
		// zero would be indistinguishable from a member that had not reported.
		ginkgo.By("re-authoring the split to give the second member nothing")
		gomega.Eventually(func(g gomega.Gomega) {
			var src v1beta1.AcceleratorQuota
			g.Expect(controlCluster.client.Get(controlCluster.ctx,
				types.NamespacedName{Name: team}, &src)).To(gomega.Succeed())
			src.Spec.Distribution = &v1beta1.AcceleratorQuotaDistribution{
				Policy: v1beta1.AcceleratorQuotaDistributionExplicit,
			}
			src.Spec.Budgets[0].PerCluster = []v1beta1.AcceleratorClusterShare{
				{Cluster: wcA, Nominal: resource.MustParse("120")},
				{Cluster: wcB, Nominal: resource.MustParse("0")},
			}
			g.Expect(controlCluster.client.Update(controlCluster.ctx, &src)).To(gomega.Succeed())
		}, eventuallyTimeout, eventuallyPoll).Should(gomega.Succeed())

		gomega.Eventually(func() (string, error) { return nominalOn(member1Cluster, team) },
			eventuallyTimeout, eventuallyPoll).Should(gomega.Equal("120"))

		// Not a queue that admits nothing -- no queue. A zero-budget
		// ClusterQueue would leave a tenant waiting on capacity that was never
		// coming, with nothing to say the fleet had stopped funding it there.
		ginkgo.By("removing the leaf from the member that draws nothing")
		gomega.Eventually(func() bool { return absentOn(member2Cluster, team) },
			eventuallyTimeout, eventuallyPoll).Should(gomega.BeTrue(),
			"a member apportioned zero kept its queue")

		// The tier travelled only because a leaf beneath it needed a parent to
		// resolve against. With the leaf gone it is topology for a tenant that
		// is not there, and leaving it behind is how a member accumulates
		// grouping nodes nothing hangs from.
		ginkgo.By("taking the ancestor that only that leaf needed with it")
		gomega.Eventually(func() bool { return absentOn(member2Cluster, org) },
			eventuallyTimeout, eventuallyPoll).Should(gomega.BeTrue(),
			"the grouping tier outlived the only leaf that required it")

		// The other member is untouched throughout: a sweep is scoped to the
		// cluster whose share changed.
		var kept v1beta1.AcceleratorQuota
		gomega.Expect(member1Cluster.client.Get(member1Cluster.ctx,
			types.NamespacedName{Name: org}, &kept)).To(gomega.Succeed())
	})

	// Consumption moves without anything in the tree changing. The hub is woken
	// by edits to its own nodes and by a resync tick, and neither fires when a
	// tenant's work starts or finishes on a member -- so without watching the
	// members its account of the fleet is stale for up to a whole interval,
	// which for this suite is ten minutes.
	//
	// That long interval is what makes this spec mean something: if the hub
	// picks the figure up inside the timeout below, an event carried it.
	ginkgo.It("follows a member's consumption without waiting for a resync", func() {
		authorTree("120")
		gomega.Eventually(func() (string, error) { return nominalOn(member1Cluster, team) },
			eventuallyTimeout, eventuallyPoll).Should(gomega.Equal("90"))

		// Wait for the hub to go quiet first. Authoring a tree sets off several
		// passes -- each status write is an event on the hub's own nodes -- and
		// one of those landing after the member update would read the figure back
		// for reasons that have nothing to do with watching members. Settling
		// first is what makes the assertion below attributable.
		ginkgo.By("letting the hub settle, so the next pass has only one cause")
		var quiet string
		gomega.Eventually(func(g gomega.Gomega) {
			var src v1beta1.AcceleratorQuota
			g.Expect(controlCluster.client.Get(controlCluster.ctx,
				types.NamespacedName{Name: team}, &src)).To(gomega.Succeed())
			// Recorded BEFORE the assertion: a failing g.Expect aborts this
			// function, so asserting first would leave the previous value never
			// updated and the comparison could never succeed.
			seen := src.ResourceVersion
			previous := quiet
			quiet = seen
			g.Expect(seen).To(gomega.Equal(previous), "the hub is still writing status")
		}).WithPolling(2 * time.Second).WithTimeout(eventuallyTimeout).Should(gomega.Succeed())

		// The member reports what its own Kueue is holding, which is the only
		// place these numbers can come from: the hub runs no Kueue and holds no
		// credential to read one.
		ginkgo.By("reporting consumption on the member's own copy")
		gomega.Eventually(func(g gomega.Gomega) {
			var copyOnMember v1beta1.AcceleratorQuota
			g.Expect(member1Cluster.client.Get(member1Cluster.ctx,
				types.NamespacedName{Name: team}, &copyOnMember)).To(gomega.Succeed())
			copyOnMember.Status.Budgets = []v1beta1.AcceleratorBudgetStatus{{
				ResourceName:   tpu,
				ResourceFlavor: flavor,
				Admitted:       resource.MustParse("42"),
			}}
			g.Expect(member1Cluster.client.Status().Update(
				member1Cluster.ctx, &copyOnMember)).To(gomega.Succeed())
		}, eventuallyTimeout, eventuallyPoll).Should(gomega.Succeed())

		ginkgo.By("seeing it on the hub, long before the resync would have")
		gomega.Eventually(func() (string, error) {
			var src v1beta1.AcceleratorQuota
			if err := controlCluster.client.Get(controlCluster.ctx,
				types.NamespacedName{Name: team}, &src); err != nil {
				return "", err
			}
			for _, b := range src.Status.Budgets {
				for _, pc := range b.PerCluster {
					if pc.Cluster == wcA {
						return pc.Admitted.String(), nil
					}
				}
			}
			return "", nil
		}, eventuallyTimeout, eventuallyPoll).Should(gomega.Equal("42"),
			"the hub never picked up the member's consumption, so the fleet view "+
				"only moves on the resync tick")
	})

	ginkgo.It("follows the fleet total when an admin changes it", func() {
		authorTree("120")
		gomega.Eventually(func() (string, error) { return nominalOn(member1Cluster, team) },
			eventuallyTimeout, eventuallyPoll).Should(gomega.Equal("90"))

		gomega.Eventually(func(g gomega.Gomega) {
			var src v1beta1.AcceleratorQuota
			g.Expect(controlCluster.client.Get(controlCluster.ctx,
				types.NamespacedName{Name: team}, &src)).To(gomega.Succeed())
			src.Spec.Budgets[0].Nominal = resource.MustParse("40")
			g.Expect(controlCluster.client.Update(controlCluster.ctx, &src)).To(gomega.Succeed())
		}, eventuallyTimeout, eventuallyPoll).Should(gomega.Succeed())

		// Re-split against the same 3:1 basis: 30 and 10.
		gomega.Eventually(func() (string, error) { return nominalOn(member1Cluster, team) },
			eventuallyTimeout, eventuallyPoll).Should(gomega.Equal("30"))
		gomega.Eventually(func() (string, error) { return nominalOn(member2Cluster, team) },
			eventuallyTimeout, eventuallyPoll).Should(gomega.Equal("10"))
	})
})
