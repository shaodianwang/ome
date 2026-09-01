package quotatree

import (
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/quota/tree"
)

// fixtureNow is the clock a rendered fixture is measured against, passed as
// RenderOptions.Now so an AGE column reads the same on two runs. Every node
// gets a creationTimestamp by default, because a node without one cannot exist
// in a cluster and a golden showing the empty case would document nothing.
var (
	fixtureNow = time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	fixtureAge = 3 * time.Hour
)

const (
	gpu   = "nvidia.com/gpu"
	tpu   = "google.com/tpu"
	a100  = "a100"
	h100  = "h100"
	tpu7x = "tpu7x"
)

func qty(s string) resource.Quantity { return resource.MustParse(s) }

func specBudget(resourceName, flavor, nominal string) v1beta1.AcceleratorBudget {
	return v1beta1.AcceleratorBudget{
		ResourceName:   resourceName,
		ResourceFlavor: flavor,
		Nominal:        qty(nominal),
	}
}

func statusBudget(resourceName, flavor, nominal, admitted string) v1beta1.AcceleratorBudgetStatus {
	return v1beta1.AcceleratorBudgetStatus{
		ResourceName:   resourceName,
		ResourceFlavor: flavor,
		Nominal:        qty(nominal),
		Admitted:       qty(admitted),
	}
}

// share is one member's slice of a status budget, as a management plane
// reports it: the nominal it assigned, and what that member holds against it.
func share(cluster, nominal, admitted string) v1beta1.AcceleratorClusterBudgetStatus {
	return v1beta1.AcceleratorClusterBudgetStatus{
		Cluster:  cluster,
		Nominal:  qty(nominal),
		Admitted: qty(admitted),
	}
}

// splitAcross attaches a per-cluster breakdown to a status budget.
func splitAcross(b v1beta1.AcceleratorBudgetStatus,
	shares ...v1beta1.AcceleratorClusterBudgetStatus,
) v1beta1.AcceleratorBudgetStatus {
	b.PerCluster = append(b.PerCluster, shares...)
	return b
}

func withPolicy(policy v1beta1.AcceleratorQuotaDistributionPolicy) nodeOpt {
	return func(q *v1beta1.AcceleratorQuota) {
		q.Spec.Distribution = &v1beta1.AcceleratorQuotaDistribution{Policy: policy}
	}
}

// node builds one CR. Options mutate it so a test states only what it cares
// about, keeping the shape of an eight-node fixture readable.
type nodeOpt func(*v1beta1.AcceleratorQuota)

func node(name string, role v1beta1.AcceleratorQuotaRole, parent string, opts ...nodeOpt) v1beta1.AcceleratorQuota {
	q := v1beta1.AcceleratorQuota{
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			CreationTimestamp: metav1.NewTime(fixtureNow.Add(-fixtureAge)),
		},
		Spec: v1beta1.AcceleratorQuotaSpec{Role: role},
	}
	if parent != "" {
		q.Spec.ParentRef = &v1beta1.AcceleratorQuotaParentRef{Name: parent}
	}
	for _, o := range opts {
		o(&q)
	}
	return q
}

// withAge backdates a node, so a table proves AGE is per-node rather than one
// value repeated down the column.
func withAge(d time.Duration) nodeOpt {
	return func(q *v1beta1.AcceleratorQuota) {
		q.CreationTimestamp = metav1.NewTime(fixtureNow.Add(-d))
	}
}

func cohort(name, parent string, opts ...nodeOpt) v1beta1.AcceleratorQuota {
	return node(name, v1beta1.AcceleratorQuotaRoleCohort, parent, opts...)
}

func leaf(name, parent string, opts ...nodeOpt) v1beta1.AcceleratorQuota {
	return node(name, v1beta1.AcceleratorQuotaRoleClusterQueue, parent, opts...)
}

func withSpec(budgets ...v1beta1.AcceleratorBudget) nodeOpt {
	return func(q *v1beta1.AcceleratorQuota) { q.Spec.Budgets = append(q.Spec.Budgets, budgets...) }
}

func withStatus(budgets ...v1beta1.AcceleratorBudgetStatus) nodeOpt {
	return func(q *v1beta1.AcceleratorQuota) { q.Status.Budgets = append(q.Status.Budgets, budgets...) }
}

func withNamespaces(ns ...string) nodeOpt {
	return func(q *v1beta1.AcceleratorQuota) { q.Spec.Namespaces = append(q.Spec.Namespaces, ns...) }
}

func withTier(tier string) nodeOpt {
	return func(q *v1beta1.AcceleratorQuota) { q.Spec.PriorityTier = tier }
}

func withPath(path string) nodeOpt {
	return func(q *v1beta1.AcceleratorQuota) { q.Status.Path = path }
}

func withCondition(condType, status string) nodeOpt {
	return func(q *v1beta1.AcceleratorQuota) {
		q.Status.Conditions = append(q.Status.Conditions, metav1.Condition{
			Type:               condType,
			Status:             metav1.ConditionStatus(status),
			Reason:             "Test",
			LastTransitionTime: metav1.Now(),
		})
	}
}

func withCapacity(resourceName, flavor, allocatable, peak string) nodeOpt {
	return func(q *v1beta1.AcceleratorQuota) {
		q.Status.Capacity = append(q.Status.Capacity, v1beta1.AcceleratorCapacityStatus{
			ResourceName:   resourceName,
			ResourceFlavor: flavor,
			Allocatable:    qty(allocatable),
			HighWaterMark:  qty(peak),
		})
	}
}

func ready() nodeOpt { return withCondition(v1beta1.AcceleratorQuotaReady, "True") }

// buildView assembles quotas the same way the command does.
func buildView(t *testing.T, quotas []v1beta1.AcceleratorQuota, maxDepth int) *View {
	t.Helper()
	tr, vs, err := tree.Build(quotas, tree.Options{
		RootName: v1beta1.AcceleratorQuotaRootName,
		MaxDepth: maxDepth,
	})
	if err != nil {
		t.Fatalf("tree.Build: %v", err)
	}
	return BuildView(tr, vs)
}

// healthyFleet is a two-org tree on one accelerator type, fully reconciled.
// Numbers are contained at every level so the fixture itself is valid.
func healthyFleet() []v1beta1.AcceleratorQuota {
	return []v1beta1.AcceleratorQuota{
		cohort("root", "",
			withSpec(specBudget(gpu, a100, "512")),
			withStatus(statusBudget(gpu, a100, "512", "288")),
			withCapacity(gpu, a100, "520", "544"),
			withAge(50*time.Hour),
			withPath("/root"), ready()),
		cohort("org-research", "root",
			withSpec(specBudget(gpu, a100, "256")),
			withStatus(statusBudget(gpu, a100, "256", "160")),
			withPath("/root/org-research"), ready()),
		leaf("team-nlp", "org-research",
			withSpec(specBudget(gpu, a100, "128")),
			withStatus(statusBudget(gpu, a100, "128", "96")),
			withNamespaces("nlp-prod", "nlp-dev"), withTier("high"),
			withPath("/root/org-research/team-nlp"), ready()),
		leaf("team-vision", "org-research",
			withSpec(specBudget(gpu, a100, "128")),
			withStatus(statusBudget(gpu, a100, "128", "64")),
			withNamespaces("vision-prod"),
			withPath("/root/org-research/team-vision"), ready()),
		cohort("org-product", "root",
			withSpec(specBudget(gpu, a100, "256")),
			withStatus(statusBudget(gpu, a100, "256", "128")),
			withPath("/root/org-product"), ready()),
		leaf("team-serving", "org-product",
			withSpec(specBudget(gpu, a100, "256")),
			withStatus(statusBudget(gpu, a100, "256", "128")),
			withNamespaces("serving-prod"),
			withPath("/root/org-product/team-serving"),
			withCondition(v1beta1.AcceleratorQuotaReady, "False")),
	}
}

// splitFleet is a management plane's tree: a fleet total on every tier, and on
// the one leaf the split into the share each member was assigned. The Cohorts
// carry none because only leaves are apportioned — a grouping travels to every
// member as pure topology.
func splitFleet() []v1beta1.AcceleratorQuota {
	return []v1beta1.AcceleratorQuota{
		cohort("root", "",
			withSpec(specBudget(tpu, tpu7x, "192")),
			withStatus(statusBudget(tpu, tpu7x, "192", "0")), ready()),
		cohort("tenants", "root",
			withSpec(specBudget(tpu, tpu7x, "192")),
			withStatus(statusBudget(tpu, tpu7x, "192", "0")), ready()),
		leaf("serving", "tenants",
			withSpec(specBudget(tpu, tpu7x, "192")),
			withPolicy(v1beta1.AcceleratorQuotaDistributionProportional),
			withStatus(splitAcross(statusBudget(tpu, tpu7x, "192", "24"),
				share("member-a", "128", "16"),
				share("member-b", "64", "8"))),
			withNamespaces("serving-prod"), ready()),
	}
}
