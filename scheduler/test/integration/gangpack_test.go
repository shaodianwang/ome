package integration

import (
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/kubernetes/pkg/scheduler"
	schedconfig "k8s.io/kubernetes/pkg/scheduler/apis/config"
	fwkruntime "k8s.io/kubernetes/pkg/scheduler/framework/runtime"

	schedutil "sigs.k8s.io/scheduler-plugins/test/util"

	"github.com/ome-projects/ome/scheduler/pkg/plugins/gangpack"
)

// enableGangPack turns on OMEGangPack at every extension point it implements,
// keeping the default plugins otherwise intact.
func enableGangPack(p *schedconfig.Plugins) {
	p.PreFilter.Enabled = append(p.PreFilter.Enabled, schedconfig.Plugin{Name: gangpack.Name})
	p.Filter.Enabled = append(p.Filter.Enabled, schedconfig.Plugin{Name: gangpack.Name})
	p.Permit.Enabled = append(p.Permit.Enabled, schedconfig.Plugin{Name: gangpack.Name})
	p.Reserve.Enabled = append(p.Reserve.Enabled, schedconfig.Plugin{Name: gangpack.Name})
}

// gangPackOptions builds the scheduler.Options that register and enable the
// plugin — the exact wiring cmd/ome-scheduler uses, driven from a component
// config so the profile carries our extension-point enrollment.
func gangPackOptions(t *testing.T) []scheduler.Option {
	t.Helper()
	cfg, err := schedutil.NewDefaultSchedulerComponentConfig()
	if err != nil {
		t.Fatalf("default scheduler config: %v", err)
	}
	enableGangPack(cfg.Profiles[0].Plugins)
	return []scheduler.Option{
		scheduler.WithProfiles(cfg.Profiles...),
		scheduler.WithFrameworkOutOfTreeRegistry(fwkruntime.Registry{gangpack.Name: gangpack.New}),
	}
}

// TestGangLandsInOneDomain is the happy path: an N-pod gang best-fits into a
// single topology domain and every member binds to a node of that domain.
//
// Layout: domain "a" has 2 gpu nodes, domain "b" has 3. A 2-pod gang fits both,
// so best-fit (fewest free that still fits) picks "a". The gate holds both
// members until the whole gang is present, then binds them — one per node,
// because each node has exactly one gpu.
func TestGangLandsInOneDomain(t *testing.T) {
	tc := startScheduler(t, globalKubeConfig, gangPackOptions(t)...)
	defer tc.teardown(t)

	const ns = "gang-onedomain"
	createNamespace(t, tc, ns)

	// Node names are unique per test: the suite shares one envtest apiserver, and a
	// prior test's bound pods linger on its (deleted) nodes — reusing a node name
	// would resurrect them as phantom occupancy.
	nodes := []struct {
		name, domain string
	}{
		{"od-a1", "a"}, {"od-a2", "a"},
		{"od-b1", "b"}, {"od-b2", "b"}, {"od-b3", "b"},
	}
	for _, n := range nodes {
		if _, err := tc.ClientSet.CoreV1().Nodes().Create(tc.Ctx, makeGPUNode(n.name, n.domain, 1), metav1.CreateOptions{}); err != nil {
			t.Fatalf("create node %s: %v", n.name, err)
		}
	}

	// PodGroup declares the gang: 2 members, domain = the domainLabelKey label.
	pg := schedutil.MakePG("gang", ns, 2, nil, nil)
	pg.Annotations = map[string]string{topologyKeyAnnotation: domainLabelKey}
	if _, err := tc.SchedClient.SchedulingV1alpha1().PodGroups(ns).Create(tc.Ctx, pg, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create podgroup: %v", err)
	}

	podNames := []string{"gp-0", "gp-1"}
	for _, name := range podNames {
		if _, err := tc.ClientSet.CoreV1().Pods(ns).Create(tc.Ctx, makeGangPod(name, ns, "gang"), metav1.CreateOptions{}); err != nil {
			t.Fatalf("create pod %s: %v", name, err)
		}
	}

	domainA := map[string]bool{"od-a1": true, "od-a2": true}
	bound := map[string]bool{}
	for _, name := range podNames {
		node := waitForPodBound(t, tc, ns, name, 30*time.Second)
		if !domainA[node] {
			t.Errorf("pod %s bound to %s, want a node of domain a (od-a1/od-a2)", name, node)
		}
		if bound[node] {
			t.Errorf("two gang pods bound to the same node %s (whole-node model expects one each)", node)
		}
		bound[node] = true
	}
}

// TestGangGateHoldsUntilComplete is the all-or-nothing guarantee: a gang member
// must NOT bind while its gang is incomplete, and both members bind the moment
// the gang is whole. This is what makes it a gang scheduler rather than a
// bin-packer — without the Permit gate, the first pod would bind immediately and
// strand the workload half-scheduled.
//
// Two nodes in one domain, a 2-member PodGroup. Creating just one pod, it best-
// fits, pins, and gates — but stays unbound. Adding the second opens the gate and
// both bind. The PodGroup's gate timeout is set well above the hold window so the
// test never races the timeout unwind.
func TestGangGateHoldsUntilComplete(t *testing.T) {
	tc := startScheduler(t, globalKubeConfig, gangPackOptions(t)...)
	defer tc.teardown(t)

	const ns = "gang-gate"
	createNamespace(t, tc, ns)

	for _, name := range []string{"g1", "g2"} {
		if _, err := tc.ClientSet.CoreV1().Nodes().Create(tc.Ctx, makeGPUNode(name, "a", 1), metav1.CreateOptions{}); err != nil {
			t.Fatalf("create node %s: %v", name, err)
		}
	}

	pg := schedutil.MakePG("gang", ns, 2, nil, nil)
	pg.Annotations = map[string]string{topologyKeyAnnotation: domainLabelKey}
	// Override MakePG's 10s default so the hold window below can't race the gate
	// timeout (which would unwind the lone member and mask the property).
	longTimeout := int32(120)
	pg.Spec.ScheduleTimeoutSeconds = &longTimeout
	if _, err := tc.SchedClient.SchedulingV1alpha1().PodGroups(ns).Create(tc.Ctx, pg, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create podgroup: %v", err)
	}

	// One of two members: the gate must hold it unbound.
	if _, err := tc.ClientSet.CoreV1().Pods(ns).Create(tc.Ctx, makeGangPod("gp-0", ns, "gang"), metav1.CreateOptions{}); err != nil {
		t.Fatalf("create pod gp-0: %v", err)
	}
	ensureNotBound(t, tc, ns, "gp-0", 4*time.Second)

	// Complete the gang: the gate opens and both bind.
	if _, err := tc.ClientSet.CoreV1().Pods(ns).Create(tc.Ctx, makeGangPod("gp-1", ns, "gang"), metav1.CreateOptions{}); err != nil {
		t.Fatalf("create pod gp-1: %v", err)
	}
	for _, name := range []string{"gp-0", "gp-1"} {
		node := waitForPodBound(t, tc, ns, name, 30*time.Second)
		if node != "g1" && node != "g2" {
			t.Errorf("pod %s bound to %s, want g1/g2", name, node)
		}
	}
}

// TestIncompleteGangTimesOutThenRecovers is the failure-cleanup guarantee: a
// gang that cannot form must time out and unwind cleanly — releasing its pin and
// never leaving a member partially bound — and must still schedule once it
// becomes completable. This exercises the Permit-timeout -> Unreserve path that
// the gate test deliberately avoids (it raises the timeout).
//
// A 2-member PodGroup with a short gate timeout, but only one pod. The lone
// member best-fits, pins, gates, and repeatedly times out; across that churn it
// stays unbound (proving no half-scheduled strand and that Unreserve releases the
// pin each cycle). Then the gang is made completable by dropping minMember to 1 —
// a single-pod recovery, so there is no fragile two-pod realignment to race — and
// the member binds.
func TestIncompleteGangTimesOutThenRecovers(t *testing.T) {
	tc := startScheduler(t, globalKubeConfig, gangPackOptions(t)...)
	defer tc.teardown(t)

	const ns = "gang-timeout"
	createNamespace(t, tc, ns)

	for _, name := range []string{"t1", "t2"} {
		if _, err := tc.ClientSet.CoreV1().Nodes().Create(tc.Ctx, makeGPUNode(name, "a", 1), metav1.CreateOptions{}); err != nil {
			t.Fatalf("create node %s: %v", name, err)
		}
	}

	pg := schedutil.MakePG("gang", ns, 2, nil, nil)
	pg.Annotations = map[string]string{topologyKeyAnnotation: domainLabelKey}
	// Short gate timeout so the lone member cycles through gate -> timeout ->
	// unwind several times within the hold window below.
	shortTimeout := int32(3)
	pg.Spec.ScheduleTimeoutSeconds = &shortTimeout
	if _, err := tc.SchedClient.SchedulingV1alpha1().PodGroups(ns).Create(tc.Ctx, pg, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create podgroup: %v", err)
	}

	if _, err := tc.ClientSet.CoreV1().Pods(ns).Create(tc.Ctx, makeGangPod("gp-0", ns, "gang"), metav1.CreateOptions{}); err != nil {
		t.Fatalf("create pod gp-0: %v", err)
	}
	// Over ~2 timeout cycles the lone member must never bind — the gate times it
	// out and Unreserve releases the pin each round rather than stranding it.
	ensureNotBound(t, tc, ns, "gp-0", 7*time.Second)

	// Make the gang completable by the lone member: minMember 2 -> 1. The plugin
	// re-reads the PodGroup each cycle, so once its backoff expires the member's
	// gate opens and it binds.
	cur, err := tc.SchedClient.SchedulingV1alpha1().PodGroups(ns).Get(tc.Ctx, "gang", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get podgroup: %v", err)
	}
	cur.Spec.MinMember = 1
	if _, err := tc.SchedClient.SchedulingV1alpha1().PodGroups(ns).Update(tc.Ctx, cur, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("shrink podgroup minMember: %v", err)
	}

	// The plugin registers no EnqueueExtensions, so a Permit-rejected member is
	// requeued only on the scheduler's slow periodic unschedulable flush, and the
	// PodGroup edit alone doesn't wake it (the scheduling queue doesn't watch
	// PodGroups). Nudge with an irrelevant node event — a gpu-less, domain-less
	// node the member can never land on (NodeResourcesFit rejects it, freeByDomain
	// ignores it) — to flush the queue promptly. Once requeue hints land this
	// nudge can go. (See the recovery-latency follow-up.)
	if _, err := tc.ClientSet.CoreV1().Nodes().Create(tc.Ctx,
		&v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "nudge"}}, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create nudge node: %v", err)
	}

	node := waitForPodBound(t, tc, ns, "gp-0", 30*time.Second)
	if node != "t1" && node != "t2" {
		t.Errorf("pod gp-0 bound to %s, want t1/t2", node)
	}
}

// TestTwoGangsDoNotOverCommitDomain is the capacity-claim guarantee: two gangs
// whose best-fit both prefers the same tight domain must not both pile into it
// and over-commit. When one gang pins a domain it reserves its whole-node
// capacity there, so the other best-fits into what's left.
//
// Domain "a" has 3 nodes, domain "b" has 4. Both gangs need 3, and "a" is the
// tightest fit for 3 — so without the reservation both would pin "a", leaving 6
// pods to fight over 3 nodes and stranding the loser. With it, whichever gang
// pins "a" first reserves all of it; the other lands wholly in "b". The invariant
// asserted (robust to arrival order): every pod binds, each gang sits in a single
// domain, and the two gangs occupy different domains.
func TestTwoGangsDoNotOverCommitDomain(t *testing.T) {
	tc := startScheduler(t, globalKubeConfig, gangPackOptions(t)...)
	defer tc.teardown(t)

	const ns = "gang-nooverlap"
	createNamespace(t, tc, ns)

	// Unique node names per test (shared apiserver — see TestGangLandsInOneDomain).
	nodes := []struct{ name, domain string }{
		{"oc-a1", "a"}, {"oc-a2", "a"}, {"oc-a3", "a"},
		{"oc-b1", "b"}, {"oc-b2", "b"}, {"oc-b3", "b"}, {"oc-b4", "b"},
	}
	for _, n := range nodes {
		if _, err := tc.ClientSet.CoreV1().Nodes().Create(tc.Ctx, makeGPUNode(n.name, n.domain, 1), metav1.CreateOptions{}); err != nil {
			t.Fatalf("create node %s: %v", n.name, err)
		}
	}

	// Two independent 3-member gangs. Generous gate timeout so completion, not a
	// timeout, is what we observe.
	longTimeout := int32(60)
	for _, name := range []string{"ga", "gb"} {
		pg := schedutil.MakePG(name, ns, 3, nil, nil)
		pg.Annotations = map[string]string{topologyKeyAnnotation: domainLabelKey}
		pg.Spec.ScheduleTimeoutSeconds = &longTimeout
		if _, err := tc.SchedClient.SchedulingV1alpha1().PodGroups(ns).Create(tc.Ctx, pg, metav1.CreateOptions{}); err != nil {
			t.Fatalf("create podgroup %s: %v", name, err)
		}
	}

	gangPods := map[string][]string{
		"ga": {"ga-0", "ga-1", "ga-2"},
		"gb": {"gb-0", "gb-1", "gb-2"},
	}
	for pgName, pods := range gangPods {
		for _, name := range pods {
			if _, err := tc.ClientSet.CoreV1().Pods(ns).Create(tc.Ctx, makeGangPod(name, ns, pgName), metav1.CreateOptions{}); err != nil {
				t.Fatalf("create pod %s: %v", name, err)
			}
		}
	}

	// Resolve each gang's domain set from where its members bound.
	gangDomains := map[string]map[string]bool{}
	for pgName, pods := range gangPods {
		domains := map[string]bool{}
		for _, name := range pods {
			node := waitForPodBound(t, tc, ns, name, 60*time.Second)
			domains[domainOfNode(t, tc, node)] = true
		}
		if len(domains) != 1 {
			t.Errorf("gang %s spans %d domains %v, want exactly 1 (gang integrity)", pgName, len(domains), keys(domains))
		}
		gangDomains[pgName] = domains
	}

	// The two gangs must occupy different domains — else they over-committed one.
	if sameDomain(gangDomains["ga"], gangDomains["gb"]) {
		t.Errorf("both gangs landed in the same domain (over-commit): ga=%v gb=%v",
			keys(gangDomains["ga"]), keys(gangDomains["gb"]))
	}
}
