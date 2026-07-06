package integration

import (
	"testing"
	"time"

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

	nodes := []struct {
		name, domain string
	}{
		{"a1", "a"}, {"a2", "a"},
		{"b1", "b"}, {"b2", "b"}, {"b3", "b"},
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

	domainA := map[string]bool{"a1": true, "a2": true}
	bound := map[string]bool{}
	for _, name := range podNames {
		node := waitForPodBound(t, tc, ns, name, 30*time.Second)
		if !domainA[node] {
			t.Errorf("pod %s bound to %s, want a node of domain a (a1/a2)", name, node)
		}
		if bound[node] {
			t.Errorf("two gang pods bound to the same node %s (whole-node model expects one each)", node)
		}
		bound[node] = true
	}
}
