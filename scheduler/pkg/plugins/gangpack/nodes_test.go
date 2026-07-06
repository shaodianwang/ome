package gangpack

import (
	"reflect"
	"testing"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/kubernetes/pkg/scheduler/framework"

	"github.com/ome-projects/ome/scheduler/pkg/topology"
)

const testKey = "nvidia.com/gpu.clique"

var gpu = v1.ResourceName("nvidia.com/gpu")

func gpuNode(name, domain, alloc string) *v1.Node {
	labels := map[string]string{}
	if domain != "" {
		labels[testKey] = domain
	}
	status := v1.NodeStatus{}
	if alloc != "" {
		status.Allocatable = v1.ResourceList{gpu: resource.MustParse(alloc)}
	}
	return &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: labels}, Status: status}
}

func gpuPod(req string) *v1.Pod {
	return &v1.Pod{Spec: v1.PodSpec{Containers: []v1.Container{{
		Resources: v1.ResourceRequirements{Requests: v1.ResourceList{gpu: resource.MustParse(req)}},
	}}}}
}

func nodeInfo(node *v1.Node, pods ...*v1.Pod) *framework.NodeInfo {
	ni := framework.NewNodeInfo(pods...)
	ni.SetNode(node)
	return ni
}

func TestDomainOf(t *testing.T) {
	if got := domainOf(gpuNode("n", "d7", "4"), testKey); got != "d7" {
		t.Fatalf("domainOf = %q want d7", got)
	}
	if got := domainOf(gpuNode("n", "", "4"), testKey); got != "" {
		t.Fatalf("domainOf(no label) = %q want empty", got)
	}
	if got := domainOf(nil, testKey); got != "" {
		t.Fatalf("domainOf(nil) = %q want empty", got)
	}
}

// TestFreeByDomain: build topology.FreeByDomain from a framework node snapshot.
// Only accelerator nodes (allocatable > 0) with a domain label participate; a
// node is free when none of its accelerator is requested; an all-occupied
// accelerator domain still appears at 0; non-accel and no-label nodes are ignored.
func TestFreeByDomain(t *testing.T) {
	nodes := []*framework.NodeInfo{
		nodeInfo(gpuNode("n1", "a", "4")),              // free
		nodeInfo(gpuNode("n2", "a", "4")),              // free
		nodeInfo(gpuNode("n3", "a", "4"), gpuPod("4")), // occupied -> a stays 2
		nodeInfo(gpuNode("n4", "b", "4")),              // free
		nodeInfo(gpuNode("n5", "d", "4"), gpuPod("4")), // only-occupied domain -> d:0
		nodeInfo(gpuNode("n6", "c", "")),               // labeled but no GPU -> ignored
		nodeInfo(gpuNode("n7", "", "4")),               // no domain -> ignored
	}
	got := freeByDomain(nodes, testKey, gpu)
	want := topology.FreeByDomain{"a": 2, "b": 1, "d": 0}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("freeByDomain = %v want %v", got, want)
	}
}
