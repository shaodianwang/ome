package gangpack

import (
	"fmt"
	"testing"

	v1 "k8s.io/api/core/v1"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

// BenchmarkFreeByDomain measures the per-cycle node projection. PreFilter runs
// this over the WHOLE node snapshot for every gang pod, so its cost scales with
// cluster size — the sub-benchmarks make that visible.
func BenchmarkFreeByDomain(b *testing.B) {
	pod := gpuPod("4")
	for _, n := range []int{100, 1000, 10000} {
		b.Run(fmt.Sprintf("nodes=%d", n), func(b *testing.B) {
			nodes := make([]*framework.NodeInfo, 0, n)
			for i := 0; i < n; i++ {
				// ~18 nodes per NVL72-style domain.
				dom := fmt.Sprintf("dom-%d", i/18)
				nodes = append(nodes, nodeInfo(gpuNode(fmt.Sprintf("n-%d", i), dom, "4")))
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = freeByDomain(nodes, testKey, pod)
			}
		})
	}
}

// benchWaitingPod is a minimal WaitingPod carrying a gang-labelled pod, for the
// Permit iteration benchmark.
type benchWaitingPod struct {
	framework.WaitingPod
	pod *v1.Pod
}

func (w *benchWaitingPod) GetPod() *v1.Pod { return w.pod }
func (w *benchWaitingPod) Allow(string)    {}

// BenchmarkPermitCountWaiting measures the sibling scan in Permit, which walks
// ALL waiting pods to count same-gang members. With many gangs gating at once,
// this is the cost each arriving member pays.
func BenchmarkPermitCountWaiting(b *testing.B) {
	for _, n := range []int{100, 1000, 10000} {
		b.Run(fmt.Sprintf("waiting=%d", n), func(b *testing.B) {
			waiting := make([]framework.WaitingPod, 0, n)
			for i := 0; i < n; i++ {
				// Many distinct gangs waiting concurrently.
				waiting = append(waiting, &benchWaitingPod{pod: gangPod("team", fmt.Sprintf("pg-%d", i%256))})
			}
			h := &fakeHandle{waiting: waiting}
			g := &GangPack{handle: h}
			gangKey := "team/pg-0"
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				count := 0
				g.handle.IterateOverWaitingPods(func(wp framework.WaitingPod) {
					if sameGang(wp.GetPod(), gangKey) {
						count++
					}
				})
				_ = count
			}
		})
	}
}
