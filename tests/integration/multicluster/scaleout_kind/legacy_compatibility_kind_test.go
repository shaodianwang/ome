package scaleout_kind

import (
	"slices"

	"github.com/google/go-cmp/cmp"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/util/retry"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/placement/protocol"
)

var _ = ginkgo.DescribeTable("Legacy placement compatibility (KIND)",
	func(mode v1beta1.PlacementMode, spread bool, wantInitial, wantUpdated int32) {
		env := setupSpecEnv()
		model, runtime := env.seedModelRuntimeOnMembers()
		source := env.createSource(model, runtime, mode, 3, func(s *v1beta1.InferenceService) {
			s.Spec.Placement = &v1beta1.PlacementSpec{Mode: mode, Requirements: labels.SelectorFromSet(memberLabels()).String()}
			if mode == v1beta1.PlacementModeSplit {
				s.Spec.Placement.Split = &v1beta1.SplitSpec{Replicas: ptr.To[int32](3), Spread: spread}
			}
		})
		gomega.Eventually(func() int32 { return env.legacyReadyReplicasAcrossHomes() }).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.Equal(wantInitial))
		var before map[string][]string
		gomega.Eventually(func(g gomega.Gomega) {
			snapshot := map[string][]string{}
			total := 0
			for _, m := range env.members {
				for _, pod := range enginePodsOn(g, m.c, env.isvcNS, env.isvcName) {
					snapshot[m.wcName] = append(snapshot[m.wcName], string(pod.UID))
					total++
				}
				slices.Sort(snapshot[m.wcName])
			}
			g.Expect(total).To(gomega.Equal(int(wantInitial)), "wait for surplus pod cleanup before the restart snapshot")
			g.Expect(env.legacyReadyReplicasAcrossHomes()).To(gomega.Equal(wantInitial))
			before = snapshot
		}).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.Succeed())
		ginkgo.By("restarting placement with stored legacy intent and standing pods")
		controlCluster.stopManager()
		startControlPlaneManager(&controlCluster)
		gomega.Eventually(func(g gomega.Gomega) {
			ps := placementStatusOf(env.isvcNS, env.isvcName)
			g.Expect(ps).NotTo(gomega.BeNil())
			g.Expect(ps.Plan).To(gomega.BeNil())
			g.Expect(env.legacyReadyReplicasAcrossHomes()).To(gomega.Equal(wantInitial))
			after := map[string][]string{}
			for _, m := range env.members {
				for _, pod := range enginePodsOn(g, m.c, env.isvcNS, env.isvcName) {
					after[m.wcName] = append(after[m.wcName], string(pod.UID))
				}
				slices.Sort(after[m.wcName])
				if derived, exists := getDerivedOn(m.c, env.isvcNS, env.isvcName); exists {
					g.Expect(protocol.IsAffinityMember(derived)).To(gomega.BeFalse())
				}
			}
			g.Expect(cmp.Diff(before, after)).To(gomega.BeEmpty(), "controller restart replaced serving pods")
		}).WithTimeout(mediumTimeout).WithPolling(pollInterval).Should(gomega.Succeed())
		ginkgo.By("propagating a replica update under the legacy allocation contract")
		gomega.Expect(retry.RetryOnConflict(retry.DefaultRetry, func() error {
			current := &v1beta1.InferenceService{}
			if err := controlCluster.client.Get(controlCluster.ctx, client.ObjectKeyFromObject(source), current); err != nil {
				return err
			}
			if mode == v1beta1.PlacementModeSplit {
				*current.Spec.Placement.Split.Replicas = 5
			} else {
				*current.Spec.Engine.MinReplicas = 2
			}
			return controlCluster.client.Update(controlCluster.ctx, current)
		})).To(gomega.Succeed())
		gomega.Eventually(func(g gomega.Gomega) {
			g.Expect(env.legacyReadyReplicasAcrossHomes()).To(gomega.Equal(wantUpdated))
			total := 0
			for _, m := range env.members {
				pods := enginePodsOn(g, m.c, env.isvcNS, env.isvcName)
				total += len(pods)
				retained := 0
				for _, pod := range pods {
					if slices.Contains(before[m.wcName], string(pod.UID)) {
						retained++
					}
				}
				wantRetained := len(before[m.wcName])
				if mode != v1beta1.PlacementModeSplit {
					wantRetained = min(wantRetained, 2)
				}
				g.Expect(cmp.Diff(wantRetained, retained)).To(gomega.BeEmpty(), "replica update replaced retained pods on %s", m.wcName)
			}
			g.Expect(total).To(gomega.Equal(int(wantUpdated)))
		}).WithTimeout(longTimeout).WithPolling(pollInterval).Should(gomega.Succeed())
	},
	ginkgo.Entry("Single retains its winner", v1beta1.PlacementModeSingle, false, int32(3), int32(2)),
	ginkgo.Entry("All retains independent homes", v1beta1.PlacementModeAll, false, int32(6), int32(4)),
	ginkgo.Entry("Packed fills admitted homes", v1beta1.PlacementModeSplit, false, int32(3), int32(5)),
	ginkgo.Entry("spread retains per-home ceiling rounding", v1beta1.PlacementModeSplit, true, int32(4), int32(6)),
)

func (e *specEnv) legacyReadyReplicasAcrossHomes() int32 {
	ps := placementStatusOf(e.isvcNS, e.isvcName)
	if ps == nil {
		return 0
	}
	var total int32
	for i := range ps.Candidates {
		total += ps.Candidates[i].ReadyReplicas
	}
	return total
}
