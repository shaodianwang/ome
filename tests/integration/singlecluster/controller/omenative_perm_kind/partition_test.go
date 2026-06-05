package omenative_perm_kind

import (
	"context"
	"fmt"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/tests/integration/util"
)

// partitionFor is the StatefulSet-style hold count (k) for a Component: Instances
// with index < k stay on the old revision, so (N-k) roll to the new one.
func (pc permCase) partitionFor(c v1beta1.ComponentType) int {
	if l := pc.cfg(c); l != nil && l.partition != nil {
		return int(*l.partition)
	}
	return 0
}

// runPartition drives the partition (staged canary) verb — the Go port of
// run.py's _partition. It bumps the revision on every configured Component, then
// asserts the STAGED split holds: exactly (N-k) Instances roll to the new
// revision and k stay on the old, sustained (the held ones must not creep to the
// new revision), with peak alive within the authored ceiling.
func runPartition(ns string, pc permCase) {
	isvc := buildPermISVC(ns, pc)
	gomega.Expect(k8sClient.Create(suiteCtx, isvc)).To(gomega.Succeed(), "create ISVC")
	ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, isvc) })

	obsCtx, cancel := context.WithCancel(suiteCtx)
	defer cancel()
	obs, err := util.NewRolloutObserver(restCfg, ns, pc.id)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	obs.Start(obsCtx)

	configured := pc.configured()
	want := map[string]int{}
	modes := map[string]string{}
	bumped := map[string]bool{} // partition rolls every configured Component
	sumN := 0
	for _, c := range configured {
		want[string(c)] = pc.replicas(c)
		sumN += pc.replicas(c)
		bumped[string(c)] = true
		if inPlace(pc.cfg(c).strat) {
			modes[string(c)] = "inplace"
		} else {
			modes[string(c)] = "surge"
		}
	}
	initBudget := time.Duration(120+25*sumN) * time.Second
	rollBudget := time.Duration(120+60*sumN) * time.Second

	// Phase 1: initial Ready.
	ginkgo.By("waiting for initial Ready")
	gomega.Eventually(func() bool {
		cnt := obs.Counts()
		for _, c := range configured {
			if cnt[string(c)][1] != want[string(c)] {
				return false
			}
		}
		return true
	}).WithTimeout(initBudget).WithPolling(time.Second).Should(gomega.BeTrue(), "initial Ready")

	// Phase 2: baseline + bump every configured Component.
	obs.MarkBaseline(want, bumped, modes)
	ginkgo.By("bumping trigger on all configured Components")
	gomega.Eventually(func(g gomega.Gomega) {
		cur := &v1beta1.InferenceService{}
		g.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(isvc), cur)).To(gomega.Succeed())
		for _, c := range configured {
			setTrigger(cur, c)
		}
		g.Expect(k8sClient.Update(suiteCtx, cur)).To(gomega.Succeed())
	}).WithTimeout(2*time.Minute).WithPolling(time.Second).Should(gomega.Succeed(), "bump trigger")

	// staged reports whether every Component sits at the staged split: N alive,
	// N ready, and exactly (N-k) on the new revision (k held on the old).
	staged := func() bool {
		cnt := obs.Counts()
		for _, c := range configured {
			n, k := want[string(c)], pc.partitionFor(c)
			v := cnt[string(c)]
			if v[0] != n || v[1] != n || v[2] != n-k {
				return false
			}
		}
		return true
	}

	// Phase 3: converge to the staged split, then confirm it HOLDS (the held
	// Instances must not creep onto the new revision).
	ginkgo.By("watching the staged roll converge")
	gomega.Eventually(staged).WithTimeout(rollBudget).WithPolling(time.Second).Should(gomega.BeTrue(),
		"staged split (N-k new, k held) [%s]", pc.desc)
	ginkgo.By("confirming the partition holds")
	gomega.Consistently(staged).WithTimeout(12*time.Second).WithPolling(2*time.Second).Should(gomega.BeTrue(),
		"held Instances must stay on the old revision [%s]", pc.desc)

	// Verdict + checks.
	final := obs.Counts()
	fmt.Fprintf(ginkgo.GinkgoWriter, "\n  VERDICT(partition) [%s] %s\n", pc.id, pc.desc)
	for _, c := range configured {
		n, k := want[string(c)], pc.partitionFor(c)
		fmt.Fprintf(ginkgo.GinkgoWriter, "    %-8s N=%d k=%d peak=%d ceil=%d new=%d (want %d) held=%d (want %d)\n",
			c, n, k, obs.Peak(string(c)), pc.ceil(c), final[string(c)][2], n-k, final[string(c)][0]-final[string(c)][2], k)
	}
	for _, c := range configured {
		gomega.Expect(obs.Peak(string(c))).To(gomega.BeNumerically("<=", pc.ceil(c)),
			"PEAK: %s peak %d > ceiling %d [%s]", c, obs.Peak(string(c)), pc.ceil(c), pc.desc)
	}
}

// buildPartitionCases — Go ports of the partition fixtures: a single-pod roll
// held at a partition boundary. k held on old, N-k roll to new.
func buildPartitionCases() []permCase {
	stP := func(n, k int) *lc {
		return &lc{n: n, strat: v1beta1.UpdateStrategySurgeThenDrain, surge: pi(1), unavail: pi(0), partition: ptr(int32(k))}
	}
	return []permCase{
		{id: "pp01", desc: "engine N=4 partition k=1 (3 roll, 1 held)", eng: stP(4, 1), ceilE: 5},
		{id: "pp02", desc: "engine N=4 partition k=2 (2 roll, 2 held)", eng: stP(4, 2), ceilE: 5},
		{id: "pp03", desc: "engine N=4 partition k=3 (1 rolls, 3 held)", eng: stP(4, 3), ceilE: 5},
		{id: "pp04", desc: "engine N=4 partition k=0 (full roll, no hold)", eng: stP(4, 0), ceilE: 5},
		{id: "pp05", desc: "PD e4 d2 per-Component partition e=1 d=1", eng: stP(4, 1), dec: stP(2, 1), ceilE: 5, ceilD: 3},
	}
}

var _ = ginkgo.Describe("OMENative perm partition", ginkgo.Ordered, ginkgo.ContinueOnFailure, func() {
	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "perm-part-")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		_ = util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)
	})

	entries := make([]ginkgo.TableEntry, 0, len(buildPartitionCases()))
	for _, pc := range buildPartitionCases() {
		entries = append(entries, ginkgo.Entry(fmt.Sprintf("%s: %s", pc.id, pc.desc), pc))
	}
	ginkgo.DescribeTable("partition holds the staged split", func(pc permCase) {
		if pc.skip != "" {
			ginkgo.Skip(pc.skip)
		}
		runPartition(ns.Name, pc)
	}, entries)
})
