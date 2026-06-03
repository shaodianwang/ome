package omenative_coordination_kind

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/tests/integration/util"
)

// DIAG-1 — diagnostic spec that creates an ISVC, waits 90s, and
// dumps everything controller-relevant for human inspection. Used
// when porting a behavior test surfaces a "phase stays Surging
// forever" mystery so we can see whether pods exist, whether
// ome.io/serving is flipped, whether ContainersReady is True, etc.
//
// Gated by an env var so it doesn't run in the regular suite —
// it always passes but spams stdout.
//
//	KIND_DIAG=1 go test ./tests/integration/singlecluster/controller/omenative_coordination_kind/ \
//	    -run TestCoordinationKindController -ginkgo.focus=DIAG-1 -v
var _ = ginkgo.Describe("DIAG-1 — inspect post-create ISVC state", ginkgo.Ordered, func() {
	if os.Getenv("KIND_DIAG") != "1" {
		return
	}
	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "diag-")
		Expect(err).NotTo(HaveOccurred())
		// DO NOT cleanup the namespace — leaving it lets us
		// `kubectl get` after the test ends.
	})

	ginkgo.It("creates an ISVC + waits 90s + dumps state", func() {
		fix := newCoordFixture(ns.Name, WithISVCName("diag"),
			WithEngineReplicas(1),
			WithCoordinationGroup(v1beta1.RolloutCoordinationGroup{
				Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
				Policy:     v1beta1.CoordinationPolicyBlueGreen,
			}))
		applyFixture(Default, fix)
		// NOTE: no deferFixtureCleanup — we want the state to survive.

		ginkgo.GinkgoWriter.Printf("\n=== diagnostic: waiting 90s for steady state in ns=%s ===\n", ns.Name)
		time.Sleep(90 * time.Second)

		isvc := freshISVC(Default, ns.Name, "diag")
		dumpISVC(isvc)
		dumpPods(ns.Name)
	})
})

func dumpISVC(isvc *v1beta1.InferenceService) {
	b, _ := json.MarshalIndent(isvc.Status, "", "  ")
	fmt.Printf("\n=== ISVC %s/%s status ===\n%s\n", isvc.Namespace, isvc.Name, b)
}

func dumpPods(namespace string) {
	pods := &corev1.PodList{}
	if err := k8sClient.List(context.Background(), pods, client.InNamespace(namespace)); err != nil {
		fmt.Printf("list pods: %v\n", err)
		return
	}
	fmt.Printf("\n=== pods in %s ===\n", namespace)
	for _, p := range pods.Items {
		fmt.Printf("- %s phase=%s node=%s\n", p.Name, p.Status.Phase, p.Spec.NodeName)
		for _, c := range p.Status.Conditions {
			fmt.Printf("    cond %s=%s reason=%q\n", c.Type, c.Status, c.Reason)
		}
		for _, cs := range p.Status.ContainerStatuses {
			fmt.Printf("    container %s ready=%v image=%s state=%+v\n",
				cs.Name, cs.Ready, cs.Image, cs.State)
		}
		for _, ig := range p.Spec.ReadinessGates {
			fmt.Printf("    readinessGate: %s\n", ig.ConditionType)
		}
	}
}
