package omenative_upgrade_kind

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/irprojector"
	v1beta1testing "github.com/ome-projects/ome/pkg/utils/testing/v1beta1"
	"github.com/ome-projects/ome/tests/integration/kindutil"
	"github.com/ome-projects/ome/tests/integration/util"
)

// fixtureModel / fixtureRuntime are the shared runtime fixtures
// deploy-ome-kind.sh seeds into upgradeNamespace: a PVC-backed
// ClusterBaseModel and its ClusterServingRuntime. Every ISVC in this suite
// references them rather than minting a model of its own — this suite runs
// the real (non-faked) BaseModel controller, which needs a model it can
// actually resolve, unlike omenative_coordination_kind's in-process fake.
//
// fixtureMultiNodeRuntime is a separate, already-established fixture in the
// same render path: its engineConfig declares per-role leader/worker
// runners, unlike fixtureRuntime's single top-level runner. MergeEngineSpec
// carries a runtime's Worker.Runner onto the merged ISVC only when the
// runtime itself declares a Worker block — an ISVC with Leader/Worker set
// against a runtime with neither renders an empty pod spec (no container),
// so the gang engine below must reference this runtime, never
// fixtureRuntime. See TestMergeEngineSpec_RunnerResolvesForBothSuiteShapes
// in pkg/controller/v1beta1/inferenceservice/components for the pinned
// contract.
const (
	fixtureModel            = "ome-coordtest-noop"
	fixtureRuntime          = "ome-coordtest-cpu"
	fixtureMultiNodeRuntime = "ome-coordtest-multinode"
)

// singlePodISVC builds a minimum-viable single-pod OMENative engine.
// PodSpec is left unset: fixtureRuntime's engineConfig.runner supplies the
// container, the same way every other eval-lane ISVC sources its runner.
func singlePodISVC(namespace, name string) *v1beta1.InferenceService {
	one := 1
	return v1beta1testing.MakeInferenceService(name, namespace).
		Model(fixtureModel).
		Runtime(fixtureRuntime).
		Engine(&v1beta1.EngineSpec{
			ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
				MinReplicas: &one,
				MaxReplicas: one,
				Annotations: map[string]string{constants.DeploymentMode: string(constants.OMENative)},
			},
		}).
		Obj()
}

// gangISVC builds a Leader+Worker gang engine (one Instance, two pods:
// leader + worker), against fixtureMultiNodeRuntime. Leader/Worker both
// leave PodSpec and Runner unset: that runtime's engineConfig.leader.runner
// / engineConfig.worker.runner supply the containers.
func gangISVC(namespace, name string) *v1beta1.InferenceService {
	one := 1
	return v1beta1testing.MakeInferenceService(name, namespace).
		Model(fixtureModel).
		Runtime(fixtureMultiNodeRuntime).
		Engine(&v1beta1.EngineSpec{
			ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
				MinReplicas: &one,
				MaxReplicas: one,
				Annotations: map[string]string{constants.DeploymentMode: string(constants.OMENative)},
			},
			Leader: &v1beta1.LeaderSpec{},
			Worker: &v1beta1.WorkerSpec{Size: &one},
		}).
		Obj()
}

// deleteISVC removes name from upgradeNamespace and waits for it to be
// fully gone. Tolerant of the ISVC already being absent, so it doubles as
// pre-spec cleanup for a reused cluster and post-spec DeferCleanup.
func deleteISVC(name string) {
	ginkgo.GinkgoHelper()
	key := client.ObjectKey{Namespace: upgradeNamespace, Name: name}
	isvc := &v1beta1.InferenceService{}
	err := k8sClient.Get(suiteCtx, key, isvc)
	if err == nil {
		err = k8sClient.Delete(suiteCtx, isvc)
	}
	gomega.Expect(client.IgnoreNotFound(err)).To(gomega.Succeed(), "delete ISVC %s", name)
	gomega.Eventually(func(g gomega.Gomega) {
		current := &v1beta1.InferenceService{}
		g.Expect(apierrors.IsNotFound(k8sClient.Get(suiteCtx, key, current))).To(gomega.BeTrue())
	}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(gomega.Succeed())
}

// freshIR re-reads the InferenceReplica projected for one ISVC Component.
func freshIR(g gomega.Gomega, isvcName string, component v1beta1.ComponentType) *v1beta1.InferenceReplica {
	out := &v1beta1.InferenceReplica{}
	name := irprojector.InferenceReplicaName(isvcName, component)
	g.ExpectWithOffset(1, k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: upgradeNamespace, Name: name}, out)).
		To(gomega.Succeed())
	return out
}

// podPlacement pins one pod's identity and scheduling decision — the two
// facts an operator upgrade must never change for a live Instance.
type podPlacement struct {
	uid  types.UID
	node string
}

// snapshotPods maps every live OMENative pod for isvcName by name to its
// podPlacement.
func snapshotPods(g gomega.Gomega, isvcName string) map[string]podPlacement {
	snap, err := util.SnapshotPods(suiteCtx, k8sClient, upgradeNamespace, isvcName)
	g.ExpectWithOffset(1, err).NotTo(gomega.HaveOccurred())
	out := map[string]podPlacement{}
	for _, pods := range snap.ByComponent {
		for _, p := range pods {
			g.ExpectWithOffset(1, p.DeletionTimestamp).To(gomega.BeNil(), "pod %s must be alive, not terminating", p.Name)
			g.ExpectWithOffset(1, p.Spec.NodeName).NotTo(gomega.BeEmpty(), "pod %s must already be scheduled", p.Name)
			out[p.Name] = podPlacement{uid: p.UID, node: p.Spec.NodeName}
		}
	}
	return out
}

// revisionPair is one Component's IR revision lineage. Current == Update at
// steady state; a real rollout diverges them before re-converging.
type revisionPair struct {
	current string
	update  string
}

func revisionOf(ir *v1beta1.InferenceReplica) revisionPair {
	return revisionPair{current: ir.Status.CurrentRevision, update: ir.Status.UpdateRevision}
}

// snapshotControllerRevisions maps every ControllerRevision owned by
// isvcName to its UID, so a delete-and-recreate under an identical name
// still surfaces as a mismatch.
func snapshotControllerRevisions(g gomega.Gomega, isvcName string) map[string]types.UID {
	revisions := &appsv1.ControllerRevisionList{}
	g.ExpectWithOffset(1, k8sClient.List(suiteCtx, revisions,
		client.InNamespace(upgradeNamespace),
		client.MatchingLabels{constants.InferenceServicePodLabelKey: isvcName},
	)).To(gomega.Succeed())
	out := map[string]types.UID{}
	for _, r := range revisions.Items {
		out[r.Name] = r.UID
	}
	return out
}

// mergeCRs unions per-ISVC ControllerRevision snapshots into one map keyed
// by name — the two ISVCs this suite drives never share a ControllerRevision
// name, so a plain union loses no information.
func mergeCRs(maps ...map[string]types.UID) map[string]types.UID {
	out := map[string]types.UID{}
	for _, m := range maps {
		for k, v := range m {
			out[k] = v
		}
	}
	return out
}

// snapshotManagerPods maps every live controller-manager pod (by name) to
// its UID. The chart's Deployment selects control-plane=ome-controller-manager
// (charts/ome-resources/templates/ome-controller/deployment.yaml).
func snapshotManagerPods(g gomega.Gomega) map[string]types.UID {
	pods := &corev1.PodList{}
	g.ExpectWithOffset(1, k8sClient.List(suiteCtx, pods,
		client.InNamespace(omeNamespace),
		client.MatchingLabels{"control-plane": "ome-controller-manager"},
	)).To(gomega.Succeed())
	out := map[string]types.UID{}
	for _, p := range pods.Items {
		if p.DeletionTimestamp != nil {
			continue
		}
		out[p.Name] = p.UID
	}
	return out
}

// upgradeToCurrentBuild re-runs deploy-ome-kind.sh in its normal
// (build-from-worktree) mode against the suite's already-running cluster: a
// second `helm upgrade --install` against the same release, landing the
// current commit's image over whatever upgradeFromImageEnv installed. This
// second pass is the operator upgrade the gate proves is safe — it already
// waits for `kubectl rollout status` and the webhook endpoints internally,
// so a manager that never comes back healthy fails here, not later.
func upgradeToCurrentBuild() error {
	root, err := kindutil.RepoRoot()
	if err != nil {
		return fmt.Errorf("upgradeToCurrentBuild: %w", err)
	}
	script := filepath.Join(root, "hack", "omenative-testing", "deploy-ome-kind.sh")
	ctx, cancel := context.WithTimeout(suiteCtx, upgradeDeployTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, script, "--cluster", kindutil.Suite().ClusterName)
	cmd.Dir = root
	cmd.Env = append(os.Environ(),
		"OME_TEST_NS="+upgradeNamespace,
		"OME_NAMESPACE="+omeNamespace,
		"OME_KIND_PROMETHEUS_ENABLED=false",
	)
	cmd.Stdout = ginkgo.GinkgoWriter
	cmd.Stderr = ginkgo.GinkgoWriter
	return cmd.Run()
}

// TestUpgradeScriptResolvesFromRepoRoot guards the path resolution
// upgradeToCurrentBuild depends on. It needs no cluster and runs
// unconditionally (unlike TestOMENativeUpgradeIdentity, which Skips without
// a previous-image env var), so a regression here surfaces on its own in
// under a second instead of only after a full KIND cluster and chart
// install have already completed.
func TestUpgradeScriptResolvesFromRepoRoot(t *testing.T) {
	root, err := kindutil.RepoRoot()
	if err != nil {
		t.Fatalf("kindutil.RepoRoot(): %v", err)
	}
	script := filepath.Join(root, "hack", "omenative-testing", "deploy-ome-kind.sh")
	if _, err := os.Stat(script); err != nil {
		t.Fatalf("resolved repo root %s does not contain deploy-ome-kind.sh: %v", root, err)
	}
}
