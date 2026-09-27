package alfred_kind

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/tests/integration/kindutil"
)

// alfredImageRef is the image the alfred Deployment runs, split the way the
// chart values want it.
type alfredImageRef struct {
	Repository string
	Tag        string
}

func (r alfredImageRef) String() string { return r.Repository + ":" + r.Tag }

// splitImageRef splits a tagged reference on the last ':' after the last '/';
// a reference without a tag gets "latest". Digest references are not handled
// (resolveAlfredImage rejects them).
func splitImageRef(ref string) alfredImageRef {
	slash := strings.LastIndex(ref, "/")
	colon := strings.LastIndex(ref, ":")
	if colon <= slash {
		return alfredImageRef{Repository: ref, Tag: "latest"}
	}
	return alfredImageRef{Repository: ref[:colon], Tag: ref[colon+1:]}
}

// resolveAlfredImage makes the alfred image available in the cluster.
// OME_ALFRED_IMAGE names a tagged reference already present on the host's
// Docker daemon; it is loaded as-is and the build is skipped. Otherwise
// dockerfiles/alfred.Dockerfile is built at the commit in .alfred-oss-ref,
// which needs BuildKit and access to github.com.
func resolveAlfredImage(ctx context.Context, cluster string) (alfredImageRef, error) {
	if ref := strings.TrimSpace(os.Getenv("OME_ALFRED_IMAGE")); ref != "" {
		if strings.Contains(ref, "@") {
			return alfredImageRef{}, fmt.Errorf("OME_ALFRED_IMAGE %q: digest references are not supported; use a tag", ref)
		}
		if err := kindutil.LoadImage(ctx, cluster, ref); err != nil {
			return alfredImageRef{}, fmt.Errorf("load %s into kind: %w", ref, err)
		}
		return splitImageRef(ref), nil
	}
	root, err := kindutil.RepoRoot()
	if err != nil {
		return alfredImageRef{}, err
	}
	pin, err := os.ReadFile(filepath.Join(root, ".alfred-oss-ref"))
	if err != nil {
		return alfredImageRef{}, fmt.Errorf("read .alfred-oss-ref: %w", err)
	}
	spec := kindutil.ImageBuildSpec{
		ContextDir:      "dockerfiles",
		Dockerfile:      "alfred.Dockerfile",
		Tag:             "alfred:kind",
		BuildArgs:       map[string]string{"ALFRED_OSS_REF": strings.TrimSpace(string(pin))},
		RequireBuildKit: true,
	}
	if err := kindutil.BuildAndLoadImages(ctx, cluster, []kindutil.ImageBuildSpec{spec}); err != nil {
		return alfredImageRef{}, fmt.Errorf("build alfred image (set OME_ALFRED_IMAGE to skip the build): %w", err)
	}
	return splitImageRef(spec.Tag), nil
}

// helmBinary prefers the repo-pinned bin/helm, then PATH.
func helmBinary() string {
	root, err := kindutil.RepoRoot()
	gomega.ExpectWithOffset(1, err).NotTo(gomega.HaveOccurred())
	if p := filepath.Join(root, "bin", "helm"); fileExists(p) {
		return p
	}
	p, err := exec.LookPath("helm")
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), "helm is required: run `make helm` or install it")
	return p
}

func fileExists(p string) bool {
	_, err := os.Stat(p)
	return err == nil
}

var alfredTemplates = []string{
	"deployment.yaml", "rbac.yaml", "configmap.yaml", "service.yaml", "migration-guard.yaml", "dispatch-state.yaml",
}

// runStamp is computed once per test process and stamped onto alfred's pod
// template, so every run rolls fresh alfred pods (cold in-memory cooldowns and
// journal state) even when the cluster is reused.
var runStamp = time.Now().UTC().Format("20060102T150405Z")

// Cooldowns alfredValues deploys, in the minutes alfred's config counts them
// in; one minute is the config minimum. The waits the specs derive from them
// live in timeouts_test.go.
const (
	recentPlacementCooldownMinutes = 1
	healthCooldownFloorMinutes     = 1
)

// alfredValues are the chart values every spec starts from: execute-capable,
// second-scale loops, one-minute cooldowns (the config minimum), defrag off
// (specs that need it turn it on), a per-hour budget no spec can exhaust, and
// the per-run pod annotation.
func alfredValues(img alfredImageRef) []string {
	return []string{
		"--set", "global.hub=",
		"--set", "alfred.enabled=true",
		"--set-string", "alfred.image.repository=" + img.Repository,
		"--set-string", "alfred.image.tag=" + img.Tag,
		"--set", "alfred.image.pullPolicy=Never",
		"--set", "alfred.migration.apiVersion=v1",
		"--set", "alfred.migration.acknowledgementTimeout=" + acknowledgementTimeout.String(),
		"--set", "alfred.migration.failureBackoff=" + failureBackoff.String(),
		"--set", "alfred.simulation.configMapName=" + alfredSimConfigMap,
		"--set", "alfred.simulation.timeout=10s",
		"--set", "alfred.config.mode=execute",
		"--set", "alfred.config.decisionLoopInterval=" + decisionInterval.String(),
		"--set", "alfred.config.observationLoopInterval=2s",
		"--set", fmt.Sprintf("alfred.config.recentPlacementCooldownMinutes=%d", recentPlacementCooldownMinutes),
		"--set", "alfred.config.perWorkloadCooldownMinutes=1",
		"--set", "alfred.config.perNodeCooldownMinutes=1",
		"--set", "alfred.config.emergencyPendingAgeMinutes=1",
		"--set", "alfred.config.maxInFlightMigrations=3",
		"--set", "alfred.config.maxMigrationsPerHour=1000",
		"--set", "alfred.config.policies.defragmentation.enabled=false",
		"--set", fmt.Sprintf("alfred.config.policies.nodeHealth.healthCooldownFloorMinutes=%d", healthCooldownFloorMinutes),
		"--set", "alfred.config.policies.nodeHealth.nodeSuspicionWindowMinutes=1",
		"--set", "alfred.config.lwsRecommendationsEnabled=false",
		"--set-string", "alfred.podAnnotations.alfred-kind/run=" + runStamp,
	}
}

func helmTemplate(img alfredImageRef, profilesJSON string, templates []string) []byte {
	root, err := kindutil.RepoRoot()
	gomega.ExpectWithOffset(1, err).NotTo(gomega.HaveOccurred())
	args := []string{"template", "ome", filepath.Join(root, "charts", "ome-resources"), "--namespace", alfredNamespace}
	for _, t := range templates {
		args = append(args, "--show-only", "templates/alfred/"+t)
	}
	args = append(args, alfredValues(img)...)
	args = append(args, "--set-json", "alfred.config.scheduling.profiles="+profilesJSON)
	cmd := exec.Command(helmBinary(), args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	gomega.ExpectWithOffset(1, cmd.Run()).To(gomega.Succeed(), "helm template: %s", stderr.String())
	return stdout.Bytes()
}

// renderAlfred renders the alfred templates from the repo chart. The second
// result is the admission guard alone, kept for the guard spec.
func renderAlfred(img alfredImageRef, profilesJSON string) (all []byte, guard []byte) {
	return helmTemplate(img, profilesJSON, alfredTemplates), helmTemplate(img, profilesJSON, []string{"migration-guard.yaml"})
}

// applyManifest server-side-applies a rendered multi-document manifest.
func applyManifest(raw []byte) {
	f, err := os.CreateTemp("", "alfred-kind-*.yaml")
	gomega.ExpectWithOffset(1, err).NotTo(gomega.HaveOccurred())
	defer os.Remove(f.Name())
	_, err = f.Write(raw)
	gomega.ExpectWithOffset(1, err).NotTo(gomega.HaveOccurred())
	gomega.ExpectWithOffset(1, f.Close()).To(gomega.Succeed())
	gomega.ExpectWithOffset(1, kindutil.ApplyManifest(suiteCtx, restCfg, f.Name())).To(gomega.Succeed(), "apply alfred manifest")
}

// waitAlfredReady blocks until the Deployment has rolled out the applied
// template (observed generation, updated and total replicas all current, so a
// previous run's pods can never satisfy it), every replica is Ready, and one
// holds the lease.
func waitAlfredReady() {
	gomega.EventuallyWithOffset(1, func(g gomega.Gomega) {
		dep := &appsv1.Deployment{}
		g.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{Namespace: alfredNamespace, Name: alfredDeployment}, dep)).To(gomega.Succeed())
		want := *dep.Spec.Replicas
		g.Expect(dep.Status.ObservedGeneration).To(gomega.BeNumerically(">=", dep.Generation), "alfred deployment observed")
		g.Expect(dep.Status.UpdatedReplicas).To(gomega.Equal(want), "alfred replicas updated")
		g.Expect(dep.Status.Replicas).To(gomega.Equal(want), "alfred replicas current")
		g.Expect(dep.Status.ReadyReplicas).To(gomega.Equal(want), "alfred replicas ready")
		leaderPod(g)
	}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(gomega.Succeed())
}

// ---- simulation bootstrap --------------------------------------------------

// simulationSchedulerConfig is the suite's one simulation worker: the default
// scheduler name every fixture selects, with OMEGangPack enrolled so that a
// multi-pod Instance simulates against a gang-capable profile. The fallback
// topologyKey is a label every node shares, so an untopologied gang is gated
// as a gang yet free to spread, and domain packing is off so nothing scores.
const simulationSchedulerConfig = `apiVersion: kubescheduler.config.k8s.io/v1
kind: KubeSchedulerConfiguration
profiles:
  - schedulerName: default-scheduler
    plugins:
      multiPoint:
        enabled:
          - name: OMEGangPack
        disabled:
          - name: DefaultPreemption
    pluginConfig:
      - name: OMEGangPack
        args:
          topologyKey: kubernetes.io/os
          podGroupTopologyKeyAnnotation: ome.io/topology-key
          unsupportedPlacementGroupLabel: ome.io/placement-group
          defaultPermitTimeoutSeconds: 600
          podGroupSyncTimeoutSeconds: 30
          gcIntervalSeconds: 60
          standaloneDomainPacking: false
`

const (
	simulatorBinary  = "/alfred-simulator"
	simulatorBackend = "alfred-default-v1"
	simulatorMount   = "/etc/alfred-simulation"
	simulatorConfig  = "default.yaml"
	printProfileJob  = "alfred-print-profile"
)

type profileIdentity struct {
	SchedulerName    string `json:"schedulerName"`
	Backend          string `json:"backend"`
	SchedulerVersion string `json:"schedulerVersion"`
	ConfigurationID  string `json:"configurationID"`
}

type printedProfile struct {
	Identity       profileIdentity `json:"identity"`
	GangScheduling bool            `json:"gangScheduling"`
}

// bootstrapSimulation performs the operator bootstrap: run the simulator
// from the alfred image with --print-profile, record the printed identity in
// workers.json next to the scheduler config, and return the matching
// scheduling.profiles document for the chart. The printed profile must be
// gang-capable, or every multi-pod finding would be withheld as unsupported.
func bootstrapSimulation(img alfredImageRef) string {
	g := gomega.Default
	upsertConfigMap(g, alfredSimConfigMap, map[string]string{simulatorConfig: simulationSchedulerConfig})

	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: printProfileJob, Namespace: alfredNamespace},
		Spec: batchv1.JobSpec{
			BackoffLimit:          ptrInt32(0),
			ActiveDeadlineSeconds: ptrInt64(180),
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Containers: []corev1.Container{{
						Name:            "profile",
						Image:           img.String(),
						ImagePullPolicy: corev1.PullNever,
						Command: []string{simulatorBinary, "--backend", simulatorBackend,
							"--scheduler-config", simulatorMount + "/" + simulatorConfig, "--print-profile"},
						VolumeMounts: []corev1.VolumeMount{{Name: "sim", MountPath: simulatorMount, ReadOnly: true}},
					}},
					Volumes: []corev1.Volume{{Name: "sim", VolumeSource: corev1.VolumeSource{
						ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: alfredSimConfigMap}},
					}}},
				},
			},
		},
	}
	propagation := metav1.DeletePropagationForeground
	_ = client.IgnoreNotFound(k8sClient.Delete(suiteCtx, job, &client.DeleteOptions{PropagationPolicy: &propagation}))
	g.Eventually(func(gg gomega.Gomega) {
		err := k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(job), &batchv1.Job{})
		gg.Expect(apierrors.IsNotFound(err)).To(gomega.BeTrue(), "stale print-profile job still present")
	}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(gomega.Succeed())
	g.Expect(k8sClient.Create(suiteCtx, job)).To(gomega.Succeed(), "create print-profile job")

	var podName string
	g.Eventually(func(gg gomega.Gomega) {
		fresh := &batchv1.Job{}
		gg.Expect(k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(job), fresh)).To(gomega.Succeed())
		gg.Expect(fresh.Status.Succeeded).To(gomega.BeNumerically(">=", 1), "print-profile job has not succeeded (failed=%d)", fresh.Status.Failed)
		pods := &corev1.PodList{}
		gg.Expect(k8sClient.List(suiteCtx, pods, client.InNamespace(alfredNamespace), client.MatchingLabels{"job-name": printProfileJob})).To(gomega.Succeed())
		gg.Expect(pods.Items).NotTo(gomega.BeEmpty())
		podName = pods.Items[0].Name
	}).WithTimeout(kindMediumTimeout).WithPolling(kindPollInterval).Should(gomega.Succeed())

	logs, err := clientset.CoreV1().Pods(alfredNamespace).GetLogs(podName, &corev1.PodLogOptions{Container: "profile"}).DoRaw(suiteCtx)
	g.Expect(err).NotTo(gomega.HaveOccurred(), "read print-profile logs")
	var printed printedProfile
	g.Expect(json.Unmarshal(bytes.TrimSpace(logs), &printed)).To(gomega.Succeed(), "parse --print-profile output: %s", string(logs))
	g.Expect(printed.Identity.ConfigurationID).NotTo(gomega.BeEmpty())
	g.Expect(printed.GangScheduling).To(gomega.BeTrue(), "simulation profile is not gang-capable: %s", string(logs))

	workersManifest, err := json.Marshal(map[string]any{"workers": []map[string]any{{
		"binaryPath":          simulatorBinary,
		"schedulerConfigPath": simulatorMount + "/" + simulatorConfig,
		"identity":            printed.Identity,
		"gangScheduling":      printed.GangScheduling,
	}}})
	g.Expect(err).NotTo(gomega.HaveOccurred())
	upsertConfigMap(g, alfredSimConfigMap, map[string]string{
		simulatorConfig: simulationSchedulerConfig,
		"workers.json":  string(workersManifest),
	})

	profiles, err := json.Marshal(map[string]any{printed.Identity.SchedulerName: map[string]any{
		"backend":          printed.Identity.Backend,
		"schedulerVersion": printed.Identity.SchedulerVersion,
		"configurationID":  printed.Identity.ConfigurationID,
		"gangScheduling":   printed.GangScheduling,
	}})
	g.Expect(err).NotTo(gomega.HaveOccurred())
	return string(profiles)
}

func upsertConfigMap(g gomega.Gomega, name string, data map[string]string) {
	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: alfredNamespace}}
	err := k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(cm), cm)
	if apierrors.IsNotFound(err) {
		cm.Data = data
		g.ExpectWithOffset(1, k8sClient.Create(suiteCtx, cm)).To(gomega.Succeed(), "create %s", name)
		return
	}
	g.ExpectWithOffset(1, err).NotTo(gomega.HaveOccurred())
	cm.Data = data
	g.ExpectWithOffset(1, k8sClient.Update(suiteCtx, cm)).To(gomega.Succeed(), "update %s", name)
}

func ptrInt32(v int32) *int32 { return &v }
func ptrInt64(v int64) *int64 { return &v }

func TestSplitImageRef(t *testing.T) {
	cases := []struct {
		ref  string
		want alfredImageRef
	}{
		{"alfred:kind", alfredImageRef{Repository: "alfred", Tag: "kind"}},
		{"registry.example.com:5000/repo/alfred:v1", alfredImageRef{Repository: "registry.example.com:5000/repo/alfred", Tag: "v1"}},
		{"registry.example.com:5000/repo/alfred", alfredImageRef{Repository: "registry.example.com:5000/repo/alfred", Tag: "latest"}},
	}
	for _, tc := range cases {
		if got := splitImageRef(tc.ref); got != tc.want {
			t.Errorf("splitImageRef(%q) = %+v, want %+v", tc.ref, got, tc.want)
		}
	}
}
