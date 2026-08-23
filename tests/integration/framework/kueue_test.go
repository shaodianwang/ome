package framework_test

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	crconfig "sigs.k8s.io/controller-runtime/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	"sigs.k8s.io/yaml"

	configapi "sigs.k8s.io/kueue/apis/config/v1beta2"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	podtesting "sigs.k8s.io/kueue/pkg/util/testingjobs/pod"

	"github.com/ome-projects/ome/tests/integration/framework"
)

// TestStartKueue guards framework.StartKueue + framework.ApplyKueueQuota:
// it stands up ONE envtest apiserver, wires the Kueue admission stack via
// StartKueue, applies an admissible quota via ApplyKueueQuota, and asserts
// a queue-labeled pod is GATED by the pod webhook then UNGATED once the
// scheduler admits it — with no kubelet. It exists so a Kueue bump that
// breaks the wiring fails loudly here.
//
// Run (make kueue-crds first: dep-crds is gitignored, so the vendored
// CRDs may be left over from an older Kueue and not serve v1beta2):
//
//	make kueue-crds
//	export KUBEBUILDER_ASSETS="$(bin/setup-envtest use "$(cat .envtest-k8s-version)" -p path)"
//	go test ./tests/integration/framework/ -run TestStartKueue -v -timeout 5m
//
// It's a plain testing.T test (not part of the Ginkgo TestFramework
// suite) because it runs its own bespoke envtest with the Kueue webhook
// installed — the shared Framework webhook path targets OME's webhooks,
// not Kueue's module-vendored ones.
func TestStartKueue(t *testing.T) {
	g := gomega.NewWithT(t)

	// Resolve the vendored Kueue CRD dir (committed-on-demand by
	// `make kueue-crds`) and the Kueue webhook dir (module-sourced;
	// not vendored into dep-crds, which is CRDs-only).
	crdDir := filepath.Join(projectRoot(t), "tests", "integration", "dep-crds", "kueue")
	if _, err := os.Stat(crdDir); err != nil {
		t.Fatalf("vendored Kueue CRDs missing at %s — run `make kueue-crds`: %v", crdDir, err)
	}
	requireCRDServesVersion(t, filepath.Join(crdDir, "kueue.x-k8s.io_clusterqueues.yaml"), kueue.GroupVersion.Version)
	webhookDir, err := framework.KueueWebhookDir()
	g.Expect(err).NotTo(gomega.HaveOccurred(), "resolve Kueue webhook dir")

	testEnv := &envtest.Environment{
		CRDDirectoryPaths:     []string{crdDir},
		ErrorIfCRDPathMissing: true,
	}
	testEnv.WebhookInstallOptions.Paths = []string{webhookDir}

	cfg, err := testEnv.Start()
	g.Expect(err).NotTo(gomega.HaveOccurred(), "envtest start")
	defer func() { _ = testEnv.Stop() }()

	scheme := runtime.NewScheme()
	g.Expect(clientgoscheme.AddToScheme(scheme)).To(gomega.Succeed())
	g.Expect(configapi.AddToScheme(scheme)).To(gomega.Succeed())
	g.Expect(kueue.AddToScheme(scheme)).To(gomega.Succeed())

	k8s, err := client.New(cfg, client.Options{Scheme: scheme})
	g.Expect(err).NotTo(gomega.HaveOccurred(), "direct client")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	whOpts := &testEnv.WebhookInstallOptions
	mgr, err := ctrl.NewManager(cfg, manager.Options{
		Scheme:  scheme,
		Metrics: metricsserver.Options{BindAddress: "0"},
		WebhookServer: webhook.NewServer(webhook.Options{
			Host:    whOpts.LocalServingHost,
			Port:    whOpts.LocalServingPort,
			CertDir: whOpts.LocalServingCertDir,
		}),
		Controller: crconfig.Controller{SkipNameValidation: ptr.To(true)},
	})
	g.Expect(err).NotTo(gomega.HaveOccurred(), "new manager")

	// The helper under test.
	g.Expect(framework.StartKueue(ctx, mgr)).To(gomega.Succeed(), "StartKueue")

	go func() { _ = mgr.Start(ctx) }()

	// Wait for the webhook server to be dialable, else pod creation slips
	// past the gate before the listener is bound.
	addrPort := fmt.Sprintf("%s:%d", whOpts.LocalServingHost, whOpts.LocalServingPort)
	g.Eventually(func(gg gomega.Gomega) {
		conn, derr := tls.DialWithDialer(&net.Dialer{Timeout: time.Second}, "tcp", addrPort, &tls.Config{InsecureSkipVerify: true})
		gg.Expect(derr).NotTo(gomega.HaveOccurred())
		_ = conn.Close()
	}, 30*time.Second, 200*time.Millisecond).Should(gomega.Succeed(), "webhook ready")

	// Admissible quota: 1 CPU admits a 500m pod.
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "kueue-smoke"}}
	g.Expect(k8s.Create(ctx, ns)).To(gomega.Succeed())
	g.Expect(framework.ApplyKueueQuota(ctx, k8s, ns.Name, "lq", "1")).To(gomega.Succeed(), "ApplyKueueQuota")

	// Pod within quota: GATED by the webhook, then UNGATED by the scheduler.
	p1 := podtesting.MakePod("p1", ns.Name).Queue("lq").Request(corev1.ResourceCPU, "500m").Obj()
	g.Expect(k8s.Create(ctx, p1)).To(gomega.Succeed())

	g.Eventually(func(gg gomega.Gomega) {
		got := &corev1.Pod{}
		gg.Expect(k8s.Get(ctx, types.NamespacedName{Namespace: ns.Name, Name: "p1"}, got)).To(gomega.Succeed())
		gg.Expect(got.Spec.SchedulingGates).NotTo(gomega.BeEmpty(), "p1 should be gated by the kueue webhook")
	}, 30*time.Second, 200*time.Millisecond).Should(gomega.Succeed())

	g.Eventually(func(gg gomega.Gomega) {
		got := &corev1.Pod{}
		gg.Expect(k8s.Get(ctx, types.NamespacedName{Namespace: ns.Name, Name: "p1"}, got)).To(gomega.Succeed())
		gg.Expect(got.Spec.SchedulingGates).To(gomega.BeEmpty(), "p1 should be ungated once admitted by quota")
	}, 60*time.Second, 200*time.Millisecond).Should(gomega.Succeed())

	t.Log("SMOKE PASS: StartKueue + ApplyKueueQuota gated then ungated a queue-labeled pod in envtest (no kubelet)")
}

// requireCRDServesVersion fails if the vendored CRD does not serve version.
// dep-crds is gitignored and regenerated by `make kueue-crds`, so a checkout
// that last vendored an older Kueue carries CRDs without the version this
// harness speaks. Catching it here names the fix; without it the run dies
// much later on an opaque "no matches for kind" from the apiserver.
func requireCRDServesVersion(t *testing.T, crdPath, version string) {
	t.Helper()
	raw, err := os.ReadFile(crdPath)
	if err != nil {
		t.Fatalf("read vendored CRD %s — run `make kueue-crds`: %v", crdPath, err)
	}
	var crd struct {
		Spec struct {
			Versions []struct {
				Name   string `json:"name"`
				Served bool   `json:"served"`
			} `json:"versions"`
		} `json:"spec"`
	}
	if err := yaml.Unmarshal(raw, &crd); err != nil {
		t.Fatalf("parse vendored CRD %s: %v", crdPath, err)
	}
	served := make([]string, 0, len(crd.Spec.Versions))
	for _, v := range crd.Spec.Versions {
		if !v.Served {
			continue
		}
		if v.Name == version {
			return
		}
		served = append(served, v.Name)
	}
	t.Fatalf("vendored Kueue CRDs are stale: %s serves %v, not %s — run `make kueue-crds`",
		filepath.Base(crdPath), served, version)
}

// projectRoot walks up from this test file to the repo root (go.mod).
func projectRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatalf("no go.mod found above %s", dir)
		}
		dir = parent
	}
}
