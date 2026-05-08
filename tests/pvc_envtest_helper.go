package integration_tests

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	"github.com/sgl-project/ome/pkg/apis/ome/v1beta1"
	"github.com/sgl-project/ome/pkg/constants"
)

// startPVCEnvTest spins up envtest with the CRDs and schemes the PVC
// integration tests need: BaseModel/ClusterBaseModel CRDs, plus
// corev1 (Namespace/PVC/ConfigMap) and batchv1 (Job) for live API calls.
//
// Returns the rest.Config, a controller-runtime client, and a stop func
// the caller MUST defer.
func startPVCEnvTest(t *testing.T, g *gomega.GomegaWithT) (*rest.Config, client.Client, func()) {
	t.Helper()

	env := &envtest.Environment{
		// tests/ is one level deep — config/ resolves at ../config/
		CRDDirectoryPaths:     []string{filepath.Join("..", "config", "crd", "full")},
		ErrorIfCRDPathMissing: true,
	}

	cfg, err := env.Start()
	g.Expect(err).NotTo(gomega.HaveOccurred(), "failed to start envtest")
	g.Expect(cfg).NotTo(gomega.BeNil())

	scheme := runtime.NewScheme()
	g.Expect(corev1.AddToScheme(scheme)).NotTo(gomega.HaveOccurred())
	g.Expect(batchv1.AddToScheme(scheme)).NotTo(gomega.HaveOccurred())
	g.Expect(v1beta1.AddToScheme(scheme)).NotTo(gomega.HaveOccurred())

	c, err := client.New(cfg, client.Options{Scheme: scheme})
	g.Expect(err).NotTo(gomega.HaveOccurred())

	// Pre-create the OME namespace so per-PVC ConfigMaps land somewhere.
	g.Expect(c.Create(context.Background(), &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: constants.OMENamespace},
	})).To(gomega.Or(gomega.BeNil(), gomega.MatchError(gomega.ContainSubstring("already exists"))))

	stop := func() { _ = env.Stop() }
	return cfg, c, stop
}
