// Package clustercache hosts integration tests that wire the REAL
// clustercache.NewClient provider against an httptest stub of the
// cluster_cache HTTP API, then run the BaseModel reconciler on top.
// Catches integration bugs that neither the unit tests in
// pkg/modelcache/providers/clustercache (no controller wiring) nor
// the basemodel suite (uses a fake provider, no HTTP) can catch.
package clustercache

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/controller/v1beta1/basemodel/backends/sharded"
	_ "github.com/ome-projects/ome/pkg/controller/v1beta1/basemodel/backends/sharded" // register provider factory

	"github.com/ome-projects/ome/tests/integration/components"
	"github.com/ome-projects/ome/tests/integration/framework"
	"github.com/ome-projects/ome/tests/integration/util"
)

func TestClusterCacheProvider(t *testing.T) {
	util.RunSuite(t, "ClusterCache Provider Integration Suite")
}

var (
	suiteCtx    context.Context
	suiteCancel context.CancelFunc
	suiteFwk    *framework.Framework
	suiteCfg    *rest.Config
	k8sClient   client.Client

	// suiteServer is the httptest-backed cluster_cache HTTP API stub.
	// suiteHandler is the per-spec mutable handler the server delegates
	// to. Each It overrides it before creating its CR; BeforeEach
	// resets it to a default 404-for-everything (a baseline that
	// surfaces any missing per-spec handler override as a clear test
	// failure, not a silent unreachable).
	suiteServer  *httptest.Server
	suiteHandler http.Handler
	handlerMu    sync.Mutex
)

func setHandler(h http.Handler) {
	handlerMu.Lock()
	suiteHandler = h
	handlerMu.Unlock()
}

func currentHandler() http.Handler {
	handlerMu.Lock()
	defer handlerMu.Unlock()
	return suiteHandler
}

var _ = ginkgo.BeforeSuite(func() {
	framework.SetupTestLogger()

	s, err := framework.NewSuite(&framework.Framework{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	suiteFwk = s.Fwk
	suiteCfg = s.Cfg
	k8sClient = s.Client
	suiteCtx, suiteCancel = context.WithCancel(s.Ctx)

	// Server delegates to the per-spec handler. Spec assigns the
	// handler before creating its CR; the controller's polling loop
	// (every 5s) hits this same server endpoint repeatedly, so the
	// handler can change behavior between calls if the spec wants
	// to script a sequence (e.g. transient 503 → 200).
	suiteServer = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := currentHandler()
		if h == nil {
			http.NotFound(w, r)
			return
		}
		h.ServeHTTP(w, r)
	}))

	// Build the REAL clustercache provider — registered via the
	// blank-import in this file. Endpoint is the httptest server URL.
	provider, err := sharded.BuildProvider(sharded.ProviderConfig{
		Provider: sharded.ProviderClusterCache,
		Endpoint: suiteServer.URL,
	})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	mgrSetup := func(ctx context.Context, mgr ctrl.Manager) error {
		return components.SetupBaseModel(ctx, mgr,
			components.WithModelCacheProvider(provider))
	}
	gomega.Expect(suiteFwk.StartManager(suiteCtx, suiteCfg, mgrSetup)).To(gomega.Succeed())
})

var _ = ginkgo.AfterSuite(func() {
	if suiteServer != nil {
		suiteServer.Close()
	}
	if suiteCancel != nil {
		suiteCancel()
	}
	if suiteFwk != nil {
		_ = suiteFwk.StopManager(context.Background())
		_ = suiteFwk.Teardown()
	}
})
