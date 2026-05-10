package framework

import (
	"context"

	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Suite bundles the values every integration suite needs in nearly
// every test: the framework itself, the rest.Config, a client, and a
// context. The struct removes the package-level
// `var (cfg, k8sClient, ctx, fwk)` boilerplate from each suite_test.go.
//
// Construct via NewSuite from BeforeSuite (Ginkgo) or TestMain
// (plain go test).
type Suite struct {
	Fwk    *Framework
	Cfg    *rest.Config
	Client client.Client
	Ctx    context.Context
}

// NewSuite runs Init + SetupClient on the supplied Framework and
// returns a populated Suite. The caller is responsible for calling
// s.Fwk.Teardown() in AfterSuite — NewSuite deliberately does NOT
// register cleanup, because Ginkgo and plain `testing.T` express
// cleanup differently and we don't want to favor one or the other.
func NewSuite(f *Framework) (*Suite, error) {
	cfg, err := f.Init()
	if err != nil {
		return nil, err
	}
	ctx, c, err := f.SetupClient(cfg)
	if err != nil {
		// Best-effort teardown so a failure here doesn't leak envtest.
		_ = f.Teardown()
		return nil, err
	}
	return &Suite{Fwk: f, Cfg: cfg, Client: c, Ctx: ctx}, nil
}
