package kindutil

import (
	"context"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/tests/integration/util"
)

// InvariantSampling runs the shared OMENative invariant sampler around every
// spec of a suite. The sampler watches the InferenceServices in the
// namespaces the running process created through util.CreateNamespaceFromPrefix
// and reports the invariants that stayed broken for the sustain window.
type InvariantSampling struct {
	// Enforce fails a spec on a sustained violation; the default only reports
	// it in the spec's log and report entries.
	Enforce bool
}

// invariantTargets lists the InferenceServices in the namespaces this process
// owns right now, so one created mid-spec is picked up on the next sample.
func invariantTargets(reader client.Reader) util.InvariantTargetSource {
	return func(ctx context.Context) ([]util.InvariantTarget, error) {
		var out []util.InvariantTarget
		for _, ns := range util.OwnedNamespaces() {
			list := &v1beta1.InferenceServiceList{}
			if err := reader.List(ctx, list, client.InNamespace(ns)); err != nil {
				return nil, err
			}
			for i := range list.Items {
				out = append(out, util.InvariantTarget{Namespace: ns, Name: list.Items[i].Name})
			}
		}
		return out, nil
	}
}

// registerInvariantSampler adds the root-level nodes that run the sampler
// around every spec: BeforeEach starts it before the spec's own setup, and
// JustAfterEach stops and reports it after the spec body, before the spec's
// cleanup deletes its namespaces. Must run before RunSpecs, while Ginkgo
// still accepts top-level nodes.
func registerInvariantSampler(state *SuiteState, opts *InvariantSampling) {
	if opts == nil {
		return
	}
	source := invariantTargets(state.Client)
	var sampler *util.InvariantSampler
	ginkgo.BeforeEach(func() {
		parent := state.Ctx
		if parent == nil {
			parent = context.Background()
		}
		sampler = util.NewInvariantSamplerFromSource(state.Client, source, util.InvariantSamplerOptions{Enforce: opts.Enforce})
		sampler.Start(parent)
	})
	ginkgo.JustAfterEach(func() {
		if sampler == nil {
			return
		}
		s := sampler
		sampler = nil
		s.Stop()
		s.Report(gomega.NewGomega(ginkgo.Fail))
	})
}
