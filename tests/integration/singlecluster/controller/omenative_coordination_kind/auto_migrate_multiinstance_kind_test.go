package omenative_coordination_kind

import (
	"sync"
	"time"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/tests/integration/util"
)

// Multi-in-flight auto-migration within one Component's index space.
//
// A single engine Component pinned to minReplicas=maxReplicas=3 with a
// bad image leaves all three source Instances stuck in Phase=Creating
// simultaneously. Under MigrationPolicy.Mode=Auto with a short
// ProgressDeadlineSeconds, the controller self-triggers a migration for
// each stuck source in the same window.
//
// This is the fleet-wipe class: if surge allocation collapsed colliding
// gangs into a single index bucket, three sources would fight over one
// surge index and the Component could implode to a single alive Instance.
// The spec pins three invariants the fleet-wipe SEV violated:
//
//   - distinct-surge-index: the ledger reaches three Started entries with
//     three DISTINCT SurgeInstance indices (no shared surge index).
//   - capacity-floor / no mass fan-out: total alive engine pods stays
//     <= replicas + per-source surge headroom throughout (100ms sampler).
//   - source->surge bijection: each source index maps to exactly one
//     surge index and vice-versa.
//
// As with auto_migrate_kind_test.go, InstanceReadyTimeout is deliberately
// left unset: the timeouts escalator would otherwise stamp Phase=Failed
// (for which isStuckPhase() is false), suppressing auto-migration before
// the Started entries land.
var _ = ginkgo.Describe("OMENative auto-migration multi-Instance surge fan-out (KIND)", ginkgo.Ordered, func() {
	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "omenative-auto-mig-mi-")
		Expect(err).NotTo(HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)).To(Succeed())
	})

	ginkgo.It("three stuck Instances each get a DISTINCT surge index (no fleet-wipe collapse)", func() {
		const (
			isvcName = "auto-mig-mi1"
			replicas = 3
		)
		// replicas + one concurrent surge per source. Sources never drain
		// because the bad image never promotes a surge to Ready, so all
		// three surges coexist with all three sources — 2x replicas is the
		// hard alive ceiling. Anything above it is a mass fan-out.
		ceiling := int32(replicas * 2)

		// 30s is the webhook-enforced ProgressDeadlineSeconds minimum;
		// MaxAutoRetries=1 keeps the ledger to one auto-recover attempt per
		// source so the distinct-index assertion reads a clean 3-entry set
		// (a higher cap would append retry rows for the same source).
		deadline := int32(30)
		maxRetries := int32(1)

		fix := newCoordFixture(ns.Name, WithISVCName(isvcName),
			WithoutDecoder(),
			WithEngineReplicas(replicas),
			WithEngineImage("ghcr.io/ome-projects/this-image-does-not-exist:v0"))
		// Pin maxReplicas=minReplicas so no autoscaler drift changes the
		// index space mid-test, and attach the Auto migration policy.
		fix.ISVC.Spec.Engine.MaxReplicas = replicas
		fix.ISVC.Spec.Engine.Lifecycle = &v1beta1.LifecycleSpec{
			MigrationPolicy: &v1beta1.MigrationPolicy{
				Mode:                    v1beta1.MigrationPolicyModeAuto,
				ProgressDeadlineSeconds: &deadline,
				MaxAutoRetries:          &maxRetries,
			},
		}
		applyFixture(Default, fix)
		deferFixtureCleanup(fix)

		ginkgo.By("waiting for all three source Instances to land and stick in Phase=Creating")
		Eventually(func(g Gomega) {
			c := freshISVC(g, ns.Name, isvcName)
			cs := c.Status.Components[v1beta1.EngineComponent]
			g.Expect(cs.Lifecycle).NotTo(BeNil())
			insts := instanceStatuses(g, ns.Name, isvcName, v1beta1.EngineComponent)
			creating := map[int32]struct{}{}
			for _, s := range insts {
				if s.Phase == v1beta1.OMENativeInstanceCreating {
					creating[s.Index] = struct{}{}
				}
			}
			g.Expect(creating).To(HaveLen(replicas),
				"expected %d source Instances stuck in Phase=Creating; got %+v",
				replicas, insts)
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		// 100ms sampler pins the capacity-floor invariant for the whole
		// fan-out window: alive engine pods must never exceed the ceiling.
		// A shared-surge-index collapse or a mass fan-out both trip this.
		// Track the max observed so a passing run still proves the surge
		// path fired (max > replicas) rather than trivially passing because
		// no surge ever surfaced.
		var maxAlive int32
		var maxAliveMu sync.Mutex
		stop := util.StartSampler(suiteCtx, 100*time.Millisecond, func(g Gomega) {
			snap, err := util.SnapshotPods(suiteCtx, k8sClient, ns.Name, isvcName)
			g.Expect(err).NotTo(HaveOccurred())
			alive := int32(snap.Alive(v1beta1.EngineComponent))
			maxAliveMu.Lock()
			if alive > maxAlive {
				maxAlive = alive
			}
			maxAliveMu.Unlock()
			g.Expect(alive).To(BeNumerically("<=", ceiling),
				"engine alive pods must stay <= replicas(%d)+surge-headroom=%d throughout the fan-out; got alive=%d",
				replicas, ceiling, alive)
		})
		defer stop()

		ginkgo.By("waiting for three Started AutoRecover ledger entries with three DISTINCT surge indices")
		Eventually(func(g Gomega) {
			ledger := loadLedger(g, ns.Name, isvcName)
			// Collect the surge index of each distinct Started AutoRecover
			// migration, keyed by source index so we can assert the
			// source->surge bijection.
			surgeBySource := map[int32]int32{}
			for _, e := range ledger.Entries {
				if e.Component != "engine" || e.Phase != "Started" || e.Reason != "AutoRecover" {
					continue
				}
				// The surge index is stamped once Migrate dispatches the
				// synthetic request (auto_migrate seeds the source-side
				// Started; migrate.go rewrites it with SurgeInstance). Only
				// count entries that have advanced to a real surge index.
				if e.SurgeInstance == 0 {
					continue
				}
				surgeBySource[e.SourceInstance] = e.SurgeInstance
			}
			g.Expect(surgeBySource).To(HaveLen(replicas),
				"expected %d sources with a dispatched surge; got %+v (ledger=%+v)",
				replicas, surgeBySource, ledger.Entries)

			// (a) distinct-surge-index: the surge indices must be pairwise
			// distinct — the fleet-wipe class is multiple sources sharing
			// one surge index.
			surgeSet := map[int32]struct{}{}
			for _, surge := range surgeBySource {
				surgeSet[surge] = struct{}{}
			}
			g.Expect(surgeSet).To(HaveLen(replicas),
				"surge indices must be pairwise DISTINCT (no shared surge index); source->surge=%+v",
				surgeBySource)

			// (c) source->surge bijection: no surge index may collide with
			// a source index (that would re-target a live source), and the
			// map is already a function source->surge with distinct images
			// (checked above), so distinct + disjoint domains == bijection
			// onto the surge index space.
			for source, surge := range surgeBySource {
				g.Expect(surge).NotTo(Equal(source),
					"surge index %d must differ from its source index %d", surge, source)
				_, sourceIsAlsoSurge := surgeSet[source]
				g.Expect(sourceIsAlsoSurge).To(BeFalse(),
					"source index %d must not also be a surge target (index-space overlap)", source)
			}
		}).WithTimeout(kindLongTimeout).WithPolling(kindPollInterval).Should(Succeed())

		stop()

		// (b) sanity floor: the surge path must have actually fired. With
		// three sources stuck and three surges created, max alive must
		// exceed replicas; if it stayed at replicas no surge ever landed
		// and the distinct-index assertion above would have passed for the
		// wrong reason.
		maxAliveMu.Lock()
		observed := maxAlive
		maxAliveMu.Unlock()
		Expect(observed).To(BeNumerically(">", int32(replicas)),
			"surge path must fire (max alive engine pods > replicas=%d); got max=%d — "+
				"auto-migration never surged", replicas, observed)
	})
})
