package clustercache

import (
	"net/http"
	"sync/atomic"
	"time"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/types"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	v1beta1testing "github.com/ome-projects/ome/pkg/utils/testing/v1beta1"

	"github.com/ome-projects/ome/tests/integration/util"
)

// successfulManifestHandler installs a handler that serves the
// "happy" file_metadata + remote_object(config.json) pair so any spec
// that just wants to get a model into Ready state can do it in one
// line. healthy is atomic so a spec can flip it mid-flight to script
// recovery / degradation scenarios.
func successfulManifestHandler(rootURI string, healthy *atomic.Bool) http.HandlerFunc {
	configPath := rootURI + "/config.json"
	return func(w http.ResponseWriter, r *http.Request) {
		if !healthy.Load() {
			http.Error(w, "scripted source unreachability", http.StatusServiceUnavailable)
			return
		}
		switch r.URL.Path {
		case "/file_metadata":
			writeJSON(w, singleConfigJSONManifest(rootURI, configPath, uint64(len(minimalLlamaConfigJSON))))
		case "/remote_object":
			if r.URL.Query().Get("path") != configPath {
				http.Error(w, "unexpected remote_object path", http.StatusBadRequest)
				return
			}
			w.Header().Set("Content-Type", "application/octet-stream")
			_, _ = w.Write([]byte(minimalLlamaConfigJSON))
		default:
			http.NotFound(w, r)
		}
	}
}

// Lifecycle behavior specs — what happens to the state machine
// across time, mutations, and intermittent backend health. The
// single-shot end-state specs live in inspect_source_test.go; this
// file is about transitions, idempotency, and spec drift.
var _ = ginkgo.Describe("ClusterCache Sharded BaseModel lifecycle behavior", ginkgo.Ordered, func() {

	ginkgo.BeforeEach(func() {
		setHandler(http.NotFoundHandler())
	})

	ginkgo.It("once Ready, an externally-degraded source does NOT bounce the BaseModel back to Failed", func() {
		// reconcile.go:67-75 deliberately short-circuits when Ready=True
		// — the comment there spells out that running the status path
		// would force state back to Importing on every poll, churning
		// a healthy live-serving model. The contract is: once Ready,
		// only a spec change (generation bump) can re-trigger
		// CheckSource. This Consistently pin catches a regression
		// where someone adds aggressive periodic re-checking, which
		// would visibly degrade a Ready BaseModel during a transient
		// cluster_cache hiccup and ripple into ISVCs holding it.
		const root = "s3://bucket/llama-ready-sticky"
		var healthy atomic.Bool
		healthy.Store(true)
		setHandler(successfulManifestHandler(root, &healthy))

		cbm := v1beta1testing.MakeClusterBaseModel("cc-ready-sticky").
			StorageURI(root).
			Distribution(v1beta1.DistributionSharded).
			ModelFormat("safetensors").Obj()
		Expect(k8sClient.Create(suiteCtx, cbm)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, cbm) })

		ginkgo.By("phase 1: source reachable → state=Ready")
		Eventually(func(g Gomega) {
			got := &v1beta1.ClusterBaseModel{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "cc-ready-sticky"}, got)).To(Succeed())
			g.Expect(got.Status.State).To(Equal(v1beta1.LifeCycleStateReady))
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("phase 2: flip backend to 503; Ready model must NOT degrade")
		healthy.Store(false)
		// Hold the assertion across a window comfortably longer than
		// the controller's 5s poll interval so we observe at least
		// one full reconcile after the backend went down.
		Consistently(func(g Gomega) {
			got := &v1beta1.ClusterBaseModel{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "cc-ready-sticky"}, got)).To(Succeed())

			source := apimeta.FindStatusCondition(got.Status.Conditions, v1beta1.ModelConditionSourceReachable)
			g.Expect(source).NotTo(BeNil())
			g.Expect(string(source.Status)).To(Equal("True"),
				"Ready models must not have SourceReachable re-checked on every poll — reconcile.go:67-75")

			ready := apimeta.FindStatusCondition(got.Status.Conditions, v1beta1.ModelConditionReady)
			g.Expect(ready).NotTo(BeNil())
			g.Expect(string(ready.Status)).To(Equal("True"),
				"Ready model must remain Ready across backend downtime")
			g.Expect(got.Status.State).To(Equal(v1beta1.LifeCycleStateReady),
				"state must stay Ready — only a spec change can re-trigger source check")
		}).WithTimeout(8 * time.Second).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.It("once Ready, conditions are idempotent — LastTransitionTime does not churn across requeues", func() {
		// Each requeue of an already-Ready model runs the full
		// reconcile, including a fresh CheckSource HTTP call. The
		// apimachinery condition helper only bumps LastTransitionTime
		// when Status changes — but if a refactor accidentally writes
		// the conditions through a path that always updates, every
		// reconcile would touch the resource version. That breaks
		// downstream controllers that watch for "real" changes and is
		// invisible to single-shot assertions. Consistently-asserting
		// LastTransitionTime stability pins the no-churn contract at
		// the integration boundary.
		const root = "s3://bucket/llama-idempotent"
		var healthy atomic.Bool
		healthy.Store(true)
		setHandler(successfulManifestHandler(root, &healthy))

		cbm := v1beta1testing.MakeClusterBaseModel("cc-idempotent").
			StorageURI(root).
			Distribution(v1beta1.DistributionSharded).
			ModelFormat("safetensors").Obj()
		Expect(k8sClient.Create(suiteCtx, cbm)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, cbm) })

		// Wait for Ready before sampling LastTransitionTime.
		var readyAt time.Time
		Eventually(func(g Gomega) {
			got := &v1beta1.ClusterBaseModel{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "cc-idempotent"}, got)).To(Succeed())
			ready := apimeta.FindStatusCondition(got.Status.Conditions, v1beta1.ModelConditionReady)
			g.Expect(ready).NotTo(BeNil())
			g.Expect(string(ready.Status)).To(Equal("True"))
			readyAt = ready.LastTransitionTime.Time
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())

		// Across the next observation window, the controller will
		// have requeued at least once (poll interval is 5s, window is
		// 6s of polling). LastTransitionTime must NOT advance — that
		// would indicate the controller is rewriting the same
		// condition value through a non-meta.SetStatusCondition path.
		Consistently(func(g Gomega) {
			got := &v1beta1.ClusterBaseModel{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "cc-idempotent"}, got)).To(Succeed())
			ready := apimeta.FindStatusCondition(got.Status.Conditions, v1beta1.ModelConditionReady)
			g.Expect(ready).NotTo(BeNil())
			g.Expect(ready.LastTransitionTime.Time.Equal(readyAt)).To(BeTrue(),
				"Ready.LastTransitionTime drifted from %v to %v — controller is rewriting same-value conditions",
				readyAt, ready.LastTransitionTime.Time)
		}).WithTimeout(6 * time.Second).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.It("spec.storage.storageUri updated mid-flight → controller re-checks new URI", func() {
		// CheckSource and InspectSource take the URI from the live
		// spec, not a one-shot capture, so a mutation must be reflected
		// in the next reconcile. Catches a regression where the
		// controller (or status.cache.SourceUri drift detection in
		// reconcile.go:474) caches the original URI and never
		// re-checks against the updated one.
		const rootA = "s3://bucket/llama-uri-a"
		const rootB = "s3://bucket/llama-uri-b"
		var calls atomic.Int32
		setHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			calls.Add(1)
			// Route by query param so we can prove the controller
			// switches URIs and stops hitting the old one.
			path := r.URL.Query().Get("path")
			switch r.URL.Path {
			case "/file_metadata":
				// /file_metadata's path query is the *root*, so
				// match on that.
				switch path {
				case rootA:
					writeJSON(w, singleConfigJSONManifest(rootA, rootA+"/config.json", uint64(len(minimalLlamaConfigJSON))))
				case rootB:
					writeJSON(w, singleConfigJSONManifest(rootB, rootB+"/config.json", uint64(len(minimalLlamaConfigJSON))))
				default:
					http.Error(w, "unknown root: "+path, http.StatusNotFound)
				}
			case "/remote_object":
				switch path {
				case rootA + "/config.json", rootB + "/config.json":
					w.Header().Set("Content-Type", "application/octet-stream")
					_, _ = w.Write([]byte(minimalLlamaConfigJSON))
				default:
					http.Error(w, "unknown object path: "+path, http.StatusNotFound)
				}
			default:
				http.NotFound(w, r)
			}
		}))

		cbm := v1beta1testing.MakeClusterBaseModel("cc-uri-update").
			StorageURI(rootA).
			Distribution(v1beta1.DistributionSharded).
			ModelFormat("safetensors").Obj()
		Expect(k8sClient.Create(suiteCtx, cbm)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, cbm) })

		ginkgo.By("URI A → state=Ready, status.cache.sourceUri == A")
		Eventually(func(g Gomega) {
			got := &v1beta1.ClusterBaseModel{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "cc-uri-update"}, got)).To(Succeed())
			g.Expect(got.Status.State).To(Equal(v1beta1.LifeCycleStateReady))
			g.Expect(got.Status.Cache).NotTo(BeNil())
			g.Expect(got.Status.Cache.SourceUri).To(Equal(rootA))
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("flipping spec.storage.storageUri to URI B")
		// Read-modify-write loop so we don't fight the controller's
		// own status updates on the same object generation.
		Eventually(func(g Gomega) {
			latest := &v1beta1.ClusterBaseModel{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "cc-uri-update"}, latest)).To(Succeed())
			latest.Spec.Storage.StorageUri = &[]string{rootB}[0]
			g.Expect(k8sClient.Update(suiteCtx, latest)).To(Succeed())
		}).WithTimeout(util.ShortTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("URI B → status.cache.sourceUri == B (drift detection picks up the new URI)")
		Eventually(func(g Gomega) {
			got := &v1beta1.ClusterBaseModel{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "cc-uri-update"}, got)).To(Succeed())
			g.Expect(got.Status.Cache).NotTo(BeNil())
			g.Expect(got.Status.Cache.SourceUri).To(Equal(rootB),
				"status.cache.SourceUri must follow spec.storage.storageUri across mutations")
			g.Expect(got.Status.State).To(Equal(v1beta1.LifeCycleStateReady),
				"new URI is also healthy; state must remain Ready after the cutover")
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})
})
