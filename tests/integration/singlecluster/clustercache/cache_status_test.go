package clustercache

import (
	"net/http"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/types"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	v1beta1testing "github.com/ome-projects/ome/pkg/utils/testing/v1beta1"

	"github.com/ome-projects/ome/tests/integration/util"
)

// status.cache content. ModelCacheStatus.Backend + SourceUri identify
// the cache mapping for a Sharded model. The contract is that this
// pair is set as soon as the controller decides a provider is
// applicable, independent of source reachability or metadata parse
// outcome. These specs pin that invariant — catches a regression
// where status.cache is gated on the model being Ready (which would
// strand operators trying to debug a stuck Failed model by removing
// the very field that tells them which backend / source the
// controller used).
var _ = ginkgo.Describe("ClusterCache ModelCacheStatus content", ginkgo.Ordered, func() {

	ginkgo.BeforeEach(func() {
		setHandler(http.NotFoundHandler())
	})

	ginkgo.It("happy path: status.cache.{backend,sourceUri} populated to match provider config + spec.storage.storageUri", func() {
		// Both fields must populate exactly. Backend reflects the
		// configured provider name ("cluster_cache"); SourceUri
		// mirrors spec.storage.storageUri verbatim. The unit tests
		// cover the applyInitialModelStatus shape; this pins the
		// integration boundary, including the suite's choice of
		// provider name flowing through to the live status object.
		const root = "s3://bucket/llama-cache-happy"
		const configPath = root + "/config.json"
		setHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Path {
			case "/file_metadata":
				writeJSON(w, singleConfigJSONManifest(root, configPath, uint64(len(minimalLlamaConfigJSON))))
			case "/remote_object":
				w.Header().Set("Content-Type", "application/octet-stream")
				_, _ = w.Write([]byte(minimalLlamaConfigJSON))
			default:
				http.NotFound(w, r)
			}
		}))

		cbm := v1beta1testing.MakeClusterBaseModel("cc-cache-happy").
			StorageURI(root).
			Distribution(v1beta1.DistributionSharded).
			ModelFormat("safetensors").Obj()
		Expect(k8sClient.Create(suiteCtx, cbm)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, cbm) })

		Eventually(func(g Gomega) {
			got := &v1beta1.ClusterBaseModel{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "cc-cache-happy"}, got)).To(Succeed())
			g.Expect(got.Status.State).To(Equal(v1beta1.LifeCycleStateReady))

			g.Expect(got.Status.Cache).NotTo(BeNil(), "Sharded BaseModel must have status.cache populated")
			g.Expect(got.Status.Cache.Backend).To(Equal("cluster_cache"),
				"status.cache.Backend must match the configured provider name")
			g.Expect(got.Status.Cache.SourceUri).To(Equal(root),
				"status.cache.SourceUri must mirror spec.storage.storageUri")
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.It("source unreachable: status.cache stays populated (cache mapping is independent of source health)", func() {
		// Even when CheckSource fails (empty file_metadata → terminal
		// SourceNotReachable per check_source_test.go), status.cache
		// must still carry Backend + SourceUri. Operators read those
		// to identify which provider + URI the controller wired up
		// before debugging the reachability failure. If a refactor
		// gates the cache stamp on a successful CheckSource, this
		// spec fails — Status.Cache.Backend would be "" because
		// applyInitialModelStatus never ran.
		const root = "s3://bucket/llama-unreachable"
		setHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/file_metadata" {
				http.NotFound(w, r)
				return
			}
			writeJSON(w, fileMetadataResponse{Root: root, Files: nil})
		}))

		cbm := v1beta1testing.MakeClusterBaseModel("cc-cache-unreachable").
			StorageURI(root).
			Distribution(v1beta1.DistributionSharded).
			ModelFormat("safetensors").Obj()
		Expect(k8sClient.Create(suiteCtx, cbm)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, cbm) })

		Eventually(func(g Gomega) {
			got := &v1beta1.ClusterBaseModel{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "cc-cache-unreachable"}, got)).To(Succeed())

			// Failed source: confirms the spec is testing the
			// unreachable branch and not an unrelated success.
			source := apimeta.FindStatusCondition(got.Status.Conditions, v1beta1.ModelConditionSourceReachable)
			g.Expect(source).NotTo(BeNil())
			g.Expect(string(source.Status)).To(Equal("False"))
			g.Expect(got.Status.State).To(Equal(v1beta1.LifeCycleStateFailed))

			// Cache mapping must still be populated.
			g.Expect(got.Status.Cache).NotTo(BeNil(),
				"status.cache must be set even when the source is unreachable — operators need it to debug")
			g.Expect(got.Status.Cache.Backend).To(Equal("cluster_cache"))
			g.Expect(got.Status.Cache.SourceUri).To(Equal(root))
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.It("metadata unparsable: status.cache stays populated (cache mapping is independent of metadata parse outcome)", func() {
		// Same invariant as the unreachable case, but at the next
		// boundary: the source IS reachable, but it lacks
		// config.json / model_index.json so InspectSource lands in
		// terminal MetadataUnparseable. status.cache must still
		// reflect the chosen provider + source — debugging an
		// unparsable model needs both. Catches a regression where
		// the terminal-unparsable status writer clears Cache as part
		// of its reset.
		const root = "s3://bucket/llama-unparsable"
		setHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/file_metadata" {
				http.NotFound(w, r)
				return
			}
			// Files present (so CheckSource passes), but no
			// config.json — InspectSource will return
			// ErrSourceUnparseable.
			writeJSON(w, fileMetadataResponse{
				Root: root,
				Files: []fileMetadataEntry{
					{Path: root + "/model-00001-of-00001.safetensors", RelativePath: "model-00001-of-00001.safetensors", Size: 1024},
				},
			})
		}))

		cbm := v1beta1testing.MakeClusterBaseModel("cc-cache-unparsable").
			StorageURI(root).
			Distribution(v1beta1.DistributionSharded).
			ModelFormat("safetensors").Obj()
		Expect(k8sClient.Create(suiteCtx, cbm)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, cbm) })

		Eventually(func(g Gomega) {
			got := &v1beta1.ClusterBaseModel{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "cc-cache-unparsable"}, got)).To(Succeed())

			// Confirm we landed in the unparsable branch.
			metadata := apimeta.FindStatusCondition(got.Status.Conditions, v1beta1.ModelConditionMetadataExtracted)
			g.Expect(metadata).NotTo(BeNil())
			g.Expect(string(metadata.Status)).To(Equal("False"))
			g.Expect(metadata.Reason).To(Equal(v1beta1.ModelConditionReasonMetadataUnparseable))
			g.Expect(got.Status.State).To(Equal(v1beta1.LifeCycleStateFailed))

			// Cache mapping must still be populated.
			g.Expect(got.Status.Cache).NotTo(BeNil(),
				"status.cache must be set even when the source is unparsable — operators need it to debug")
			g.Expect(got.Status.Cache.Backend).To(Equal("cluster_cache"))
			g.Expect(got.Status.Cache.SourceUri).To(Equal(root))
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})
})
