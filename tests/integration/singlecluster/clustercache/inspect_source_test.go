package clustercache

import (
	"net/http"
	"sync/atomic"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/types"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	v1beta1testing "github.com/ome-projects/ome/pkg/utils/testing/v1beta1"

	"github.com/ome-projects/ome/tests/integration/util"
)

// minimalLlamaConfigJSON is the smallest config.json that the model
// parser will enrich into ModelType=llama / ModelArchitecture=
// LlamaForCausalLM. The deeper parser logic is covered by
// pkg/modelparser unit tests; the integration layer just needs to see
// the controller hand bytes to the parser and apply the result onto
// the BaseModel spec.
const minimalLlamaConfigJSON = `{
  "model_type": "llama",
  "architectures": ["LlamaForCausalLM"],
  "hidden_size": 4096,
  "num_hidden_layers": 32,
  "num_attention_heads": 32,
  "intermediate_size": 11008,
  "max_position_embeddings": 4096,
  "vocab_size": 32000,
  "transformers_version": "4.33.2"
}`

// singleConfigJSONManifest builds the file_metadata response a model
// with a single config.json file (no safetensors, no index) would
// produce. Used by the inspect-source specs that want to exercise the
// MetadataExtracted state machine without dragging in the
// safetensors-header byte-range protocol.
func singleConfigJSONManifest(rootURI, configPath string, size uint64) fileMetadataResponse {
	return fileMetadataResponse{
		Root: rootURI,
		Files: []fileMetadataEntry{
			{Path: configPath, RelativePath: "config.json", Size: size},
		},
	}
}

// InspectSource integration through the real clustercache provider.
// CheckSource pins SourceReachable=True via /file_metadata; this suite
// pins what happens *after* that — the metadata extraction state
// machine that lazy-fetch introduced and that has zero
// pre-existing integration coverage.
var _ = ginkgo.Describe("ClusterCache InspectSource → MetadataExtracted state machine", ginkgo.Ordered, func() {

	ginkgo.BeforeEach(func() {
		setHandler(http.NotFoundHandler())
	})

	ginkgo.It("file_metadata + remote_object(config.json) → MetadataExtracted=True, state=Ready, BaseModel spec enriched", func() {
		// Happy-path end-to-end pin. file_metadata lists config.json,
		// /remote_object returns valid JSON, the parser enriches
		// spec.{ModelType,ModelArchitecture}, and the controller flips
		// MetadataExtracted=True reason=MetadataParsed plus Ready=True
		// / state=Ready. Catches a regression where any link in the
		// chain (file_metadata → /remote_object wiring → parser
		// hand-off → spec update → condition write) silently breaks
		// while the unit-level tests still pass against fakes.
		const root = "s3://bucket/llama-config-only"
		const configPath = root + "/config.json"
		setHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Path {
			case "/file_metadata":
				writeJSON(w, singleConfigJSONManifest(root, configPath, uint64(len(minimalLlamaConfigJSON))))
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
		}))

		cbm := v1beta1testing.MakeClusterBaseModel("cc-inspect-happy").
			StorageURI(root).
			Distribution(v1beta1.DistributionSharded).
			ModelFormat("safetensors").Obj()
		Expect(k8sClient.Create(suiteCtx, cbm)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, cbm) })

		Eventually(func(g Gomega) {
			got := &v1beta1.ClusterBaseModel{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "cc-inspect-happy"}, got)).To(Succeed())

			metadata := apimeta.FindStatusCondition(got.Status.Conditions, v1beta1.ModelConditionMetadataExtracted)
			g.Expect(metadata).NotTo(BeNil(), "MetadataExtracted condition must be written for a Sharded BaseModel")
			g.Expect(string(metadata.Status)).To(Equal("True"))
			g.Expect(metadata.Reason).To(Equal(v1beta1.ModelConditionReasonMetadataParsed))

			ready := apimeta.FindStatusCondition(got.Status.Conditions, v1beta1.ModelConditionReady)
			g.Expect(ready).NotTo(BeNil())
			g.Expect(string(ready.Status)).To(Equal("True"))
			g.Expect(got.Status.State).To(Equal(v1beta1.LifeCycleStateReady))

			g.Expect(got.Spec.ModelArchitecture).NotTo(BeNil(),
				"InspectSource must enrich spec.ModelArchitecture from config.json")
			g.Expect(*got.Spec.ModelArchitecture).To(Equal("LlamaForCausalLM"))
			g.Expect(got.Spec.ModelType).NotTo(BeNil())
			g.Expect(*got.Spec.ModelType).To(Equal("llama"))
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.It("file_metadata returns no config.json/model_index.json → MetadataExtracted=False reason=MetadataUnparseable, state=Failed (terminal)", func() {
		// inspect.go:58-60 requires config.json OR model_index.json to
		// proceed. Without either, InspectSource wraps
		// ErrSourceUnparseable, and parseModelMetadata
		// (reconcile.go:125) lands the model in MetadataUnparseable
		// with NO requeue — only an operator-driven spec change can
		// re-trigger. Pinning this prevents a regression where a
		// structural fault degrades into a MetadataPending retry loop
		// (the eager-fetch behavior, which the unit test
		// TestInspectSourceRequiresConfigOrModelIndex explicitly calls
		// out as the wrong outcome).
		const root = "s3://bucket/safetensors-only-no-config"
		setHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/file_metadata" {
				http.NotFound(w, r)
				return
			}
			writeJSON(w, fileMetadataResponse{
				Root: root,
				Files: []fileMetadataEntry{
					{Path: root + "/model-00001-of-00001.safetensors", RelativePath: "model-00001-of-00001.safetensors", Size: 1024},
				},
			})
		}))

		cbm := v1beta1testing.MakeClusterBaseModel("cc-no-config-json").
			StorageURI(root).
			Distribution(v1beta1.DistributionSharded).
			ModelFormat("safetensors").Obj()
		Expect(k8sClient.Create(suiteCtx, cbm)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, cbm) })

		Eventually(func(g Gomega) {
			got := &v1beta1.ClusterBaseModel{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "cc-no-config-json"}, got)).To(Succeed())

			metadata := apimeta.FindStatusCondition(got.Status.Conditions, v1beta1.ModelConditionMetadataExtracted)
			g.Expect(metadata).NotTo(BeNil())
			g.Expect(string(metadata.Status)).To(Equal("False"))
			g.Expect(metadata.Reason).To(Equal(v1beta1.ModelConditionReasonMetadataUnparseable),
				"missing config.json/model_index.json is a TERMINAL structural fault, not a retryable pending")

			g.Expect(got.Status.State).To(Equal(v1beta1.LifeCycleStateFailed),
				"applyMetadataUnparseable lands state=Failed without a requeue")

			ready := apimeta.FindStatusCondition(got.Status.Conditions, v1beta1.ModelConditionReady)
			g.Expect(ready).NotTo(BeNil())
			g.Expect(string(ready.Status)).To(Equal("False"))
			g.Expect(ready.Reason).To(Equal(v1beta1.ModelConditionReasonMetadataUnparseable))
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.It("file_metadata 200, remote_object returns malformed JSON → MetadataPending (NOT terminal, retries)", func() {
		// Parser failure path (reconcile.go:133-137). The cache
		// provider successfully delivered bytes — that's not a source
		// fault, so the controller must NOT terminate as Unparseable.
		// Instead it sits in MetadataPending with the parser error
		// surfaced, and reconcile.go's RequeueAfter:pollInterval
		// re-tries on the chance the source is being rewritten.
		// Catches a regression where a parser blow-up gets misrouted
		// into the terminal Unparseable branch (subtle but it'd ground
		// a model permanently for a transient upload race).
		const root = "s3://bucket/llama-malformed"
		const configPath = root + "/config.json"
		const garbage = `{ this is not valid json `
		setHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Path {
			case "/file_metadata":
				writeJSON(w, singleConfigJSONManifest(root, configPath, uint64(len(garbage))))
			case "/remote_object":
				w.Header().Set("Content-Type", "application/octet-stream")
				_, _ = w.Write([]byte(garbage))
			default:
				http.NotFound(w, r)
			}
		}))

		cbm := v1beta1testing.MakeClusterBaseModel("cc-malformed-config").
			StorageURI(root).
			Distribution(v1beta1.DistributionSharded).
			ModelFormat("safetensors").Obj()
		Expect(k8sClient.Create(suiteCtx, cbm)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, cbm) })

		Eventually(func(g Gomega) {
			got := &v1beta1.ClusterBaseModel{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "cc-malformed-config"}, got)).To(Succeed())

			metadata := apimeta.FindStatusCondition(got.Status.Conditions, v1beta1.ModelConditionMetadataExtracted)
			g.Expect(metadata).NotTo(BeNil())
			g.Expect(string(metadata.Status)).To(Equal("False"))
			g.Expect(metadata.Reason).To(Equal(v1beta1.ModelConditionReasonMetadataPending),
				"parser failures are retryable, NOT terminal — reason must be MetadataPending not MetadataUnparseable")

			g.Expect(got.Status.State).NotTo(Equal(v1beta1.LifeCycleStateFailed),
				"parser failures must NOT flip state=Failed (that's reserved for structural source faults)")
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.It("remote_object 500 transiently → controller retries → MetadataExtracted=True", func() {
		// Transient HTTP failure path. The first 2 calls to
		// /remote_object 500; on call 3 we return valid JSON. The
		// controller's poll loop must retry through the failures and
		// eventually flip MetadataExtracted=True. Pins the integration:
		// a transient 5xx on metadata read does NOT degrade to either
		// MetadataUnparseable or a stuck state.
		const root = "s3://bucket/llama-transient-5xx"
		const configPath = root + "/config.json"
		var remoteCalls atomic.Int32
		setHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Path {
			case "/file_metadata":
				writeJSON(w, singleConfigJSONManifest(root, configPath, uint64(len(minimalLlamaConfigJSON))))
			case "/remote_object":
				if remoteCalls.Add(1) <= 2 {
					http.Error(w, "service temporarily unavailable", http.StatusServiceUnavailable)
					return
				}
				w.Header().Set("Content-Type", "application/octet-stream")
				_, _ = w.Write([]byte(minimalLlamaConfigJSON))
			default:
				http.NotFound(w, r)
			}
		}))

		cbm := v1beta1testing.MakeClusterBaseModel("cc-transient-remote-500").
			StorageURI(root).
			Distribution(v1beta1.DistributionSharded).
			ModelFormat("safetensors").Obj()
		Expect(k8sClient.Create(suiteCtx, cbm)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, cbm) })

		Eventually(func(g Gomega) {
			got := &v1beta1.ClusterBaseModel{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "cc-transient-remote-500"}, got)).To(Succeed())

			metadata := apimeta.FindStatusCondition(got.Status.Conditions, v1beta1.ModelConditionMetadataExtracted)
			g.Expect(metadata).NotTo(BeNil())
			g.Expect(string(metadata.Status)).To(Equal("True"),
				"transient 500 on /remote_object must NOT block MetadataExtracted — the controller must retry until success")
			g.Expect(metadata.Reason).To(Equal(v1beta1.ModelConditionReasonMetadataParsed))
			g.Expect(got.Status.State).To(Equal(v1beta1.LifeCycleStateReady))
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())

		// Sanity: prove the retry loop actually engaged (>=3 calls:
		// the two 500s plus the eventual success). Without this guard
		// the test could pass on a single lucky call that arrived
		// after the suite-level CheckSource had already retried past
		// the 500s.
		Expect(remoteCalls.Load()).To(BeNumerically(">=", 3),
			"controller must have retried /remote_object through the transient 500s (got %d calls)", remoteCalls.Load())
	})

	ginkgo.It("remote_object 500 persistently → MetadataPending (NOT terminal, never state=Failed)", func() {
		// Persistent backend failure. /remote_object always 500s. The
		// controller must NOT escalate to MetadataUnparseable / state=
		// Failed — those are reserved for structural source faults,
		// not infrastructure outages. The model stays in
		// MetadataPending with a requeue; an operator fixing the
		// backend later would allow it to recover. Catches a
		// regression where exhausted retries collapse into a
		// terminal state and require manual recovery.
		const root = "s3://bucket/llama-persistent-5xx"
		const configPath = root + "/config.json"
		var remoteCalls atomic.Int32
		setHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Path {
			case "/file_metadata":
				writeJSON(w, singleConfigJSONManifest(root, configPath, uint64(len(minimalLlamaConfigJSON))))
			case "/remote_object":
				remoteCalls.Add(1)
				http.Error(w, "permanent backend failure", http.StatusInternalServerError)
			default:
				http.NotFound(w, r)
			}
		}))

		cbm := v1beta1testing.MakeClusterBaseModel("cc-persistent-remote-500").
			StorageURI(root).
			Distribution(v1beta1.DistributionSharded).
			ModelFormat("safetensors").Obj()
		Expect(k8sClient.Create(suiteCtx, cbm)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, cbm) })

		// Wait until we've observed the condition at least once so we
		// know the controller has reconciled past CheckSource into
		// InspectSource and seen the failure.
		Eventually(func(g Gomega) {
			got := &v1beta1.ClusterBaseModel{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "cc-persistent-remote-500"}, got)).To(Succeed())
			metadata := apimeta.FindStatusCondition(got.Status.Conditions, v1beta1.ModelConditionMetadataExtracted)
			g.Expect(metadata).NotTo(BeNil())
			g.Expect(string(metadata.Status)).To(Equal("False"))
			g.Expect(metadata.Reason).To(Equal(v1beta1.ModelConditionReasonMetadataPending),
				"persistent /remote_object failures must remain in retryable MetadataPending, NEVER escalate to terminal MetadataUnparseable")
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())

		// Hold the assertion: across the suite's poll window the
		// state must NEVER flip to Failed, no matter how many retries
		// have piled up. Consistently asserts the "never terminal"
		// half of the contract.
		Consistently(func(g Gomega) {
			got := &v1beta1.ClusterBaseModel{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "cc-persistent-remote-500"}, got)).To(Succeed())
			g.Expect(got.Status.State).NotTo(Equal(v1beta1.LifeCycleStateFailed),
				"persistent infra failure must NEVER flip state=Failed (got state=%q after %d /remote_object calls)",
				got.Status.State, remoteCalls.Load())
		}).WithTimeout(util.ShortTimeout).WithPolling(util.PollInterval).Should(Succeed())

		Expect(remoteCalls.Load()).To(BeNumerically(">=", 1),
			"controller must have actually called /remote_object at least once")
	})
})
