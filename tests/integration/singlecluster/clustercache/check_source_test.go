package clustercache

import (
	"encoding/json"
	"net/http"
	"sync/atomic"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/types"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	v1beta1testing "github.com/ome-projects/ome/pkg/utils/testing/v1beta1"

	"github.com/ome-projects/ome/tests/integration/util"
)

// writeJSON marshals v as the HTTP response body. Marshal failures
// surface as 500 so the controller sees a clear failure mode rather
// than a truncated body.
func writeJSON(w http.ResponseWriter, v any) {
	body, err := json.Marshal(v)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(body)
}

// Local copies of the cluster_cache HTTP API response shapes.
// They're unexported in pkg/modelcache/providers/clustercache, so
// duplicating them here keeps the JSON the test produces visible at
// the call site — every key matches a field the client decodes.
type fileMetadataResponse struct {
	Root  string              `json:"root"`
	Files []fileMetadataEntry `json:"files"`
}

type fileMetadataEntry struct {
	Path         string  `json:"path"`
	RelativePath string  `json:"relative_path,omitempty"`
	Size         uint64  `json:"size"`
	MTime        *uint64 `json:"mtime,omitempty"`
}

type headResponse struct {
	Size  uint64 `json:"size"`
	MTime uint64 `json:"mtime"`
}

// CheckSource integration via the real clustercache provider against
// an httptest stub. Each spec wires its own http.HandlerFunc into
// the suite-level server. BeforeEach restores a 404-everything
// default so a missing per-spec override fails loudly instead of
// passing accidentally on a stale handler.
var _ = ginkgo.Describe("ClusterCache CheckSource HTTP behavior", ginkgo.Ordered, func() {

	ginkgo.BeforeEach(func() {
		setHandler(http.NotFoundHandler())
	})

	ginkgo.It("file_metadata returns 200 with empty files → SourceReachable=False reason=SourceNotReachable, state=Failed", func() {
		// CheckSource (clustercache/client.go:181-187) treats a
		// successful file_metadata response with len(Files)==0 as
		// SourceNotReachable. The controller's applySourceStatus
		// branch for !Reachable also flips state=Failed and writes
		// Ready=False with the same reason. Pin both at the
		// integration layer so a future refactor that unwires the
		// status update from the source check fails here.
		setHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/file_metadata" {
				http.NotFound(w, r)
				return
			}
			writeJSON(w, fileMetadataResponse{
				Root:  "s3://bucket/empty-model",
				Files: nil,
			})
		}))

		cbm := v1beta1testing.MakeClusterBaseModel("cc-empty-files").
			StorageURI("s3://bucket/empty-model").
			Distribution(v1beta1.DistributionSharded).
			ModelFormat("safetensors").Obj()
		Expect(k8sClient.Create(suiteCtx, cbm)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, cbm) })

		Eventually(func(g Gomega) {
			got := &v1beta1.ClusterBaseModel{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "cc-empty-files"}, got)).To(Succeed())
			cond := meta.FindStatusCondition(got.Status.Conditions, v1beta1.ModelConditionSourceReachable)
			g.Expect(cond).NotTo(BeNil())
			g.Expect(string(cond.Status)).To(Equal("False"))
			g.Expect(cond.Reason).To(Equal(v1beta1.ModelConditionReasonSourceNotReachable))
			g.Expect(cond.Message).To(ContainSubstring("no files"),
				"controller must surface the provider's 'no files' message in the condition")

			g.Expect(got.Status.State).To(Equal(v1beta1.LifeCycleStateFailed),
				"applySourceStatus must flip state=Failed when source is unreachable (sharded_cache_status.go:378)")

			ready := meta.FindStatusCondition(got.Status.Conditions, v1beta1.ModelConditionReady)
			g.Expect(ready).NotTo(BeNil())
			g.Expect(string(ready.Status)).To(Equal("False"))
			g.Expect(ready.Reason).To(Equal(v1beta1.ModelConditionReasonSourceNotReachable))
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.It("file_metadata returns 503 transiently → controller retries, eventually flips SourceReachable=True", func() {
		// 503 is in the transient-error class (isTransientStatusCode
		// in clustercache/client.go). CheckSource returns (nil, err)
		// and the reconcile bails with the error — controller-runtime
		// re-queues with backoff. This pins the integration: a
		// transient HTTP failure must NOT degrade to
		// SourceReachable=False (it'd take a non-transient 4xx for
		// that). Catches a regression where the retry bucket grows
		// to include transient codes (or vice versa).
		var requestCount atomic.Int32
		setHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/file_metadata" {
				http.NotFound(w, r)
				return
			}
			n := requestCount.Add(1)
			if n <= 2 {
				http.Error(w, "service temporarily unavailable", http.StatusServiceUnavailable)
				return
			}
			writeJSON(w, fileMetadataResponse{
				Root: "s3://bucket/transient-model",
				Files: []fileMetadataEntry{
					{Path: "s3://bucket/transient-model/config.json", RelativePath: "config.json", Size: 100},
				},
			})
		}))

		cbm := v1beta1testing.MakeClusterBaseModel("cc-transient-503").
			StorageURI("s3://bucket/transient-model").
			Distribution(v1beta1.DistributionSharded).
			ModelFormat("safetensors").Obj()
		Expect(k8sClient.Create(suiteCtx, cbm)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, cbm) })

		Eventually(func(g Gomega) {
			got := &v1beta1.ClusterBaseModel{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "cc-transient-503"}, got)).To(Succeed())
			cond := meta.FindStatusCondition(got.Status.Conditions, v1beta1.ModelConditionSourceReachable)
			g.Expect(cond).NotTo(BeNil())
			g.Expect(string(cond.Status)).To(Equal("True"),
				"transient 503 must NOT degrade to SourceReachable=False — controller must retry until success")
			g.Expect(cond.Reason).To(Equal(v1beta1.ModelConditionReasonSourceReachable))

			// State must NOT be Failed — that'd indicate the
			// controller treated the transient error as terminal.
			g.Expect(got.Status.State).NotTo(Equal(v1beta1.LifeCycleStateFailed),
				"transient HTTP failure must not flip state=Failed")
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())

		// Sanity: the handler observed at least 3 requests (2 503s
		// + the success). Proves the retry loop actually engaged
		// instead of accidentally waiting on a single slow request.
		Expect(requestCount.Load()).To(BeNumerically(">=", 3),
			"controller must have retried CheckSource through the transient 503s (got %d requests)", requestCount.Load())
	})

	ginkgo.It("file_metadata 404 → /head 200 fallback → SourceReachable=True", func() {
		// checkSourceWithHead (clustercache/client.go:208) is the
		// fallback for backends that don't implement /file_metadata.
		// Production clusters can run a mix of versions, so the
		// fallback is hot. This pins the controller observes
		// Reachable=True via the fallback the same as via the
		// primary path — different code path, same observable.
		var headCalled atomic.Int32
		setHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Path {
			case "/file_metadata":
				http.Error(w, "endpoint not implemented", http.StatusNotFound)
			case "/head":
				headCalled.Add(1)
				writeJSON(w, headResponse{Size: 100, MTime: 1710000000})
			default:
				http.NotFound(w, r)
			}
		}))

		cbm := v1beta1testing.MakeClusterBaseModel("cc-head-fallback").
			StorageURI("s3://bucket/head-fallback-model").
			Distribution(v1beta1.DistributionSharded).
			ModelFormat("safetensors").Obj()
		Expect(k8sClient.Create(suiteCtx, cbm)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, cbm) })

		Eventually(func(g Gomega) {
			got := &v1beta1.ClusterBaseModel{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "cc-head-fallback"}, got)).To(Succeed())
			cond := meta.FindStatusCondition(got.Status.Conditions, v1beta1.ModelConditionSourceReachable)
			g.Expect(cond).NotTo(BeNil())
			g.Expect(string(cond.Status)).To(Equal("True"),
				"checkSourceWithHead fallback must drive SourceReachable=True the same as the primary path")
			g.Expect(cond.Reason).To(Equal(v1beta1.ModelConditionReasonSourceReachable))
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())

		Expect(headCalled.Load()).To(BeNumerically(">=", 1),
			"the /head fallback handler must have been invoked at least once (got %d)", headCalled.Load())
	})
})
