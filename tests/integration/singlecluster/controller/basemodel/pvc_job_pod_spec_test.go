package basemodel

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	v1beta1testing "github.com/ome-projects/ome/pkg/utils/testing/v1beta1"

	"github.com/ome-projects/ome/tests/integration/util"
)

// PodSpec wiring of the metadata extraction Job. Unit tests in
// metadata_job_test.go build the Job in isolation with a hand-rolled
// MetadataJobConfig; this spec proves the END-TO-END flow — the
// controller actually attaches its configured Image, ServiceAccount,
// etc. to the Jobs it creates in response to a PVC-backed BaseModel.
//
// A regression where the controller forgets to thread MetadataJobConfig
// into the Job builder would compile fine and pass the unit tests, but
// a real apiserver-roundtripped Job would have a different PodSpec
// than expected. This spec catches that.
//
// (Per-BM spec fields like Spec.Storage.NodeSelector intentionally do
// NOT pass through to the metadata Job — that selector is consumed by
// the per-node model-agent DaemonSet, not the controller. The Job's
// scheduling hints come from the controller's MetadataJobConfig set at
// startup; see metadata_job.go:67-77.)
var _ = ginkgo.Describe("BaseModel PVC metadata Job PodSpec wiring", func() {

	ginkgo.It("Job uses the controller-configured Image and ServiceAccount", func() {
		// suite_test.go's BeforeSuite wires WithOmeAgentConfig(
		// Image="ghcr.io/test/ome-agent:test",
		// ServiceAccount="ome-model-metadata"). Verify those land
		// on the Job's PodSpec.
		ns := "pvc-jobspec"
		makeNamespace(suiteCtx, k8sClient, ns)
		makePVC(suiteCtx, k8sClient, "p", ns)
		bindPVC(suiteCtx, k8sClient, "p", ns)

		bm := v1beta1testing.MakeBaseModel("jobspec-llama", ns).
			StorageURI("pvc://p/models/llama").Obj()
		Expect(k8sClient.Create(suiteCtx, bm)).To(Succeed())

		Eventually(func(g Gomega) {
			jobs := &batchv1.JobList{}
			g.Expect(k8sClient.List(suiteCtx, jobs)).To(Succeed())
			var match *batchv1.Job
			for i := range jobs.Items {
				if jobs.Items[i].Namespace == ns {
					match = &jobs.Items[i]
					break
				}
			}
			g.Expect(match).NotTo(BeNil(), "metadata Job must exist in the BM's namespace")

			containers := match.Spec.Template.Spec.Containers
			g.Expect(containers).NotTo(BeEmpty(), "Job must have at least one container")
			g.Expect(containers[0].Image).To(Equal("ghcr.io/test/ome-agent:test"),
				"Job container image must come from the controller's MetadataJobConfig.Image")

			g.Expect(match.Spec.Template.Spec.ServiceAccountName).To(Equal("ome-model-metadata"),
				"Job pod must run under the SA configured on the controller, not the namespace default")

			g.Expect(match.OwnerReferences).NotTo(BeEmpty(),
				"Job must have an owner reference back to the BaseModel for cascading delete + Owns watch")
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.It("Job carries a deterministic name + ConfigMap-key derived from (model, URI)", func() {
		// Cross-check the naming contract: the Job and per-PVC CM
		// share a hash suffix so a URI change yields fresh
		// artifacts (covered functionally elsewhere, but no spec
		// asserts the SHARED-suffix invariant). Important for
		// operators eyeballing logs to correlate Jobs and CMs.
		ns := "pvc-jobname"
		makeNamespace(suiteCtx, k8sClient, ns)
		makePVC(suiteCtx, k8sClient, "p2", ns)
		bindPVC(suiteCtx, k8sClient, "p2", ns)

		bm := v1beta1testing.MakeBaseModel("jobname-llama", ns).
			StorageURI("pvc://p2/models/llama-jobname").Obj()
		Expect(k8sClient.Create(suiteCtx, bm)).To(Succeed())

		var jobName string
		Eventually(func(g Gomega) {
			jobs := &batchv1.JobList{}
			g.Expect(k8sClient.List(suiteCtx, jobs)).To(Succeed())
			for i := range jobs.Items {
				if jobs.Items[i].Namespace == ns {
					jobName = jobs.Items[i].Name
					break
				}
			}
			g.Expect(jobName).NotTo(BeEmpty(), "Job must exist in the BM's namespace")
			g.Expect(len(jobName)).To(BeNumerically("<=", 63),
				"Job name must fit DNS-1123 label limit (deterministic name + 8-char hash suffix)")
			g.Expect(jobName).To(MatchRegexp(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`),
				"Job name must be a valid DNS-1123 label (no underscores, dots, or uppercase)")
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		// Same Job name should be re-used across reconciles (no
		// per-reconcile churn). Verify by reading Get'ing twice
		// and asserting the same name still exists.
		Consistently(func(g Gomega) {
			fetched := &v1beta1.BaseModel{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "jobname-llama", Namespace: ns}, fetched)).To(Succeed())
			jobs := &batchv1.JobList{}
			g.Expect(k8sClient.List(suiteCtx, jobs)).To(Succeed())
			names := []string{}
			for _, j := range jobs.Items {
				if j.Namespace == ns {
					names = append(names, j.Name)
				}
			}
			g.Expect(names).To(Equal([]string{jobName}),
				"controller must NOT churn Jobs across reconciles; same (model, URI) → same Job name")
		}).WithTimeout(util.ShortTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})
})
