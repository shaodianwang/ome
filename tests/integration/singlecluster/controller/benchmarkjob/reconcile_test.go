package benchmarkjob

import (
	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"

	"github.com/ome-projects/ome/tests/integration/util"
)

// finalizerName mirrors the unexported constant in
// pkg/controller/v1beta1/benchmark/controller.go. We can't import
// the unexported value, so duplicate it here with a comment so a
// rename in the controller surfaces as a test failure.
const finalizerName = "benchmarkjob.finalizers"

// makeBenchmarkJob builds a minimal-but-valid BenchmarkJobSpec
// pointing at a direct URL endpoint (the simplest path; the
// InferenceService-endpoint path requires a Ready ISVC that's
// hard to fabricate without real pods). All required CRD fields
// are populated.
func makeBenchmarkJob(name, namespace string) *v1beta1.BenchmarkJob {
	return &v1beta1.BenchmarkJob{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec: v1beta1.BenchmarkJobSpec{
			Endpoint: v1beta1.EndpointSpec{
				Endpoint: &v1beta1.Endpoint{
					URL:       "http://test-endpoint.example.com",
					APIFormat: "openai",
					ModelName: "test-model",
				},
			},
			Task:                    "text-to-text",
			MaxTimePerIteration:     ptr.To(5),
			MaxRequestsPerIteration: ptr.To(100),
			OutputLocation: &v1beta1.StorageSpec{
				StorageUri: ptr.To("oci://n/test/b/results/o/benchmark"),
			},
		},
	}
}

// BenchmarkJobReconciler integration. The reconciler creates a
// batch/v1 Job per BenchmarkJob CR and adds a finalizer to gate
// deletion. These specs cover the finalizer + Job-creation
// contract end-to-end with the real apiserver.
var _ = ginkgo.Describe("BenchmarkJob reconciler", func() {
	var nsName string

	ginkgo.BeforeEach(func() {
		ns, err := util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "bench-")
		Expect(err).NotTo(HaveOccurred())
		nsName = ns.Name
	})

	ginkgo.AfterEach(func() {
		Expect(util.DeleteNamespace(suiteCtx, k8sClient, nsName)).To(Succeed())
	})

	ginkgo.It("BenchmarkJob with URL endpoint → finalizer added + child Job created", func() {
		bj := makeBenchmarkJob("bench-url", nsName)
		Expect(k8sClient.Create(suiteCtx, bj)).To(Succeed())

		ginkgo.By("controller adds the finalizer")
		Eventually(func(g Gomega) {
			got := &v1beta1.BenchmarkJob{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "bench-url", Namespace: nsName}, got)).To(Succeed())
			g.Expect(got.Finalizers).To(ContainElement(finalizerName),
				"reconciler must add the benchmark finalizer on first reconcile")
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		ginkgo.By("child batch/v1 Job appears in the BenchmarkJob's namespace")
		Eventually(func(g Gomega) {
			jobs := &batchv1.JobList{}
			g.Expect(k8sClient.List(suiteCtx, jobs, client.InNamespace(nsName))).To(Succeed())
			g.Expect(jobs.Items).NotTo(BeEmpty(),
				"reconcileJob must produce at least one batch/v1 Job per BenchmarkJob")
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.It("BenchmarkJob with InferenceService endpoint pointing at a missing ISVC → no Job (controller errors out)", func() {
		// reconcile.go: if Endpoint.InferenceService is set and the
		// ISVC doesn't resolve, the controller returns an error
		// before reaching reconcileJob — no batch Job should
		// appear. Catches a regression where a missing ISVC
		// silently produces a benchmark Job pointing nowhere.
		bj := makeBenchmarkJob("bench-missing-isvc", nsName)
		bj.Spec.Endpoint = v1beta1.EndpointSpec{
			InferenceService: &v1beta1.InferenceServiceReference{
				Name:      "definitely-not-here",
				Namespace: nsName,
			},
		}
		Expect(k8sClient.Create(suiteCtx, bj)).To(Succeed())

		// Use Consistently to prove the negative — no Job should
		// appear across multiple reconcile attempts.
		Consistently(func(g Gomega) {
			jobs := &batchv1.JobList{}
			g.Expect(k8sClient.List(suiteCtx, jobs, client.InNamespace(nsName))).To(Succeed())
			g.Expect(jobs.Items).To(BeEmpty(),
				"controller must NOT create a benchmark Job for a missing ISVC reference")
		}).WithTimeout(util.ShortTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.It("BenchmarkJob with PodOverride.Image → produced Job's container image is overridden", func() {
		// PodOverride.Image takes precedence over the default
		// benchmarkjob-config image (createPodSpec reads cfg first
		// then applies override). This catches a regression where
		// the override field is silently dropped.
		bj := makeBenchmarkJob("bench-image-override", nsName)
		bj.Spec.PodOverride = &v1beta1.PodOverride{
			Image: "ghcr.io/test/custom-bench:override",
		}
		Expect(k8sClient.Create(suiteCtx, bj)).To(Succeed())

		Eventually(func(g Gomega) {
			jobs := &batchv1.JobList{}
			g.Expect(k8sClient.List(suiteCtx, jobs, client.InNamespace(nsName))).To(Succeed())
			g.Expect(jobs.Items).To(HaveLen(1))
			containers := jobs.Items[0].Spec.Template.Spec.Containers
			g.Expect(containers).NotTo(BeEmpty())
			g.Expect(containers[0].Image).To(Equal("ghcr.io/test/custom-bench:override"),
				"PodOverride.Image must override the benchmarkjob-config default image")
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.It("BenchmarkJob deletion: finalizer is cleared and the CR fully GC's", func() {
		bj := makeBenchmarkJob("bench-delete", nsName)
		Expect(k8sClient.Create(suiteCtx, bj)).To(Succeed())

		// Wait for finalizer so the Delete below can prove the
		// handler clears it (otherwise Delete would just succeed
		// because there's nothing to clean up).
		Eventually(func(g Gomega) {
			got := &v1beta1.BenchmarkJob{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "bench-delete", Namespace: nsName}, got)).To(Succeed())
			g.Expect(got.Finalizers).To(ContainElement(finalizerName))
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())

		Expect(k8sClient.Delete(suiteCtx, bj)).To(Succeed())

		Eventually(func(g Gomega) {
			err := k8sClient.Get(suiteCtx, types.NamespacedName{Name: "bench-delete", Namespace: nsName}, &v1beta1.BenchmarkJob{})
			g.Expect(apierrors.IsNotFound(err)).To(BeTrue(),
				"BenchmarkJob CR must be fully GC'd after the finalizer is cleared")
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})

	ginkgo.It("BenchmarkJob with custom Env on PodOverride → env vars propagate to the Job container", func() {
		bj := makeBenchmarkJob("bench-env", nsName)
		bj.Spec.PodOverride = &v1beta1.PodOverride{
			Env: []corev1.EnvVar{{Name: "BENCH_FOO", Value: "bar"}},
		}
		Expect(k8sClient.Create(suiteCtx, bj)).To(Succeed())

		Eventually(func(g Gomega) {
			jobs := &batchv1.JobList{}
			g.Expect(k8sClient.List(suiteCtx, jobs, client.InNamespace(nsName))).To(Succeed())
			g.Expect(jobs.Items).To(HaveLen(1))
			containers := jobs.Items[0].Spec.Template.Spec.Containers
			g.Expect(containers).NotTo(BeEmpty())
			g.Expect(containers[0].Env).To(ContainElement(corev1.EnvVar{Name: "BENCH_FOO", Value: "bar"}),
				"PodOverride.Env entries must appear on the Job's container")
		}).WithTimeout(util.MediumTimeout).WithPolling(util.PollInterval).Should(Succeed())
	})
})
