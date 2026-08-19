package canarystatus

import (
	"time"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

// newISVC returns a minimal InferenceService the CRD accepts without webhooks
// or referenced resources — this suite exercises only the status schema.
func newISVC(name string) *v1beta1.InferenceService {
	return &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNamespace},
	}
}

// fullCanaryStatus is a CanaryStatus exercising every analysis-facing field the
// canary executor writes, with second-granularity times (the wire format).
func fullCanaryStatus(now time.Time) *v1beta1.CanaryStatus {
	entered := metav1.NewTime(now.Add(-5 * time.Minute))
	evaluated := metav1.NewTime(now.Add(-30 * time.Second))
	conclusive := metav1.NewTime(now.Add(-90 * time.Second))
	return &v1beta1.CanaryStatus{
		CanaryRevisionHash:           "canary-a-rev2",
		StableRevisionHash:           "canary-a-rev1",
		CurrentStep:                  1,
		StepEnteredTime:              &entered,
		ObservedTrafficWeight:        25,
		AnalysisFailedChecks:         2,
		LastEvaluationTime:           &evaluated,
		LastConclusiveEvaluationTime: &conclusive,
		MetricResults: []v1beta1.AnalysisMetricResult{
			{
				Name:      "error-rate",
				Value:     "0.012",
				Threshold: "0.05",
				Operator:  v1beta1.ComparisonLTE,
				Passed:    true,
				Time:      &evaluated,
			},
			{
				Name:      "p99-latency",
				Threshold: "250",
				Operator:  v1beta1.ComparisonLT,
				Passed:    false,
				Message:   "no data",
				Time:      &evaluated,
			},
		},
	}
}

var _ = ginkgo.Describe("Canary analysis status surface", func() {

	ginkgo.It("round-trips every analysis field through the status subresource", func() {
		isvc := newISVC("canary-a-roundtrip")
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, isvc) })

		now := time.Now().Truncate(time.Second)
		want := fullCanaryStatus(now)
		isvc.Status.Canary = want.DeepCopy()
		Expect(k8sClient.Status().Update(suiteCtx, isvc)).To(Succeed())

		got := &v1beta1.InferenceService{}
		Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Namespace: testNamespace, Name: isvc.Name}, got)).To(Succeed())
		cs := got.Status.Canary
		Expect(cs).NotTo(BeNil(), "status.canary must persist")

		Expect(cs.CanaryRevisionHash).To(Equal(want.CanaryRevisionHash))
		Expect(cs.StableRevisionHash).To(Equal(want.StableRevisionHash))
		Expect(cs.CurrentStep).To(Equal(want.CurrentStep))
		Expect(cs.ObservedTrafficWeight).To(Equal(want.ObservedTrafficWeight))
		Expect(cs.AnalysisFailedChecks).To(Equal(want.AnalysisFailedChecks),
			"the failure budget must survive the apiserver — the rollback decision is computed from the persisted count")
		Expect(cs.StepEnteredTime.Time).To(BeTemporally("==", want.StepEnteredTime.Time))
		Expect(cs.LastEvaluationTime.Time).To(BeTemporally("==", want.LastEvaluationTime.Time),
			"the interval throttle reads this back after a restart; losing it would double-sample")
		Expect(cs.LastConclusiveEvaluationTime.Time).To(BeTemporally("==", want.LastConclusiveEvaluationTime.Time),
			"the stall timeout is measured from this persisted timestamp")

		Expect(cs.MetricResults).To(HaveLen(2))
		Expect(cs.MetricResults[0].Name).To(Equal("error-rate"))
		Expect(cs.MetricResults[0].Value).To(Equal("0.012"))
		Expect(cs.MetricResults[0].Threshold).To(Equal("0.05"))
		Expect(cs.MetricResults[0].Operator).To(Equal(v1beta1.ComparisonLTE))
		Expect(cs.MetricResults[0].Passed).To(BeTrue())
		Expect(cs.MetricResults[0].Time.Time).To(BeTemporally("==", want.MetricResults[0].Time.Time))
		Expect(cs.MetricResults[1].Name).To(Equal("p99-latency"))
		Expect(cs.MetricResults[1].Value).To(BeEmpty(), "an unevaluated metric surfaces no value")
		Expect(cs.MetricResults[1].Passed).To(BeFalse())
		Expect(cs.MetricResults[1].Message).To(Equal("no data"))
	})

	ginkgo.It("rejects duplicate metric result names via the map list key", func() {
		isvc := newISVC("canary-a-duplicate")
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, isvc) })

		now := metav1.NewTime(time.Now().Truncate(time.Second))
		isvc.Status.Canary = &v1beta1.CanaryStatus{
			CanaryRevisionHash: "canary-a-rev2",
			MetricResults: []v1beta1.AnalysisMetricResult{
				{Name: "error-rate", Passed: true, Time: &now},
				{Name: "error-rate", Passed: false, Time: &now},
			},
		}
		err := k8sClient.Status().Update(suiteCtx, isvc)
		Expect(err).To(HaveOccurred(),
			"metricResults is a map list keyed by name; the apiserver must reject duplicates")
		Expect(apierrors.IsInvalid(err)).To(BeTrue(), "want an Invalid rejection, got: %v", err)
	})

	ginkgo.It("ignores status changes on a plain update (status is a subresource)", func() {
		isvc := newISVC("canary-a-subresource")
		Expect(k8sClient.Create(suiteCtx, isvc)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, isvc) })

		now := time.Now().Truncate(time.Second)
		isvc.Status.Canary = fullCanaryStatus(now)
		Expect(k8sClient.Status().Update(suiteCtx, isvc)).To(Succeed())

		// A plain (non-subresource) update that mutates status must leave the
		// persisted status untouched — the canary state machine's durability
		// depends on spec writers being unable to clobber it.
		fresh := &v1beta1.InferenceService{}
		Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Namespace: testNamespace, Name: isvc.Name}, fresh)).To(Succeed())
		fresh.Status.Canary = nil
		if fresh.Labels == nil {
			fresh.Labels = map[string]string{}
		}
		fresh.Labels["canary-a-touch"] = "true"
		Expect(k8sClient.Update(suiteCtx, fresh)).To(Succeed())

		got := &v1beta1.InferenceService{}
		Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Namespace: testNamespace, Name: isvc.Name}, got)).To(Succeed())
		Expect(got.Labels).To(HaveKeyWithValue("canary-a-touch", "true"))
		Expect(got.Status.Canary).NotTo(BeNil(),
			"a plain update must not clear status.canary; only the status subresource may write it")
		Expect(got.Status.Canary.AnalysisFailedChecks).To(Equal(int32(2)))
	})
})
