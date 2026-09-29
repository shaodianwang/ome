package inferencereplicatpuslice

import (
	"time"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	dto "github.com/prometheus/client_model/go"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/tpuslice/gke"
)

// metricsTopology is the on-demand topology only the metrics spec
// provisions, so the series it reads count its slices alone.
const metricsTopology = "2x2x2"

// The controller's TPU slice metrics.
const (
	createdTotal     = "ome_tpu_slice_created_total"
	releasedTotal    = "ome_tpu_slice_released_total"
	provisionSeconds = "ome_tpu_slice_provision_duration_seconds"
	slicesByState    = "ome_tpu_slices"
)

// sliceSeries gathers the series of the metric name for the on-demand slice
// type and metricsTopology from the registry the controller's metrics are
// registered in.
func sliceSeries(name string) []*dto.Metric {
	families, err := ctrlmetrics.Registry.Gather()
	Expect(err).NotTo(HaveOccurred())
	var series []*dto.Metric
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
		for _, m := range f.GetMetric() {
			if labelValue(m, "slice_type") == sliceConfig.Accelerators[onDemandAccelerator].SliceType &&
				labelValue(m, "topology") == metricsTopology {
				series = append(series, m)
			}
		}
	}
	return series
}

func labelValue(m *dto.Metric, name string) string {
	for _, l := range m.GetLabel() {
		if l.GetName() == name {
			return l.GetValue()
		}
	}
	return ""
}

// counted reads the counter name.
func counted(name string) float64 {
	var n float64
	for _, m := range sliceSeries(name) {
		n += m.GetCounter().GetValue()
	}
	return n
}

// provisionTimes reads how many provisioning times the histogram holds, and
// their sum in seconds.
func provisionTimes() (count uint64, seconds float64) {
	for _, m := range sliceSeries(provisionSeconds) {
		count += m.GetHistogram().GetSampleCount()
		seconds += m.GetHistogram().GetSampleSum()
	}
	return count, seconds
}

func timed() uint64 {
	count, _ := provisionTimes()
	return count
}

// sliceStates reads how many slices the gauge reports in each state. The
// gauge has no series for a state no slice is in.
func sliceStates() map[string]float64 {
	states := map[string]float64{}
	for _, m := range sliceSeries(slicesByState) {
		states[labelValue(m, "state")] = m.GetGauge().GetValue()
	}
	return states
}

// awaitStates waits until the gauge reports exactly the states in want.
func awaitStates(want map[string]float64) {
	EventuallyWithOffset(1, sliceStates).WithTimeout(waitTimeout).WithPolling(pollInterval).Should(Equal(want))
}

var _ = ginkgo.Describe("InferenceReplica TPU slice metrics", func() {
	ginkgo.It("counts, times and reports by state the slices it provisions", func() {
		ns := createNamespace("tpuslice-metrics")
		released := func() float64 { return counted(releasedTotal) }

		ginkgo.By("exporting the counters and the histogram before the topology's first slice")
		for _, name := range []string{createdTotal, releasedTotal, provisionSeconds} {
			Expect(sliceSeries(name)).To(HaveLen(1), "%s must have a series before the first slice", name)
		}
		Expect(counted(createdTotal)).To(BeZero())
		Expect(released()).To(BeZero())
		Expect(timed()).To(BeZero())
		Expect(sliceStates()).To(BeEmpty())

		ginkgo.By("counting the Instance's slice as created and reporting it as pending")
		irCreated := time.Now()
		ir := sliceIR("granite", ns, onDemandAccelerator, metricsTopology, 4, 1)
		Expect(k8sClient.Create(suiteCtx, ir)).To(Succeed())
		name := awaitOwnedSlices(ir, 1)[0]
		sliceSeen := time.Now()
		Eventually(func() float64 { return counted(createdTotal) }).
			WithTimeout(waitTimeout).WithPolling(pollInterval).Should(Equal(1.0))
		awaitStates(map[string]float64{"pending": 1})

		ginkgo.By("timing nothing while the slice reports a state that is not ready")
		reportSliceState(name, stateCreating, "binding partitions")
		awaitWithheld(ir, name, stateCreating)
		Consistently(timed).WithTimeout(holdWindow).WithPolling(pollInterval).Should(BeZero(),
			"a slice must not be timed before it is ready")

		ginkgo.By("timing the slice from its create to its ready state and reporting it as ready")
		readyAt := time.Now()
		reportSliceState(name, stateActive, "")
		awaitPods(ns, 2)
		Eventually(timed).WithTimeout(waitTimeout).WithPolling(pollInterval).Should(Equal(uint64(1)))
		_, seconds := provisionTimes()
		Expect(seconds).To(BeNumerically(">", readyAt.Sub(sliceSeen).Seconds()),
			"the time must start at the create, before the spec saw the slice")
		Expect(seconds).To(BeNumerically("<", time.Since(irCreated).Seconds()),
			"the time must fall between the InferenceReplica's creation and now")
		awaitStates(map[string]float64{"ready": 1})

		ginkgo.By("reporting the slice as pending after a failed probe, and not timing it again once it recovers")
		reportSliceState(name, "SliceHealthProbeFailed", "probe failed")
		awaitStates(map[string]float64{"pending": 1})
		reportSliceState(name, stateActive, "recovered")
		awaitStates(map[string]float64{"ready": 1})
		Consistently(timed).WithTimeout(holdWindow).WithPolling(pollInterval).Should(Equal(uint64(1)),
			"a slice's provisioning must be timed once")

		ginkgo.By("reporting a slice whose InferenceReplica does not exist as orphaned")
		gone := sliceIR("gone", ns, onDemandAccelerator, metricsTopology, 4, 1)
		gone.UID = "gone-engine-uid"
		orphan := provisionForRowlessInstance(gone, 0)
		awaitStates(map[string]float64{"ready": 1, "orphaned": 1})

		ginkgo.By("reporting the orphan as terminating while the provider holds it, and counting it as released once removed")
		setFinalizer(gke.NewObject(), client.ObjectKey{Name: orphan}, sliceFinalizer, true)
		Expect(k8sClient.Delete(suiteCtx, getSlice(Default, orphan))).To(Succeed())
		awaitStates(map[string]float64{"ready": 1, "terminating": 1})
		Consistently(released).WithTimeout(holdWindow).WithPolling(pollInterval).Should(BeZero(),
			"a slice the provider holds must not count as released")
		setFinalizer(gke.NewObject(), client.ObjectKey{Name: orphan}, sliceFinalizer, false)
		Eventually(released).WithTimeout(waitTimeout).WithPolling(pollInterval).Should(Equal(1.0))
		awaitStates(map[string]float64{"ready": 1})

		ginkgo.By("counting the Instance's slice as released once teardown releases it and the provider lets it go")
		setFinalizer(gke.NewObject(), client.ObjectKey{Name: name}, sliceFinalizer, true)
		Expect(k8sClient.Delete(suiteCtx, ir)).To(Succeed())
		awaitStates(map[string]float64{"terminating": 1})
		Expect(released()).To(Equal(1.0))
		setFinalizer(gke.NewObject(), client.ObjectKey{Name: name}, sliceFinalizer, false)
		Eventually(released).WithTimeout(waitTimeout).WithPolling(pollInterval).Should(Equal(2.0))
		Consistently(released).WithTimeout(holdWindow).WithPolling(pollInterval).Should(Equal(2.0),
			"each slice must count as released once")
		awaitStates(map[string]float64{})
		Eventually(func(g Gomega) {
			err := k8sClient.Get(suiteCtx, client.ObjectKeyFromObject(ir), &v1beta1.InferenceReplica{})
			g.Expect(apierrors.IsNotFound(err)).To(BeTrue(), "the InferenceReplica must be removed, got %v", err)
		}).WithTimeout(waitTimeout).WithPolling(pollInterval).Should(Succeed())

		// The spec provisioned the orphan as the controller does, so its
		// create counts too.
		Expect(counted(createdTotal)).To(Equal(2.0))
		Expect(timed()).To(Equal(uint64(1)))
	})
})
