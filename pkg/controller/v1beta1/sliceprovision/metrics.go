package sliceprovision

import (
	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	"github.com/ome-projects/ome/pkg/tpuslice/gke"
)

// sliceLabels are bounded by the configured slice types and topologies.
var sliceLabels = []string{"slice_type", "topology"}

var (
	slicesCreated = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "ome_tpu_slice_created_total",
		Help: "TPU slices the controller created for its workloads' pods, by slice type and topology.",
	}, sliceLabels)

	slicesReleased = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "ome_tpu_slice_released_total",
		Help: "TPU slices the controller provisioned that the API server has removed, whoever deleted them, by slice type and topology. A slice the provider holds behind a finalizer counts once the provider lets it go. Counted from the leader's watch, so a slice removed while no leader watches is not counted.",
	}, sliceLabels)
)

func init() {
	ctrlmetrics.Registry.MustRegister(slicesCreated, slicesReleased)
}

// ObserveDeleted counts obj as released when it is a slice provisioned for an
// owner. The caller calls it once for each slice the API server removes.
func ObserveDeleted(obj client.Object) {
	u, ok := obj.(*unstructured.Unstructured)
	if !ok || u.GetLabels()[LabelOwnerUID] == "" {
		return
	}
	s, err := gke.Parse(u)
	if err != nil {
		return
	}
	slicesReleased.WithLabelValues(s.Type, s.Topology).Inc()
}
