package placement

import (
	"testing"

	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

func wc(name string, ready bool, labels map[string]string) v1beta1.WorkloadCluster {
	st := metav1.ConditionFalse
	if ready {
		st = metav1.ConditionTrue
	}
	return v1beta1.WorkloadCluster{
		ObjectMeta: metav1.ObjectMeta{Name: name, Labels: labels},
		Status: v1beta1.WorkloadClusterStatus{Conditions: []metav1.Condition{
			{Type: v1beta1.WorkloadClusterReady, Status: st, Reason: "x"},
		}},
	}
}

func TestMatchingClusters(t *testing.T) {
	clusters := []v1beta1.WorkloadCluster{
		wc("cw", true, map[string]string{"gpu": "gb300", "provider": "cw"}),
		wc("gcp", true, map[string]string{"gpu": "gb300", "provider": "gcp"}),
		wc("cw-h100", true, map[string]string{"gpu": "h100", "provider": "cw"}),
		wc("cw-down", false, map[string]string{"gpu": "gb300", "provider": "cw"}),
	}
	// selector "gpu=gb300" -> the two Ready gb300 clusters, sorted by name.
	got, err := MatchingClusters("gpu=gb300", clusters)
	assert.NoError(t, err)
	assert.Equal(t, []string{"cw", "gcp"}, got)

	// multi-term selector.
	got, err = MatchingClusters("gpu=gb300,provider=cw", clusters)
	assert.NoError(t, err)
	assert.Equal(t, []string{"cw"}, got)

	// empty selector -> all Ready clusters.
	got, err = MatchingClusters("", clusters)
	assert.NoError(t, err)
	assert.Equal(t, []string{"cw", "cw-h100", "gcp"}, got)

	// invalid selector -> error.
	_, err = MatchingClusters("!!!", clusters)
	assert.Error(t, err)
}
