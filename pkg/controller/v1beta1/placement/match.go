package placement

import (
	"sort"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/labels"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

// MatchingClusters returns the names (sorted) of Ready WorkloadClusters whose
// labels satisfy the selector string. An empty selector matches every Ready
// cluster. Returns an error if the selector string is malformed.
func MatchingClusters(selector string, clusters []v1beta1.WorkloadCluster) ([]string, error) {
	sel, err := labels.Parse(selector)
	if err != nil {
		return nil, err
	}
	var out []string
	for i := range clusters {
		c := &clusters[i]
		if !apimeta.IsStatusConditionTrue(c.Status.Conditions, v1beta1.WorkloadClusterReady) {
			continue
		}
		if sel.Matches(labels.Set(c.Labels)) {
			out = append(out, c.Name)
		}
	}
	sort.Strings(out)
	return out, nil
}
