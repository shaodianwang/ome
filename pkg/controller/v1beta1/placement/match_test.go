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

// isvcReq builds a source ISVC whose accelerator requirement / cluster selector
// annotations drive candidate matching.
func isvcReq(accel, selector string) *v1beta1.InferenceService {
	ann := map[string]string{}
	if accel != "" {
		ann[AcceleratorRequirementsAnnotation] = accel
	}
	if selector != "" {
		ann[ClusterSelectorAnnotation] = selector
	}
	return &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "svc", Namespace: "prod", Annotations: ann}}
}

func TestMatchCandidates(t *testing.T) {
	clusters := []v1beta1.WorkloadCluster{
		wc("cluster-a", true, map[string]string{"gpu": "gb300", "provider": "prov-a"}),
		wc("cluster-b", true, map[string]string{"gpu": "gb300", "provider": "prov-b"}),
		wc("cluster-a-h100", true, map[string]string{"gpu": "h100", "provider": "prov-a"}),
		wc("cluster-a-down", false, map[string]string{"gpu": "gb300", "provider": "prov-a"}),
	}

	// accelerator requirement "gpu=gb300" -> the two Ready gb300 clusters, sorted.
	got, reason, err := MatchCandidates(isvcReq("gpu=gb300", ""), clusters)
	assert.NoError(t, err)
	assert.Equal(t, []string{"cluster-a", "cluster-b"}, got)
	assert.Equal(t, MatchReason(""), reason)

	// accelerator requirement AND-ed with cluster-selector.
	got, _, err = MatchCandidates(isvcReq("gpu=gb300", "provider=prov-a"), clusters)
	assert.NoError(t, err)
	assert.Equal(t, []string{"cluster-a"}, got)

	// cluster-selector alone is a sufficient requirement (counts as a requirement).
	got, _, err = MatchCandidates(isvcReq("", "gpu=gb300,provider=prov-b"), clusters)
	assert.NoError(t, err)
	assert.Equal(t, []string{"cluster-b"}, got)

	// HIGH-bug guard: an ISVC with NO requirement must NOT fan out to every
	// Ready cluster — it matches NOTHING with reason NoRequirements.
	got, reason, err = MatchCandidates(isvcReq("", ""), clusters)
	assert.NoError(t, err)
	assert.Nil(t, got)
	assert.Equal(t, MatchReasonNoRequirements, reason)

	// requirements declared but nothing satisfies them.
	got, reason, err = MatchCandidates(isvcReq("gpu=mi300x", ""), clusters)
	assert.NoError(t, err)
	assert.Nil(t, got)
	assert.Equal(t, MatchReasonNoMatch, reason)

	// requirements declared but no Ready clusters at all.
	got, reason, err = MatchCandidates(isvcReq("gpu=gb300", ""), []v1beta1.WorkloadCluster{
		wc("cluster-a-down", false, map[string]string{"gpu": "gb300"}),
	})
	assert.NoError(t, err)
	assert.Nil(t, got)
	assert.Equal(t, MatchReasonNoReadyClusters, reason)

	// malformed selector -> error + MalformedSelector reason.
	_, reason, err = MatchCandidates(isvcReq("!!!", ""), clusters)
	assert.Error(t, err)
	assert.Equal(t, MatchReasonMalformedSelector, reason)
}
