package keda

import (
	"testing"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

func TestCalculateMinMaxReplicas(t *testing.T) {
	testCases := []struct {
		name               string
		componentExt       *v1beta1.ComponentExtensionSpec
		expectedMinReplica int32
		expectedMaxReplica int32
	}{
		{
			name: "Default min/max replicas",
			componentExt: &v1beta1.ComponentExtensionSpec{
				MinReplicas: nil,
				MaxReplicas: 0,
			},
			expectedMinReplica: 1,
			expectedMaxReplica: 1,
		},
		{
			name: "Custom min/max replicas",
			componentExt: &v1beta1.ComponentExtensionSpec{
				MinReplicas: intPtr(2),
				MaxReplicas: 10,
			},
			expectedMinReplica: 2,
			expectedMaxReplica: 10,
		},
		{
			name: "Max replicas less than min - should use min",
			componentExt: &v1beta1.ComponentExtensionSpec{
				MinReplicas: intPtr(5),
				MaxReplicas: 3,
			},
			expectedMinReplica: 5,
			expectedMaxReplica: 5,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			minReplicas := calculateMinReplicas(tt.componentExt)
			maxReplicas := calculateMaxReplicas(tt.componentExt, minReplicas)

			if minReplicas != tt.expectedMinReplica {
				t.Errorf("Expected minReplicas %d, got %d", tt.expectedMinReplica, minReplicas)
			}
			if maxReplicas != tt.expectedMaxReplica {
				t.Errorf("Expected maxReplicas %d, got %d", tt.expectedMaxReplica, maxReplicas)
			}
		})
	}
}

func intPtr(i int) *int {
	return &i
}
