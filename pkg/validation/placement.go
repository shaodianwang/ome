package validation

import (
	"fmt"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/placement/affinity"
)

// ValidatePlacement checks explicit placement syntax independently of member
// health or runtime resolution. An absent block leaves local services unchanged.
func ValidatePlacement(spec *v1beta1.InferenceServiceSpec) error {
	if spec == nil || spec.Placement == nil {
		return nil
	}
	p := spec.Placement
	split := p.Mode == v1beta1.PlacementModeSplit || p.Mode == v1beta1.PlacementModeSplitByCapacity
	if !split && p.Mode != v1beta1.PlacementModeSingle && p.Mode != v1beta1.PlacementModeAll {
		return fmt.Errorf("spec.placement.mode must explicitly select Single, All, Split, or SplitByCapacity")
	}
	if p.HasLegacyFields() {
		return fmt.Errorf("spec.placement requirements, clusterSelector, and capacityFactors are unsupported; migrate selectors to clusterAffinity and traffic factors to spec.routing.capacityFactors")
	}
	if _, err := affinity.Compile(p.ClusterAffinity, p.Mode == v1beta1.PlacementModeSplit); err != nil {
		return fmt.Errorf("spec.placement.%w", err)
	}
	if p.MaxSurge != nil && *p.MaxSurge < 0 {
		return fmt.Errorf("spec.placement.maxSurge must be nonnegative whole replicas")
	}
	if p.Split == nil {
		return nil
	}
	if !split {
		return fmt.Errorf("spec.placement.split is permitted only for Split and SplitByCapacity")
	}
	s := p.Split
	if s.HasLegacyFields() {
		return fmt.Errorf("spec.placement.split spread and minReplicasPerCluster are unsupported; mode and affinity weights determine exact shares")
	}
	if s.Replicas != nil && *s.Replicas < 1 {
		return fmt.Errorf("spec.placement.split.replicas must be positive when specified")
	}
	if s.MaxReplicasPerCluster < 0 {
		return fmt.Errorf("spec.placement.split.maxReplicasPerCluster must be nonnegative")
	}
	return nil
}

// ValidatePlacementIntent rejects obsolete annotations even when their values
// are empty. Silently dropping such restrictions could widen the matched fleet.
func ValidatePlacementIntent(isvc *v1beta1.InferenceService) error {
	if isvc == nil {
		return nil
	}
	for _, key := range []string{constants.AcceleratorRequirements, constants.ClusterSelector} {
		if _, exists := isvc.Annotations[key]; exists {
			return fmt.Errorf("annotation %s is unsupported; declare spec.placement.mode and migrate restrictions to clusterAffinity", key)
		}
	}
	return ValidatePlacement(&isvc.Spec)
}
