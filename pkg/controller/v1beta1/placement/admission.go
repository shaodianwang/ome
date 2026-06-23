package placement

import "github.com/ome-projects/ome/pkg/apis/ome/v1beta1"

// AnyInstanceAdmitted reports whether any component of the (derived) ISVC has at
// least one Instance that has left the Kueue admission gate. This is the
// OEP-0018 race signal: the first candidate cluster to report admitted wins.
func AnyInstanceAdmitted(isvc *v1beta1.InferenceService) bool {
	for _, cs := range isvc.Status.Components {
		if cs.Lifecycle == nil {
			continue
		}
		for _, inst := range cs.Lifecycle.InstanceStatuses {
			if inst.Admitted {
				return true
			}
		}
	}
	return false
}
