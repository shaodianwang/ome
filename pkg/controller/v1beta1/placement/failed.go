package placement

import "github.com/ome-projects/ome/pkg/apis/ome/v1beta1"

// IsTerminallyFailed reports whether the (derived) ISVC has any component
// Instance escalated to Phase=Failed (stuck-pod / InstanceReadyTimeout
// backstop). The OEP-0018 control plane treats this as a failed placement —
// distinct from a derived ISVC that was merely deleted (which is re-placed).
func IsTerminallyFailed(isvc *v1beta1.InferenceService) bool {
	for _, cs := range isvc.Status.Components {
		if cs.Lifecycle == nil {
			continue
		}
		for _, inst := range cs.Lifecycle.InstanceStatuses {
			if inst.Phase == v1beta1.OMENativeInstanceFailed {
				return true
			}
		}
	}
	return false
}
