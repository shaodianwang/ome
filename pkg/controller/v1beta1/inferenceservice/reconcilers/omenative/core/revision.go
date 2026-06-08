package core

import (
	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

// isvcGVK is the GroupVersionKind every OMENative-on-ISVC adapter
// emits on owner references. Centralized so a future API version bump
// flips this in one place. Consumed by render.go's RenderWithRevisionForISVC.
var isvcGVK = v1beta1.SchemeGroupVersion.WithKind("InferenceService")
