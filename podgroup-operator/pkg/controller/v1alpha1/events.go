package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

func (c *Controller) recordNormal(obj runtime.Object, reason, message string) {
	if c.recorder == nil {
		return
	}
	c.recorder.Event(obj, corev1.EventTypeNormal, reason, message)
}

func (c *Controller) recordWarning(obj runtime.Object, reason, message string) {
	if c.recorder == nil {
		return
	}
	c.recorder.Event(obj, corev1.EventTypeWarning, reason, message)
}
