package v1alpha1

import (
	"encoding/json"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/record"
)

type Controller struct {
	kube     kubernetes.Interface
	dynamic  dynamic.Interface
	recorder record.EventRecorder
}

func New(kube kubernetes.Interface, dynamic dynamic.Interface) *Controller {
	return &Controller{kube: kube, dynamic: dynamic}
}

func (c *Controller) WithEventRecorder(recorder record.EventRecorder) *Controller {
	c.recorder = recorder
	return c
}

func decodePodGroupDeployment(obj *unstructured.Unstructured) (*PodGroupDeployment, error) {
	raw, err := json.Marshal(obj.Object)
	if err != nil {
		return nil, err
	}
	var pgd PodGroupDeployment
	if err := json.Unmarshal(raw, &pgd); err != nil {
		return nil, err
	}
	return &pgd, nil
}

func decodePodGroupDeploymentRollout(obj *unstructured.Unstructured) (*PodGroupDeploymentRollout, error) {
	raw, err := json.Marshal(obj.Object)
	if err != nil {
		return nil, err
	}
	var pgdr PodGroupDeploymentRollout
	if err := json.Unmarshal(raw, &pgdr); err != nil {
		return nil, err
	}
	return &pgdr, nil
}
