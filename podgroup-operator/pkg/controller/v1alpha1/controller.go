package v1alpha1

import (
	"context"
	"encoding/json"
	"log"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
)

type Controller struct {
	kube    kubernetes.Interface
	dynamic dynamic.Interface
}

func New(kube kubernetes.Interface, dynamic dynamic.Interface) *Controller {
	return &Controller{kube: kube, dynamic: dynamic}
}

func (c *Controller) Run(ctx context.Context) {
	reconcileInterval := 2 * time.Second
	ticker := time.NewTicker(reconcileInterval)
	defer ticker.Stop()

	log.Printf("starting podgroup operator reconcile_interval=%s", reconcileInterval)
	for {
		if err := c.reconcileAll(ctx); err != nil {
			log.Printf("reconcile cycle failed: %v", err)
		}

		select {
		case <-ctx.Done():
			log.Printf("stopping podgroup operator: %v", ctx.Err())
			return
		case <-ticker.C:
		}
	}
}

func (c *Controller) reconcileAll(ctx context.Context) error {
	if err := c.reconcileRollouts(ctx); err != nil {
		return err
	}

	list, err := c.dynamic.Resource(podGroupDeploymentGVR).Namespace("").List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}

	for i := range list.Items {
		pgd, err := decodePodGroupDeployment(&list.Items[i])
		if err != nil {
			log.Printf("decode %s/%s failed: %v", list.Items[i].GetNamespace(), list.Items[i].GetName(), err)
			continue
		}
		if err := c.reconcileOne(ctx, pgd); err != nil {
			log.Printf("reconcile %s/%s failed: %v", pgd.Namespace, pgd.Name, err)
		}
	}

	return nil
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
