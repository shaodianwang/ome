package v1alpha1

import (
	"context"
	"log"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

func NewFromConfig(cfg *rest.Config) (*Controller, error) {
	kube, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}
	dyn, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}
	return New(kube, dyn), nil
}

func (c *Controller) SetupWithManager(mgr manager.Manager) error {
	c.WithEventRecorder(mgr.GetEventRecorderFor(managerName))
	if err := (&PodGroupDeploymentReconciler{controller: c}).SetupWithManager(mgr); err != nil {
		return err
	}
	if err := (&PodGroupDeploymentRolloutReconciler{controller: c}).SetupWithManager(mgr); err != nil {
		return err
	}
	return mgr.Add(&staleRolloutGateCleaner{
		controller: c,
		interval:   30 * time.Second,
	})
}

type PodGroupDeploymentReconciler struct {
	controller *Controller
}

func (r *PodGroupDeploymentReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	obj, err := r.controller.dynamic.Resource(podGroupDeploymentGVR).Namespace(req.Namespace).Get(ctx, req.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return ctrl.Result{}, nil
	}
	if err != nil {
		return ctrl.Result{}, err
	}
	pgd, err := decodePodGroupDeployment(obj)
	if err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, r.controller.reconcileOne(ctx, pgd)
}

func (r *PodGroupDeploymentReconciler) SetupWithManager(mgr manager.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&PodGroupDeployment{}).
		Watches(&corev1.Pod{}, handler.EnqueueRequestsFromMapFunc(enqueuePGDFromChild)).
		Watches(&corev1.Service{}, handler.EnqueueRequestsFromMapFunc(enqueuePGDFromChild)).
		Watches(schedulerPodGroupObject(), handler.EnqueueRequestsFromMapFunc(enqueuePGDFromChild)).
		Complete(r)
}

type PodGroupDeploymentRolloutReconciler struct {
	controller *Controller
}

func (r *PodGroupDeploymentRolloutReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	obj, err := r.controller.dynamic.Resource(podGroupDeploymentRolloutGVR).Namespace(req.Namespace).Get(ctx, req.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return ctrl.Result{}, nil
	}
	if err != nil {
		return ctrl.Result{}, err
	}
	pgdr, err := decodePodGroupDeploymentRollout(obj)
	if err != nil {
		return ctrl.Result{}, err
	}
	_, err = r.controller.reconcileOneRollout(ctx, pgdr)
	return ctrl.Result{}, err
}

func (r *PodGroupDeploymentRolloutReconciler) SetupWithManager(mgr manager.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&PodGroupDeploymentRollout{}).
		Watches(&PodGroupDeployment{}, handler.EnqueueRequestsFromMapFunc(enqueuePGDRFromChildPGD)).
		Watches(&corev1.Pod{}, handler.EnqueueRequestsFromMapFunc(r.enqueuePGDRFromChildObject)).
		Watches(&corev1.Service{}, handler.EnqueueRequestsFromMapFunc(r.enqueuePGDRFromChildObject)).
		Watches(schedulerPodGroupObject(), handler.EnqueueRequestsFromMapFunc(r.enqueuePGDRFromChildObject)).
		Complete(r)
}

type staleRolloutGateCleaner struct {
	controller *Controller
	interval   time.Duration
}

func (c *staleRolloutGateCleaner) Start(ctx context.Context) error {
	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()

	for {
		if err := c.runOnce(ctx); err != nil {
			log.Printf("stale rollout gate cleanup failed: %v", err)
		}

		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

func (c *staleRolloutGateCleaner) runOnce(ctx context.Context) error {
	list, err := c.controller.dynamic.Resource(podGroupDeploymentRolloutGVR).Namespace("").List(ctx, metav1.ListOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}

	referenced := map[string]struct{}{}
	for i := range list.Items {
		pgdr, err := decodePodGroupDeploymentRollout(&list.Items[i])
		if err != nil {
			log.Printf("decode rollout %s/%s failed during stale gate cleanup: %v", list.Items[i].GetNamespace(), list.Items[i].GetName(), err)
			continue
		}
		members, err := rolloutMembers(pgdr)
		if err != nil {
			log.Printf("resolve rollout members %s/%s failed during stale gate cleanup: %v", pgdr.Namespace, pgdr.Name, err)
			continue
		}
		for _, member := range members {
			referenced[pgdr.Namespace+"/"+member.Name] = struct{}{}
		}
	}
	return c.controller.clearStaleRolloutGates(ctx, referenced)
}

func rolloutMembers(pgdr *PodGroupDeploymentRollout) ([]rolloutMember, error) {
	if !rolloutSpecManagesChildren(pgdr.Spec) {
		return rolloutMembersForMode(pgdr.Name, pgdr.Spec.Mode)
	}
	children, err := desiredRolloutChildren(pgdr)
	if err != nil {
		return nil, err
	}
	members := make([]rolloutMember, 0, len(children))
	for _, child := range children {
		members = append(members, child.member)
	}
	return members, nil
}

func enqueuePGDFromChild(ctx context.Context, obj client.Object) []reconcile.Request {
	_ = ctx
	labels := obj.GetLabels()
	if labels[labelManagedBy] != managerName {
		return nil
	}
	name := labels[labelInstance]
	if name == "" {
		return nil
	}
	return pgdrRequest(obj.GetNamespace(), name)
}

func enqueuePGDRFromChildPGD(ctx context.Context, obj client.Object) []reconcile.Request {
	_ = ctx
	labels := obj.GetLabels()
	if labels[labelManagedBy] != managerName {
		return nil
	}
	name := labels[labelRollout]
	if name == "" {
		return nil
	}
	return []reconcile.Request{{NamespacedName: client.ObjectKey{
		Namespace: obj.GetNamespace(),
		Name:      name,
	}}}
}

func (r *PodGroupDeploymentRolloutReconciler) enqueuePGDRFromChildObject(ctx context.Context, obj client.Object) []reconcile.Request {
	labels := obj.GetLabels()
	if labels[labelManagedBy] != managerName {
		return nil
	}
	if rolloutName := labels[labelRollout]; rolloutName != "" {
		return pgdrRequest(obj.GetNamespace(), rolloutName)
	}
	pgdName := labels[labelInstance]
	if pgdName == "" {
		return nil
	}
	pgd, err := r.controller.dynamic.Resource(podGroupDeploymentGVR).Namespace(obj.GetNamespace()).Get(ctx, pgdName, metav1.GetOptions{})
	if err != nil {
		return nil
	}
	return enqueuePGDRFromChildPGD(ctx, pgd)
}

func pgdrRequest(namespace, name string) []reconcile.Request {
	return []reconcile.Request{{NamespacedName: client.ObjectKey{
		Namespace: namespace,
		Name:      name,
	}}}
}

func schedulerPodGroupObject() *unstructured.Unstructured {
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(schedulerPodGroupGVR.GroupVersion().WithKind("PodGroup"))
	return obj
}
