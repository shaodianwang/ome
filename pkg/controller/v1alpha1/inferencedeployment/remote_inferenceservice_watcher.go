package inferencedeployment

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/go-logr/logr"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/event"

	omev1alpha1 "github.com/ome-projects/ome/pkg/apis/ome/v1alpha1"
	omev1beta1 "github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	versioned "github.com/ome-projects/ome/pkg/client/clientset/versioned"
	informers "github.com/ome-projects/ome/pkg/client/informers/externalversions"
)

const (
	DefaultRemoteInferenceServiceEventBufferSize = 1024
	remoteInferenceServiceWatchResync            = 5 * time.Minute
)

// RemoteInferenceServiceWatcher watches IDO-managed InferenceServices in each
// startup-loaded workload cluster and enqueues the owning local
// InferenceDeployment for reconciliation.
type RemoteInferenceServiceWatcher struct {
	configs map[string]*rest.Config
	events  chan<- event.GenericEvent
	log     logr.Logger
	resync  time.Duration
}

func NewRemoteInferenceServiceWatcher(configs map[string]*rest.Config, events chan<- event.GenericEvent, log logr.Logger) *RemoteInferenceServiceWatcher {
	copiedConfigs := make(map[string]*rest.Config, len(configs))
	for name, cfg := range configs {
		copiedConfigs[name] = rest.CopyConfig(cfg)
	}
	return &RemoteInferenceServiceWatcher{
		configs: copiedConfigs,
		events:  events,
		log:     log,
		resync:  remoteInferenceServiceWatchResync,
	}
}

func (w *RemoteInferenceServiceWatcher) NeedLeaderElection() bool {
	return true
}

func (w *RemoteInferenceServiceWatcher) Start(ctx context.Context) error {
	if w.events == nil {
		return fmt.Errorf("remote inference service event channel is required")
	}
	if len(w.configs) == 0 {
		return fmt.Errorf("remote inference service watcher requires at least one workload cluster")
	}

	var wg sync.WaitGroup
	started := 0
	for clusterName, cfg := range w.configs {
		clusterLog := w.log.WithValues("target", clusterName)
		clientset, err := versioned.NewForConfig(rest.CopyConfig(cfg))
		if err != nil {
			clusterLog.Error(err, "Failed to create remote watch client")
			continue
		}
		factory := informers.NewSharedInformerFactory(clientset, w.resync)
		informer := factory.Ome().V1beta1().InferenceServices().Informer()
		if _, err := informer.AddEventHandler(w.eventHandler(ctx, clusterName, clusterLog)); err != nil {
			clusterLog.Error(err, "Failed to register remote InferenceService event handler")
			continue
		}

		started++
		wg.Add(1)
		go func() {
			defer wg.Done()
			w.log.Info(fmt.Sprintf("Starting remote InferenceService watch: targetCluster=%s resync=%s", clusterName, w.resync))
			factory.Start(ctx.Done())
			if synced := factory.WaitForCacheSync(ctx.Done()); !allInformerCachesSynced(synced) {
				w.log.Info(fmt.Sprintf("Remote InferenceService watch stopped before cache sync: targetCluster=%s", clusterName))
				factory.Shutdown()
				return
			}
			w.log.Info(fmt.Sprintf("Remote InferenceService watch cache synced: targetCluster=%s", clusterName))
			<-ctx.Done()
			factory.Shutdown()
			w.log.Info(fmt.Sprintf("Stopped remote InferenceService watch: targetCluster=%s", clusterName))
		}()
	}

	if started == 0 {
		return fmt.Errorf("failed to start any remote inference service watches")
	}
	wg.Wait()
	return nil
}

func (w *RemoteInferenceServiceWatcher) eventHandler(ctx context.Context, clusterName string, log logr.Logger) cache.ResourceEventHandlerFuncs {
	return cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			w.enqueueRemoteObjects(ctx, log, clusterName, "create", obj)
		},
		UpdateFunc: func(oldObj, newObj any) {
			w.enqueueRemoteObjects(ctx, log, clusterName, "update", oldObj, newObj)
		},
		DeleteFunc: func(obj any) {
			w.enqueueRemoteObjects(ctx, log, clusterName, "delete", obj)
		},
	}
}

func (w *RemoteInferenceServiceWatcher) enqueueRemoteObjects(ctx context.Context, log logr.Logger, clusterName, action string, objects ...any) {
	keys := remoteInferenceDeploymentKeysFromObjects(clusterName, objects...)
	if len(keys) == 0 {
		if action == "delete" {
			w.log.Info("Ignored remote InferenceService delete event: " + remoteObjectLogMessage(clusterName, objects...))
		}
		return
	}
	for _, key := range keys {
		localObject := &omev1alpha1.InferenceDeployment{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: key.Namespace,
				Name:      key.Name,
			},
		}
		select {
		case w.events <- event.GenericEvent{Object: localObject}:
			logRemoteInferenceServiceEvent(w.log, clusterName, action, key)
		case <-ctx.Done():
			return
		default:
			w.log.Info(fmt.Sprintf("Dropped remote InferenceService event because queue is full: action=%s inferenceDeployment=%s/%s targetCluster=%s", action, key.Namespace, key.Name, clusterName))
		}
	}
}

func logRemoteInferenceServiceEvent(log logr.Logger, clusterName, action string, key types.NamespacedName) {
	log.Info(fmt.Sprintf("Enqueued InferenceDeployment from remote InferenceService event: action=%s inferenceDeployment=%s/%s targetCluster=%s", action, key.Namespace, key.Name, clusterName))
}

func remoteObjectLogMessage(clusterName string, objects ...any) string {
	for _, obj := range objects {
		isvc, ok := remoteInferenceServiceFromObject(obj)
		if !ok {
			continue
		}
		objectLabels := isvc.GetLabels()
		return fmt.Sprintf(
			"targetCluster=%s remoteInferenceService=%s/%s managedBy=%s inferenceDeployment=%s/%s targetLabel=%s",
			clusterName,
			isvc.Namespace,
			isvc.Name,
			objectLabels[labelManagedBy],
			objectLabels[labelDeploymentNamespace],
			objectLabels[labelDeployment],
			objectLabels[labelTarget],
		)
	}
	return fmt.Sprintf("targetCluster=%s objectCount=%d", clusterName, len(objects))
}

func remoteInferenceDeploymentKeysFromObjects(clusterName string, objects ...any) []types.NamespacedName {
	out := make([]types.NamespacedName, 0, len(objects))
	seen := make(map[types.NamespacedName]struct{}, len(objects))
	for _, obj := range objects {
		key, ok := inferenceDeploymentKeyFromRemoteObject(clusterName, obj)
		if !ok {
			continue
		}
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, key)
	}
	return out
}

func inferenceDeploymentKeyFromRemoteObject(clusterName string, obj any) (types.NamespacedName, bool) {
	isvc, ok := remoteInferenceServiceFromObject(obj)
	if !ok {
		return types.NamespacedName{}, false
	}
	objectLabels := isvc.GetLabels()
	if objectLabels[labelManagedBy] != managedByValue {
		return types.NamespacedName{}, false
	}
	if objectLabels[labelTarget] != "" && objectLabels[labelTarget] != clusterName {
		return types.NamespacedName{}, false
	}
	name := objectLabels[labelDeployment]
	namespace := objectLabels[labelDeploymentNamespace]
	if name == "" || namespace == "" {
		return types.NamespacedName{}, false
	}
	return types.NamespacedName{Namespace: namespace, Name: name}, true
}

func remoteInferenceServiceFromObject(obj any) (*omev1beta1.InferenceService, bool) {
	switch typed := obj.(type) {
	case *omev1beta1.InferenceService:
		return typed, true
	case cache.DeletedFinalStateUnknown:
		return remoteInferenceServiceFromObject(typed.Obj)
	default:
		return nil, false
	}
}

func allInformerCachesSynced(synced map[reflect.Type]bool) bool {
	for _, ok := range synced {
		if !ok {
			return false
		}
	}
	return true
}
