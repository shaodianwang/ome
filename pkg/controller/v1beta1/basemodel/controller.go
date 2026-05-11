package basemodel

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"time"

	"github.com/go-logr/logr"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/controllerconfig"
	"github.com/ome-projects/ome/pkg/modelagent"
	"github.com/ome-projects/ome/pkg/modelcache"
)

// +kubebuilder:rbac:groups=ome.io,resources=basemodels,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=ome.io,resources=basemodels/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=ome.io,resources=basemodels/finalizers,verbs=update
// +kubebuilder:rbac:groups=ome.io,resources=clusterbasemodels,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=ome.io,resources=clusterbasemodels/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=ome.io,resources=clusterbasemodels/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch;update;delete
// +kubebuilder:rbac:groups="",resources=nodes,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=persistentvolumeclaims,verbs=get;list;watch
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=batch,resources=jobs/status,verbs=get

// BaseModelReconciler reconciles BaseModel objects
type BaseModelReconciler struct {
	client.Client
	Log            logr.Logger
	Scheme         *runtime.Scheme
	ModelCache     modelcache.Provider
	OmeAgentConfig *controllerconfig.OmeAgentConfig
}

// ClusterBaseModelReconciler reconciles ClusterBaseModel objects
type ClusterBaseModelReconciler struct {
	client.Client
	Log            logr.Logger
	Scheme         *runtime.Scheme
	ModelCache     modelcache.Provider
	OmeAgentConfig *controllerconfig.OmeAgentConfig
}

// Reconcile handles BaseModel reconciliation
func (r *BaseModelReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := r.Log.WithValues("basemodel", req.NamespacedName)

	// Fetch the BaseModel instance
	baseModel := &v1beta1.BaseModel{}
	if err := r.Get(ctx, req.NamespacedName, baseModel); err != nil {
		if errors.IsNotFound(err) {
			// Object not found, return without error since it was likely deleted
			return ctrl.Result{}, nil
		}
		log.Error(err, "Failed to get BaseModel")
		return ctrl.Result{}, err
	}

	log.Info("Reconciling BaseModel")

	// Handle deletion. PVC-backed models go through a dedicated cleanup
	// (one ConfigMap, looked up by name) instead of the per-node deletion
	// handshake handleModelDeletion expects.
	if !baseModel.DeletionTimestamp.IsZero() {
		log.Info("Handling BaseModel deletion")
		if isPVCStorage(&baseModel.Spec) {
			return handlePVCModelDeletion(ctx, r.Client, log, baseModel, false, constants.BaseModelFinalizer)
		}
		return r.handleDeletion(ctx, baseModel)
	}

	// Add finalizer if not present
	if !controllerutil.ContainsFinalizer(baseModel, constants.BaseModelFinalizer) {
		log.Info("Adding finalizer to BaseModel")
		controllerutil.AddFinalizer(baseModel, constants.BaseModelFinalizer)
		if err := r.Update(ctx, baseModel); err != nil {
			log.Error(err, "Failed to add finalizer")
			return ctrl.Result{}, err
		}
	}

	if isShardedDistribution(baseModel.Spec.Distribution) {
		result, err := r.updateShardedModelStatus(ctx, baseModel)
		if err != nil {
			log.Error(err, "Failed to update Sharded BaseModel status")
			return ctrl.Result{RequeueAfter: time.Minute}, err
		}
		return result, nil
	}

	if isPVCStorage(&baseModel.Spec) {
		// PVC-backed models do NOT participate in the per-node
		// processModelStatus flow. reconcilePVCStorage handles validation,
		// Job ensure, and applies status from the per-PVC ConfigMap by
		// direct Get — no wide List, no synthetic "node" entries in
		// Status.NodesReady.
		return reconcilePVCStorage(ctx, r.Client, r.Scheme, log, baseModel, &baseModel.Spec, false, r.metadataJobConfig())
	}

	// Storage URI is non-PVC (or unset). If the model was previously
	// PVC-backed, clean up the orphaned Job + per-PVC ConfigMap +
	// stale PVC-specific conditions before falling through to the
	// per-node flow. Idempotent / no-op when no PVC artifacts exist.
	if err := cleanupStalePVCArtifacts(ctx, r.Client, log, baseModel, false); err != nil {
		log.Error(err, "Failed to clean up stale PVC artifacts after URI swap")
		return ctrl.Result{RequeueAfter: 10 * time.Second}, err
	}

	// Update status based on ConfigMaps
	if err := r.updateModelStatus(ctx, baseModel); err != nil {
		log.Error(err, "Failed to update BaseModel status")
		return ctrl.Result{RequeueAfter: time.Minute}, err
	}

	// Requeue while downloading to ensure status is updated regularly
	if baseModel.Status.State == v1beta1.LifeCycleStateImporting || baseModel.Status.State == v1beta1.LifeCycleStateInTransit {
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}

	return ctrl.Result{}, nil
}

// Reconcile handles ClusterBaseModel reconciliation
func (r *ClusterBaseModelReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := r.Log.WithValues("clusterbasemodel", req.NamespacedName)

	// Fetch the ClusterBaseModel instance
	clusterBaseModel := &v1beta1.ClusterBaseModel{}
	if err := r.Get(ctx, req.NamespacedName, clusterBaseModel); err != nil {
		if errors.IsNotFound(err) {
			// Object not found, return without error since it was likely deleted
			return ctrl.Result{}, nil
		}
		log.Error(err, "Failed to get ClusterBaseModel")
		return ctrl.Result{}, err
	}

	log.Info("Reconciling ClusterBaseModel")

	// Handle deletion. PVC-backed models go through the dedicated PVC
	// cleanup path; per-node-style models go through the existing handshake.
	if !clusterBaseModel.DeletionTimestamp.IsZero() {
		log.Info("Handling ClusterBaseModel deletion")
		if isPVCStorage(&clusterBaseModel.Spec) {
			return handlePVCModelDeletion(ctx, r.Client, log, clusterBaseModel, true, constants.ClusterBaseModelFinalizer)
		}
		return r.handleDeletion(ctx, clusterBaseModel)
	}

	// Add finalizer if not present
	if !controllerutil.ContainsFinalizer(clusterBaseModel, constants.ClusterBaseModelFinalizer) {
		log.Info("Adding finalizer to ClusterBaseModel")
		controllerutil.AddFinalizer(clusterBaseModel, constants.ClusterBaseModelFinalizer)
		if err := r.Update(ctx, clusterBaseModel); err != nil {
			log.Error(err, "Failed to add finalizer")
			return ctrl.Result{}, err
		}
	}

	if isShardedDistribution(clusterBaseModel.Spec.Distribution) {
		result, err := r.updateShardedModelStatus(ctx, clusterBaseModel)
		if err != nil {
			log.Error(err, "Failed to update Sharded ClusterBaseModel status")
			return ctrl.Result{RequeueAfter: time.Minute}, err
		}
		return result, nil
	}

	if isPVCStorage(&clusterBaseModel.Spec) {
		// Direct read of the per-PVC ConfigMap — see BaseModel branch.
		return reconcilePVCStorage(ctx, r.Client, r.Scheme, log, clusterBaseModel, &clusterBaseModel.Spec, true, r.metadataJobConfig())
	}

	// Same URI-swap cleanup as the namespaced BaseModel branch.
	if err := cleanupStalePVCArtifacts(ctx, r.Client, log, clusterBaseModel, true); err != nil {
		log.Error(err, "Failed to clean up stale PVC artifacts after URI swap")
		return ctrl.Result{RequeueAfter: 10 * time.Second}, err
	}

	// Update status based on ConfigMaps
	if err := r.updateModelStatus(ctx, clusterBaseModel); err != nil {
		log.Error(err, "Failed to update ClusterBaseModel status")
		return ctrl.Result{RequeueAfter: time.Minute}, err
	}

	// Requeue while downloading to ensure status is updated regularly
	if clusterBaseModel.Status.State == v1beta1.LifeCycleStateImporting || clusterBaseModel.Status.State == v1beta1.LifeCycleStateInTransit {
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}

	return ctrl.Result{}, nil
}

// handleDeletion handles BaseModel deletion
func (r *BaseModelReconciler) handleDeletion(ctx context.Context, baseModel *v1beta1.BaseModel) (ctrl.Result, error) {
	return handleModelDeletion(ctx, r.Client, baseModel, constants.BaseModelFinalizer)
}

// handleDeletion handles ClusterBaseModel deletion
func (r *ClusterBaseModelReconciler) handleDeletion(ctx context.Context, clusterBaseModel *v1beta1.ClusterBaseModel) (ctrl.Result, error) {
	return handleModelDeletion(ctx, r.Client, clusterBaseModel, constants.ClusterBaseModelFinalizer)
}

// handleModelDeletion is a shared utility function for handling model deletion
func handleModelDeletion(ctx context.Context, kubeClient client.Client, obj client.Object, finalizer string) (ctrl.Result, error) {
	log := ctrl.LoggerFrom(ctx)

	if controllerutil.ContainsFinalizer(obj, finalizer) {
		// Before removing the finalizer, make sure all entries are cleared from ConfigMaps
		// This prevents orphaned models when nodes are down or agents aren't running

		// Determine model name, namespace and if it's cluster-scoped
		modelName := obj.GetName()
		var modelNamespace string
		var isClusterScope bool

		// Set namespace and scope based on object type
		switch typedObj := obj.(type) {
		case *v1beta1.BaseModel:
			modelNamespace = typedObj.Namespace
			isClusterScope = false
		case *v1beta1.ClusterBaseModel:
			modelNamespace = ""
			isClusterScope = true
		default:
			log.Error(fmt.Errorf("unknown model type"), "Invalid model type for deletion handler")
			return ctrl.Result{}, fmt.Errorf("unknown model type for deletion")
		}

		// Get the model's ConfigMap key
		modelKey := constants.GetModelConfigMapKey(modelNamespace, modelName, isClusterScope)

		// List all ConfigMaps with model status label in the ome namespace
		configMaps := &corev1.ConfigMapList{}
		listOpts := []client.ListOption{
			client.InNamespace(constants.OMENamespace),
			client.MatchingLabels{constants.ModelStatusConfigMapLabel: "true"},
		}

		if err := kubeClient.List(ctx, configMaps, listOpts...); err != nil {
			log.Error(err, "Failed to list ConfigMaps during model deletion")
			return ctrl.Result{RequeueAfter: time.Second * 10}, err
		}

		// Check if any ConfigMap still has an entry for this model that is not marked as deleted
		var modelsNotDeleted []string
		nodesWithModel := 0

		for i := range configMaps.Items {
			configMap := &configMaps.Items[i]
			// Check if the model exists in this ConfigMap
			data, exists := configMap.Data[modelKey]
			if !exists {
				continue
			}
			nodesWithModel++

			// Check if it's already marked for deletion
			var modelEntry modelagent.ModelEntry
			if err := json.Unmarshal([]byte(data), &modelEntry); err == nil {
				// If model entry is present but not marked as deleted, add it to the list
				if modelEntry.Status != modelagent.ModelStatusDeleted {
					modelsNotDeleted = append(modelsNotDeleted, configMap.Name)
				}
			} else {
				// Can't parse the entry, consider it not deleted for safety
				modelsNotDeleted = append(modelsNotDeleted, configMap.Name)
			}
		}

		modelInfo := modelName
		if !isClusterScope {
			modelInfo = modelNamespace + "/" + modelName
		}

		// If models are still present in ConfigMaps and not deleted, requeue
		if len(modelsNotDeleted) > 0 {
			log.Info("Waiting for model to be cleared from ConfigMaps",
				"model", modelInfo,
				"nodesWithModel", nodesWithModel,
				"nodesNotDeleted", len(modelsNotDeleted),
				"nodes", modelsNotDeleted)

			// Requeue to check again later
			return ctrl.Result{RequeueAfter: time.Second * 30}, nil
		}

		log.Info("All model entries have been cleared or marked as deleted", "model", modelInfo)

		// All entries are either cleared or marked for deletion, safe to remove finalizer
		controllerutil.RemoveFinalizer(obj, finalizer)
		if err := kubeClient.Update(ctx, obj); err != nil {
			log.Error(err, "Failed to remove finalizer", "model", modelInfo)
			return ctrl.Result{}, err
		}
		log.Info("Finalizer removed, deletion complete", "model", modelInfo)
	}
	return ctrl.Result{}, nil
}

// updateModelStatus updates BaseModel status based on ConfigMap data
func (r *BaseModelReconciler) updateModelStatus(ctx context.Context, baseModel *v1beta1.BaseModel) error {
	return processModelStatus(ctx, r.Client, r.Log, baseModel.Namespace, baseModel.Name, false,
		func(ctx context.Context, config *modelagent.ModelConfig) error {
			return r.updateModelSpecWithRetry(ctx, baseModel, config)
		},
		func(ctx context.Context, nodesReady, nodesFailed []string) error {
			return r.updateStatusWithRetry(ctx, baseModel, nodesReady, nodesFailed)
		})
}

// updateModelStatus updates ClusterBaseModel status based on ConfigMap data
func (r *ClusterBaseModelReconciler) updateModelStatus(ctx context.Context, clusterBaseModel *v1beta1.ClusterBaseModel) error {
	return processModelStatus(ctx, r.Client, r.Log, "", clusterBaseModel.Name, true,
		func(ctx context.Context, config *modelagent.ModelConfig) error {
			return r.updateModelSpecWithRetry(ctx, clusterBaseModel, config)
		},
		func(ctx context.Context, nodesReady, nodesFailed []string) error {
			return r.updateStatusWithRetry(ctx, clusterBaseModel, nodesReady, nodesFailed)
		})
}

func (r *BaseModelReconciler) updateShardedModelStatus(ctx context.Context, baseModel *v1beta1.BaseModel) (ctrl.Result, error) {
	return reconcileShardedModelCacheStatus(ctx, r.Client, r.Log, r.ModelCache, baseModel, "BaseModel")
}

func (r *ClusterBaseModelReconciler) updateShardedModelStatus(ctx context.Context, clusterBaseModel *v1beta1.ClusterBaseModel) (ctrl.Result, error) {
	return reconcileShardedModelCacheStatus(ctx, r.Client, r.Log, r.ModelCache, clusterBaseModel, "ClusterBaseModel")
}

// processModelStatus is a shared utility function for processing ConfigMaps and updating model status
func processModelStatus(ctx context.Context, kubeClient client.Client, log logr.Logger, namespace, name string, isClusterScope bool,
	specUpdateFunc func(context.Context, *modelagent.ModelConfig) error,
	statusUpdateFunc func(context.Context, []string, []string) error) error {

	modelInfo := name
	if !isClusterScope {
		modelInfo = namespace + "/" + name
	}
	log = log.WithValues("model", modelInfo)

	// List all ConfigMaps with model status label in the ome namespace
	configMaps := &corev1.ConfigMapList{}
	listOpts := []client.ListOption{
		client.InNamespace(constants.OMENamespace),
		client.MatchingLabels{constants.ModelStatusConfigMapLabel: "true"},
	}
	if err := kubeClient.List(ctx, configMaps, listOpts...); err != nil {
		log.Error(err, "Failed to list ConfigMaps")
		return fmt.Errorf("failed to list ConfigMaps: %w", err)
	}

	log.Info("Processing model status from ConfigMaps", "configMapsTotal", len(configMaps.Items))

	// Track counters for logging
	var processedNodes, validNodes, readyNodes, failedNodes int
	var nodesReady []string
	var nodesFailed []string
	var specUpdateErrors []string

	// Process each ConfigMap to find this model's status. PVC-backed
	// models do NOT use this path — they read their per-PVC ConfigMap
	// directly via reconcilePVCStorage. Per-node ConfigMaps require a
	// live Node here.
	for _, configMap := range configMaps.Items {
		processedNodes++

		node := &corev1.Node{}
		if err := kubeClient.Get(ctx, types.NamespacedName{Name: configMap.Name}, node); err != nil {
			if errors.IsNotFound(err) {
				// Node was deleted, skip silently
				continue
			}
			log.Error(err, "Failed to get node", "node", configMap.Name)
			continue
		}
		validNodes++

		// Look for this model in the ConfigMap
		modelKey := constants.GetModelConfigMapKey(namespace, name, isClusterScope)
		data, exists := configMap.Data[modelKey]
		if !exists {
			// Model not found in this ConfigMap, continue silently
			continue
		}

		// Parse the model entry
		var modelEntry modelagent.ModelEntry
		if err := json.Unmarshal([]byte(data), &modelEntry); err != nil {
			log.Error(err, "Failed to parse model entry", "node", configMap.Name, "key", modelKey)
			continue
		}

		log.V(1).Info("Processing model entry", "node", configMap.Name, "status", modelEntry.Status, "hasConfig", modelEntry.Config != nil, "hasProgress", modelEntry.Progress != nil)

		// Update model spec with config if available
		if modelEntry.Config != nil {
			if err := specUpdateFunc(ctx, modelEntry.Config); err != nil {
				log.Error(err, "Failed to update model spec", "node", configMap.Name)
				specUpdateErrors = append(specUpdateErrors, configMap.Name)
				// Continue processing other nodes even if spec update fails
			}
		}

		// Update status arrays based on model status
		switch modelEntry.Status {
		case modelagent.ModelStatusReady:
			nodesReady = addToSlice(nodesReady, configMap.Name)
			readyNodes++
		case modelagent.ModelStatusFailed:
			nodesFailed = addToSlice(nodesFailed, configMap.Name)
			failedNodes++
		case modelagent.ModelStatusUpdating:
			// Don't add to either array for updating status
		case modelagent.ModelStatusDeleted:
			// Remove from both arrays (though it shouldn't be in ConfigMap if deleted)
		default:
			log.V(1).Info("Unknown model status", "node", configMap.Name, "status", modelEntry.Status)
		}
	}

	// Sort the arrays for consistency
	slices.Sort(nodesReady)
	slices.Sort(nodesFailed)

	// Log summary - important for observability
	log.Info("Model status summary",
		"readyNodes", readyNodes,
		"failedNodes", failedNodes,
		"totalProcessed", processedNodes,
		"validNodes", validNodes)

	// Log spec update errors if any occurred
	if len(specUpdateErrors) > 0 {
		log.Info("Some nodes failed spec updates", "failedNodes", specUpdateErrors)
	}

	// Update the model status with retry logic
	return statusUpdateFunc(ctx, nodesReady, nodesFailed)
}

// updateModelSpec updates BaseModel spec with configuration from ConfigMap
func (r *BaseModelReconciler) updateModelSpec(ctx context.Context, baseModel *v1beta1.BaseModel, config *modelagent.ModelConfig) error {
	return updateModelSpecWithConfig(ctx, r.Client, r.Log, baseModel, &baseModel.Spec, config, "BaseModel")
}

// updateModelSpec updates ClusterBaseModel spec with configuration from ConfigMap
func (r *ClusterBaseModelReconciler) updateModelSpec(ctx context.Context, clusterBaseModel *v1beta1.ClusterBaseModel, config *modelagent.ModelConfig) error {
	return updateModelSpecWithConfig(ctx, r.Client, r.Log, clusterBaseModel, &clusterBaseModel.Spec, config, "ClusterBaseModel")
}

// updateModelSpecWithConfig is a shared utility function for updating model specs
func updateModelSpecWithConfig(ctx context.Context, kubeClient client.Client, log logr.Logger, obj client.Object, spec *v1beta1.BaseModelSpec, config *modelagent.ModelConfig, modelType string) error {
	// Use the shared utility function to update the spec
	if updated := updateSpecWithConfig(spec, config); updated {
		if err := kubeClient.Update(ctx, obj); err != nil {
			return fmt.Errorf("failed to update %s spec: %w", modelType, err)
		}
		log.Info(fmt.Sprintf("Updated %s spec with configuration data", modelType),
			"name", obj.GetName(), "namespace", obj.GetNamespace())
	}
	return nil
}

// metadataJobConfig converts the controllerconfig OmeAgentConfig to the
// internal MetadataJobConfig the Job builder consumes. Returns a zero-value
// config (with no Image) if no config is supplied; reconcilePVCStorage
// surfaces a clear error to the user via the model status in that case.
func (r *BaseModelReconciler) metadataJobConfig() MetadataJobConfig {
	return omeAgentConfigToMetadataJobConfig(r.OmeAgentConfig)
}

func (r *ClusterBaseModelReconciler) metadataJobConfig() MetadataJobConfig {
	return omeAgentConfigToMetadataJobConfig(r.OmeAgentConfig)
}

func omeAgentConfigToMetadataJobConfig(c *controllerconfig.OmeAgentConfig) MetadataJobConfig {
	if c == nil {
		return MetadataJobConfig{}
	}
	return MetadataJobConfig{
		Image:                   c.Image,
		ServiceAccount:          c.ServiceAccount,
		CPURequest:              c.CPURequest,
		MemoryRequest:           c.MemoryRequest,
		CPULimit:                c.CPULimit,
		MemoryLimit:             c.MemoryLimit,
		BackoffLimit:            c.BackoffLimit,
		TTLSecondsAfterFinished: c.TTLSecondsAfterFinished,
		NodeSelector:            c.NodeSelector,
		Tolerations:             c.Tolerations,
		Affinity:                c.Affinity,
		PriorityClassName:       c.PriorityClassName,
	}
}

// SetupWithManager sets up the BaseModel controller with the Manager
func (r *BaseModelReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1beta1.BaseModel{}).
		Owns(&batchv1.Job{}).
		Watches(
			&corev1.ConfigMap{},
			handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, obj client.Object) []reconcile.Request {
				return r.mapConfigMapToBaseModels(obj)
			}),
			// Two predicates OR'd: per-node basemodel-status ConfigMaps
			// (agent flow) AND per-PVC pvc-status ConfigMaps (extraction
			// Job's output). Both shapes carry one or more Data entries
			// keyed by GetModelConfigMapKey, so the same mapper works for
			// both.
			builder.WithPredicates(predicate.Or(createModelStatusConfigMapPredicate(), createPVCStatusConfigMapPredicate())),
		).
		Watches(
			&corev1.Node{},
			handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, obj client.Object) []reconcile.Request {
				return handleNodeDeletion(ctx, r.Client, r.Log, obj)
			}),
			builder.WithPredicates(createNodeDeletionPredicate()),
		).
		Watches(
			&corev1.PersistentVolumeClaim{},
			handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, obj client.Object) []reconcile.Request {
				return mapPVCToBaseModels(ctx, r.Client, r.Log, obj)
			}),
			builder.WithPredicates(createPVCPhasePredicate()),
		).
		Complete(r)
}

// SetupWithManager sets up the ClusterBaseModel controller with the Manager
func (r *ClusterBaseModelReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1beta1.ClusterBaseModel{}).
		Owns(&batchv1.Job{}).
		Watches(
			&corev1.ConfigMap{},
			handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, obj client.Object) []reconcile.Request {
				return r.mapConfigMapToClusterBaseModels(obj)
			}),
			builder.WithPredicates(predicate.Or(createModelStatusConfigMapPredicate(), createPVCStatusConfigMapPredicate())),
		).
		Watches(
			&corev1.Node{},
			handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, obj client.Object) []reconcile.Request {
				return handleNodeDeletion(ctx, r.Client, r.Log, obj)
			}),
			builder.WithPredicates(createNodeDeletionPredicate()),
		).
		Watches(
			&corev1.PersistentVolumeClaim{},
			handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, obj client.Object) []reconcile.Request {
				return mapPVCToClusterBaseModels(ctx, r.Client, r.Log, obj)
			}),
			builder.WithPredicates(createPVCPhasePredicate()),
		).
		Complete(r)
}

// createPVCStatusConfigMapPredicate fires on ConfigMaps in the OME
// namespace that carry the PVC-status label. Lets the controller observe
// the agent's writes immediately, without the 30s timer backstop.
func createPVCStatusConfigMapPredicate() predicate.Predicate {
	isPVC := func(obj client.Object) bool {
		if obj.GetNamespace() != constants.OMENamespace {
			return false
		}
		return obj.GetLabels()[constants.PVCStorageConfigMapLabel] == "true"
	}
	return predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool { return isPVC(e.Object) },
		UpdateFunc: func(e event.UpdateEvent) bool { return isPVC(e.ObjectNew) },
		DeleteFunc: func(e event.DeleteEvent) bool { return isPVC(e.Object) },
	}
}

// createNodeDeletionPredicate creates a predicate that only triggers on Node deletions
func createNodeDeletionPredicate() predicate.Predicate {
	return predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool {
			return false // Don't trigger on node creation
		},
		UpdateFunc: func(e event.UpdateEvent) bool {
			return false // Don't trigger on node updates
		},
		DeleteFunc: func(e event.DeleteEvent) bool {
			return true // Only trigger on node deletion
		},
	}
}

// handleNodeDeletion handles Node deletion events by cleaning up the corresponding ConfigMap.
// If no ConfigMap exists for this node, it simply skips without error.
// This is a shared utility function used by both BaseModel and ClusterBaseModel controllers.
func handleNodeDeletion(ctx context.Context, kubeClient client.Client, log logr.Logger, obj client.Object) []reconcile.Request {
	node, ok := obj.(*corev1.Node)
	if !ok {
		return nil
	}

	nodeName := node.GetName()
	log = log.WithValues("node", nodeName)

	// Check if a ConfigMap exists for this node
	configMap := &corev1.ConfigMap{}
	configMapKey := types.NamespacedName{
		Namespace: constants.OMENamespace,
		Name:      nodeName,
	}

	if err := kubeClient.Get(ctx, configMapKey, configMap); err != nil {
		if errors.IsNotFound(err) {
			// No ConfigMap for this node, nothing to clean up - this is normal
			// for nodes that never had model-agent running
			return nil
		}
		log.Error(err, "Failed to check ConfigMap for deleted node")
		return nil
	}

	// Verify this is a model status ConfigMap before deleting
	if !isModelStatusConfigMap(configMap) {
		return nil
	}

	// Delete the stale ConfigMap
	log.Info("Node deleted, cleaning up associated model status ConfigMap")
	if err := kubeClient.Delete(ctx, configMap); err != nil {
		if !errors.IsNotFound(err) {
			log.Error(err, "Failed to delete ConfigMap for deleted node")
		}
		return nil
	}

	log.Info("Successfully deleted ConfigMap for deleted node")
	return nil
}

// createModelStatusConfigMapPredicate creates the shared predicate for ConfigMap events
func createModelStatusConfigMapPredicate() predicate.Predicate {
	return predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool {
			return isModelStatusConfigMap(e.Object)
		},
		UpdateFunc: func(e event.UpdateEvent) bool {
			return isModelStatusConfigMap(e.ObjectNew)
		},
		DeleteFunc: func(e event.DeleteEvent) bool {
			return isModelStatusConfigMap(e.Object)
		},
	}
}

// isModelStatusConfigMap checks if a ConfigMap is a model status ConfigMap
func isModelStatusConfigMap(obj client.Object) bool {
	if obj.GetNamespace() != constants.OMENamespace {
		return false
	}
	labels := obj.GetLabels()
	if labels == nil {
		return false
	}
	return labels[constants.ModelStatusConfigMapLabel] == "true"
}

// mapConfigMapToBaseModels maps ConfigMap events to BaseModel reconcile requests
func (r *BaseModelReconciler) mapConfigMapToBaseModels(obj client.Object) []reconcile.Request {
	return mapConfigMapToModelRequests(obj, "basemodel", r.Log, true) // true = namespaced
}

// mapConfigMapToClusterBaseModels maps ConfigMap events to ClusterBaseModel reconcile requests
func (r *ClusterBaseModelReconciler) mapConfigMapToClusterBaseModels(obj client.Object) []reconcile.Request {
	return mapConfigMapToModelRequests(obj, "clusterbasemodel", r.Log, false) // false = cluster-scoped
}

// mapConfigMapToModelRequests is a shared utility for mapping ConfigMap events to model reconcile requests
func mapConfigMapToModelRequests(obj client.Object, keyPrefix string, log logr.Logger, isNamespaced bool) []reconcile.Request {
	var requests []reconcile.Request

	configMap, ok := obj.(*corev1.ConfigMap)
	if !ok {
		return requests
	}

	// Parse the ConfigMap data to find model references
	for key, data := range configMap.Data {
		var request reconcile.Request
		var shouldProcess bool

		// Parse using the centralized parsing function
		namespace, modelName, isClusterBaseModel, success := constants.ParseModelInfoFromConfigMapKey(key)
		if success {
			// Check if this matches the expected model type
			if (isNamespaced && !isClusterBaseModel) || (!isNamespaced && isClusterBaseModel) {
				if isNamespaced {
					request = reconcile.Request{
						NamespacedName: types.NamespacedName{
							Namespace: namespace,
							Name:      modelName,
						},
					}
				} else {
					request = reconcile.Request{
						NamespacedName: types.NamespacedName{
							Name: modelName,
						},
					}
				}
				shouldProcess = true
			}
		}

		if shouldProcess {
			// Parse the model entry to validate it's a valid entry
			var modelEntry modelagent.ModelEntry
			if err := json.Unmarshal([]byte(data), &modelEntry); err != nil {
				log.V(1).Info("Failed to parse model entry in ConfigMap", "configMap", configMap.Name, "key", key, "error", err)
				continue
			}
			requests = append(requests, request)
		}
	}

	return requests
}

// addToSlice adds an item to a slice if it doesn't already exist
func addToSlice(s []string, item string) []string {
	for _, existing := range s {
		if existing == item {
			return s
		}
	}
	return append(s, item)
}

// calculateLifecycleState determines the lifecycle state based on node status
func calculateLifecycleState(nodesReady, nodesFailed []string) v1beta1.LifeCycleState {
	if len(nodesReady) > 0 {
		return v1beta1.LifeCycleStateReady
	} else if len(nodesFailed) > 0 {
		return v1beta1.LifeCycleStateFailed
	} else {
		return v1beta1.LifeCycleStateInTransit
	}
}

// updateModelSpecWithRetry updates ClusterBaseModel spec with retry logic for resource conflicts
func (r *ClusterBaseModelReconciler) updateModelSpecWithRetry(ctx context.Context, clusterBaseModel *v1beta1.ClusterBaseModel, config *modelagent.ModelConfig) error {
	return retrySpecUpdate(ctx, r.Client, r.Log, clusterBaseModel, config,
		func(ctx context.Context, client client.Client, obj client.Object, config *modelagent.ModelConfig) error {
			return r.updateModelSpec(ctx, obj.(*v1beta1.ClusterBaseModel), config)
		})
}

// updateStatusWithRetry updates ClusterBaseModel status with retry logic for resource conflicts
func (r *ClusterBaseModelReconciler) updateStatusWithRetry(ctx context.Context, clusterBaseModel *v1beta1.ClusterBaseModel, nodesReady, nodesFailed []string) error {
	return updateModelStatusWithRetry(ctx, r.Client, r.Log, clusterBaseModel, nodesReady, nodesFailed, "ClusterBaseModel")
}

// updateStatusWithRetry updates BaseModel status with retry logic for resource conflicts
func (r *BaseModelReconciler) updateStatusWithRetry(ctx context.Context, baseModel *v1beta1.BaseModel, nodesReady, nodesFailed []string) error {
	return updateModelStatusWithRetry(ctx, r.Client, r.Log, baseModel, nodesReady, nodesFailed, "BaseModel")
}

// updateModelSpecWithRetry updates BaseModel spec with retry logic for resource conflicts
func (r *BaseModelReconciler) updateModelSpecWithRetry(ctx context.Context, baseModel *v1beta1.BaseModel, config *modelagent.ModelConfig) error {
	return retrySpecUpdate(ctx, r.Client, r.Log, baseModel, config,
		func(ctx context.Context, client client.Client, obj client.Object, config *modelagent.ModelConfig) error {
			return r.updateModelSpec(ctx, obj.(*v1beta1.BaseModel), config)
		})
}

// retryUpdate is a shared utility function for retrying updates with conflict resolution
func retryUpdate(ctx context.Context, kubeClient client.Client, log logr.Logger, obj client.Object, updateType string, updateFunc func(context.Context, client.Client, client.Object) error) error {
	const maxRetries = 3

	for i := 0; i < maxRetries; i++ {
		// Get the latest version
		latest := obj.DeepCopyObject().(client.Object)
		if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(obj), latest); err != nil {
			return fmt.Errorf("failed to get latest object version: %w", err)
		}

		// Execute the update function
		if err := updateFunc(ctx, kubeClient, latest); err != nil {
			if errors.IsConflict(err) && i < maxRetries-1 {
				// Exponential backoff: wait 100ms, 200ms, 400ms
				backoff := time.Millisecond * time.Duration(100<<uint(i))
				log.V(1).Info("Resource conflict during update, retrying with backoff",
					"updateType", updateType, "retry", i+1, "backoff", backoff, "object", client.ObjectKeyFromObject(obj))
				time.Sleep(backoff)
				continue
			}
			if errors.IsConflict(err) {
				return fmt.Errorf("failed to update %s after %d retries due to conflicts", updateType, maxRetries)
			}
			return fmt.Errorf("failed to update %s: %w", updateType, err)
		}
		return nil
	}
	return fmt.Errorf("failed to update %s after %d retries", updateType, maxRetries)
}

// updateModelStatusWithRetry is a shared utility function for updating model status with retry logic
func updateModelStatusWithRetry(ctx context.Context, kubeClient client.Client, log logr.Logger, obj client.Object, nodesReady, nodesFailed []string, modelType string) error {
	updateFunc := func(ctx context.Context, client client.Client, obj client.Object) error {
		// Get current status and update it
		var currentNodesReady, currentNodesFailed []string
		var currentState v1beta1.LifeCycleState

		// Type switch to handle both BaseModel and ClusterBaseModel
		switch model := obj.(type) {
		case *v1beta1.BaseModel:
			currentNodesReady = model.Status.NodesReady
			currentNodesFailed = model.Status.NodesFailed
			currentState = model.Status.State
		case *v1beta1.ClusterBaseModel:
			currentNodesReady = model.Status.NodesReady
			currentNodesFailed = model.Status.NodesFailed
			currentState = model.Status.State
		default:
			return fmt.Errorf("unsupported model type: %T", obj)
		}

		// Check if status needs update
		updated := false
		if !slices.Equal(currentNodesReady, nodesReady) {
			updated = true
		}
		if !slices.Equal(currentNodesFailed, nodesFailed) {
			updated = true
		}

		// Update lifecycle state
		newState := calculateLifecycleState(nodesReady, nodesFailed)
		if currentState != newState {
			updated = true
		}

		// Update status if changed
		if updated {
			// Apply the updates based on type
			switch model := obj.(type) {
			case *v1beta1.BaseModel:
				model.Status.NodesReady = nodesReady
				model.Status.NodesFailed = nodesFailed
				model.Status.State = newState
			case *v1beta1.ClusterBaseModel:
				model.Status.NodesReady = nodesReady
				model.Status.NodesFailed = nodesFailed
				model.Status.State = newState
			}

			if err := client.Status().Update(ctx, obj); err != nil {
				return err
			}
			log.Info(fmt.Sprintf("Updated %s status", modelType),
				"nodesReady", len(nodesReady),
				"nodesFailed", len(nodesFailed),
				"state", newState)
		}
		return nil
	}

	return retryUpdate(ctx, kubeClient, log, obj, "status", updateFunc)
}

// retrySpecUpdate is a shared utility function for retrying spec updates with conflict resolution
func retrySpecUpdate(ctx context.Context, kubeClient client.Client, log logr.Logger, obj client.Object, config *modelagent.ModelConfig, updateFunc func(context.Context, client.Client, client.Object, *modelagent.ModelConfig) error) error {
	wrappedUpdateFunc := func(ctx context.Context, client client.Client, obj client.Object) error {
		return updateFunc(ctx, client, obj, config)
	}
	return retryUpdate(ctx, kubeClient, log, obj, "spec", wrappedUpdateFunc)
}

// updateSpecWithConfig updates model spec fields with configuration from ConfigMap
// This utility function works with both BaseModel and ClusterBaseModel specs
func updateSpecWithConfig(spec *v1beta1.BaseModelSpec, config *modelagent.ModelConfig) bool {
	if spec == nil || config == nil {
		return false
	}

	updated := false

	// Update ModelType if not set
	if spec.ModelType == nil && config.ModelType != "" {
		modelType := config.ModelType
		spec.ModelType = &modelType
		updated = true
	}

	// Update ModelArchitecture if not set
	if spec.ModelArchitecture == nil && config.ModelArchitecture != "" {
		architecture := config.ModelArchitecture
		spec.ModelArchitecture = &architecture
		updated = true
	}

	// Update ModelParameterSize if not set
	if spec.ModelParameterSize == nil && config.ModelParameterSize != "" {
		paramSize := config.ModelParameterSize
		spec.ModelParameterSize = &paramSize
		updated = true
	}

	// Update capabilities if not set
	if len(spec.ModelCapabilities) == 0 && len(config.ModelCapabilities) > 0 {
		spec.ModelCapabilities = make([]string, len(config.ModelCapabilities))
		copy(spec.ModelCapabilities, config.ModelCapabilities)
		updated = true
	}

	// Update framework if not set
	if spec.ModelFramework == nil && config.ModelFramework != nil {
		name := config.ModelFramework["name"]
		version := config.ModelFramework["version"]
		if name != "" {
			framework := &v1beta1.ModelFrameworkSpec{Name: name}
			if version != "" {
				framework.Version = &version
			}
			spec.ModelFramework = framework
			updated = true
		}
	}

	// Update model format if not set
	if config.ModelFormat != nil {
		name := config.ModelFormat["name"]
		version := config.ModelFormat["version"]

		if name != "" && spec.ModelFormat.Name == "" {
			spec.ModelFormat.Name = name
			updated = true
		}

		if version != "" && spec.ModelFormat.Version == nil {
			versionValue := version
			spec.ModelFormat.Version = &versionValue
			updated = true
		}
	}

	// Update MaxTokens if not set and valid
	if spec.MaxTokens == nil && config.MaxTokens > 0 {
		spec.MaxTokens = &config.MaxTokens
		updated = true
	}

	return updated
}
