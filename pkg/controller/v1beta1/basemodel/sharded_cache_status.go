package basemodel

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/go-logr/logr"
	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/modelcache"
	"github.com/ome-projects/ome/pkg/modelparser"
)

const shardedModelCachePollInterval = 5 * time.Second

type shardedStatusTransition func(generation int64, spec *v1beta1.BaseModelSpec, status *v1beta1.ModelStatusSpec) bool

func reconcileShardedModelCacheStatus(ctx context.Context, kubeClient client.Client, log logr.Logger, provider modelcache.Provider, obj client.Object, modelType string) (ctrl.Result, error) {
	spec, status, err := modelSpecAndStatus(obj)
	if err != nil {
		return ctrl.Result{}, err
	}

	source := modelCacheSource(spec)
	if !provider.Configured() {
		transition := func(generation int64, spec *v1beta1.BaseModelSpec, status *v1beta1.ModelStatusSpec) bool {
			return applyProviderNotConfiguredStatus(generation, status)
		}
		return ctrl.Result{}, updateShardedModelStatusWithRetry(ctx, kubeClient, log, obj, modelType, provider.Name, transition)
	}
	if shardedCacheIdentityChanged(status, source.URI, provider.Name) {
		return ctrl.Result{RequeueAfter: shardedModelCachePollInterval}, updateShardedModelStatusWithRetry(ctx, kubeClient, log, obj, modelType, provider.Name, nil)
	}

	cacheClient := provider.Client
	sourceCondition := meta.FindStatusCondition(status.Conditions, v1beta1.ModelConditionSourceReachable)
	if sourceCondition == nil || sourceCondition.Status != metav1.ConditionTrue {
		sourceStatus, err := cacheClient.CheckSource(ctx, source)
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("check model cache source: %w", err)
		}

		result := ctrl.Result{}
		sourceReachable := sourceStatus == nil || sourceStatus.Reachable
		if sourceReachable {
			result.RequeueAfter = shardedModelCachePollInterval
		}
		transition := func(generation int64, spec *v1beta1.BaseModelSpec, status *v1beta1.ModelStatusSpec) bool {
			return applySourceStatus(generation, status, sourceStatus)
		}
		return result, updateShardedModelStatusWithRetry(ctx, kubeClient, log, obj, modelType, provider.Name, transition)
	}

	cachedCondition := meta.FindStatusCondition(status.Conditions, v1beta1.ModelConditionCached)
	if cachedCondition != nil && cachedCondition.Status == metav1.ConditionTrue {
		readyCondition := meta.FindStatusCondition(status.Conditions, v1beta1.ModelConditionReady)
		if readyCondition != nil && readyCondition.Status == metav1.ConditionTrue {
			return ctrl.Result{}, updateShardedModelStatusWithRetry(ctx, kubeClient, log, obj, modelType, provider.Name, nil)
		}
		return reconcileShardedModelMetadata(ctx, kubeClient, log, cacheClient, source, obj, modelType, provider.Name)
	}

	prewarmID := ""
	if status.Cache != nil {
		prewarmID = status.Cache.PrewarmID
	}
	if prewarmID == "" {
		inspection, result, err := reconcileShardedModelMetadataSpec(ctx, kubeClient, log, cacheClient, source, obj, modelType, provider.Name)
		if err != nil {
			return result, err
		}
		if result.Requeue || result.RequeueAfter > 0 {
			return result, nil
		}

		objects := shardedPrewarmObjectRefs(source, inspection)
		if len(objects) == 0 {
			return markShardedModelMetadataPending(ctx, kubeClient, log, obj, modelType, provider.Name, "model cache source inspection returned no objects to prewarm")
		}

		operation, err := cacheClient.StartPrewarm(ctx, modelcache.PrewarmRequest{
			Source:         source,
			Objects:        objects,
			IdempotencyKey: modelPrewarmIdempotencyKey(obj, source),
		})
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("start model cache prewarm: %w", err)
		}
		if operation == nil || operation.ID == "" {
			return ctrl.Result{}, fmt.Errorf("start model cache prewarm returned empty operation ID")
		}

		transition := func(generation int64, spec *v1beta1.BaseModelSpec, status *v1beta1.ModelStatusSpec) bool {
			return applyPrewarmStarted(generation, status, operation.ID)
		}
		return ctrl.Result{RequeueAfter: shardedModelCachePollInterval}, updateShardedModelStatusWithRetry(ctx, kubeClient, log, obj, modelType, provider.Name, transition)
	}

	prewarmStatus, err := cacheClient.GetPrewarmStatus(ctx, modelcache.PrewarmOperation{ID: prewarmID})
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("get model cache prewarm status: %w", err)
	}

	result := ctrl.Result{}
	if prewarmStatus == nil || prewarmStatus.Phase == modelcache.PrewarmPhasePending || prewarmStatus.Phase == modelcache.PrewarmPhaseRunning {
		result.RequeueAfter = shardedModelCachePollInterval
	}
	transition := func(generation int64, spec *v1beta1.BaseModelSpec, status *v1beta1.ModelStatusSpec) bool {
		return applyPrewarmStatus(generation, status, prewarmStatus)
	}
	if err := updateShardedModelStatusWithRetry(ctx, kubeClient, log, obj, modelType, provider.Name, transition); err != nil {
		return result, err
	}
	if prewarmStatus != nil && prewarmStatus.Phase == modelcache.PrewarmPhaseSucceeded {
		return reconcileShardedModelMetadata(ctx, kubeClient, log, cacheClient, source, obj, modelType, provider.Name)
	}
	return result, nil
}

func reconcileShardedModelMetadata(ctx context.Context, kubeClient client.Client, log logr.Logger, cacheClient modelcache.Client, source modelcache.Source, obj client.Object, modelType, backend string) (ctrl.Result, error) {
	parser, metadata, _, result, err := parseShardedModelMetadata(ctx, kubeClient, log, cacheClient, source, obj, modelType, backend)
	if err != nil {
		return result, err
	}
	if result.Requeue || result.RequeueAfter > 0 {
		return result, nil
	}

	return ctrl.Result{}, updateShardedModelMetadataWithRetry(ctx, kubeClient, log, obj, modelType, backend, parser, *metadata)
}

func reconcileShardedModelMetadataSpec(ctx context.Context, kubeClient client.Client, log logr.Logger, cacheClient modelcache.Client, source modelcache.Source, obj client.Object, modelType, backend string) (*modelcache.SourceInspection, ctrl.Result, error) {
	parser, metadata, inspection, result, err := parseShardedModelMetadata(ctx, kubeClient, log, cacheClient, source, obj, modelType, backend)
	if err != nil {
		return nil, result, err
	}
	if result.Requeue || result.RequeueAfter > 0 {
		return nil, result, nil
	}

	return inspection, ctrl.Result{}, updateShardedModelMetadataSpecWithRetry(ctx, kubeClient, log, obj, modelType, parser, *metadata)
}

func parseShardedModelMetadata(ctx context.Context, kubeClient client.Client, log logr.Logger, cacheClient modelcache.Client, source modelcache.Source, obj client.Object, modelType, backend string) (*modelparser.ModelConfigParser, *modelparser.ModelMetadata, *modelcache.SourceInspection, ctrl.Result, error) {
	parser := newShardedModelConfigParser()
	inspection, inputs, err := shardedModelMetadataInputs(ctx, cacheClient, source)
	if err != nil {
		result, updateErr := markShardedModelMetadataPending(ctx, kubeClient, log, obj, modelType, backend, fmt.Sprintf("model metadata is pending: %v", err))
		return nil, nil, nil, result, updateErr
	}

	metadata, err := parser.ParseModelConfigFromFiles(ctx, inputs, nil, nil)
	if err != nil {
		result, updateErr := markShardedModelMetadataPending(ctx, kubeClient, log, obj, modelType, backend, fmt.Sprintf("model metadata parsing is pending: %v", err))
		return nil, nil, nil, result, updateErr
	}
	if metadata == nil {
		result, updateErr := markShardedModelMetadataPending(ctx, kubeClient, log, obj, modelType, backend, "model metadata parsing produced no metadata")
		return nil, nil, nil, result, updateErr
	}

	return parser, metadata, inspection, ctrl.Result{}, nil
}

func newShardedModelConfigParser() *modelparser.ModelConfigParser {
	return modelparser.NewModelConfigParser(nil, zap.NewNop().Sugar())
}

func markShardedModelMetadataPending(ctx context.Context, kubeClient client.Client, log logr.Logger, obj client.Object, modelType, backend, message string) (ctrl.Result, error) {
	transition := func(generation int64, spec *v1beta1.BaseModelSpec, status *v1beta1.ModelStatusSpec) bool {
		return applyMetadataPending(generation, status, message)
	}
	return ctrl.Result{RequeueAfter: shardedModelCachePollInterval}, updateShardedModelStatusWithRetry(ctx, kubeClient, log, obj, modelType, backend, transition)
}

func updateShardedModelMetadataSpecWithRetry(ctx context.Context, kubeClient client.Client, log logr.Logger, obj client.Object, modelType string, parser *modelparser.ModelConfigParser, metadata modelparser.ModelMetadata) error {
	updateFunc := func(ctx context.Context, client client.Client, obj client.Object) error {
		spec, _, err := modelSpecAndStatus(obj)
		if err != nil {
			return err
		}

		if !parser.ApplyModelMetadata(spec, metadata) {
			return nil
		}
		if err := client.Update(ctx, obj); err != nil {
			return err
		}
		log.Info(fmt.Sprintf("Updated Sharded %s spec with cache metadata", modelType),
			"name", obj.GetName(), "namespace", obj.GetNamespace())

		return nil
	}

	return retryUpdate(ctx, kubeClient, log, obj, "sharded metadata spec", updateFunc)
}

func updateShardedModelMetadataWithRetry(ctx context.Context, kubeClient client.Client, log logr.Logger, obj client.Object, modelType, backend string, parser *modelparser.ModelConfigParser, metadata modelparser.ModelMetadata) error {
	updateFunc := func(ctx context.Context, client client.Client, obj client.Object) error {
		spec, status, err := modelSpecAndStatus(obj)
		if err != nil {
			return err
		}

		specUpdated := parser.ApplyModelMetadata(spec, metadata)
		if specUpdated {
			if err := client.Update(ctx, obj); err != nil {
				return err
			}
			log.Info(fmt.Sprintf("Updated Sharded %s spec with cache metadata", modelType),
				"name", obj.GetName(), "namespace", obj.GetNamespace())
		}

		statusUpdated := applyInitialShardedModelStatus(obj.GetGeneration(), spec, status, backend)
		statusUpdated = applyMetadataReady(obj.GetGeneration(), status) || statusUpdated
		if statusUpdated {
			if err := client.Status().Update(ctx, obj); err != nil {
				return err
			}
			log.Info(fmt.Sprintf("Updated Sharded %s status with cache metadata", modelType),
				"state", status.State,
				"cacheBackend", status.Cache.Backend,
				"sourceUri", status.Cache.SourceUri)
		}

		return nil
	}

	return retryUpdate(ctx, kubeClient, log, obj, "sharded metadata", updateFunc)
}

func updateShardedModelStatusWithRetry(ctx context.Context, kubeClient client.Client, log logr.Logger, obj client.Object, modelType string, backend string, transition shardedStatusTransition) error {
	updateFunc := func(ctx context.Context, client client.Client, obj client.Object) error {
		spec, status, err := modelSpecAndStatus(obj)
		if err != nil {
			return err
		}

		updated := applyInitialShardedModelStatus(obj.GetGeneration(), spec, status, backend)
		if transition != nil {
			updated = transition(obj.GetGeneration(), spec, status) || updated
		}
		if !updated {
			return nil
		}

		if err := client.Status().Update(ctx, obj); err != nil {
			return err
		}
		log.Info(fmt.Sprintf("Updated Sharded %s status", modelType),
			"state", status.State,
			"cacheBackend", status.Cache.Backend,
			"sourceUri", status.Cache.SourceUri)

		return nil
	}

	return retryUpdate(ctx, kubeClient, log, obj, "sharded status", updateFunc)
}

func applyInitialShardedModelStatus(generation int64, spec *v1beta1.BaseModelSpec, status *v1beta1.ModelStatusSpec, backend string) bool {
	updated := false

	if len(status.NodesReady) != 0 {
		status.NodesReady = nil
		updated = true
	}
	if len(status.NodesFailed) != 0 {
		status.NodesFailed = nil
		updated = true
	}
	if status.State != v1beta1.LifeCycleStateImporting {
		status.State = v1beta1.LifeCycleStateImporting
		updated = true
	}

	sourceURI := storageSourceURI(spec)
	if status.Cache == nil {
		status.Cache = &v1beta1.ModelCacheStatus{}
		updated = true
	}
	cacheIdentityChanged := shardedCacheIdentityChanged(status, sourceURI, backend)
	if status.Cache.Backend != backend {
		status.Cache.Backend = backend
		updated = true
	}
	if cacheIdentityChanged {
		updated = resetShardedCacheProgress(status.Cache) || updated
	}
	if status.Cache.SourceUri != sourceURI {
		status.Cache.SourceUri = sourceURI
		updated = true
	}

	updated = setInitialModelCondition(status, cacheIdentityChanged, metav1.Condition{
		Type:               v1beta1.ModelConditionSourceReachable,
		Status:             metav1.ConditionUnknown,
		Reason:             v1beta1.ModelConditionReasonNotChecked,
		Message:            "model cache source reachability has not been checked yet",
		ObservedGeneration: generation,
	}) || updated
	updated = setInitialModelCondition(status, cacheIdentityChanged, metav1.Condition{
		Type:               v1beta1.ModelConditionCached,
		Status:             metav1.ConditionFalse,
		Reason:             v1beta1.ModelConditionReasonPrewarmPending,
		Message:            "model cache prewarm has not started yet",
		ObservedGeneration: generation,
	}) || updated
	updated = setInitialModelCondition(status, cacheIdentityChanged, metav1.Condition{
		Type:               v1beta1.ModelConditionReady,
		Status:             metav1.ConditionFalse,
		Reason:             v1beta1.ModelConditionReasonCacheNotReady,
		Message:            "model is not ready until model cache prewarm and metadata parsing complete",
		ObservedGeneration: generation,
	}) || updated

	return updated
}

func applyProviderNotConfiguredStatus(generation int64, status *v1beta1.ModelStatusSpec) bool {
	updated := false
	if status.State != v1beta1.LifeCycleStateImporting {
		status.State = v1beta1.LifeCycleStateImporting
		updated = true
	}

	message := "model cache provider is not configured"
	updated = setModelCondition(status, metav1.Condition{
		Type:               v1beta1.ModelConditionSourceReachable,
		Status:             metav1.ConditionUnknown,
		Reason:             v1beta1.ModelConditionReasonProviderNotConfigured,
		Message:            message,
		ObservedGeneration: generation,
	}) || updated
	updated = setModelCondition(status, metav1.Condition{
		Type:               v1beta1.ModelConditionCached,
		Status:             metav1.ConditionFalse,
		Reason:             v1beta1.ModelConditionReasonProviderNotConfigured,
		Message:            message,
		ObservedGeneration: generation,
	}) || updated
	updated = setModelCondition(status, metav1.Condition{
		Type:               v1beta1.ModelConditionReady,
		Status:             metav1.ConditionFalse,
		Reason:             v1beta1.ModelConditionReasonProviderNotConfigured,
		Message:            message,
		ObservedGeneration: generation,
	}) || updated

	return updated
}

func applySourceStatus(generation int64, status *v1beta1.ModelStatusSpec, sourceStatus *modelcache.SourceStatus) bool {
	if sourceStatus == nil {
		sourceStatus = &modelcache.SourceStatus{Reachable: true}
	}

	if sourceStatus.Reachable {
		return setModelCondition(status, metav1.Condition{
			Type:               v1beta1.ModelConditionSourceReachable,
			Status:             metav1.ConditionTrue,
			Reason:             reasonOrDefault(sourceStatus.Reason, v1beta1.ModelConditionReasonSourceReachable),
			Message:            messageOrDefault(sourceStatus.Message, "model source is reachable through the model cache"),
			ObservedGeneration: generation,
		})
	}

	updated := false
	if status.State != v1beta1.LifeCycleStateFailed {
		status.State = v1beta1.LifeCycleStateFailed
		updated = true
	}
	updated = setModelCondition(status, metav1.Condition{
		Type:               v1beta1.ModelConditionSourceReachable,
		Status:             metav1.ConditionFalse,
		Reason:             reasonOrDefault(sourceStatus.Reason, v1beta1.ModelConditionReasonSourceNotReachable),
		Message:            messageOrDefault(sourceStatus.Message, "model source is not reachable through the model cache"),
		ObservedGeneration: generation,
	}) || updated
	updated = setModelCondition(status, metav1.Condition{
		Type:               v1beta1.ModelConditionReady,
		Status:             metav1.ConditionFalse,
		Reason:             reasonOrDefault(sourceStatus.Reason, v1beta1.ModelConditionReasonSourceNotReachable),
		Message:            messageOrDefault(sourceStatus.Message, "model source is not reachable through the model cache"),
		ObservedGeneration: generation,
	}) || updated

	return updated
}

func applyPrewarmStarted(generation int64, status *v1beta1.ModelStatusSpec, operationID string) bool {
	updated := false
	if status.Cache == nil {
		status.Cache = &v1beta1.ModelCacheStatus{}
		updated = true
	}
	if status.Cache.PrewarmID != operationID {
		status.Cache.PrewarmID = operationID
		updated = true
	}
	if status.State != v1beta1.LifeCycleStateImporting {
		status.State = v1beta1.LifeCycleStateImporting
		updated = true
	}
	updated = setModelCondition(status, metav1.Condition{
		Type:               v1beta1.ModelConditionCached,
		Status:             metav1.ConditionFalse,
		Reason:             v1beta1.ModelConditionReasonPrewarmPending,
		Message:            "model cache prewarm has been submitted",
		ObservedGeneration: generation,
	}) || updated
	updated = setModelCondition(status, metav1.Condition{
		Type:               v1beta1.ModelConditionReady,
		Status:             metav1.ConditionFalse,
		Reason:             v1beta1.ModelConditionReasonCacheNotReady,
		Message:            "model is not ready until model cache prewarm and metadata parsing complete",
		ObservedGeneration: generation,
	}) || updated

	return updated
}

func applyPrewarmStatus(generation int64, status *v1beta1.ModelStatusSpec, prewarmStatus *modelcache.PrewarmStatus) bool {
	if prewarmStatus == nil {
		prewarmStatus = &modelcache.PrewarmStatus{Phase: modelcache.PrewarmPhaseRunning}
	}

	updated := applyPrewarmProgress(status, prewarmStatus.Progress)
	if status.Cache != nil && prewarmStatus.Operation.ID != "" && status.Cache.PrewarmID != prewarmStatus.Operation.ID {
		status.Cache.PrewarmID = prewarmStatus.Operation.ID
		updated = true
	}

	switch prewarmStatus.Phase {
	case modelcache.PrewarmPhaseSucceeded:
		if status.State != v1beta1.LifeCycleStateImporting {
			status.State = v1beta1.LifeCycleStateImporting
			updated = true
		}
		updated = setModelCondition(status, metav1.Condition{
			Type:               v1beta1.ModelConditionCached,
			Status:             metav1.ConditionTrue,
			Reason:             reasonOrDefault(prewarmStatus.Reason, v1beta1.ModelConditionReasonPrewarmSucceeded),
			Message:            messageOrDefault(prewarmStatus.Message, "model cache prewarm completed"),
			ObservedGeneration: generation,
		}) || updated
		updated = setModelCondition(status, metav1.Condition{
			Type:               v1beta1.ModelConditionReady,
			Status:             metav1.ConditionFalse,
			Reason:             v1beta1.ModelConditionReasonMetadataPending,
			Message:            "model metadata parsing has not completed",
			ObservedGeneration: generation,
		}) || updated
	case modelcache.PrewarmPhaseFailed:
		updated = applyTerminalPrewarmFailure(generation, status, v1beta1.ModelConditionReasonPrewarmFailed, prewarmStatus) || updated
	case modelcache.PrewarmPhaseCanceled:
		updated = applyTerminalPrewarmFailure(generation, status, v1beta1.ModelConditionReasonPrewarmCanceled, prewarmStatus) || updated
	case modelcache.PrewarmPhasePending:
		updated = applyActivePrewarmCondition(generation, status, v1beta1.ModelConditionReasonPrewarmPending, "model cache prewarm is pending", prewarmStatus) || updated
	default:
		updated = applyActivePrewarmCondition(generation, status, v1beta1.ModelConditionReasonPrewarmRunning, "model cache prewarm is running", prewarmStatus) || updated
	}

	return updated
}

func applyPrewarmProgress(status *v1beta1.ModelStatusSpec, progress modelcache.PrewarmProgress) bool {
	updated := false
	if status.Cache == nil {
		status.Cache = &v1beta1.ModelCacheStatus{}
		updated = true
	}

	if progress.TotalBytes != nil && setInt64Ptr(&status.Cache.TotalBytes, *progress.TotalBytes) {
		updated = true
	}
	if progress.CachedBytes != nil && setInt64Ptr(&status.Cache.CachedBytes, *progress.CachedBytes) {
		updated = true
	}
	if progress.TotalChunks != nil && setInt32Ptr(&status.Cache.TotalChunks, *progress.TotalChunks) {
		updated = true
	}
	if progress.CachedChunks != nil && setInt32Ptr(&status.Cache.CachedChunks, *progress.CachedChunks) {
		updated = true
	}

	return updated
}

func applyActivePrewarmCondition(generation int64, status *v1beta1.ModelStatusSpec, defaultReason, defaultMessage string, prewarmStatus *modelcache.PrewarmStatus) bool {
	updated := false
	if status.State != v1beta1.LifeCycleStateImporting {
		status.State = v1beta1.LifeCycleStateImporting
		updated = true
	}
	updated = setModelCondition(status, metav1.Condition{
		Type:               v1beta1.ModelConditionCached,
		Status:             metav1.ConditionFalse,
		Reason:             reasonOrDefault(prewarmStatus.Reason, defaultReason),
		Message:            messageOrDefault(prewarmStatus.Message, defaultMessage),
		ObservedGeneration: generation,
	}) || updated
	updated = setModelCondition(status, metav1.Condition{
		Type:               v1beta1.ModelConditionReady,
		Status:             metav1.ConditionFalse,
		Reason:             v1beta1.ModelConditionReasonCacheNotReady,
		Message:            "model is not ready until model cache prewarm and metadata parsing complete",
		ObservedGeneration: generation,
	}) || updated

	return updated
}

func applyTerminalPrewarmFailure(generation int64, status *v1beta1.ModelStatusSpec, defaultReason string, prewarmStatus *modelcache.PrewarmStatus) bool {
	updated := false
	if status.State != v1beta1.LifeCycleStateFailed {
		status.State = v1beta1.LifeCycleStateFailed
		updated = true
	}

	reason := reasonOrDefault(prewarmStatus.Reason, defaultReason)
	message := messageOrDefault(prewarmStatus.Message, "model cache prewarm did not complete")
	updated = setModelCondition(status, metav1.Condition{
		Type:               v1beta1.ModelConditionCached,
		Status:             metav1.ConditionFalse,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: generation,
	}) || updated
	updated = setModelCondition(status, metav1.Condition{
		Type:               v1beta1.ModelConditionReady,
		Status:             metav1.ConditionFalse,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: generation,
	}) || updated

	return updated
}

func applyMetadataPending(generation int64, status *v1beta1.ModelStatusSpec, message string) bool {
	updated := false
	if status.State != v1beta1.LifeCycleStateImporting {
		status.State = v1beta1.LifeCycleStateImporting
		updated = true
	}
	updated = setModelCondition(status, metav1.Condition{
		Type:               v1beta1.ModelConditionReady,
		Status:             metav1.ConditionFalse,
		Reason:             v1beta1.ModelConditionReasonMetadataPending,
		Message:            messageOrDefault(message, "model metadata parsing has not completed"),
		ObservedGeneration: generation,
	}) || updated
	return updated
}

func applyMetadataReady(generation int64, status *v1beta1.ModelStatusSpec) bool {
	updated := false
	if status.State != v1beta1.LifeCycleStateReady {
		status.State = v1beta1.LifeCycleStateReady
		updated = true
	}
	updated = setModelCondition(status, metav1.Condition{
		Type:               v1beta1.ModelConditionReady,
		Status:             metav1.ConditionTrue,
		Reason:             v1beta1.ModelConditionReasonMetadataParsed,
		Message:            "model cache prewarm and metadata parsing completed",
		ObservedGeneration: generation,
	}) || updated
	return updated
}

func modelSpecAndStatus(obj client.Object) (*v1beta1.BaseModelSpec, *v1beta1.ModelStatusSpec, error) {
	switch model := obj.(type) {
	case *v1beta1.BaseModel:
		return &model.Spec, &model.Status, nil
	case *v1beta1.ClusterBaseModel:
		return &model.Spec, &model.Status, nil
	default:
		return nil, nil, fmt.Errorf("unsupported model type: %T", obj)
	}
}

func isShardedDistribution(distribution *v1beta1.Distribution) bool {
	return distribution != nil && *distribution == v1beta1.DistributionSharded
}

func storageSourceURI(spec *v1beta1.BaseModelSpec) string {
	if spec == nil || spec.Storage == nil || spec.Storage.StorageUri == nil {
		return ""
	}
	return *spec.Storage.StorageUri
}

func modelCacheSource(spec *v1beta1.BaseModelSpec) modelcache.Source {
	source := modelcache.Source{
		URI: storageSourceURI(spec),
	}
	if spec == nil || spec.Storage == nil || spec.Storage.Parameters == nil {
		return source
	}

	parameters := make(map[string]string, len(*spec.Storage.Parameters))
	for key, value := range *spec.Storage.Parameters {
		parameters[key] = value
	}
	source.Parameters = parameters
	return source
}

func modelPrewarmIdempotencyKey(obj client.Object, source modelcache.Source) string {
	return fmt.Sprintf("%s/%s/%s/%d/%s", modelKind(obj), obj.GetNamespace(), obj.GetName(), obj.GetGeneration(), modelCacheSourceHash(source))
}

func modelKind(obj client.Object) string {
	switch obj.(type) {
	case *v1beta1.BaseModel:
		return "BaseModel"
	case *v1beta1.ClusterBaseModel:
		return "ClusterBaseModel"
	default:
		return "Model"
	}
}

func modelCacheSourceHash(source modelcache.Source) string {
	sourceBytes, err := json.Marshal(source)
	if err != nil {
		return source.URI
	}
	sum := sha256.Sum256(sourceBytes)
	return hex.EncodeToString(sum[:])
}

func shardedCacheSourceChanged(status *v1beta1.ModelStatusSpec, sourceURI string) bool {
	if status.Cache == nil || status.Cache.SourceUri == sourceURI {
		return false
	}
	return status.Cache.SourceUri != "" || shardedCacheProgressStarted(status.Cache)
}

func shardedCacheBackendChanged(status *v1beta1.ModelStatusSpec, backend string) bool {
	if status.Cache == nil || status.Cache.Backend == backend {
		return false
	}
	return status.Cache.Backend != "" || shardedCacheProgressStarted(status.Cache)
}

func shardedCacheIdentityChanged(status *v1beta1.ModelStatusSpec, sourceURI, backend string) bool {
	return shardedCacheSourceChanged(status, sourceURI) || shardedCacheBackendChanged(status, backend)
}

func shardedCacheProgressStarted(cache *v1beta1.ModelCacheStatus) bool {
	return cache.PrewarmID != "" ||
		cache.TotalBytes != nil ||
		cache.CachedBytes != nil ||
		cache.TotalChunks != nil ||
		cache.CachedChunks != nil
}

func resetShardedCacheProgress(status *v1beta1.ModelCacheStatus) bool {
	updated := false
	if status.PrewarmID != "" {
		status.PrewarmID = ""
		updated = true
	}
	if status.TotalBytes != nil {
		status.TotalBytes = nil
		updated = true
	}
	if status.CachedBytes != nil {
		status.CachedBytes = nil
		updated = true
	}
	if status.TotalChunks != nil {
		status.TotalChunks = nil
		updated = true
	}
	if status.CachedChunks != nil {
		status.CachedChunks = nil
		updated = true
	}
	return updated
}

func setModelCondition(status *v1beta1.ModelStatusSpec, condition metav1.Condition) bool {
	existing := meta.FindStatusCondition(status.Conditions, condition.Type)
	if existing != nil &&
		existing.Status == condition.Status &&
		existing.Reason == condition.Reason &&
		existing.Message == condition.Message &&
		existing.ObservedGeneration == condition.ObservedGeneration {
		return false
	}

	meta.SetStatusCondition(&status.Conditions, condition)
	return true
}

func setInitialModelCondition(status *v1beta1.ModelStatusSpec, reset bool, condition metav1.Condition) bool {
	if reset {
		return setModelCondition(status, condition)
	}
	return setModelConditionIfMissing(status, condition)
}

func setModelConditionIfMissing(status *v1beta1.ModelStatusSpec, condition metav1.Condition) bool {
	existing := meta.FindStatusCondition(status.Conditions, condition.Type)
	if existing == nil {
		meta.SetStatusCondition(&status.Conditions, condition)
		return true
	}
	if existing.Status == condition.Status &&
		existing.Reason == condition.Reason &&
		existing.Message == condition.Message {
		return setModelCondition(status, condition)
	}
	return false
}

func reasonOrDefault(reason, defaultReason string) string {
	if reason != "" {
		return reason
	}
	return defaultReason
}

func messageOrDefault(message, defaultMessage string) string {
	if message != "" {
		return message
	}
	return defaultMessage
}

func setInt64Ptr(target **int64, value int64) bool {
	if *target != nil && **target == value {
		return false
	}
	valueCopy := value
	*target = &valueCopy
	return true
}

func setInt32Ptr(target **int32, value int32) bool {
	if *target != nil && **target == value {
		return false
	}
	valueCopy := value
	*target = &valueCopy
	return true
}
