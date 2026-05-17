package sharded

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/go-logr/logr"
	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/basemodel/shared"
	"github.com/ome-projects/ome/pkg/modelparser"
)

const (
	pollInterval = 5 * time.Second
)

type statusTransition func(generation int64, spec *v1beta1.BaseModelSpec, status *v1beta1.ModelStatusSpec) bool

// Reconcile drives the lazy-fetch flow: check source reachability via
// the provider, then parse metadata. The daemon pulls bytes on demand
// at first inference, so Ready = source reachable + metadata parsed,
// not "bytes placed."
func Reconcile(ctx context.Context, kubeClient client.Client, log logr.Logger, provider Provider, obj client.Object, modelType string) (ctrl.Result, error) {
	spec, status, err := shared.ModelSpecAndStatus(obj)
	if err != nil {
		return ctrl.Result{}, err
	}
	source := modelCacheSource(spec)

	if !provider.Configured() {
		transition := func(generation int64, spec *v1beta1.BaseModelSpec, status *v1beta1.ModelStatusSpec) bool {
			return applyProviderNotConfiguredStatus(generation, status)
		}
		return ctrl.Result{}, updateModelStatusWithRetry(ctx, kubeClient, log, obj, modelType, provider.Name, transition)
	}
	if cacheIdentityChanged(status, source.URI, provider.Name) {
		return ctrl.Result{RequeueAfter: pollInterval}, updateModelStatusWithRetry(ctx, kubeClient, log, obj, modelType, provider.Name, nil)
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
			result.RequeueAfter = pollInterval
		}
		transition := func(generation int64, spec *v1beta1.BaseModelSpec, status *v1beta1.ModelStatusSpec) bool {
			return applySourceStatus(generation, status, sourceStatus)
		}
		return result, updateModelStatusWithRetry(ctx, kubeClient, log, obj, modelType, provider.Name, transition)
	}

	readyCondition := meta.FindStatusCondition(status.Conditions, v1beta1.ModelConditionReady)
	if readyCondition != nil && readyCondition.Status == metav1.ConditionTrue {
		// Calling updateModelStatusWithRetry here would re-run
		// applyInitialModelStatus and force state back to Importing —
		// silently regressing a Ready model on every poll. Just stamp
		// observedGeneration if the spec has changed since we last
		// reconciled.
		return ctrl.Result{}, stampObservedGenerationIfStale(ctx, kubeClient, log, obj)
	}
	return reconcileModelMetadata(ctx, kubeClient, log, cacheClient, source, obj, modelType, provider.Name)
}

// stampObservedGenerationIfStale advances status.observedGeneration to
// match obj.Generation without mutating state or conditions. Idempotent:
// returns immediately when already in sync. Used by the Ready=True
// reconcile path so that a healthy model isn't churned every poll.
func stampObservedGenerationIfStale(ctx context.Context, kubeClient client.Client, log logr.Logger, obj client.Object) error {
	return shared.RetryUpdate(ctx, kubeClient, log, obj, "sharded stamp generation", func(ctx context.Context, c client.Client, latest client.Object) error {
		_, status, err := shared.ModelSpecAndStatus(latest)
		if err != nil {
			return err
		}
		if status.ObservedGeneration == latest.GetGeneration() {
			return nil
		}
		shared.StampObservedReconcile(latest, status)
		return c.Status().Update(ctx, latest)
	})
}

func reconcileModelMetadata(ctx context.Context, kubeClient client.Client, log logr.Logger, cacheClient Client, source Source, obj client.Object, modelType, backend string) (ctrl.Result, error) {
	parser, metadata, _, result, err := parseModelMetadata(ctx, kubeClient, log, cacheClient, source, obj, modelType, backend)
	if err != nil {
		return result, err
	}
	if result.Requeue || result.RequeueAfter > 0 || metadata == nil {
		return result, nil
	}

	return ctrl.Result{}, updateModelMetadataWithRetry(ctx, kubeClient, log, obj, modelType, backend, parser, *metadata)
}

func reconcileModelMetadataSpec(ctx context.Context, kubeClient client.Client, log logr.Logger, cacheClient Client, source Source, obj client.Object, modelType, backend string) (*SourceInspection, ctrl.Result, error) {
	parser, metadata, inspection, result, err := parseModelMetadata(ctx, kubeClient, log, cacheClient, source, obj, modelType, backend)
	if err != nil {
		return nil, result, err
	}
	if result.Requeue || result.RequeueAfter > 0 || metadata == nil {
		return nil, result, nil
	}

	return inspection, ctrl.Result{}, updateModelMetadataSpecWithRetry(ctx, kubeClient, log, obj, modelType, parser, *metadata)
}

func parseModelMetadata(ctx context.Context, kubeClient client.Client, log logr.Logger, cacheClient Client, source Source, obj client.Object, modelType, backend string) (*modelparser.ModelConfigParser, *modelparser.ModelMetadata, *SourceInspection, ctrl.Result, error) {
	parser := newModelConfigParser()
	inspection, inputs, err := modelMetadataInputs(ctx, cacheClient, source)
	if err != nil {
		if errors.Is(err, ErrSourceUnparseable) {
			updateErr := markModelMetadataUnparseable(ctx, kubeClient, log, obj, modelType, backend, fmt.Sprintf("model metadata cannot be parsed: %v", err))
			return nil, nil, nil, ctrl.Result{}, updateErr
		}
		result, updateErr := markModelMetadataPending(ctx, kubeClient, log, obj, modelType, backend, fmt.Sprintf("model metadata is pending: %v", err))
		return nil, nil, nil, result, updateErr
	}

	metadata, err := parser.ParseModelConfigFromFiles(ctx, inputs, nil, nil)
	if err != nil {
		result, updateErr := markModelMetadataPending(ctx, kubeClient, log, obj, modelType, backend, fmt.Sprintf("model metadata parsing is pending: %v", err))
		return nil, nil, nil, result, updateErr
	}
	if metadata == nil {
		result, updateErr := markModelMetadataPending(ctx, kubeClient, log, obj, modelType, backend, "model metadata parsing produced no metadata")
		return nil, nil, nil, result, updateErr
	}

	return parser, metadata, inspection, ctrl.Result{}, nil
}

func newModelConfigParser() *modelparser.ModelConfigParser {
	return modelparser.NewModelConfigParser(nil, zap.NewNop().Sugar())
}

func markModelMetadataPending(ctx context.Context, kubeClient client.Client, log logr.Logger, obj client.Object, modelType, backend, message string) (ctrl.Result, error) {
	transition := func(generation int64, spec *v1beta1.BaseModelSpec, status *v1beta1.ModelStatusSpec) bool {
		return applyMetadataPending(generation, status, message)
	}
	return ctrl.Result{RequeueAfter: pollInterval}, updateModelStatusWithRetry(ctx, kubeClient, log, obj, modelType, backend, transition)
}

// markModelMetadataUnparseable lands the model in Failed without a
// requeue — only an operator-driven spec change (generation bump)
// should re-trigger.
func markModelMetadataUnparseable(ctx context.Context, kubeClient client.Client, log logr.Logger, obj client.Object, modelType, backend, message string) error {
	transition := func(generation int64, spec *v1beta1.BaseModelSpec, status *v1beta1.ModelStatusSpec) bool {
		return applyMetadataUnparseable(generation, status, message)
	}
	return updateModelStatusWithRetry(ctx, kubeClient, log, obj, modelType, backend, transition)
}

func updateModelMetadataSpecWithRetry(ctx context.Context, kubeClient client.Client, log logr.Logger, obj client.Object, modelType string, parser *modelparser.ModelConfigParser, metadata modelparser.ModelMetadata) error {
	updateFunc := func(ctx context.Context, client client.Client, obj client.Object) error {
		spec, _, err := shared.ModelSpecAndStatus(obj)
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

	return shared.RetryUpdate(ctx, kubeClient, log, obj, "sharded metadata spec", updateFunc)
}

func updateModelMetadataWithRetry(ctx context.Context, kubeClient client.Client, log logr.Logger, obj client.Object, modelType, backend string, parser *modelparser.ModelConfigParser, metadata modelparser.ModelMetadata) error {
	updateFunc := func(ctx context.Context, client client.Client, obj client.Object) error {
		spec, status, err := shared.ModelSpecAndStatus(obj)
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

		statusUpdated := applyInitialModelStatus(obj.GetGeneration(), spec, status, backend)
		statusUpdated = applyMetadataReady(obj.GetGeneration(), status) || statusUpdated
		if statusUpdated {
			shared.StampObservedReconcile(obj, status)
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

	return shared.RetryUpdate(ctx, kubeClient, log, obj, "sharded metadata", updateFunc)
}

func updateModelStatusWithRetry(ctx context.Context, kubeClient client.Client, log logr.Logger, obj client.Object, modelType string, backend string, transition statusTransition) error {
	updateFunc := func(ctx context.Context, client client.Client, obj client.Object) error {
		spec, status, err := shared.ModelSpecAndStatus(obj)
		if err != nil {
			return err
		}

		updated := applyInitialModelStatus(obj.GetGeneration(), spec, status, backend)
		if transition != nil {
			updated = transition(obj.GetGeneration(), spec, status) || updated
		}
		if !updated {
			return nil
		}
		shared.StampObservedReconcile(obj, status)

		if err := client.Status().Update(ctx, obj); err != nil {
			return err
		}
		log.Info(fmt.Sprintf("Updated Sharded %s status", modelType),
			"state", status.State,
			"cacheBackend", status.Cache.Backend,
			"sourceUri", status.Cache.SourceUri)

		return nil
	}

	return shared.RetryUpdate(ctx, kubeClient, log, obj, "sharded status", updateFunc)
}

func applyInitialModelStatus(generation int64, spec *v1beta1.BaseModelSpec, status *v1beta1.ModelStatusSpec, backend string) bool {
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
	identityChanged := cacheIdentityChanged(status, sourceURI, backend)
	if status.Cache.Backend != backend {
		status.Cache.Backend = backend
		updated = true
	}
	if status.Cache.SourceUri != sourceURI {
		status.Cache.SourceUri = sourceURI
		updated = true
	}

	updated = setInitialModelCondition(status, identityChanged, metav1.Condition{
		Type:               v1beta1.ModelConditionSourceReachable,
		Status:             metav1.ConditionUnknown,
		Reason:             v1beta1.ModelConditionReasonNotChecked,
		Message:            "model cache source reachability has not been checked yet",
		ObservedGeneration: generation,
	}) || updated
	updated = setInitialModelCondition(status, identityChanged, metav1.Condition{
		Type:               v1beta1.ModelConditionMetadataExtracted,
		Status:             metav1.ConditionFalse,
		Reason:             v1beta1.ModelConditionReasonMetadataPending,
		Message:            "model metadata has not been extracted yet",
		ObservedGeneration: generation,
	}) || updated
	updated = setInitialModelCondition(status, identityChanged, metav1.Condition{
		Type:               v1beta1.ModelConditionReady,
		Status:             metav1.ConditionFalse,
		Reason:             v1beta1.ModelConditionReasonMetadataPending,
		Message:            "model is not ready until model metadata parsing completes",
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
		Type:               v1beta1.ModelConditionReady,
		Status:             metav1.ConditionFalse,
		Reason:             v1beta1.ModelConditionReasonProviderNotConfigured,
		Message:            message,
		ObservedGeneration: generation,
	}) || updated

	return updated
}

func applySourceStatus(generation int64, status *v1beta1.ModelStatusSpec, sourceStatus *SourceStatus) bool {
	if sourceStatus == nil {
		sourceStatus = &SourceStatus{Reachable: true}
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

func applyMetadataPending(generation int64, status *v1beta1.ModelStatusSpec, message string) bool {
	updated := false
	if status.State != v1beta1.LifeCycleStateImporting {
		status.State = v1beta1.LifeCycleStateImporting
		updated = true
	}
	pendingMsg := messageOrDefault(message, "model metadata parsing has not completed")
	updated = setModelCondition(status, metav1.Condition{
		Type:               v1beta1.ModelConditionMetadataExtracted,
		Status:             metav1.ConditionFalse,
		Reason:             v1beta1.ModelConditionReasonMetadataPending,
		Message:            pendingMsg,
		ObservedGeneration: generation,
	}) || updated
	updated = setModelCondition(status, metav1.Condition{
		Type:               v1beta1.ModelConditionReady,
		Status:             metav1.ConditionFalse,
		Reason:             v1beta1.ModelConditionReasonMetadataPending,
		Message:            pendingMsg,
		ObservedGeneration: generation,
	}) || updated
	return updated
}

func applyMetadataUnparseable(generation int64, status *v1beta1.ModelStatusSpec, message string) bool {
	updated := false
	if status.State != v1beta1.LifeCycleStateFailed {
		status.State = v1beta1.LifeCycleStateFailed
		updated = true
	}
	unparseableMsg := messageOrDefault(message, "model metadata cannot be parsed by the configured cache provider")
	updated = setModelCondition(status, metav1.Condition{
		Type:               v1beta1.ModelConditionMetadataExtracted,
		Status:             metav1.ConditionFalse,
		Reason:             v1beta1.ModelConditionReasonMetadataUnparseable,
		Message:            unparseableMsg,
		ObservedGeneration: generation,
	}) || updated
	updated = setModelCondition(status, metav1.Condition{
		Type:               v1beta1.ModelConditionReady,
		Status:             metav1.ConditionFalse,
		Reason:             v1beta1.ModelConditionReasonMetadataUnparseable,
		Message:            unparseableMsg,
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
		Type:               v1beta1.ModelConditionMetadataExtracted,
		Status:             metav1.ConditionTrue,
		Reason:             v1beta1.ModelConditionReasonMetadataParsed,
		Message:            "model metadata parsing completed",
		ObservedGeneration: generation,
	}) || updated
	updated = setModelCondition(status, metav1.Condition{
		Type:               v1beta1.ModelConditionReady,
		Status:             metav1.ConditionTrue,
		Reason:             v1beta1.ModelConditionReasonMetadataParsed,
		Message:            "model metadata parsing completed",
		ObservedGeneration: generation,
	}) || updated
	return updated
}

// IsDistribution reports whether distribution is Sharded.
func IsDistribution(distribution *v1beta1.Distribution) bool {
	return distribution != nil && *distribution == v1beta1.DistributionSharded
}

func storageSourceURI(spec *v1beta1.BaseModelSpec) string {
	if spec == nil || spec.Storage == nil || spec.Storage.StorageUri == nil {
		return ""
	}
	return *spec.Storage.StorageUri
}

func modelCacheSource(spec *v1beta1.BaseModelSpec) Source {
	source := Source{
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

func cacheSourceChanged(status *v1beta1.ModelStatusSpec, sourceURI string) bool {
	if status.Cache == nil || status.Cache.SourceUri == sourceURI {
		return false
	}
	return status.Cache.SourceUri != ""
}

func cacheBackendChanged(status *v1beta1.ModelStatusSpec, backend string) bool {
	if status.Cache == nil || status.Cache.Backend == backend {
		return false
	}
	return status.Cache.Backend != ""
}

func cacheIdentityChanged(status *v1beta1.ModelStatusSpec, sourceURI, backend string) bool {
	return cacheSourceChanged(status, sourceURI) || cacheBackendChanged(status, backend)
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
