package components

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

// ReplicaTemplates are the component's rendered templates before workload
// projection and pod admission. Deployment mode determines whether workers run.
type ReplicaTemplates struct {
	ObjectMeta metav1.ObjectMeta
	Primary    *corev1.PodSpec
	Worker     *corev1.PodSpec
	WorkerSize int
}

// RenderEngineReplica uses the Engine's pod renderer without reconciling
// workloads or status. The spec must already have runtime merges and deployment
// defaults applied. Model and service lookups use deps.Client.
func RenderEngineReplica(ctx context.Context, deps *ComponentDeps, in ComponentInputs, service *v1beta1.InferenceService, spec *v1beta1.EngineSpec) (ReplicaTemplates, error) {
	if deps == nil || deps.Client == nil || service == nil || spec == nil {
		return ReplicaTemplates{}, fmt.Errorf("engine rendering requires dependencies, a member client, service, and merged spec")
	}
	service = service.DeepCopy()
	engine := NewEngine(deps, in, spec.DeepCopy()).(*Engine)
	if service.Spec.Model != nil && len(service.Spec.Model.FineTunedWeights) > 0 {
		if err := ReconcileFineTunedWeights(&engine.BaseComponentFields, service); err != nil {
			return ReplicaTemplates{}, err
		}
	}
	meta, err := engine.reconcileObjectMeta(ctx, service)
	if err != nil {
		return ReplicaTemplates{}, err
	}
	primary, err := engine.reconcilePodSpec(service, &meta)
	if err != nil {
		return ReplicaTemplates{}, err
	}
	worker, err := engine.reconcileWorkerPodSpec(service, &meta)
	if err != nil {
		return ReplicaTemplates{}, err
	}
	return ReplicaTemplates{ObjectMeta: meta, Primary: primary, Worker: worker, WorkerSize: engine.getWorkerSize()}, nil
}

// RenderDecoderReplica is the Decoder counterpart of RenderEngineReplica.
func RenderDecoderReplica(ctx context.Context, deps *ComponentDeps, in ComponentInputs, service *v1beta1.InferenceService, spec *v1beta1.DecoderSpec) (ReplicaTemplates, error) {
	if deps == nil || deps.Client == nil || service == nil || spec == nil {
		return ReplicaTemplates{}, fmt.Errorf("decoder rendering requires dependencies, a member client, service, and merged spec")
	}
	service = service.DeepCopy()
	decoder := NewDecoder(deps, in, spec.DeepCopy()).(*Decoder)
	if service.Spec.Model != nil && len(service.Spec.Model.FineTunedWeights) > 0 {
		if err := ReconcileFineTunedWeights(&decoder.BaseComponentFields, service); err != nil {
			return ReplicaTemplates{}, err
		}
	}
	meta, err := decoder.reconcileObjectMeta(ctx, service)
	if err != nil {
		return ReplicaTemplates{}, err
	}
	primary, err := decoder.reconcilePodSpec(service, &meta)
	if err != nil {
		return ReplicaTemplates{}, err
	}
	worker, err := decoder.reconcileWorkerPodSpec(service, &meta)
	if err != nil {
		return ReplicaTemplates{}, err
	}
	return ReplicaTemplates{ObjectMeta: meta, Primary: primary, Worker: worker, WorkerSize: decoder.getWorkerSize()}, nil
}
