package resolution

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"maps"
	"slices"

	corev1 "k8s.io/api/core/v1"
	nodev1 "k8s.io/api/node/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/acceleratorclassselector"
	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/controllerconfig"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/components"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/specdefaults"
	isvcutils "github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/utils"
	"github.com/ome-projects/ome/pkg/placement/capacity"
	"github.com/ome-projects/ome/pkg/runtimeselector"
)

// ResolvedUnit combines the member's rendering and nominal accelerator demand.
// Check verifies read dependencies, not pod admission or member application.
type ResolvedUnit struct {
	Runtime *Runtime
	Demand  capacity.UnitDemand
	Engine  components.ReplicaTemplates
	Decoder *components.ReplicaTemplates
}

func (u *ResolvedUnit) Check(ctx context.Context) error {
	if u == nil {
		return fmt.Errorf("replica unit has no verified inputs")
	}
	return u.Runtime.Check(ctx)
}

// ResolveUnit renders the intended member service without writing any objects.
// ResourceFlavor attribution and proof of member application remain required
// before the result can authorize a capacity plan.
func (r Resolver) ResolveUnit(ctx context.Context, desired, standing *v1beta1.InferenceService) (*ResolvedUnit, error) {
	if r.OperatorNamespace == "" {
		return nil, fmt.Errorf("replica rendering requires the member operator namespace")
	}
	rt, err := r.Resolve(ctx, desired, standing)
	if err != nil {
		return nil, err
	}
	service := desired.DeepCopy()
	cm := &corev1.ConfigMap{}
	if err := rt.reads.Get(ctx, client.ObjectKey{Namespace: r.OperatorNamespace, Name: constants.InferenceServiceConfigMapName}, cm); err != nil {
		return nil, err
	}
	cfg, deploy, err := controllerconfig.RenderingConfig(cm)
	if err != nil {
		return nil, err
	}
	if len(cfg.AcceleratorResources) == 0 {
		return nil, fmt.Errorf("replica rendering requires configured member accelerator resources")
	}
	engine, decoder := rt.Engine.DeepCopy(), rt.Decoder.DeepCopy()
	engineMode, decoderMode, _, err := isvcutils.DetermineDeploymentModes(engine, decoder, rt.Router, rt.Spec, service.Spec.DeploymentMode)
	if err != nil {
		return nil, err
	}
	specdefaults.Engine(engine, engineMode, deploy)
	specdefaults.Decoder(decoder, decoderMode, deploy)
	overlays, err := isvcutils.ResolveOverlays(rt.reads, service)
	if err != nil {
		return nil, err
	}
	selector := runtimeselector.New(rt.reads)
	format := selector.GetSupportedModelFormat(ctx, rt.Spec, rt.Model, service.Spec.Runtime != nil && service.Spec.Runtime.Name != "")
	deps := &components.ComponentDeps{Client: rt.reads, Config: cfg}
	inputs := components.ComponentInputs{Runtime: rt.Spec, RuntimeName: rt.Name, BaseModel: rt.Model, BaseModelMeta: rt.ModelMeta, ModelFormat: format, Overlays: overlays}
	var accelerators []*v1beta1.AcceleratorClass
	selectInputs := func(component v1beta1.ComponentType, mode constants.DeploymentModeType) (components.ComponentInputs, error) {
		in := inputs
		in.DeploymentMode = mode
		ac, name, err := acceleratorclassselector.New(rt.reads).GetAcceleratorClass(ctx, service, rt.Spec, component)
		if err != nil {
			return in, err
		}
		if ac != nil {
			in.AcceleratorClass, in.AcceleratorClassName = &ac.Spec, name
			identity := ac.DeepCopy()
			identity.ObjectMeta = renderingIdentity(identity.ObjectMeta)
			identity.Status = v1beta1.AcceleratorClassStatus{}
			accelerators = append(accelerators, identity)
		}
		return in, nil
	}
	out := &ResolvedUnit{Runtime: rt}
	engineInputs, err := selectInputs(v1beta1.EngineComponent, engineMode)
	if err != nil {
		return nil, err
	}
	out.Engine, err = components.RenderEngineReplica(ctx, deps, engineInputs, service, engine)
	if err != nil {
		return nil, err
	}
	unit := capacity.ReplicaUnit{}
	unit.Engine, err = renderedPodSets(engineMode, engine.Leader != nil, engine.Worker != nil, out.Engine)
	if err != nil {
		return nil, fmt.Errorf("engine demand: %w", err)
	}
	if decoder != nil {
		decoderInputs, err := selectInputs(v1beta1.DecoderComponent, decoderMode)
		if err != nil {
			return nil, err
		}
		templates, err := components.RenderDecoderReplica(ctx, deps, decoderInputs, service, decoder)
		if err != nil {
			return nil, err
		}
		out.Decoder = &templates
		unit.Decoder, err = renderedPodSets(decoderMode, decoder.Leader != nil, decoder.Worker != nil, templates)
		if err != nil {
			return nil, fmt.Errorf("decoder demand: %w", err)
		}
	}
	var classes []*nodev1.RuntimeClass
	for _, sets := range [][]capacity.PodSet{unit.Engine, unit.Decoder} {
		for _, set := range sets {
			class, err := resolveRuntimeClass(ctx, rt.reads, set.Spec)
			if err != nil {
				return nil, err
			}
			if class != nil {
				class.ObjectMeta = renderingIdentity(class.ObjectMeta)
				classes = append(classes, class)
			}
		}
	}
	var modelIdentity metav1.ObjectMeta
	if rt.ModelMeta != nil {
		modelIdentity = renderingIdentity(*rt.ModelMeta)
	}
	encoded, err := json.Marshal(struct {
		RuntimeName, RuntimeHash string
		RuntimeIsCluster         bool
		Model                    *v1beta1.BaseModelSpec
		ModelIdentity            metav1.ObjectMeta
		ConfigIdentity           metav1.ObjectMeta
		Config                   map[string]string
		Accelerators             []*v1beta1.AcceleratorClass
		Classes                  []*nodev1.RuntimeClass
	}{rt.Name, rt.Hash, rt.IsCluster, rt.Model, modelIdentity, renderingIdentity(cm.ObjectMeta), cm.Data, accelerators, classes})
	if err != nil {
		return nil, err
	}
	sum := sha256.Sum256(encoded)
	unit.InputFingerprint = hex.EncodeToString(sum[:])
	out.Demand, err = capacity.MeasureUnit(unit, cfg.AcceleratorResources)
	if err != nil {
		return nil, err
	}
	if err := out.Check(ctx); err != nil {
		return nil, err
	}
	return out, nil
}

func renderedPodSets(mode constants.DeploymentModeType, leader, worker bool, templates components.ReplicaTemplates) ([]capacity.PodSet, error) {
	sets := []capacity.PodSet{{Name: "primary", Count: 1, Spec: templates.Primary}}
	switch mode {
	case constants.RawDeployment:
		return sets, nil
	case constants.OMENative:
		if !leader && !worker {
			return sets, nil
		}
	case constants.MultiNode:
	default:
		return nil, fmt.Errorf("deployment mode %q has no supported replica shape", mode)
	}
	if !leader || !worker || templates.Worker == nil || templates.WorkerSize <= 0 {
		return nil, fmt.Errorf("a multi-pod replica requires a leader and a positive resolved worker count")
	}
	return append(sets, capacity.PodSet{Name: "workers", Count: int64(templates.WorkerSize), Spec: templates.Worker}), nil
}

func renderingIdentity(meta metav1.ObjectMeta) metav1.ObjectMeta {
	return metav1.ObjectMeta{Name: meta.Name, Namespace: meta.Namespace, UID: meta.UID, Labels: maps.Clone(meta.Labels), Annotations: maps.Clone(meta.Annotations)}
}

// RuntimeClass admission contributes overhead and required node selection.
// A conflicting authored value cannot be measured as an admitted pod.
func resolveRuntimeClass(ctx context.Context, reader client.Reader, pod *corev1.PodSpec) (*nodev1.RuntimeClass, error) {
	if pod.RuntimeClassName == nil {
		if len(pod.Overhead) != 0 {
			return nil, fmt.Errorf("pod overhead has no identified RuntimeClass")
		}
		return nil, nil
	}
	if *pod.RuntimeClassName == "" {
		return nil, fmt.Errorf("RuntimeClass name is empty")
	}
	class := &nodev1.RuntimeClass{}
	if err := reader.Get(ctx, client.ObjectKey{Name: *pod.RuntimeClassName}, class); err != nil {
		return nil, err
	}
	var overhead corev1.ResourceList
	if class.Overhead != nil {
		overhead = class.Overhead.PodFixed
	}
	if len(pod.Overhead) != 0 && !apiequality.Semantic.DeepEqual(pod.Overhead, overhead) {
		return nil, fmt.Errorf("pod overhead differs from RuntimeClass %q", class.Name)
	}
	pod.Overhead = overhead.DeepCopy()
	if class.Scheduling != nil {
		for key, value := range class.Scheduling.NodeSelector {
			if existing, ok := pod.NodeSelector[key]; ok && existing != value {
				return nil, fmt.Errorf("pod node selector conflicts with RuntimeClass %q", class.Name)
			}
			if pod.NodeSelector == nil {
				pod.NodeSelector = map[string]string{}
			}
			pod.NodeSelector[key] = value
		}
		for _, toleration := range class.Scheduling.Tolerations {
			if !slices.ContainsFunc(pod.Tolerations, func(existing corev1.Toleration) bool { return apiequality.Semantic.DeepEqual(existing, toleration) }) {
				pod.Tolerations = append(pod.Tolerations, *toleration.DeepCopy())
			}
		}
	}
	return class, nil
}
