package resolution

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"maps"

	corev1 "k8s.io/api/core/v1"
	nodev1 "k8s.io/api/node/v1"
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
	Runtime   *Runtime
	Demand    capacity.UnitDemand
	Engine    components.ReplicaTemplates
	Decoder   *components.ReplicaTemplates
	Rendering map[v1beta1.ComponentType]string
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
	unit.Engine, err = out.Engine.PodSets(engineMode, engine.Leader != nil, engine.Worker != nil)
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
		unit.Decoder, err = templates.PodSets(decoderMode, decoder.Leader != nil, decoder.Worker != nil)
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
	out.Rendering = map[v1beta1.ComponentType]string{}
	for _, component := range []struct {
		name v1beta1.ComponentType
		mode constants.DeploymentModeType
		pods []capacity.PodSet
	}{{v1beta1.EngineComponent, engineMode, unit.Engine}, {v1beta1.DecoderComponent, decoderMode, unit.Decoder}} {
		if len(component.pods) == 0 {
			continue
		}
		fingerprint, err := capacity.ComponentFingerprint(ctx, rt.reads, component.name, component.mode, component.pods)
		if err != nil {
			return nil, err
		}
		out.Rendering[component.name] = fingerprint
	}
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

func renderingIdentity(meta metav1.ObjectMeta) metav1.ObjectMeta {
	return metav1.ObjectMeta{Name: meta.Name, Namespace: meta.Namespace, UID: meta.UID, Labels: maps.Clone(meta.Labels), Annotations: maps.Clone(meta.Annotations)}
}

func resolveRuntimeClass(ctx context.Context, reader client.Reader, pod *corev1.PodSpec) (*nodev1.RuntimeClass, error) {
	return capacity.ApplyRuntimeClass(ctx, reader, pod)
}
