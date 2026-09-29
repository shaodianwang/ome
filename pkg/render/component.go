package render

import (
	"context"
	"fmt"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

// Rendered is one component's render output: the templates and the fields the
// projector copies onto the replica beside the runners.
type Rendered struct {
	Templates
	ComponentExt      *v1beta1.ComponentExtensionSpec
	TopologyKey       *string
	TopologySpread    *v1beta1.TopologySpreadPolicy
	TopologySpreadKey *string
}

// Runners is the runner list an InferenceReplica stores for the component. It
// is derived from the templates at call time, so a caller that edits a template
// after rendering (the router component stamps its service account onto
// Primary) calls it afterwards so the edit reaches the runners.
func (r Rendered) Runners() []v1beta1.Runner {
	return Runners(r.Templates, r.ComponentExt)
}

// Specs are the merged role specs a service declares; a nil entry means the
// service has no such role.
type Specs struct {
	Engine  *v1beta1.EngineSpec
	Decoder *v1beta1.DecoderSpec
	Router  *v1beta1.RouterSpec
}

// ComponentName is the object name of a component: the service name and the
// role, joined by a dash. Pods, Services and projected replicas derive from it.
func ComponentName(service *v1beta1.InferenceService, c v1beta1.ComponentType) string {
	return service.Name + "-" + string(c)
}

// RenderComponent renders the role c of service from its merged spec.
func RenderComponent(ctx context.Context, p *Piece, service *v1beta1.InferenceService, specs Specs, c v1beta1.ComponentType) (Rendered, error) {
	switch c {
	case v1beta1.EngineComponent:
		return RenderEngine(ctx, p, service, specs.Engine)
	case v1beta1.DecoderComponent:
		return RenderDecoder(ctx, p, service, specs.Decoder)
	case v1beta1.RouterComponent:
		return RenderRouter(ctx, p, service, specs.Router)
	}
	return Rendered{}, fmt.Errorf("component %q cannot be rendered", c)
}
