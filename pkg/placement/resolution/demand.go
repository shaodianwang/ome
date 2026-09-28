package resolution

import (
	"context"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	kueuev1beta2 "sigs.k8s.io/kueue/apis/kueue/v1beta2"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/placement/capacity"
	quotacapacity "github.com/ome-projects/ome/pkg/quota/capacity"
)

// ResolvedDemand retains verified member rendering, hardware mapping, and root
// identity. Consumers must match Report to fleet provenance and verify member
// application before using Reader's fresh hardware sample to authorize a plan.
type ResolvedDemand struct {
	Unit   *ResolvedUnit
	Demand capacity.Demand
	Report metav1.ObjectMeta
}

func (d *ResolvedDemand) Check(ctx context.Context) error {
	if d == nil {
		return fmt.Errorf("accelerator demand has no verified inputs")
	}
	return d.Unit.Check(ctx)
}

// ResolveDemand attributes a rendered unit using the identified member's live
// flavor catalog and local root report. The root name is explicit; quota amounts
// do not select hardware pools.
func (r Resolver) ResolveDemand(ctx context.Context, desired, standing *v1beta1.InferenceService, rootName string) (*ResolvedDemand, error) {
	if rootName == "" {
		return nil, fmt.Errorf("demand resolution requires the member capacity root name")
	}
	unit, err := r.ResolveUnit(ctx, desired, standing)
	if err != nil {
		return nil, err
	}
	reads := unit.Runtime.reads
	root := &v1beta1.AcceleratorQuota{}
	if err := reads.Get(ctx, client.ObjectKey{Name: rootName}, root); err != nil {
		return nil, err
	}
	if root.Spec.ParentRef != nil || root.Spec.Role != v1beta1.AcceleratorQuotaRoleCohort {
		return nil, fmt.Errorf("capacity report is not a local quota root")
	}
	var catalog kueuev1beta2.ResourceFlavorList
	if err := reads.List(ctx, &catalog); err != nil {
		return nil, err
	}
	flavors := make([]quotacapacity.Flavor, 0, len(catalog.Items))
	for _, flavor := range catalog.Items {
		flavors = append(flavors, quotacapacity.Flavor{Name: flavor.Name, UID: flavor.UID, NodeLabels: flavor.Spec.NodeLabels})
	}
	demand, err := capacity.AttributeUnit(unit.Demand, flavors, root.Status.Capacity)
	if err != nil {
		return nil, err
	}
	out := &ResolvedDemand{Unit: unit, Demand: demand, Report: *root.ObjectMeta.DeepCopy()}
	if err := out.Check(ctx); err != nil {
		return nil, err
	}
	return out, nil
}
