package gang

import (
	"context"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	schedulingv1alpha1 "sigs.k8s.io/scheduler-plugins/apis/scheduling/v1alpha1"

	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/podgroup"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	workload "github.com/ome-projects/ome/pkg/controller/v1beta1/workload/types"
)

// PodGroupInventory is one authoritative namespace LIST partitioned for one
// controller owner. It retains foreign objects by name so ensure can detect a
// deterministic-name collision without a per-Instance GET.
type PodGroupInventory struct {
	available bool
	ownerUID  types.UID
	byName    map[string]*schedulingv1alpha1.PodGroup
	accepted  map[string]struct{}
}

// ObservePodGroups performs exactly one authoritative namespace LIST and
// builds an owner-scoped inventory. A namespace-wide read is intentional:
// labels are mutable, while controller UID is the teardown ownership proof.
// Runtime removal of the optional PodGroup API yields an unavailable inventory
// rather than turning that optional API into a lifecycle blocker.
func ObservePodGroups(ctx context.Context, reader client.Reader, owner client.Object) (*PodGroupInventory, error) {
	if reader == nil {
		return nil, fmt.Errorf("ObservePodGroups: nil reader")
	}
	if owner == nil {
		return nil, fmt.Errorf("ObservePodGroups: nil owner")
	}
	if owner.GetUID() == "" {
		return nil, fmt.Errorf("ObservePodGroups: owner %s/%s has no UID", owner.GetNamespace(), owner.GetName())
	}
	inv := newPodGroupInventory(owner.GetUID(), true)
	list := &schedulingv1alpha1.PodGroupList{}
	if err := reader.List(ctx, list, client.InNamespace(owner.GetNamespace())); err != nil {
		if apimeta.IsNoMatchError(err) || apierrors.IsNotFound(err) || runtime.IsNotRegisteredError(err) {
			return newPodGroupInventory(owner.GetUID(), false), nil
		}
		return nil, fmt.Errorf("ObservePodGroups: list namespace %s: %w", owner.GetNamespace(), err)
	}
	for i := range list.Items {
		pg := list.Items[i].DeepCopy()
		inv.byName[pg.Name] = pg
	}
	return inv, nil
}

func newPodGroupInventory(ownerUID types.UID, available bool) *PodGroupInventory {
	return &PodGroupInventory{
		available: available,
		ownerUID:  ownerUID,
		byName:    make(map[string]*schedulingv1alpha1.PodGroup),
		accepted:  make(map[string]struct{}),
	}
}

// Available reports whether the optional PodGroup API answered the inventory
// LIST. False means lifecycle code follows the existing soft-fail contract.
func (i *PodGroupInventory) Available() bool {
	return i != nil && i.available
}

// OwnerUID returns the controller identity that partitions this inventory.
func (i *PodGroupInventory) OwnerUID() types.UID {
	if i == nil {
		return ""
	}
	return i.ownerUID
}

// ByName returns any object occupying name, including a foreign object.
func (i *PodGroupInventory) ByName(name string) (*schedulingv1alpha1.PodGroup, bool) {
	if i == nil {
		return nil, false
	}
	pg, found := i.byName[name]
	return pg, found
}

// recordReconciled overlays a successful same-pass create or update onto the
// immutable LIST view. Inline surge ensure then observes the write without a
// second read or a duplicate create.
func (i *PodGroupInventory) recordReconciled(pg *schedulingv1alpha1.PodGroup) {
	if i == nil || pg == nil {
		return
	}
	observed := pg.DeepCopy()
	i.byName[pg.Name] = observed
}

// DeleteOwnedName accepts deletion of one inventory-owned object. A foreign
// same-named object is intentionally treated as absent from this owner's
// resource set; ensure still reports it as a collision through ByName.
func (i *PodGroupInventory) DeleteOwnedName(ctx context.Context, c client.Client, name string) error {
	if i == nil || !i.available {
		return nil
	}
	if _, done := i.accepted[name]; done {
		return nil
	}
	pg, found := i.byName[name]
	if !found || !podgroup.ControlledByUID(pg, i.ownerUID) {
		return nil
	}
	if err := podgroup.DeleteObservedPodGroup(ctx, c, i.ownerUID, pg); err != nil {
		return err
	}
	i.accepted[name] = struct{}{}
	return nil
}

// FinalizeOwnedName requests deletion of an inventory-owned object and reports
// complete only when the authoritative inventory or a NotFound response proves
// it absent. A foreign occupant is absent from this owner's resource set.
func (i *PodGroupInventory) FinalizeOwnedName(ctx context.Context, c client.Client, name string) (bool, error) {
	if i == nil || !i.available {
		return true, nil
	}
	pg, found := i.byName[name]
	if !found || !podgroup.ControlledByUID(pg, i.ownerUID) {
		return true, nil
	}
	if _, done := i.accepted[name]; done {
		return false, nil
	}
	complete, err := podgroup.FinalizeObservedPodGroup(ctx, c, i.ownerUID, pg)
	if err != nil {
		return false, err
	}
	if !complete {
		i.accepted[name] = struct{}{}
	}
	return complete, nil
}

// BuildFinalizeInstanceResources adapts an inventory to
// ReconcileInput.FinalizeInstanceResources. The caller invokes it only after
// the shared authoritative Pod bucket for idx is empty.
func BuildFinalizeInstanceResources(
	c client.Client,
	reader client.Reader,
	inv *PodGroupInventory,
	owner client.Object,
	ownerName string,
	component workload.ComponentType,
) func(context.Context, int32) (bool, error) {
	return func(ctx context.Context, idx int32) (bool, error) {
		if owner == nil {
			return false, fmt.Errorf("FinalizeInstanceResources: nil owner")
		}
		if inv == nil {
			return podgroup.FinalizePodGroup(ctx, c, reader, owner, ownerName, component, idx)
		}
		if !inv.Available() {
			return true, nil
		}
		if owner.GetUID() != inv.OwnerUID() {
			return false, fmt.Errorf("FinalizeInstanceResources: inventory owner UID %q does not match owner UID %q", inv.OwnerUID(), owner.GetUID())
		}
		name := query.PodGroupName(ownerName, component, idx)
		return inv.FinalizeOwnedName(ctx, c, name)
	}
}
