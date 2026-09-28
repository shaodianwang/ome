package sliceprovision

import (
	"context"
	"errors"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"

	workload "github.com/ome-projects/ome/pkg/controller/v1beta1/workload/types"
	"github.com/ome-projects/ome/pkg/tpuslice/gke"
)

const (
	// EventReasonSliceDemandInvalid is recorded when an Instance's pods
	// select a provision-only pool but cannot fill a provisionable slice.
	EventReasonSliceDemandInvalid workload.EventReason = "SliceDemandInvalid"
	// EventReasonSliceOwnershipConflict is recorded when a slice the owner
	// does not hold has the name of one of the owner's slots.
	EventReasonSliceOwnershipConflict workload.EventReason = "SliceOwnershipConflict"
)

// Placer places the workload engine's pods on provisioned slices. It serves
// one reconcile pass: each Instance's demand is resolved, and each slot's
// slice ensured, at most once per pass, and Sweep keeps every slice the pass
// placed pods on.
type Placer struct {
	p        *Provisioner
	nodes    client.Reader
	recorder record.EventRecorder
	demands  map[demandKey]*resolution
	ensured  map[placedSlot]*ensureResult
}

var _ workload.Provisioner = (*Placer)(nil)

// demandKey identifies an Instance's pods by the templates the engine
// renders them from and the runners they fill. A pass may render an
// Instance from templates other than the desired ones, as a migration surge
// does, and a pass never modifies a template it renders from.
type demandKey struct {
	pod, worker *corev1.PodSpec
	layout      string
}

// resolution is a resolved demand. err is set only for ErrInvalidDemand.
type resolution struct {
	demand Demand
	needed bool
	err    error
	warned bool
}

// placedSlot is a slot and the demand the pass placed its pods for.
type placedSlot struct {
	slot   Slot
	demand Demand
}

// ensureResult is an ensured slot. err is set only for
// gke.ErrOwnershipConflict.
type ensureResult struct {
	placement Placement
	err       error
	warned    bool
}

// NewPlacer returns a Placer for one reconcile pass of p's owner. nodes
// serves the node reads that decide whether a pool provisions slices.
func NewPlacer(p *Provisioner, nodes client.Reader, recorder record.EventRecorder) *Placer {
	return &Placer{
		p:        p,
		nodes:    nodes,
		recorder: recorder,
		demands:  map[demandKey]*resolution{},
		ensured:  map[placedSlot]*ensureResult{},
	}
}

// Place ensures the slice of the pod's slot and confines the pod to it once
// the slice is ready. A pod on no provision-only pool is placed unconfined.
// A pod whose Instance cannot fill a provisionable slice, or whose slot's
// name is taken by a slice the owner does not hold, is withheld and a
// warning is recorded.
func (pl *Placer) Place(ctx context.Context, input workload.ReconcileInput, _ workload.ComponentPlan, inst workload.InstancePlan, _ workload.RunnerPlan, ordinal int32) (map[string]string, bool, error) {
	r, err := pl.resolve(ctx, input, inst)
	if err != nil {
		return nil, false, err
	}
	if r.err != nil {
		if !r.warned {
			r.warned = true
			workload.RecordWarning(pl.recorder, workload.EventTarget(input), EventReasonSliceDemandInvalid,
				"OMENative %s withheld: %v", workload.InstanceKey(input.Key.Component, inst.Index), r.err)
		}
		return nil, false, nil
	}
	if !r.needed {
		return nil, true, nil
	}
	e, err := pl.ensure(ctx, r.demand, SlotFor(inst, ordinal))
	if err != nil {
		return nil, false, err
	}
	if e.err != nil {
		if !e.warned {
			e.warned = true
			workload.RecordWarning(pl.recorder, workload.EventTarget(input), EventReasonSliceOwnershipConflict,
				"OMENative %s withheld: %v", workload.InstanceKey(input.Key.Component, inst.Index), e.err)
		}
		return nil, false, nil
	}
	if !e.placement.Ready {
		return nil, false, nil
	}
	return e.placement.NodeSelector, true, nil
}

// Pending reports why Place withholds the pod, from cached reads. It
// creates no slice and records nothing.
func (pl *Placer) Pending(ctx context.Context, input workload.ReconcileInput, _ workload.ComponentPlan, inst workload.InstancePlan, _ workload.RunnerPlan, ordinal int32) (string, bool, error) {
	r, err := pl.resolve(ctx, input, inst)
	if err != nil {
		return "", false, err
	}
	if r.err != nil {
		return r.err.Error(), true, nil
	}
	if !r.needed {
		return "", false, nil
	}
	placement, err := pl.p.Observe(ctx, r.demand, SlotFor(inst, ordinal))
	if errors.Is(err, gke.ErrOwnershipConflict) {
		return err.Error(), true, nil
	}
	if err != nil {
		return "", false, err
	}
	return placement.Reason, !placement.Ready, nil
}

// Sweep releases the owner's slices that no pod is pinned to and on which no
// pod may be created. A slice is kept for a slot the pass placed pods on, or
// a slot a planned Instance or the target of a row's surge may create a pod
// at, while it has the shape that slot's pods ask for.
//
// pinned must read the owner's pods live, terminating ones included.
func (pl *Placer) Sweep(ctx context.Context, input workload.ReconcileInput, plan workload.ComponentPlan, pinned func(context.Context) (map[string]struct{}, error)) error {
	wanted := map[Slot][]Demand{}
	for placed := range pl.ensured {
		wanted[placed.slot] = append(wanted[placed.slot], placed.demand)
	}
	for _, inst := range liveInstances(input, plan) {
		r, err := pl.resolve(ctx, input, inst)
		if err != nil {
			return fmt.Errorf("sweep slices: %w", err)
		}
		if r.err != nil || !r.needed {
			continue
		}
		for _, ordinal := range liveOrdinals(input, inst) {
			slot := SlotFor(inst, ordinal)
			wanted[slot] = append(wanted[slot], r.demand)
		}
	}
	return pl.p.Sweep(ctx, func(slot Slot, s gke.Slice) bool {
		for _, d := range wanted[slot] {
			if pl.p.Fits(s, d, slot) {
				return true
			}
		}
		return false
	}, pinned)
}

// resolve returns inst's demand as input renders its pods. Only a
// successful resolution or an invalid demand is remembered: a failed read
// is retried.
func (pl *Placer) resolve(ctx context.Context, input workload.ReconcileInput, inst workload.InstancePlan) (*resolution, error) {
	desired := input.DesiredSpec
	key := demandKey{pod: desired.PodSpec, worker: desired.WorkerPodSpec, layout: layoutOf(inst)}
	if r, ok := pl.demands[key]; ok {
		return r, nil
	}
	d, needed, err := Resolve(ctx, pl.nodes, pl.p.cfg, podSpecs(desired, inst))
	if err != nil && !errors.Is(err, ErrInvalidDemand) {
		return nil, err
	}
	r := &resolution{demand: d, needed: needed, err: err}
	pl.demands[key] = r
	return r, nil
}

// ensure runs Ensure once per slot and demand. Only a placement or an
// ownership conflict is remembered: a failed read or write is retried.
func (pl *Placer) ensure(ctx context.Context, d Demand, slot Slot) (*ensureResult, error) {
	key := placedSlot{slot: slot, demand: d}
	if e, ok := pl.ensured[key]; ok {
		return e, nil
	}
	placement, err := pl.p.Ensure(ctx, d, slot)
	if err != nil && !errors.Is(err, gke.ErrOwnershipConflict) {
		return nil, err
	}
	e := &ensureResult{placement: placement, err: err}
	pl.ensured[key] = e
	return e, nil
}

// podSpecs are the templates of inst's pods, one per pod: a worker renders
// from the worker template when there is one, every other pod from the pod
// template.
func podSpecs(desired workload.WorkloadDesiredSpec, inst workload.InstancePlan) []*corev1.PodSpec {
	specs := make([]*corev1.PodSpec, 0, inst.TotalPods())
	for _, runner := range inst.Runners {
		spec := desired.PodSpec
		if runner.Name == workload.RunnerWorker && desired.WorkerPodSpec != nil {
			spec = desired.WorkerPodSpec
		}
		for i := int32(0); i < runner.Size; i++ {
			specs = append(specs, spec)
		}
	}
	return specs
}

func layoutOf(inst workload.InstancePlan) string {
	var b strings.Builder
	for _, runner := range inst.Runners {
		fmt.Fprintf(&b, "%s=%d;", runner.Name, runner.Size)
	}
	return b.String()
}

// liveInstances are the Instances whose pods the engine may create: the
// planned ones, and the targets of the rows' surges, which the plan omits
// until they have rows of their own. A surge target mirrors its source's
// runners.
func liveInstances(input workload.ReconcileInput, plan workload.ComponentPlan) []workload.InstancePlan {
	live := append([]workload.InstancePlan(nil), plan.Instances...)
	planned := make(map[int32]workload.InstancePlan, len(plan.Instances))
	for _, inst := range plan.Instances {
		planned[inst.Index] = inst
	}
	for _, row := range input.ObservedState.InstanceStatuses {
		if row.Operation == nil || row.Operation.SurgeIndex == nil {
			continue
		}
		target := *row.Operation.SurgeIndex
		if _, ok := planned[target]; ok {
			continue
		}
		source, ok := planned[row.Index]
		if !ok {
			continue
		}
		inst := workload.InstancePlan{Index: target, Runners: source.Runners}
		planned[target] = inst
		live = append(live, inst)
	}
	return live
}

// liveOrdinals are the ordinals inst's pods may be created at. A single-pod
// Instance moves between ordinals 0 and 1 across surges, so while it has an
// operation in flight either may be the one it creates.
func liveOrdinals(input workload.ReconcileInput, inst workload.InstancePlan) []int32 {
	if inst.TotalPods() > 1 {
		return []int32{0}
	}
	row := input.ObservedState.Instance(inst.Index)
	switch {
	case row == nil:
		return []int32{0}
	case row.Operation != nil:
		return []int32{0, 1}
	default:
		return []int32{row.ActiveOrdinal}
	}
}
