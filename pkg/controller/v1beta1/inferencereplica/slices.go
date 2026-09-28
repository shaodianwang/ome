package inferencereplica

import (
	"context"
	"fmt"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/sliceprovision"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/v1beta1convert"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	workloadtypes "github.com/ome-projects/ome/pkg/controller/v1beta1/workload/types"
)

// sliceProvisioner returns the provisioner of ir's TPU slices, or nil when
// the controller provisions no slices.
func (r *Reconciler) sliceProvisioner(ir *v1beta1.InferenceReplica) (*sliceprovision.Provisioner, error) {
	if r.TPUSliceProvisioning == nil || r.sliceReader == nil {
		return nil, nil
	}
	return sliceprovision.New(r.TPUSliceProvisioning, r.sliceReader, r.APIReader, r.Client, sliceprovision.Owner{
		Kind:      irKind,
		Namespace: ir.Namespace,
		Name:      ir.Name,
		UID:       ir.UID,
	})
}

// slicesOptedIn reports whether ir's pods run on provisioned slices. It reads
// ir's own pod template: a rollback renders pods from a stored template that
// lacks the annotations ir inherits from its parent.
func slicesOptedIn(ir *v1beta1.InferenceReplica) bool {
	for i := range ir.Spec.Runners {
		runner := &ir.Spec.Runners[i]
		if runner.Name == v1beta1.RunnerNameDefault || runner.Name == v1beta1.RunnerNameLeader {
			return runner.Template.Annotations[constants.TPUSliceProvisioningAnnotationKey] == "true"
		}
	}
	return false
}

// slicesInUse reports whether ir's slices must be provisioned or released:
// ir opts in, or a cached read finds a slice ir holds.
func slicesInUse(ctx context.Context, ir *v1beta1.InferenceReplica, p *sliceprovision.Provisioner) (bool, error) {
	if slicesOptedIn(ir) {
		return true, nil
	}
	return p.Holds(ctx)
}

// releasingSlices extends finalize, which may be nil, to release the
// Instance's slices. The Instance is finalized only once both are complete.
func releasingSlices(finalize func(context.Context, int32) (bool, error), p *sliceprovision.Provisioner) func(context.Context, int32) (bool, error) {
	return func(ctx context.Context, idx int32) (bool, error) {
		complete := true
		if finalize != nil {
			var err error
			if complete, err = finalize(ctx, idx); err != nil {
				return false, err
			}
		}
		released, err := p.ReleaseInstance(ctx, idx)
		if err != nil {
			return false, err
		}
		return complete && released, nil
	}
}

// pinnedSlices returns a live read of the slices ir's component pods are
// confined to. Every component pod counts, whatever its controller: a pod
// holds its slice's chips until it is gone.
func (r *Reconciler) pinnedSlices(ir *v1beta1.InferenceReplica, p *sliceprovision.Provisioner) func(context.Context) (map[string]struct{}, error) {
	return func(ctx context.Context) (map[string]struct{}, error) {
		pods, err := query.LiveListPodsForComponent(ctx, r.APIReader, ir.Namespace, ir.Spec.ParentRef.Name,
			v1beta1convert.ComponentTypeToWorkload(ir.Spec.Component))
		if err != nil {
			return nil, err
		}
		return p.Pinned(pods), nil
	}
}

// slicePass is one reconcile pass's slice provisioning.
type slicePass struct {
	p *sliceprovision.Provisioner
	// placer is set when ir opts in; without it the pass only releases.
	placer *sliceprovision.Placer
	pinned func(context.Context) (map[string]struct{}, error)
}

// provisionSlices prepares the pass's slices. An opted-in ir's pods are
// placed on provisioned slices; while ir opts in or holds a slice, an
// Instance is finalized only once its slices are released. It returns nil
// when neither holds.
func (r *Reconciler) provisionSlices(ctx context.Context, ir *v1beta1.InferenceReplica, deps *workloadtypes.Deps, input *workloadtypes.ReconcileInput) (*slicePass, error) {
	p, err := r.sliceProvisioner(ir)
	if err != nil || p == nil {
		return nil, err
	}
	inUse, err := slicesInUse(ctx, ir, p)
	if err != nil || !inUse {
		return nil, err
	}
	pass := &slicePass{p: p, pinned: r.pinnedSlices(ir, p)}
	if slicesOptedIn(ir) {
		pass.placer = sliceprovision.NewPlacer(p, r.Client, r.Recorder)
		deps.Provisioner = pass.placer
	}
	input.FinalizeInstanceResources = releasingSlices(input.FinalizeInstanceResources, p)
	return pass, nil
}

// sweep releases the slices no pod of the pass may use. A nil pass releases
// nothing.
func (s *slicePass) sweep(ctx context.Context, input workloadtypes.ReconcileInput, plan workloadtypes.ComponentPlan) error {
	if s == nil {
		return nil
	}
	if s.placer != nil {
		return s.placer.Sweep(ctx, input, plan, s.pinned)
	}
	return s.p.Sweep(ctx, nil, s.pinned)
}

// releaseSlicesPastDeadline releases every slice ir holds, pods or not. Once
// the teardown finalizer lifts, background GC deletes ir's pods and no later
// pass remains to release their slices, so a failure is reported and never
// keeps the finalizer.
func (r *Reconciler) releaseSlicesPastDeadline(ctx context.Context, ir *v1beta1.InferenceReplica) {
	p, err := r.sliceProvisioner(ir)
	if err == nil && p != nil {
		_, err = p.ReleaseAll(ctx)
	}
	if err != nil {
		r.warnTeardown(ir, ReasonTeardownDeadlineExceeded, fmt.Sprintf(
			"teardown deadline exceeded; releasing owned TPU slices failed: %v; delete the slices labeled %s=%s by hand",
			err, sliceprovision.LabelOwnerUID, ir.UID))
	}
}
