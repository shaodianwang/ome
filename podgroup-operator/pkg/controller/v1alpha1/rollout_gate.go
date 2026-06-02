package v1alpha1

import (
	"context"
	"encoding/json"
	"log"
	"strconv"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
)

type parsedRolloutGate struct {
	enabled      bool
	rollout      string
	wave         int32
	start        int32
	end          int32
	action       string
	phase        string
	pausedReason string
}

func rolloutGateFromAnnotations(annotations map[string]string) parsedRolloutGate {
	if annotations[annotationRollout] == "" {
		return parsedRolloutGate{}
	}
	start, okStart := parseInt32String(annotations[annotationRolloutAllowedStart])
	end, okEnd := parseInt32String(annotations[annotationRolloutAllowedEnd])
	wave, _ := parseInt32String(annotations[annotationRolloutWave])
	if !okStart || !okEnd || end <= start {
		return parsedRolloutGate{}
	}
	action := annotations[annotationRolloutAction]
	if action == "" {
		action = rolloutActionHold
	}
	return parsedRolloutGate{
		enabled:      true,
		rollout:      annotations[annotationRollout],
		wave:         wave,
		start:        start,
		end:          end,
		action:       action,
		phase:        annotations[annotationRolloutPhase],
		pausedReason: annotations[annotationRolloutPausedReason],
	}
}

func (g parsedRolloutGate) allows(group int32) bool {
	if !g.enabled {
		return true
	}
	return group >= g.start && group < g.end
}

func (g parsedRolloutGate) status() RolloutGate {
	if !g.enabled {
		return RolloutGate{}
	}
	return RolloutGate{
		Rollout:      g.rollout,
		Wave:         g.wave,
		AllowedStart: g.start,
		AllowedEnd:   g.end,
		Action:       g.action,
		Phase:        g.phase,
		PausedReason: g.pausedReason,
	}
}

func (c *Controller) applyRolloutGate(ctx context.Context, pgdr *PodGroupDeploymentRollout, pgd *PodGroupDeployment, start, end, wave int32, action, phase, pausedReason string) error {
	annotations := mergeStringMaps(pgd.Annotations, map[string]string{
		annotationRollout:             pgdr.Name,
		annotationRolloutAllowedStart: strconv.Itoa(int(start)),
		annotationRolloutAllowedEnd:   strconv.Itoa(int(end)),
		annotationRolloutWave:         strconv.Itoa(int(wave)),
		annotationRolloutAction:       action,
		annotationRolloutPhase:        phase,
	})
	if pausedReason != "" {
		annotations[annotationRolloutPausedReason] = pausedReason
	} else {
		delete(annotations, annotationRolloutPausedReason)
	}
	if stringMapsEqual(pgd.Annotations, annotations) {
		return nil
	}
	logPGDR(pgdr, "applying rollout gate podgroupdeployment=%s/%s allowed_start=%d allowed_end=%d wave=%d action=%s phase=%s", pgd.Namespace, pgd.Name, start, end, wave, action, phase)
	c.recordNormal(pgd, reasonRolloutGate, "rollout "+pgdr.Name+" set gate action="+action+" phase="+phase+" wave="+strconv.Itoa(int(wave)))
	patch := map[string]interface{}{
		annotationRollout:             pgdr.Name,
		annotationRolloutAllowedStart: strconv.Itoa(int(start)),
		annotationRolloutAllowedEnd:   strconv.Itoa(int(end)),
		annotationRolloutWave:         strconv.Itoa(int(wave)),
		annotationRolloutAction:       action,
		annotationRolloutPhase:        phase,
	}
	if pausedReason != "" {
		patch[annotationRolloutPausedReason] = pausedReason
	} else {
		patch[annotationRolloutPausedReason] = nil
	}
	return c.patchPodGroupDeploymentAnnotations(ctx, pgd.Namespace, pgd.Name, patch)
}

func (c *Controller) clearRolloutGate(ctx context.Context, pgd *PodGroupDeployment) error {
	annotations := pgd.Annotations
	if annotations[annotationRollout] == "" {
		return nil
	}
	logPGD(pgd, "clearing rollout gate rollout=%s", annotations[annotationRollout])
	c.recordNormal(pgd, reasonNoRolloutGate, "rollout gate cleared")
	return c.patchPodGroupDeploymentAnnotations(ctx, pgd.Namespace, pgd.Name, map[string]interface{}{
		annotationRollout:             nil,
		annotationRolloutAllowedStart: nil,
		annotationRolloutAllowedEnd:   nil,
		annotationRolloutWave:         nil,
		annotationRolloutAction:       nil,
		annotationRolloutPhase:        nil,
		annotationRolloutPausedReason: nil,
	})
}

func (c *Controller) clearStaleRolloutGates(ctx context.Context, referenced map[string]struct{}) error {
	list, err := c.dynamic.Resource(podGroupDeploymentGVR).Namespace("").List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}
	for i := range list.Items {
		obj := &list.Items[i]
		annotations := obj.GetAnnotations()
		if annotations[annotationRollout] == "" {
			continue
		}
		key := obj.GetNamespace() + "/" + obj.GetName()
		if _, ok := referenced[key]; ok {
			continue
		}
		log.Printf("podgroupdeployment=%s/%s clearing stale rollout gate rollout=%s", obj.GetNamespace(), obj.GetName(), annotations[annotationRollout])
		c.recordWarning(obj, reasonNoRolloutGate, "stale rollout gate cleared")
		if err := c.patchPodGroupDeploymentAnnotations(ctx, obj.GetNamespace(), obj.GetName(), map[string]interface{}{
			annotationRollout:             nil,
			annotationRolloutAllowedStart: nil,
			annotationRolloutAllowedEnd:   nil,
			annotationRolloutWave:         nil,
			annotationRolloutAction:       nil,
			annotationRolloutPhase:        nil,
			annotationRolloutPausedReason: nil,
		}); err != nil {
			return err
		}
	}
	return nil
}

func (c *Controller) patchPodGroupDeploymentAnnotations(ctx context.Context, namespace, name string, annotations map[string]interface{}) error {
	return c.patchResourceMetadata(ctx, podGroupDeploymentGVR, namespace, name, nil, annotations, nil, nil)
}

func (c *Controller) patchPodGroupDeploymentMetadata(ctx context.Context, namespace, name string, labels map[string]string, annotations interface{}, ownerReferences []metav1.OwnerReference) error {
	return c.patchResourceMetadata(ctx, podGroupDeploymentGVR, namespace, name, labels, annotations, ownerReferences, nil)
}

func (c *Controller) patchPodGroupDeploymentFinalizers(ctx context.Context, namespace, name string, finalizers []string) error {
	return c.patchResourceMetadata(ctx, podGroupDeploymentGVR, namespace, name, nil, nil, nil, finalizers)
}

func (c *Controller) patchPodGroupDeploymentRolloutFinalizers(ctx context.Context, namespace, name string, finalizers []string) error {
	return c.patchResourceMetadata(ctx, podGroupDeploymentRolloutGVR, namespace, name, nil, nil, nil, finalizers)
}

func (c *Controller) patchResourceMetadata(ctx context.Context, resource schema.GroupVersionResource, namespace, name string, labels map[string]string, annotations interface{}, ownerReferences []metav1.OwnerReference, finalizers []string) error {
	metadata := map[string]interface{}{}
	if labels != nil {
		metadata["labels"] = labels
	}
	if annotations != nil {
		metadata["annotations"] = annotations
	}
	if ownerReferences != nil {
		metadata["ownerReferences"] = ownerReferences
	}
	if finalizers != nil {
		metadata["finalizers"] = finalizers
	}
	raw, err := json.Marshal(map[string]interface{}{
		"metadata": metadata,
	})
	if err != nil {
		return err
	}
	_, err = c.dynamic.Resource(resource).Namespace(namespace).Patch(ctx, name, types.MergePatchType, raw, metav1.PatchOptions{})
	return err
}
