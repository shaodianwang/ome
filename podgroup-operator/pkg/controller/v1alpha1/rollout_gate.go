package v1alpha1

import (
	"context"
	"log"
	"strconv"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
	obj, err := c.dynamic.Resource(podGroupDeploymentGVR).Namespace(pgd.Namespace).Get(ctx, pgd.Name, metav1.GetOptions{})
	if err != nil {
		return err
	}
	obj.SetAnnotations(annotations)
	_, err = c.dynamic.Resource(podGroupDeploymentGVR).Namespace(pgd.Namespace).Update(ctx, obj, metav1.UpdateOptions{})
	return err
}

func (c *Controller) clearRolloutGate(ctx context.Context, pgd *PodGroupDeployment) error {
	annotations := pgd.Annotations
	if annotations[annotationRollout] == "" {
		return nil
	}
	logPGD(pgd, "clearing rollout gate rollout=%s", annotations[annotationRollout])
	next := withoutRolloutGateAnnotations(annotations)
	obj, err := c.dynamic.Resource(podGroupDeploymentGVR).Namespace(pgd.Namespace).Get(ctx, pgd.Name, metav1.GetOptions{})
	if err != nil {
		return err
	}
	obj.SetAnnotations(next)
	_, err = c.dynamic.Resource(podGroupDeploymentGVR).Namespace(pgd.Namespace).Update(ctx, obj, metav1.UpdateOptions{})
	return err
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
		next := mergeStringMaps(annotations)
		next = withoutRolloutGateAnnotations(next)
		obj.SetAnnotations(next)
		log.Printf("podgroupdeployment=%s/%s clearing stale rollout gate rollout=%s", obj.GetNamespace(), obj.GetName(), annotations[annotationRollout])
		if _, err := c.dynamic.Resource(podGroupDeploymentGVR).Namespace(obj.GetNamespace()).Update(ctx, obj, metav1.UpdateOptions{}); err != nil {
			return err
		}
	}
	return nil
}

func withoutRolloutGateAnnotations(annotations map[string]string) map[string]string {
	next := mergeStringMaps(annotations)
	delete(next, annotationRollout)
	delete(next, annotationRolloutAllowedStart)
	delete(next, annotationRolloutAllowedEnd)
	delete(next, annotationRolloutWave)
	delete(next, annotationRolloutAction)
	delete(next, annotationRolloutPhase)
	delete(next, annotationRolloutPausedReason)
	return next
}
