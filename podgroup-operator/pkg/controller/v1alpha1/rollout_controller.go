package v1alpha1

import (
	"context"
	"fmt"
	"log"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func (c *Controller) reconcileRollouts(ctx context.Context) error {
	list, err := c.dynamic.Resource(podGroupDeploymentRolloutGVR).Namespace("").List(ctx, metav1.ListOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}

	referenced := map[string]struct{}{}
	for i := range list.Items {
		pgdr, err := decodePodGroupDeploymentRollout(&list.Items[i])
		if err != nil {
			log.Printf("decode rollout %s/%s failed: %v", list.Items[i].GetNamespace(), list.Items[i].GetName(), err)
			continue
		}
		memberKeys, err := c.reconcileOneRollout(ctx, pgdr)
		if err != nil {
			log.Printf("reconcile rollout %s/%s failed: %v", pgdr.Namespace, pgdr.Name, err)
			continue
		}
		for _, key := range memberKeys {
			referenced[key] = struct{}{}
		}
	}
	return c.clearStaleRolloutGates(ctx, referenced)
}

func (c *Controller) reconcileOneRollout(ctx context.Context, pgdr *PodGroupDeploymentRollout) ([]string, error) {
	status := PodGroupDeploymentRolloutStatus{
		ObservedGeneration: pgdr.Generation,
		Mode:               pgdr.Spec.Mode,
		Phase:              "Progressing",
	}
	members, err := c.reconcileRolloutChildren(ctx, pgdr)
	if err != nil {
		status.Phase = "Blocked"
		status.Reason = err.Error()
		return nil, c.updateRolloutStatus(ctx, pgdr, status)
	}

	maxUnavailablePercent := pgdr.Spec.Strategy.MaxUnavailablePercent
	if maxUnavailablePercent <= 0 {
		maxUnavailablePercent = defaultMaxUnavailablePercent
	}
	progress := make([]rolloutMemberProgress, 0, len(members))
	memberKeys := make([]string, 0, len(members))
	for _, member := range members {
		key := pgdr.Namespace + "/" + member.Name
		memberKeys = append(memberKeys, key)
		obj, err := c.dynamic.Resource(podGroupDeploymentGVR).Namespace(pgdr.Namespace).Get(ctx, member.Name, metav1.GetOptions{})
		mp := rolloutMemberProgress{member: member}
		if apierrors.IsNotFound(err) {
			mp.missing = true
			progress = append(progress, mp)
			continue
		}
		if err != nil {
			mp.err = err
			progress = append(progress, mp)
			continue
		}
		pgd, err := decodePodGroupDeployment(obj)
		if err != nil {
			mp.err = err
			progress = append(progress, mp)
			continue
		}
		spec, err := defaultAndValidateSpec(pgd.Spec)
		if err != nil {
			mp.err = err
			progress = append(progress, mp)
			continue
		}
		pgd.Spec = spec
		desiredRevision, err := deploymentRevision(&pgd.Spec)
		if err != nil {
			mp.err = err
			progress = append(progress, mp)
			continue
		}
		mp.pgd = pgd
		mp.desired = desiredRevision
		mp.waveSize = rolloutWaveSize(pgd.Spec.Groups, maxUnavailablePercent)
		mp.updatedReady = updatedReadyGroups(pgd.Status.Groups, desiredRevision)
		mp.pods, err = c.podsForDeployment(ctx, pgd)
		if err != nil {
			mp.err = err
			progress = append(progress, mp)
			continue
		}
		mp.inPlace, err = rolloutMemberInPlaceEligible(pgd, mp.pods, desiredRevision)
		if err != nil {
			mp.err = err
			progress = append(progress, mp)
			continue
		}
		progress = append(progress, mp)
	}

	planRolloutWaves(progress)
	globalCompleted := int32(-1)
	for _, mp := range progress {
		if mp.pgd == nil || mp.err != nil || mp.missing {
			continue
		}
		if globalCompleted < 0 || mp.completed < globalCompleted {
			globalCompleted = mp.completed
		}
	}
	if globalCompleted < 0 {
		globalCompleted = 0
	}
	status.CurrentWave = globalCompleted
	phase, actions := rolloutPhaseAndActions(pgdr.Spec.Mode, progress, globalCompleted)
	status.CurrentPhase = phase

	blockedReasons := []string{}
	hasDegraded := false
	for _, mp := range progress {
		memberStatus := RolloutMemberStatus{
			Role:               mp.member.Role,
			PodGroupDeployment: mp.member.Name,
			Phase:              "Progressing",
		}
		if mp.missing {
			memberStatus.Phase = "Blocked"
			memberStatus.BlockedReason = "PodGroupDeployment not found"
			blockedReasons = append(blockedReasons, fmt.Sprintf("%s missing", mp.member.Name))
			status.Members = append(status.Members, memberStatus)
			continue
		}
		if mp.err != nil {
			memberStatus.Phase = "Degraded"
			memberStatus.BlockedReason = mp.err.Error()
			hasDegraded = true
			blockedReasons = append(blockedReasons, fmt.Sprintf("%s error: %v", mp.member.Name, mp.err))
			status.Members = append(status.Members, memberStatus)
			continue
		}

		allowedWave := globalCompleted
		allowedStart, allowedEnd := rolloutMemberWindow(mp, allowedWave)

		memberStatus.Groups = mp.pgd.Spec.Groups
		memberStatus.WaveSize = allowedEnd - allowedStart
		memberStatus.AllowedStart = allowedStart
		memberStatus.AllowedEnd = allowedEnd
		memberStatus.Action = actions[mp.member.Role]
		memberStatus.UpdatedReadyGroups = mp.updatedReady
		if mp.updatedReady >= mp.pgd.Spec.Groups {
			memberStatus.Phase = "Complete"
		} else if memberStatus.Action == rolloutActionHold && phase != rolloutPhaseComplete {
			memberStatus.Phase = "Blocked"
			memberStatus.BlockedReason = "waiting for rollout phase " + phase
		}
		status.Members = append(status.Members, memberStatus)

		pausedReason := ""
		if memberStatus.Phase == "Blocked" {
			pausedReason = memberStatus.BlockedReason
		}
		if phase == rolloutPhaseComplete {
			if err := c.clearRolloutGate(ctx, mp.pgd); err != nil {
				return memberKeys, err
			}
			continue
		}
		if err := c.applyRolloutGate(ctx, pgdr, mp.pgd, allowedStart, allowedEnd, allowedWave, memberStatus.Action, phase, pausedReason); err != nil {
			return memberKeys, err
		}
	}
	status.Actions = rolloutActionsSummary(status.Members)

	if hasDegraded {
		status.Phase = "Degraded"
		status.Reason = strings.Join(blockedReasons, "; ")
	} else if len(blockedReasons) > 0 {
		status.Phase = "Blocked"
		status.Reason = strings.Join(blockedReasons, "; ")
	} else if rolloutComplete(status.Members) {
		status.Phase = "Complete"
		status.CurrentPhase = rolloutPhaseComplete
	}
	return memberKeys, c.updateRolloutStatus(ctx, pgdr, status)
}

func rolloutActionsSummary(members []RolloutMemberStatus) string {
	if len(members) == 0 {
		return ""
	}
	parts := make([]string, 0, len(members))
	for _, member := range members {
		if member.Role == "" || member.Action == "" {
			continue
		}
		parts = append(parts, member.Role+"="+member.Action)
	}
	return strings.Join(parts, ",")
}
