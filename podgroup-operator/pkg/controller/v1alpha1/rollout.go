package v1alpha1

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"
)

type rolloutMember struct {
	Role string
	Name string
}

type rolloutMemberProgress struct {
	member       rolloutMember
	pgd          *PodGroupDeployment
	desired      string
	waveSize     int32
	waveCount    int32
	completed    int32
	updatedReady int32
	inPlace      bool
	pods         []corev1.Pod
	missing      bool
	stale        bool
	err          error
}

func rolloutPhaseAndActions(mode string, progress []rolloutMemberProgress, wave int32) (string, map[string]string) {
	actions := map[string]string{}
	for _, mp := range progress {
		actions[mp.member.Role] = rolloutActionHold
	}
	if rolloutProgressComplete(progress) {
		return rolloutPhaseComplete, actions
	}

	switch mode {
	case "disagg":
		prefill := progressByRole(progress, "prefill")
		decode := progressByRole(progress, "decode")
		if prefill == nil || decode == nil {
			return "Blocked", actions
		}
		prefillStart, prefillEnd := rolloutMemberWindow(*prefill, wave)
		decodeStart, decodeEnd := rolloutMemberWindow(*decode, wave)
		if rolloutMemberUsesInPlace(*prefill) && rolloutMemberUsesInPlace(*decode) {
			if decode.updatedReady < decodeEnd {
				actions["decode"] = rolloutActionInPlace
				return rolloutPhaseUpdateDecode, actions
			}
			if prefill.updatedReady < prefillEnd {
				actions["prefill"] = rolloutActionInPlace
				return rolloutPhaseUpdatePrefill, actions
			}
			return rolloutPhaseUpdatePrefill, actions
		}
		if !oldPodsGone(prefill.pods, prefill.desired, prefillStart, prefillEnd) {
			actions["prefill"] = rolloutActionDeleteOld
			return rolloutPhaseTerminatePrefill, actions
		}
		if !oldPodsGone(decode.pods, decode.desired, decodeStart, decodeEnd) {
			actions["decode"] = rolloutActionDeleteOld
			return rolloutPhaseTerminateDecode, actions
		}
		if !newGroupsReady(decode.pgd.Status.Groups, decode.desired, decodeStart, decodeEnd) {
			actions["decode"] = rolloutActionCreateNew
			return rolloutPhaseCreateDecode, actions
		}
		if !newGroupsReady(prefill.pgd.Status.Groups, prefill.desired, prefillStart, prefillEnd) {
			actions["prefill"] = rolloutActionCreateNew
			return rolloutPhaseCreatePrefill, actions
		}
		return rolloutPhaseCreatePrefill, actions
	case "unified":
		unified := progressByRole(progress, "unified")
		if unified == nil {
			return "Blocked", actions
		}
		start, end := rolloutMemberWindow(*unified, wave)
		if rolloutMemberUsesInPlace(*unified) {
			if unified.updatedReady < end {
				actions["unified"] = rolloutActionInPlace
				return rolloutPhaseUpdateUnified, actions
			}
			return rolloutPhaseUpdateUnified, actions
		}
		if !oldPodsGone(unified.pods, unified.desired, start, end) {
			actions["unified"] = rolloutActionDeleteOld
			return rolloutPhaseTerminateUnified, actions
		}
		if !newGroupsReady(unified.pgd.Status.Groups, unified.desired, start, end) {
			actions["unified"] = rolloutActionCreateNew
			return rolloutPhaseCreateUnified, actions
		}
		return rolloutPhaseCreateUnified, actions
	default:
		return "Blocked", actions
	}
}

func rolloutMemberUsesInPlace(mp rolloutMemberProgress) bool {
	if mp.inPlace {
		return true
	}
	return mp.pgd != nil && mp.updatedReady >= mp.pgd.Spec.Groups
}

func progressByRole(progress []rolloutMemberProgress, role string) *rolloutMemberProgress {
	for i := range progress {
		if progress[i].member.Role == role && progress[i].pgd != nil && progress[i].err == nil && !progress[i].missing && !progress[i].stale {
			return &progress[i]
		}
	}
	return nil
}

func rolloutProgressComplete(progress []rolloutMemberProgress) bool {
	if len(progress) == 0 {
		return false
	}
	for _, mp := range progress {
		if mp.pgd == nil || mp.err != nil || mp.missing || mp.stale || mp.updatedReady < mp.pgd.Spec.Groups {
			return false
		}
	}
	return true
}

func planRolloutWaves(progress []rolloutMemberProgress) {
	waveCount := int32(0)
	for i := range progress {
		mp := &progress[i]
		if mp.pgd == nil || mp.err != nil || mp.missing || mp.stale {
			continue
		}
		memberWaveCount := rolloutWaveCount(mp.pgd.Spec.Groups, mp.waveSize)
		if memberWaveCount == 0 {
			continue
		}
		if waveCount == 0 || memberWaveCount < waveCount {
			waveCount = memberWaveCount
		}
	}
	for i := range progress {
		mp := &progress[i]
		if mp.pgd == nil || mp.err != nil || mp.missing || mp.stale {
			continue
		}
		mp.waveCount = waveCount
		if mp.waveCount == 0 {
			mp.waveCount = rolloutWaveCount(mp.pgd.Spec.Groups, mp.waveSize)
		}
		mp.completed = completedRolloutWaves(mp.pgd.Status.Groups, mp.desired, mp.pgd.Spec.Groups, mp.waveCount)
	}
}

func rolloutWaveCount(groups, waveSize int32) int32 {
	if groups <= 0 || waveSize <= 0 {
		return 0
	}
	return (groups + waveSize - 1) / waveSize
}

func rolloutMemberWindow(mp rolloutMemberProgress, wave int32) (int32, int32) {
	if mp.pgd == nil {
		return 0, 0
	}
	if mp.waveCount > 0 {
		return rolloutWindowByCount(mp.pgd.Spec.Groups, mp.waveCount, wave)
	}
	return rolloutWindow(mp.pgd.Spec.Groups, mp.waveSize, wave)
}

func rolloutWindowByCount(groups, waveCount, wave int32) (int32, int32) {
	if groups <= 0 || waveCount <= 0 {
		return 0, 0
	}
	if wave < 0 {
		wave = 0
	}
	if wave >= waveCount {
		return groups, groups
	}
	start := groups * wave / waveCount
	end := groups * (wave + 1) / waveCount
	if end <= start {
		end = start + 1
	}
	if end > groups {
		end = groups
	}
	return start, end
}

func rolloutWindow(groups, waveSize, wave int32) (int32, int32) {
	if groups <= 0 || waveSize <= 0 {
		return 0, 0
	}
	start := wave * waveSize
	if start > groups {
		start = groups
	}
	end := start + waveSize
	if end > groups {
		end = groups
	}
	return start, end
}

func oldPodsGone(pods []corev1.Pod, desiredRevision string, start, end int32) bool {
	for i := range pods {
		pod := &pods[i]
		group, ok := parseInt32Label(pod.Labels, labelGroupIndex)
		if !ok || group < start || group >= end {
			continue
		}
		if pod.Labels[labelRevision] != desiredRevision {
			return false
		}
	}
	return true
}

func newGroupsReady(groups []GroupStatus, desiredRevision string, start, end int32) bool {
	ready := map[int32]struct{}{}
	for _, group := range groups {
		if group.Index >= start && group.Index < end && group.Revision == desiredRevision && group.Phase == "Ready" {
			ready[group.Index] = struct{}{}
		}
	}
	return int32(len(ready)) == end-start
}

func rolloutMembersForMode(base, mode string) ([]rolloutMember, error) {
	switch mode {
	case "disagg":
		return []rolloutMember{
			{Role: "prefill", Name: base + "-prefill"},
			{Role: "decode", Name: base + "-decode"},
		}, nil
	case "unified":
		return []rolloutMember{
			{Role: "unified", Name: base + "-unified"},
		}, nil
	default:
		return nil, fmt.Errorf("spec.mode must be disagg or unified")
	}
}

func rolloutWaveSize(groups, maxUnavailablePercent int32) int32 {
	if groups <= 0 {
		return 0
	}
	if maxUnavailablePercent <= 0 {
		maxUnavailablePercent = defaultMaxUnavailablePercent
	}
	size := groups * maxUnavailablePercent / 100
	if size < 1 {
		size = 1
	}
	return size
}

func updatedReadyGroups(groups []GroupStatus, desiredRevision string) int32 {
	ready := int32(0)
	for _, group := range groups {
		if group.Revision == desiredRevision && group.Phase == "Ready" {
			ready++
		}
	}
	return ready
}

func completedRolloutWaves(groupStatus []GroupStatus, desiredRevision string, groups, waveCount int32) int32 {
	if groups <= 0 || waveCount <= 0 {
		return 0
	}
	if newGroupsReady(groupStatus, desiredRevision, 0, groups) {
		return waveCount
	}
	completed := int32(0)
	for wave := int32(0); wave < waveCount; wave++ {
		start, end := rolloutWindowByCount(groups, waveCount, wave)
		if !newGroupsReady(groupStatus, desiredRevision, start, end) {
			break
		}
		completed++
	}
	return completed
}

func rolloutComplete(members []RolloutMemberStatus) bool {
	if len(members) == 0 {
		return false
	}
	for _, member := range members {
		if member.Phase != "Complete" {
			return false
		}
	}
	return true
}
