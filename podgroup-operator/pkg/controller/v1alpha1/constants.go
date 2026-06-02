package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

const (
	operatorAPIVersion  = "podgroup.meta.com/v1alpha1"
	operatorKind        = "PodGroupDeployment"
	operatorRolloutKind = "PodGroupDeploymentRollout"

	defaultSchedulerName                 = "default-scheduler"
	defaultTopologyKey                   = corev1.LabelHostname
	defaultTerminationGracePeriodSeconds = int64(1)
	defaultMaxUnavailablePercent         = int32(20)
	defaultProgressDeadlineSeconds       = int32(3600)
	defaultMultiNodeDistInitPort         = int32(20000)

	labelManagedBy     = "app.kubernetes.io/managed-by"
	labelInstance      = "app.kubernetes.io/instance"
	labelName          = "app.kubernetes.io/name"
	labelGroupIndex    = "podgroup.meta.com/group-index"
	labelGroupInstance = "podgroup.meta.com/group-instance"
	labelOrdinal       = "podgroup.meta.com/ordinal"
	labelRevision      = "podgroup.meta.com/revision"
	labelServiceTarget = "podgroup.meta.com/service-target"
	labelSchedulerPG   = "scheduling.x-k8s.io/pod-group"
	labelRollout       = "podgroup.meta.com/rollout"
	labelRolloutRole   = "podgroup.meta.com/rollout-role"

	annotationRollout             = "podgroup.meta.com/rollout"
	annotationRolloutAllowedStart = "podgroup.meta.com/rollout-allowed-start"
	annotationRolloutAllowedEnd   = "podgroup.meta.com/rollout-allowed-end"
	annotationRolloutWave         = "podgroup.meta.com/rollout-wave"
	annotationRolloutAction       = "podgroup.meta.com/rollout-action"
	annotationRolloutPhase        = "podgroup.meta.com/rollout-phase"
	annotationRolloutPausedReason = "podgroup.meta.com/rollout-paused-reason"
	annotationRecreateRevision    = "podgroup.meta.com/recreate-revision"

	managerName = "podgroup-operator"

	rolloutActionHold      = "hold"
	rolloutActionDeleteOld = "delete-old"
	rolloutActionCreateNew = "create-new"
	rolloutActionInPlace   = "in-place"

	rolloutPhaseComplete         = "Complete"
	rolloutPhaseTerminatePrefill = "TerminatePrefill"
	rolloutPhaseTerminateDecode  = "TerminateDecode"
	rolloutPhaseCreateDecode     = "CreateDecode"
	rolloutPhaseCreatePrefill    = "CreatePrefill"
	rolloutPhaseUpdateDecode     = "UpdateDecode"
	rolloutPhaseUpdatePrefill    = "UpdatePrefill"
	rolloutPhaseTerminateUnified = "TerminateUnified"
	rolloutPhaseCreateUnified    = "CreateUnified"
	rolloutPhaseUpdateUnified    = "UpdateUnified"

	updateStrategyRecreatePod       UpdateStrategyType = "RecreatePod"
	updateStrategyInPlaceIfPossible UpdateStrategyType = "InPlaceIfPossible"
	updateStrategyInPlaceOnly       UpdateStrategyType = "InPlaceOnly"

	conditionReady         = "Ready"
	conditionProgressing   = "Progressing"
	conditionRolloutGated  = "RolloutGated"
	conditionBlocked       = "Blocked"
	conditionDegraded      = "Degraded"
	reasonAllGroupsReady   = "AllGroupsReady"
	reasonReconciling      = "Reconciling"
	reasonRolloutGate      = "RolloutGate"
	reasonNoRolloutGate    = "NoRolloutGate"
	reasonRolloutComplete  = "RolloutComplete"
	reasonRolloutBlocked   = "RolloutBlocked"
	reasonRolloutDegraded  = "RolloutDegraded"
	reasonRolloutProgress  = "RolloutProgressing"
	reasonFinalizing       = "Finalizing"
	reasonChildReconciling = "ChildReconciling"

	finalizerPodGroupDeployment        = "podgroup.meta.com/podgroupdeployment"
	finalizerPodGroupDeploymentRollout = "podgroup.meta.com/podgroupdeploymentrollout"
)

var (
	operatorGroupVersion = schema.GroupVersion{
		Group:   "podgroup.meta.com",
		Version: "v1alpha1",
	}

	podGroupDeploymentGVR = schema.GroupVersionResource{
		Group:    "podgroup.meta.com",
		Version:  "v1alpha1",
		Resource: "podgroupdeployments",
	}

	podGroupDeploymentGVK = operatorGroupVersion.WithKind(operatorKind)

	schedulerPodGroupGVR = schema.GroupVersionResource{
		Group:    "scheduling.x-k8s.io",
		Version:  "v1alpha1",
		Resource: "podgroups",
	}

	podGroupDeploymentRolloutGVR = schema.GroupVersionResource{
		Group:    "podgroup.meta.com",
		Version:  "v1alpha1",
		Resource: "podgroupdeploymentrollouts",
	}

	podGroupDeploymentRolloutGVK = operatorGroupVersion.WithKind(operatorRolloutKind)
)
