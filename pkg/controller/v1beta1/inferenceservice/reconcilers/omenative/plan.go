package omenative

import (
	"errors"
	"fmt"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

// ErrMultiPodNotYetSupported is returned by BuildPlan when the Component
// declares Leader or Worker (multi-pod Instances). The single-pod runner
// layout is what this PR implements; multi-pod (leader + worker per
// Instance) is deferred to a follow-up.
var ErrMultiPodNotYetSupported = errors.New("OMENative multi-pod Instances not yet supported")

// BuildPlan computes the desired ComponentPlan from a ReconcileParams.
//
// The function is pure: it inspects p (ISVC + ComponentExt + MultiPod
// flag + replica counts) and produces the plan the rest of the reconciler
// will drive toward. No I/O, no controller-runtime calls.
//
// Scope: single-pod Instances only. If p.MultiPod is true, returns
// ErrMultiPodNotYetSupported without partially populating ComponentPlan.
//
// Defaulting: the mutating webhook fills in `omenative.*` for OMENative-
// annotated Components, so in production the fields are non-nil. For
// robustness against pre-defaulter objects or test fixtures, BuildPlan
// also applies the same defaults inline rather than relying solely on
// the webhook.
func BuildPlan(p ReconcileParams) (ComponentPlan, error) {
	if p.ComponentExt == nil {
		return ComponentPlan{}, fmt.Errorf("nil ComponentExtensionSpec for component=%s", p.Component)
	}
	if p.MultiPod {
		return ComponentPlan{}, ErrMultiPodNotYetSupported
	}

	replicas := int32(1)
	if p.ComponentExt.MinReplicas != nil && *p.ComponentExt.MinReplicas > 0 {
		replicas = int32(*p.ComponentExt.MinReplicas)
	}

	instances := make([]InstancePlan, replicas)
	for i := range instances {
		idx := int32(i)
		instances[i] = InstancePlan{
			Index:       idx,
			Incarnation: incarnationForIndex(p.ISVC, p.Component, idx),
			Runners: []RunnerPlan{
				{Name: "default", Size: 1},
			},
		}
	}

	omenative := p.ComponentExt.OMENative
	if omenative == nil {
		omenative = &v1beta1.OMENativeComponentSpec{}
	}

	return ComponentPlan{
		Component:            p.Component,
		Replicas:             replicas,
		Instances:            instances,
		RestartPolicy:        restartPolicyOrDefault(omenative.RestartPolicy, p.MultiPod),
		UpdateStrategy:       updateStrategyWithDefaults(omenative.UpdateStrategy),
		ReadyPolicy:          readyPolicyOrDefault(omenative.ReadyPolicy, p.MultiPod),
		InstanceReadyTimeout: instanceReadyTimeoutOrDefault(omenative.InstanceReadyTimeout),
		MigrationMode:        migrationModeOrDefault(omenative.MigrationPolicy),
	}, nil
}

func restartPolicyOrDefault(p *v1beta1.OMENativeRestartPolicy, multiPod bool) v1beta1.OMENativeRestartPolicy {
	if p != nil {
		return *p
	}
	if multiPod {
		return v1beta1.OMENativeRestartPolicyRecreateInstance
	}
	return v1beta1.OMENativeRestartPolicyNone
}

func readyPolicyOrDefault(p *v1beta1.OMENativeReadyPolicy, multiPod bool) v1beta1.OMENativeReadyPolicy {
	if p != nil {
		return *p
	}
	if multiPod {
		return v1beta1.OMENativeReadyPolicyAllPodReady
	}
	return v1beta1.OMENativeReadyPolicyNone
}

func updateStrategyWithDefaults(s *v1beta1.OMENativeUpdateStrategy) v1beta1.OMENativeUpdateStrategy {
	if s == nil {
		s = &v1beta1.OMENativeUpdateStrategy{}
	}
	out := *s
	if out.Type == "" {
		out.Type = v1beta1.OMENativeUpdateStrategyInPlaceIfPossible
	}
	if out.InPlaceUpdateStrategy == nil {
		out.InPlaceUpdateStrategy = &v1beta1.OMENativeInPlaceUpdateStrategy{}
	}
	if out.InPlaceUpdateStrategy.GracePeriodSeconds == nil {
		grace := int32(30)
		out.InPlaceUpdateStrategy.GracePeriodSeconds = &grace
	}
	if out.InPlaceUpdateStrategy.MarkNotReadyDuringLifecycle == nil {
		mark := true
		out.InPlaceUpdateStrategy.MarkNotReadyDuringLifecycle = &mark
	}
	return out
}

func instanceReadyTimeoutOrDefault(d *metav1.Duration) time.Duration {
	if d == nil {
		return 30 * time.Minute
	}
	return d.Duration
}

func migrationModeOrDefault(p *v1beta1.OMENativeMigrationPolicy) v1beta1.OMENativeMigrationPolicyMode {
	if p == nil || p.Mode == "" {
		return v1beta1.OMENativeMigrationPolicyModeAuto
	}
	return p.Mode
}

// incarnationForIndex returns the current Incarnation for the (component, idx)
// pair. If the ISVC has no InstanceStatus for that pair yet — first reconcile,
// or after a delete — incarnation starts at 1. Otherwise the existing value
// is preserved. This function only reads status; the writers are the
// patchInstanceStatus* helpers that own each lifecycle transition.
func incarnationForIndex(isvc *v1beta1.InferenceService, component v1beta1.ComponentType, idx int32) int64 {
	if isvc == nil {
		return 1
	}
	cs, ok := isvc.Status.Components[component]
	if !ok || cs.OMENative == nil {
		return 1
	}
	for _, s := range cs.OMENative.InstanceStatuses {
		if s.Index == idx && s.Incarnation > 0 {
			return s.Incarnation
		}
	}
	return 1
}
