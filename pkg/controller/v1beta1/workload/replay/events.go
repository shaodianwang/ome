package replay

import (
	"context"
	"errors"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation/field"

	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
)

// eventApplier turns one timeline event into the cluster or spec change
// the table says it is, and returns the detail the trace records.
type eventApplier func(ctx context.Context, d *driver, ev TimelineEvent) (string, error)

// eventAppliers is the set of table events the driver can stage. An event
// the table declares but this map does not carry fails to load, so a
// scenario can never silently skip an observation.
var eventAppliers map[string]eventApplier

func init() {
	eventAppliers = map[string]eventApplier{
		"pod.scheduled":           applyPodScheduled,
		"pod.containersReady":     applyContainersReady,
		"pod.ready":               applyPodReady,
		"pod.servingOn":           applyServing(corev1.ConditionTrue),
		"pod.servingOff":          applyServing(corev1.ConditionFalse),
		"pod.terminating":         applyPodTerminating,
		"pod.deleted":             applyPodDeleted,
		"pod.phaseTerminal":       applyPhaseTerminal,
		"pod.waiting":             applyPodWaiting,
		"pod.unschedulable":       applyUnschedulable,
		"endpoint.rotationIn":     applyRotation(true),
		"endpoint.rotationOut":    applyRotation(false),
		"api.quotaDenied":         applyRejection(quotaDenied),
		"api.invalid":             applyRejection(invalidPodSpec),
		"spec.revision":           applySpecRevision,
		"spec.strategy":           applySpecStrategy,
		"spec.replicasUp":         applyReplicas,
		"spec.replicasDown":       applyReplicas,
		"spec.pauseTrue":          applyPause(true, false),
		"spec.pauseFreeze":        applyPause(true, true),
		"spec.unpause":            applyPause(false, false),
		"timer.operationDeadline": applyOperationDeadline,
	}
}

// apply stages one event and records it.
func (d *driver) apply(ctx context.Context, ev TimelineEvent) error {
	if ev.ID == ClockAdvance {
		detail, err := d.advance("clock.advance", ev.Args.Duration)
		if err != nil {
			return err
		}
		d.trace.applied(ev, detail)
		return nil
	}
	applier, ok := eventAppliers[ev.ID]
	if !ok {
		return fmt.Errorf("replay: no applier for event %s", ev.ID)
	}
	detail, err := applier(ctx, d, ev)
	if err != nil {
		return err
	}
	d.trace.applied(ev, detail)
	return nil
}

// advance moves the injected clock forward.
func (d *driver) advance(field, value string) (string, error) {
	delta, err := ParseDuration(field, value)
	if err != nil {
		return "", err
	}
	if delta < 0 {
		return "", fmt.Errorf("replay: %s: the clock does not run backwards", field)
	}
	d.clock.Step(delta)
	return "by=" + delta.String(), nil
}

func (d *driver) podName(ref PodRef) string {
	return query.PodName(d.opts.OwnerName, d.opts.Component, ref.Index, ref.RunnerName(), ref.Ordinal)
}

func requirePod(ev TimelineEvent) (PodRef, error) {
	if ev.Args.Pod == nil {
		return PodRef{}, fmt.Errorf("replay: event %s needs a pod reference", ev.Key())
	}
	return *ev.Args.Pod, nil
}

func applyPodScheduled(ctx context.Context, d *driver, ev TimelineEvent) (string, error) {
	ref, err := requirePod(ev)
	if err != nil {
		return "", err
	}
	node := ev.Args.Message
	if node == "" {
		node = fmt.Sprintf("node-%d", ref.Index)
	}
	name := d.podName(ref)
	return "pod=" + name + " node=" + node, d.patchPod(ctx, name, func(pod *corev1.Pod) {
		pod.Spec.NodeName = node
		setPodCondition(pod, corev1.PodScheduled, corev1.ConditionTrue, d.clock.Now())
	})
}

func applyContainersReady(ctx context.Context, d *driver, ev TimelineEvent) (string, error) {
	ref, err := requirePod(ev)
	if err != nil {
		return "", err
	}
	name := d.podName(ref)
	return "pod=" + name, d.patchPod(ctx, name, func(pod *corev1.Pod) {
		pod.Status.Phase = corev1.PodRunning
		setPodCondition(pod, corev1.ContainersReady, corev1.ConditionTrue, d.clock.Now())
		setContainerStatusesReady(pod)
	})
}

// applyPodReady is the kubelet folding every readiness gate into the
// pod's own Ready condition, which is the bar every promote reads.
func applyPodReady(ctx context.Context, d *driver, ev TimelineEvent) (string, error) {
	ref, err := requirePod(ev)
	if err != nil {
		return "", err
	}
	name := d.podName(ref)
	return "pod=" + name, d.patchPod(ctx, name, func(pod *corev1.Pod) {
		pod.Status.Phase = corev1.PodRunning
		setPodCondition(pod, corev1.ContainersReady, corev1.ConditionTrue, d.clock.Now())
		setPodCondition(pod, corev1.PodReady, corev1.ConditionTrue, d.clock.Now())
		setContainerStatusesReady(pod)
	})
}

func applyServing(status corev1.ConditionStatus) eventApplier {
	return func(ctx context.Context, d *driver, ev TimelineEvent) (string, error) {
		ref, err := requirePod(ev)
		if err != nil {
			return "", err
		}
		name := d.podName(ref)
		return "pod=" + name, d.patchPod(ctx, name, func(pod *corev1.Pod) {
			setPodCondition(pod, query.ServingConditionType, status, d.clock.Now())
		})
	}
}

func applyPodTerminating(ctx context.Context, d *driver, ev TimelineEvent) (string, error) {
	ref, err := requirePod(ev)
	if err != nil {
		return "", err
	}
	name := d.podName(ref)
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: d.opts.Namespace, Name: name}}
	if err := d.staging(func() error { return d.cli.Delete(ctx, pod) }); err != nil {
		return "", fmt.Errorf("replay: terminate pod %s: %w", name, err)
	}
	return "pod=" + name, nil
}

func applyPodDeleted(ctx context.Context, d *driver, ev TimelineEvent) (string, error) {
	ref, err := requirePod(ev)
	if err != nil {
		return "", err
	}
	name := d.podName(ref)
	return "pod=" + name, d.removePod(ctx, name)
}

func applyPhaseTerminal(ctx context.Context, d *driver, ev TimelineEvent) (string, error) {
	ref, err := requirePod(ev)
	if err != nil {
		return "", err
	}
	phase := corev1.PodPhase(ev.Variant)
	if phase == "" {
		return "", fmt.Errorf("replay: pod.phaseTerminal needs a variant")
	}
	name := d.podName(ref)
	return "pod=" + name, d.patchPod(ctx, name, func(pod *corev1.Pod) {
		pod.Status.Phase = phase
		setPodCondition(pod, corev1.ContainersReady, corev1.ConditionFalse, d.clock.Now())
		setPodCondition(pod, corev1.PodReady, corev1.ConditionFalse, d.clock.Now())
		exitCode := int32(1)
		if phase == corev1.PodSucceeded {
			exitCode = 0
		}
		statuses := make([]corev1.ContainerStatus, 0, len(pod.Spec.Containers))
		for _, c := range pod.Spec.Containers {
			statuses = append(statuses, corev1.ContainerStatus{
				Name:  c.Name,
				Image: c.Image,
				State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
					ExitCode:   exitCode,
					Reason:     string(phase),
					FinishedAt: metav1.NewTime(d.clock.Now()),
				}},
			})
		}
		pod.Status.ContainerStatuses = statuses
	})
}

func applyPodWaiting(ctx context.Context, d *driver, ev TimelineEvent) (string, error) {
	ref, err := requirePod(ev)
	if err != nil {
		return "", err
	}
	if ev.Variant == "" {
		return "", fmt.Errorf("replay: pod.waiting needs a variant naming the reason")
	}
	container := ev.Args.Container
	if container == "" {
		container = d.opts.ContainerName
	}
	name := d.podName(ref)
	return "pod=" + name + " reason=" + ev.Variant, d.patchPod(ctx, name, func(pod *corev1.Pod) {
		pod.Status.Phase = corev1.PodPending
		setPodCondition(pod, corev1.ContainersReady, corev1.ConditionFalse, d.clock.Now())
		pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
			Name:  container,
			Image: podImage(pod, container),
			State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{
				Reason:  ev.Variant,
				Message: ev.Args.Message,
			}},
		}}
	})
}

func applyUnschedulable(ctx context.Context, d *driver, ev TimelineEvent) (string, error) {
	ref, err := requirePod(ev)
	if err != nil {
		return "", err
	}
	name := d.podName(ref)
	return "pod=" + name, d.patchPod(ctx, name, func(pod *corev1.Pod) {
		pod.Status.Phase = corev1.PodPending
		for i := range pod.Status.Conditions {
			if pod.Status.Conditions[i].Type == corev1.PodScheduled {
				pod.Status.Conditions[i].Status = corev1.ConditionFalse
				pod.Status.Conditions[i].Reason = corev1.PodReasonUnschedulable
				pod.Status.Conditions[i].LastTransitionTime = metav1.NewTime(d.clock.Now())
				return
			}
		}
		pod.Status.Conditions = append(pod.Status.Conditions, corev1.PodCondition{
			Type:               corev1.PodScheduled,
			Status:             corev1.ConditionFalse,
			Reason:             corev1.PodReasonUnschedulable,
			LastTransitionTime: metav1.NewTime(d.clock.Now()),
		})
	})
}

func applyRotation(ready bool) eventApplier {
	return func(ctx context.Context, d *driver, ev TimelineEvent) (string, error) {
		ref, err := requirePod(ev)
		if err != nil {
			return "", err
		}
		name := d.podName(ref)
		return "pod=" + name, d.setRotation(ctx, name, ready)
	}
}

// applyRejection arms an apiserver refusal for the writes a scenario
// names. count defaults to one write.
func applyRejection(build func(resource, name string) error) eventApplier {
	return func(_ context.Context, d *driver, ev TimelineEvent) (string, error) {
		verb := ev.Args.Verb
		if verb == "" {
			verb = "create"
		}
		resource := ev.Args.Resource
		if resource == "" {
			resource = "pods"
		}
		count := ev.Args.Count
		if count == 0 {
			count = 1
		}
		d.rejections = append(d.rejections, &pendingRejection{
			verb:      verb,
			resource:  resource,
			remaining: count,
			err:       build(resource, d.opts.OwnerName),
			label:     ev.Key(),
		})
		return fmt.Sprintf("verb=%s resource=%s count=%d", verb, resource, count), nil
	}
}

func quotaDenied(resource, name string) error {
	return apierrors.NewForbidden(
		schema.GroupResource{Resource: resource}, name,
		errors.New("exceeded quota: compute-resources, requested: pods=1, used: pods=4, limited: pods=4"),
	)
}

func invalidPodSpec(resource, name string) error {
	return apierrors.NewInvalid(
		schema.GroupKind{Kind: "Pod"}, name,
		field.ErrorList{field.Invalid(
			field.NewPath("spec", "containers").Index(0).Child("resources", "limits"),
			"-1", "must be greater than or equal to 0",
		)},
	)
}

func applySpecRevision(ctx context.Context, d *driver, ev TimelineEvent) (string, error) {
	if ev.Args.Image == "" {
		return "", fmt.Errorf("replay: spec.revision needs the new image")
	}
	d.spec.Image = ev.Args.Image
	if err := d.bumpGeneration(ctx); err != nil {
		return "", err
	}
	return fmt.Sprintf("image=%s generation=%d", ev.Args.Image, d.owner.Generation), nil
}

// applySpecStrategy rewrites lifecycle.updateStrategy. The strategy is not
// part of the revision payload, so the edit retargets nothing: what it
// tests is which machine owns a row that already has an attempt in flight.
func applySpecStrategy(ctx context.Context, d *driver, ev TimelineEvent) (string, error) {
	if ev.Args.Strategy == "" {
		return "", fmt.Errorf("replay: spec.strategy needs the new strategy")
	}
	d.spec.Strategy = ev.Args.Strategy
	if err := d.bumpGeneration(ctx); err != nil {
		return "", err
	}
	return fmt.Sprintf("strategy=%s generation=%d", ev.Args.Strategy, d.owner.Generation), nil
}

func applyReplicas(ctx context.Context, d *driver, ev TimelineEvent) (string, error) {
	if ev.Args.To == nil {
		return "", fmt.Errorf("replay: %s needs the new replica count", ev.Key())
	}
	d.spec.Replicas = *ev.Args.To
	if err := d.bumpGeneration(ctx); err != nil {
		return "", err
	}
	return fmt.Sprintf("replicas=%d generation=%d", d.spec.Replicas, d.owner.Generation), nil
}

func applyPause(paused, freeze bool) eventApplier {
	return func(ctx context.Context, d *driver, _ TimelineEvent) (string, error) {
		d.spec.Paused = paused
		d.spec.PauseFreeze = freeze
		if err := d.bumpGeneration(ctx); err != nil {
			return "", err
		}
		return fmt.Sprintf("paused=%t freeze=%t generation=%d", paused, freeze, d.owner.Generation), nil
	}
}

// applyOperationDeadline advances the clock onto the earliest in-flight
// operation deadline, plus an optional slack, so the escalation pass sees
// it elapsed. It is the table's timer.operationDeadline expressed the only
// way a deterministic harness can express a timer: by moving the clock.
func applyOperationDeadline(_ context.Context, d *driver, ev TimelineEvent) (string, error) {
	var earliest time.Time
	for _, row := range d.store.Rows() {
		op := row.Operation
		if op == nil || op.Deadline.IsZero() {
			continue
		}
		if earliest.IsZero() || op.Deadline.Time.Before(earliest) {
			earliest = op.Deadline.Time
		}
	}
	if earliest.IsZero() {
		return "", fmt.Errorf("replay: timer.operationDeadline: no in-flight operation carries a deadline")
	}
	if ev.Args.Slack == "" {
		return "", fmt.Errorf("replay: timer.operationDeadline needs a slack: how far past the deadline the pass observes it is the scenario's choice")
	}
	slack, err := ParseDuration("timer.operationDeadline.slack", ev.Args.Slack)
	if err != nil {
		return "", err
	}
	target := earliest.Add(slack)
	now := d.clock.Now()
	if target.Before(now) {
		return "already-elapsed", nil
	}
	d.clock.SetTime(target)
	return "to=" + d.norm.at(target), nil
}

func podImage(pod *corev1.Pod, container string) string {
	for _, c := range pod.Spec.Containers {
		if c.Name == container {
			return c.Image
		}
	}
	return ""
}
