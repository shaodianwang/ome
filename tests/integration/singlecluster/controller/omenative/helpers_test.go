package omenative

import (
	"context"
	"time"

	"github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// podKubeletSimulator watches every OMENative-owned pod in the test
// envtest cluster and synthesizes the lifecycle conditions a real
// kubelet would write — without it, no OMENative pod ever reaches
// ContainersReady and the readiness-gate pipeline can't converge.
//
// The simulator runs in a goroutine and polls. Each tick, for every
// OMENative-managed pod without ContainersReady=True it stamps the
// condition via Status().Update. ContainersReady is the signal
// OMENative's Create/Update/Migrate loops wait for before flipping
// the ome.io/serving gate; flipping that gate completes the
// readiness pipeline (PodReady is the AND of all gates and is
// computed by the apiserver itself).
//
// The simulator does NOT attempt to set Spec.NodeName: envtest's
// real apiserver rejects Spec updates after pod creation (NodeName
// is set via the binding subresource which controller-runtime's
// client.Client doesn't expose). Scenarios that need NodeName-aware
// behavior (MIGRATE-5 FromNode validation, scheduler-affinity
// integration tests) need a separate approach — they can either go
// through a clientset-based binder helper or assert at the API
// level rather than the runtime level.
//
// Polling-based (not informer-based) for simplicity: envtest is
// single-process and the loop's side effects are idempotent.
type podKubeletSimulator struct {
	c         client.Client
	pollEvery time.Duration

	stopCh chan struct{}
	doneCh chan struct{}
}

const (
	simPollInterval = 100 * time.Millisecond
	// labelManagedBy + managedByOMENative mirror the omenative
	// package's pod-label contract. Redeclared here rather than
	// imported so the test suite has no transitive dependency on
	// the package-private label constants.
	labelManagedByKey = "ome.io/managed-by"
	managedByValue    = "OMENative"
)

func newPodKubeletSimulator(c client.Client) *podKubeletSimulator {
	return &podKubeletSimulator{
		c:         c,
		pollEvery: simPollInterval,
	}
}

// Start spins the simulator loop in a goroutine. Loop exits when ctx
// is cancelled or Stop is called.
func (s *podKubeletSimulator) Start(ctx context.Context) {
	s.stopCh = make(chan struct{})
	s.doneCh = make(chan struct{})
	go s.run(ctx)
}

// Stop signals the loop to exit and waits for it to finish. Safe to
// call multiple times.
func (s *podKubeletSimulator) Stop() {
	if s.stopCh == nil {
		return
	}
	select {
	case <-s.stopCh:
		// already closed
	default:
		close(s.stopCh)
	}
	<-s.doneCh
	s.stopCh = nil
}

func (s *podKubeletSimulator) run(ctx context.Context) {
	defer close(s.doneCh)
	tick := time.NewTicker(s.pollEvery)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.stopCh:
			return
		case <-tick.C:
			s.step(ctx)
		}
	}
}

func (s *podKubeletSimulator) step(ctx context.Context) {
	list := &corev1.PodList{}
	if err := s.c.List(ctx, list, client.MatchingLabels{labelManagedByKey: managedByValue}); err != nil {
		return
	}
	for i := range list.Items {
		s.advance(ctx, &list.Items[i])
	}
}

// advance stamps ContainersReady=True on pod if not already set.
// Idempotent on re-entry. Skipped for pods marked for deletion to
// avoid racing the controller's cleanup.
func (s *podKubeletSimulator) advance(ctx context.Context, pod *corev1.Pod) {
	if pod.DeletionTimestamp != nil {
		return
	}
	if hasCondition(pod.Status.Conditions, corev1.ContainersReady, corev1.ConditionTrue) {
		return
	}
	s.patchStatusCondition(ctx, pod, corev1.PodCondition{
		Type:               corev1.ContainersReady,
		Status:             corev1.ConditionTrue,
		LastTransitionTime: metav1.Now(),
	})
}

// patchStatusCondition appends or replaces a condition on pod.Status
// via Status().Update. Best-effort: a conflict on the next tick
// retries naturally because the condition is still absent / wrong.
func (s *podKubeletSimulator) patchStatusCondition(ctx context.Context, pod *corev1.Pod, cond corev1.PodCondition) {
	fresh := &corev1.Pod{}
	if err := s.c.Get(ctx, client.ObjectKeyFromObject(pod), fresh); err != nil {
		return
	}
	replaced := false
	for i := range fresh.Status.Conditions {
		if fresh.Status.Conditions[i].Type == cond.Type {
			fresh.Status.Conditions[i] = cond
			replaced = true
			break
		}
	}
	if !replaced {
		fresh.Status.Conditions = append(fresh.Status.Conditions, cond)
	}
	_ = s.c.Status().Update(ctx, fresh)
}

func hasCondition(conds []corev1.PodCondition, t corev1.PodConditionType, s corev1.ConditionStatus) bool {
	for _, c := range conds {
		if c.Type == t {
			return c.Status == s
		}
	}
	return false
}

// hasConditionTrue is the export-friendly variant used by test
// assertions. Sugar over hasCondition with Status=True.
func hasConditionTrue(conds []corev1.PodCondition, t corev1.PodConditionType) bool {
	return hasCondition(conds, t, corev1.ConditionTrue)
}

// listOMENativeRevisions returns every ControllerRevision in
// namespace tagged with the OMENative managed-by label. Used by
// revision-history assertions.
func listOMENativeRevisions(g gomega.Gomega, namespace string) []appsv1.ControllerRevision {
	crs := &appsv1.ControllerRevisionList{}
	g.Expect(k8sClient.List(suiteCtx, crs,
		client.InNamespace(namespace),
		client.MatchingLabels{labelManagedByKey: managedByValue},
	)).To(gomega.Succeed())
	return crs.Items
}
