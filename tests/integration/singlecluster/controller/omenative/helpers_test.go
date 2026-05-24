package omenative

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/podreadiness"
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
// The simulator also assigns Spec.NodeName to unscheduled pods via the
// binding subresource (the only path envtest's apiserver accepts for
// post-create node assignment). Without this, any FromNode-validation
// or scheduler-affinity scenario times out forever. Bindings are
// deterministic per pod name so a re-created pod with the same name
// lands on the same simulated node — keeps migration overlay scenarios
// reproducible.
//
// Polling-based (not informer-based) for simplicity: envtest is
// single-process and the loop's side effects are idempotent.
type podKubeletSimulator struct {
	c         client.Client
	binder    *podBinder
	pollEvery time.Duration

	stopCh chan struct{}
	doneCh chan struct{}

	// nodeOf records the per-pod-name node assignment so re-runs of
	// the same pod (e.g., after a recreate) get the same node.
	mu     sync.Mutex
	nodeOf map[string]string
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

func newPodKubeletSimulator(c client.Client, binder *podBinder) *podKubeletSimulator {
	return &podKubeletSimulator{
		c:         c,
		binder:    binder,
		pollEvery: simPollInterval,
		nodeOf:    map[string]string{},
	}
}

// simNodeNames is the pool the simulator round-robins over for
// pods that don't carry a pre-assigned node hint. Three names
// suffice for current scenarios: single-pod, source vs surge for
// migration, plus one extra for future multi-pod.
var simNodeNames = []string{"sim-node-a", "sim-node-b", "sim-node-c"}

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

// advance walks pod through the lifecycle a real kubelet would
// produce: bind to a node, stamp ContainersReady=True, and on
// deletion ack the kubelet-side cleanup by force-removing the pod
// (envtest has no kubelet, so a regular Delete leaves the pod
// sitting forever with DeletionTimestamp; restart / scale-down
// state machines that wait for the pod to actually disappear would
// hang otherwise). Idempotent on re-entry.
func (s *podKubeletSimulator) advance(ctx context.Context, pod *corev1.Pod) {
	// Deletion ack: pod is marked for deletion → finish the job by
	// force-removing via GracePeriodSeconds=0. Mimics what a real
	// kubelet does after draining containers.
	if pod.DeletionTimestamp != nil {
		zero := int64(0)
		_ = s.c.Delete(ctx, pod, &client.DeleteOptions{GracePeriodSeconds: &zero})
		return
	}

	// Step 1: bind to a node if not yet scheduled. Binding is a
	// one-shot post-create operation; once Spec.NodeName is set it
	// can't change. Re-binding an already-bound pod is harmless
	// (apiserver no-ops) but skipped here to keep traffic down.
	if pod.Spec.NodeName == "" && s.binder != nil {
		// Honor a hard NodeAffinity Required pin to kubernetes.io/hostname
		// so tests that need a specific node assignment (NodeAffinity
		// conflict specs, FromNode specs) get deterministic placement
		// regardless of the round-robin counter's spec-order drift.
		node := pinnedHostnameFromAffinity(pod)
		if node == "" {
			node = s.nodeFor(pod.Name)
		}
		// Best-effort: on bind failure we'll retry next tick.
		_ = s.binder.Bind(ctx, pod.Namespace, pod.Name, node)
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

// pinnedHostnameFromAffinity returns the single kubernetes.io/hostname
// value the pod's hard NodeAffinity pins to, or "" when the pin is
// absent or admits more than one host. Used by advance() to honor
// NodeAffinity Required pins ahead of round-robin assignment.
func pinnedHostnameFromAffinity(pod *corev1.Pod) string {
	if pod.Spec.Affinity == nil || pod.Spec.Affinity.NodeAffinity == nil {
		return ""
	}
	req := pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution
	if req == nil || len(req.NodeSelectorTerms) != 1 {
		return ""
	}
	for _, expr := range req.NodeSelectorTerms[0].MatchExpressions {
		if expr.Key != "kubernetes.io/hostname" || expr.Operator != corev1.NodeSelectorOpIn {
			continue
		}
		if len(expr.Values) == 1 {
			return expr.Values[0]
		}
	}
	return ""
}

// nodeFor returns a deterministic node name for podName. The same
// pod name always maps to the same node so a recreated pod with the
// same name (Restart's bumped-incarnation pod, Migrate's surge
// at a recycled index after a Failed migration) lands consistently.
func (s *podKubeletSimulator) nodeFor(podName string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if n, ok := s.nodeOf[podName]; ok {
		return n
	}
	n := simNodeNames[len(s.nodeOf)%len(simNodeNames)]
	s.nodeOf[podName] = n
	return n
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

// podBinder wraps a clientset for the one operation controller-
// runtime's typed client.Client refuses to expose: POSTing a
// corev1.Binding to /api/v1/namespaces/<ns>/pods/<name>/binding.
//
// Bind is the K8s contract for setting Spec.NodeName after pod create —
// direct Update of pod.Spec.NodeName is rejected (immutable subset).
// Without a binder, anything that needs a scheduled pod (FromNode
// validation, overlay-conflict detection, scheduler-affinity) is
// unrunnable.
type podBinder struct {
	cs kubernetes.Interface
}

func newPodBinder(cs kubernetes.Interface) *podBinder {
	return &podBinder{cs: cs}
}

// Bind POSTs a Binding for pod to nodeName via the binding
// subresource. Idempotent at the apiserver level: re-binding an
// already-bound pod returns a Conflict error which we swallow as a
// no-op (the desired end state — pod on that node — already holds).
func (b *podBinder) Bind(ctx context.Context, namespace, podName, nodeName string) error {
	if b == nil || b.cs == nil {
		return fmt.Errorf("podBinder: nil binder or clientset")
	}
	binding := &corev1.Binding{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: namespace,
		},
		Target: corev1.ObjectReference{
			Kind: "Node",
			Name: nodeName,
		},
	}
	return b.cs.CoreV1().Pods(namespace).Bind(ctx, binding, metav1.CreateOptions{})
}

// endpointSliceSimulator mirrors OMENative pod state into EndpointSlices
// so the drain wait + IsPodInRotation paths can converge against
// envtest's missing endpointslice-controller. Without it, every test
// that triggers a drain (Update recreate, Delete with active pods,
// Migrate's source drain) hangs forever.
//
// Behavior on each tick: list pods labeled ome.io/managed-by=OMENative,
// group by Spec.Subdomain (= headless service name), and upsert one
// EndpointSlice per service. Per-pod endpoint state is derived as:
//
//	pod DeletionTimestamp != nil    → Ready=false, Serving=true, Terminating=true
//	pod ContainersReady && IsServing → Ready=true,  Serving=true, Terminating=false
//	pod ContainersReady && !IsServing → Ready=false, Serving=true, Terminating=false
//	pod !ContainersReady             → Ready=false, Serving=false, Terminating=false
//
// The tri-state shape (Ready / Serving / Terminating) mirrors what
// kube-proxy produces in real clusters so drain regression tests can
// exercise the terminating-but-Serving case end-to-end.
//
// Polling-based for the same reasons as the kubelet simulator.
type endpointSliceSimulator struct {
	c         client.Client
	pollEvery time.Duration

	stopCh chan struct{}
	doneCh chan struct{}
}

const sliceSimSliceName = "ome-sim-slice"

func newEndpointSliceSimulator(c client.Client) *endpointSliceSimulator {
	return &endpointSliceSimulator{
		c:         c,
		pollEvery: simPollInterval,
	}
}

func (s *endpointSliceSimulator) Start(ctx context.Context) {
	s.stopCh = make(chan struct{})
	s.doneCh = make(chan struct{})
	go s.run(ctx)
}

func (s *endpointSliceSimulator) Stop() {
	if s.stopCh == nil {
		return
	}
	select {
	case <-s.stopCh:
	default:
		close(s.stopCh)
	}
	<-s.doneCh
	s.stopCh = nil
}

func (s *endpointSliceSimulator) run(ctx context.Context) {
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
			s.reconcile(ctx)
		}
	}
}

// reconcile walks the OMENative pod set once and upserts a slice per
// (namespace, subdomain) group. Pods without Spec.Subdomain are
// skipped — OMENative-rendered pods always carry a subdomain.
func (s *endpointSliceSimulator) reconcile(ctx context.Context) {
	pods := &corev1.PodList{}
	if err := s.c.List(ctx, pods, client.MatchingLabels{labelManagedByKey: managedByValue}); err != nil {
		return
	}

	type group struct {
		namespace, service string
		pods               []*corev1.Pod
	}
	byKey := map[string]*group{}
	for i := range pods.Items {
		p := &pods.Items[i]
		if p.Spec.Subdomain == "" {
			continue
		}
		key := p.Namespace + "/" + p.Spec.Subdomain
		g, ok := byKey[key]
		if !ok {
			g = &group{namespace: p.Namespace, service: p.Spec.Subdomain}
			byKey[key] = g
		}
		g.pods = append(g.pods, p)
	}
	for _, g := range byKey {
		s.upsertSlice(ctx, g.namespace, g.service, g.pods)
	}
}

// upsertSlice creates or updates the synthetic slice for service.
// The slice name is deterministic per service so re-runs of the
// reconcile loop overwrite the same object instead of accumulating.
func (s *endpointSliceSimulator) upsertSlice(ctx context.Context, namespace, service string, pods []*corev1.Pod) {
	desired := buildEndpoints(pods)
	name := service + "-" + sliceSimSliceName

	existing := &discoveryv1.EndpointSlice{}
	getErr := s.c.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, existing)
	if apierrors.IsNotFound(getErr) {
		slice := &discoveryv1.EndpointSlice{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: namespace,
				Labels:    map[string]string{discoveryv1.LabelServiceName: service},
			},
			AddressType: discoveryv1.AddressTypeIPv4,
			Endpoints:   desired,
		}
		_ = s.c.Create(ctx, slice)
		return
	}
	if getErr != nil {
		return
	}
	existing.Endpoints = desired
	_ = s.c.Update(ctx, existing)
}

// buildEndpoints derives the EndpointSlice endpoint set from pod
// state per the docstring table on endpointSliceSimulator.
func buildEndpoints(pods []*corev1.Pod) []discoveryv1.Endpoint {
	out := make([]discoveryv1.Endpoint, 0, len(pods))
	for _, p := range pods {
		ready, serving, terminating := derivePodEndpointFlags(p)
		// Synthesize a stable per-pod IP — apiserver will validate
		// AddressType=IPv4 against the slice; format doesn't matter
		// for the controller's TargetRef-keyed lookups.
		out = append(out, discoveryv1.Endpoint{
			Addresses: []string{"10.244.0." + itoaShort(len(out)+1)},
			Conditions: discoveryv1.EndpointConditions{
				Ready:       ptr.To(ready),
				Serving:     ptr.To(serving),
				Terminating: ptr.To(terminating),
			},
			TargetRef: &corev1.ObjectReference{
				Kind:      "Pod",
				Namespace: p.Namespace,
				Name:      p.Name,
				UID:       p.UID,
			},
		})
	}
	return out
}

func derivePodEndpointFlags(p *corev1.Pod) (ready, serving, terminating bool) {
	if p.DeletionTimestamp != nil {
		return false, true, true
	}
	cr := hasCondition(p.Status.Conditions, corev1.ContainersReady, corev1.ConditionTrue)
	if !cr {
		return false, false, false
	}
	if podreadiness.IsServing(p) {
		return true, true, false
	}
	return false, true, false
}

// itoaShort renders a 0–255 ordinal as an octet string. Synth IPs
// only — the controller keys lookups on TargetRef, not addresses.
func itoaShort(n int) string {
	if n < 10 {
		return string(rune('0' + n))
	}
	// Cheap formatter for the rare case where a single service has
	// 10+ pods. fmt.Sprintf is fine here; we're not on a hot path.
	return fmt.Sprintf("%d", n)
}

// crNames returns just the .Name fields of a CR list — useful in
// test failure messages where the full CR objects would be too noisy.
func crNames(items []appsv1.ControllerRevision) []string {
	out := make([]string, 0, len(items))
	for _, cr := range items {
		out = append(out, cr.Name)
	}
	return out
}
