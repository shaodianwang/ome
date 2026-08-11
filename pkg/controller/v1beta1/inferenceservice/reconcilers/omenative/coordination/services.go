package coordination

import (
	"context"
	"errors"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	workload "github.com/ome-projects/ome/pkg/controller/v1beta1/workload/types"
)

// servingPortName is the container-port NAME a runtime uses to mark its
// customer-traffic listener. It is a Kubernetes naming convention, not a
// behavioral value: it selects among the ports a runtime already declares
// and never contributes a port number of its own. A runner that declares
// no port under this name falls through to its first declared port.
const servingPortName = "http"

// ErrNoServingPort reports that a Component's merged runner declares no
// container port, so no per-revision routing Service can be built. A
// ClusterIP Service must publish at least one port, and publishing a port
// the runner does not listen on produces a Service whose EndpointSlice
// reads ready while every request is dropped — pod readiness is a property
// of the pod, not of the published port. Callers skip the routing Service
// instead of inventing a number.
var ErrNoServingPort = errors.New("component runner declares no container port")

// isvcGroupVersionKind identifies the InferenceService owner reference
// on each per-revision Service. Declared locally so this subpackage
// stays importable from either the top-level reconciler or its caller
// without closing an import cycle.
var isvcGroupVersionKind = v1beta1.SchemeGroupVersion.WithKind("InferenceService")

// PerRevisionService describes the routing + headless pair OMENative
// creates per (component, revisionHash). The two Services share the
// same selector but differ in ClusterIP semantics.
type PerRevisionService struct {
	// RoutingName is the ClusterIP Service name —
	// `<isvc>-<component>-rev-<hash>`. Used as the backend target by
	// the HTTPRoute weighted-backendRef consumer. Empty when
	// EnsurePerRevisionServices skipped the routing Service because the
	// Component's runner declares no container port.
	RoutingName string

	// HeadlessName is the headless Service name —
	// `<isvc>-<component>-rev-<hash>-headless`. Used for pod-level
	// DNS within the revision (multi-node tensor-parallel groups need
	// per-pod addressability among one revision's pods).
	HeadlessName string
}

// PerRevisionServiceName returns the per-revision Service name for one
// (component, revisionHash) pair. Format: `<isvc>-<component>-rev-<hash>`,
// bounded to the DNS1035 label limit. Delegates to the workload/query
// copy so the coordination-created name and the dispatch-path lookup
// (workload/ops) are always byte-identical.
func PerRevisionServiceName(isvcName string, component v1beta1.ComponentType, revisionHash string) string {
	return query.PerRevisionServiceName(isvcName, workload.ComponentType(component), revisionHash)
}

// PerRevisionHeadlessServiceName returns the per-revision headless
// Service name. Format: `<isvc>-<component>-rev-<hash>-headless`, bounded
// to the DNS1035 label limit.
func PerRevisionHeadlessServiceName(isvcName string, component v1beta1.ComponentType, revisionHash string) string {
	return query.PerRevisionHeadlessServiceName(isvcName, workload.ComponentType(component), revisionHash)
}

// PerRevisionServiceNames returns the routing + headless names for a
// (component, revisionHash) pair.
func PerRevisionServiceNames(isvcName string, component v1beta1.ComponentType, revisionHash string) PerRevisionService {
	return PerRevisionService{
		RoutingName:  PerRevisionServiceName(isvcName, component, revisionHash),
		HeadlessName: PerRevisionHeadlessServiceName(isvcName, component, revisionHash),
	}
}

// BuildPerRevisionRoutingService produces the ClusterIP per-revision
// Service for one (component, revisionHash). Pure compute; idempotent.
//
// `runnerPorts` are the container ports the Component's merged runner
// declares; the published port is resolved from them (see
// perRevisionRoutingPorts). Returns ErrNoServingPort when the set is
// empty — the Service is not buildable without a port the pods listen on.
//
// `multiPod` toggles the rank-0 filter on the routing selector:
//   - false (single-pod) → broad selector. SurgeThenDrain alternates
//     the pod-naming ordinal between 0 and 1; pinning ordinal=0 would
//     zero-endpoint the Service during surge.
//   - true (multi-pod) → adds `ome.io/runner=leader` + `ome.io/pod-ordinal=0`
//     so only the leader (rank-0) pod handles customer traffic. Workers run
//     distributed-init peers and cannot serve; pod-ordinal alone is not enough
//     because it is numbered per runner, so the rank-0 worker also carries 0.
//
// The routing Service's label map intentionally does NOT include the
// `pod-ordinal=0` key — Labels describe the Service itself for
// catalog/UI purposes; the Selector is the filter that gates pod
// membership.
func BuildPerRevisionRoutingService(isvc *v1beta1.InferenceService, component v1beta1.ComponentType, revisionHash string, multiPod bool, runnerPorts []corev1.ContainerPort) (*corev1.Service, error) {
	if isvc == nil {
		return nil, fmt.Errorf("nil InferenceService")
	}
	if revisionHash == "" {
		return nil, fmt.Errorf("empty revisionHash")
	}
	ports, err := perRevisionRoutingPorts(runnerPorts)
	if err != nil {
		return nil, err
	}
	labels := perRevisionServiceSelector(isvc.Name, component, revisionHash)
	selector := labels
	if multiPod {
		selector = make(map[string]string, len(labels)+2)
		for k, v := range labels {
			selector[k] = v
		}
		// Pin the leader: pod-ordinal is numbered PER RUNNER, so the rank-0
		// worker also carries pod-ordinal=0. Without runner=leader the selector
		// would match leader + worker-0 and route customer traffic to a worker
		// that cannot serve. runner=leader + pod-ordinal=0 selects only the leader.
		selector[query.LabelRunner] = string(v1beta1.RunnerNameLeader)
		selector[query.LabelPodOrdinal] = "0"
	}
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      PerRevisionServiceName(isvc.Name, component, revisionHash),
			Namespace: isvc.Namespace,
			Labels:    labels,
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(isvc, isvcGroupVersionKind),
			},
		},
		Spec: corev1.ServiceSpec{
			Type:                     corev1.ServiceTypeClusterIP,
			Selector:                 selector,
			PublishNotReadyAddresses: false,
			Ports:                    ports,
		},
	}, nil
}

// perRevisionRoutingPorts maps a Component's merged runner container ports
// onto the single ServicePort the routing Service publishes. HTTPRoute
// backendRef consumers route to that port, so it must be the one the
// runner listens on. Runtimes routinely declare several ports (serving,
// distributed-init rendezvous, PD bootstrap, metrics); the customer-traffic
// Service publishes only the serving one — the port named `http`, else the
// first declared port.
//
// TargetPort follows the container port's NAME when it has one. Kubernetes
// resolves a named targetPort per endpoint pod, so a revision whose pods
// were rendered from an older spec still receives traffic on the port that
// revision actually opened; a numeric targetPort would pin every revision
// to the current spec's number. Unnamed ports fall back to the number.
//
// ClusterIP Services require at least one port (k8s validation), so an
// empty runner-port set yields ErrNoServingPort. The headless variant
// takes no ports at all — peer DNS resolves pod IPs directly.
func perRevisionRoutingPorts(runnerPorts []corev1.ContainerPort) ([]corev1.ServicePort, error) {
	serving, ok := servingContainerPort(runnerPorts)
	if !ok {
		return nil, ErrNoServingPort
	}
	protocol := serving.Protocol
	if protocol == "" {
		protocol = corev1.ProtocolTCP
	}
	target := intstr.FromInt32(serving.ContainerPort)
	if serving.Name != "" {
		target = intstr.FromString(serving.Name)
	}
	return []corev1.ServicePort{{
		Name:       serving.Name,
		Port:       serving.ContainerPort,
		TargetPort: target,
		Protocol:   protocol,
	}}, nil
}

// servingContainerPort picks the customer-traffic port out of a runner's
// declared container ports: the one named `http`, else the first declared.
// Reports false when nothing is declared.
func servingContainerPort(ports []corev1.ContainerPort) (corev1.ContainerPort, bool) {
	if len(ports) == 0 {
		return corev1.ContainerPort{}, false
	}
	for _, p := range ports {
		if p.Name == servingPortName {
			return p, true
		}
	}
	return ports[0], true
}

// BuildPerRevisionHeadlessService produces the headless per-revision
// Service. Like the routing variant but with ClusterIP=None and
// PublishNotReadyAddresses=true so peer DNS resolves during gang init.
//
// Headless Service intentionally selects ALL pods of the revision —
// including workers — regardless of multi-pod-ness. Peer discovery
// for distributed init (NCCL all-gather, vLLM ray-init, etc.)
// requires every pod to be addressable via per-pod DNS. Only the
// per-revision ROUTING Service (BuildPerRevisionRoutingService) adds
// the rank-0 filter for multi-pod Components.
func BuildPerRevisionHeadlessService(isvc *v1beta1.InferenceService, component v1beta1.ComponentType, revisionHash string) (*corev1.Service, error) {
	if isvc == nil {
		return nil, fmt.Errorf("nil InferenceService")
	}
	if revisionHash == "" {
		return nil, fmt.Errorf("empty revisionHash")
	}
	selector := perRevisionServiceSelector(isvc.Name, component, revisionHash)
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      PerRevisionHeadlessServiceName(isvc.Name, component, revisionHash),
			Namespace: isvc.Namespace,
			Labels:    selector,
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(isvc, isvcGroupVersionKind),
			},
		},
		Spec: corev1.ServiceSpec{
			Type:                     corev1.ServiceTypeClusterIP,
			ClusterIP:                corev1.ClusterIPNone,
			Selector:                 selector,
			PublishNotReadyAddresses: true,
		},
	}, nil
}

// perRevisionServiceSelector is the label set per-revision Services
// use as their .spec.selector. The revision-hash label narrows the
// selector to one ControllerRevision's pods; the OMENative-managed
// label keeps the selector from picking up legacy pods on the
// same Component.
func perRevisionServiceSelector(isvcName string, component v1beta1.ComponentType, revisionHash string) map[string]string {
	return map[string]string{
		constants.InferenceServicePodLabelKey: isvcName,
		constants.OMEComponentLabel:           string(component),
		query.LabelRevisionHash:               revisionHash,
		query.LabelManagedBy:                  query.ManagedByOMENative,
	}
}

// EnsurePerRevisionServices ensures both the routing and headless
// Services exist for one (component, revisionHash). Idempotent and
// drift-correcting on the selector / labels / owner reference. Returns
// the names so the coordination layer can record them on
// Status.Components.<c>.Traffic[].
//
// `multiPod` tightens the routing Service selector to rank-0 only;
// the headless Service is unaffected (workers must stay discoverable
// for peer DNS).
//
// `runnerPorts` are the Component's merged runner container ports. When
// they are empty the routing Service is skipped and logged rather than
// published on a guessed port: a routing Service on a port nothing listens
// on is indistinguishable from a healthy one (its EndpointSlice still reads
// ready) and silently drops every request. The headless Service is still
// ensured — peer DNS for distributed init does not depend on the serving
// port. The reconcile does not fail: coordination is a producer layer, and
// one Component's missing port declaration must not wedge the rest of the
// InferenceService.
func EnsurePerRevisionServices(ctx context.Context, c client.Client, isvc *v1beta1.InferenceService, component v1beta1.ComponentType, revisionHash string, multiPod bool, runnerPorts []corev1.ContainerPort) (PerRevisionService, error) {
	out := PerRevisionServiceNames(isvc.Name, component, revisionHash)
	if c == nil {
		return out, fmt.Errorf("EnsurePerRevisionServices: nil client")
	}

	routingBuild := func(i *v1beta1.InferenceService, comp v1beta1.ComponentType, hash string) (*corev1.Service, error) {
		return BuildPerRevisionRoutingService(i, comp, hash, multiPod, runnerPorts)
	}
	switch err := ensureService(ctx, c, isvc, routingBuild, component, revisionHash, out.RoutingName, false); {
	case errors.Is(err, ErrNoServingPort):
		out.RoutingName = ""
		log.FromContext(ctx).Info("Skipping per-revision routing Service: component runner declares no container port",
			"component", component, "revisionHash", revisionHash, "service", PerRevisionServiceName(isvc.Name, component, revisionHash))
	case err != nil:
		return out, err
	}
	if err := ensureService(ctx, c, isvc, BuildPerRevisionHeadlessService, component, revisionHash, out.HeadlessName, true); err != nil {
		return out, err
	}
	return out, nil
}

// ensureService is the shared CreateOrUpdate helper for the routing /
// headless pair. headless distinguishes the ClusterIP semantics so the
// patch back to the desired shape doesn't try to flip an immutable
// ClusterIP after create.
func ensureService(
	ctx context.Context,
	c client.Client,
	isvc *v1beta1.InferenceService,
	build func(*v1beta1.InferenceService, v1beta1.ComponentType, string) (*corev1.Service, error),
	component v1beta1.ComponentType,
	revisionHash, name string,
	headless bool,
) error {
	desired, err := build(isvc, component, revisionHash)
	if err != nil {
		return fmt.Errorf("build per-revision service %s: %w", name, err)
	}
	target := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      desired.Name,
			Namespace: desired.Namespace,
		},
	}
	_, err = controllerutil.CreateOrUpdate(ctx, c, target, func() error {
		if target.CreationTimestamp.IsZero() {
			if headless {
				target.Spec.ClusterIP = corev1.ClusterIPNone
			}
			target.Spec.Type = desired.Spec.Type
		}
		target.Spec.PublishNotReadyAddresses = desired.Spec.PublishNotReadyAddresses
		target.Spec.Selector = desired.Spec.Selector
		target.Spec.Ports = desired.Spec.Ports
		target.Labels = desired.Labels
		target.OwnerReferences = desired.OwnerReferences
		return nil
	})
	if err != nil {
		return fmt.Errorf("CreateOrUpdate per-revision service %s/%s: %w", target.Namespace, target.Name, err)
	}
	return nil
}

// isNamespaceTerminating reports whether err is the apiserver's rejection of a
// create because the target namespace is being deleted. Nothing new can be
// created in a terminating namespace, and the ISVC is going away with it, so
// coordination treats this as a benign stop rather than a retryable error.
func isNamespaceTerminating(err error) bool {
	return apierrors.HasStatusCause(err, corev1.NamespaceTerminatingCause)
}

// PerRevisionServicesConverged reports whether the per-revision Service
// ensure + GC can be skipped this reconcile because the live revision-hash
// set already matches the set last recorded on Status.Components.<c>.Traffic.
//
// The recorded set is reconstructed by reversing PerRevisionServiceName on
// each Traffic[].RevisionName. When the live pod-hash set equals the recorded
// set, the previous (non-skipped) reconcile already ensured a Service for every
// live hash and GC'd every orphan, so re-running the CreateOrUpdate Gets and the
// namespace Service LIST is pure overhead. Any divergence — a new hash with
// pods, a hash that drained to zero, or a live hash that BuildTrafficTargets
// dropped for rounding to 0% (so it never appears in Traffic) — returns false
// and forces the full ensure/GC path, which is correctness-complete.
func PerRevisionServicesConverged(isvcName string, component v1beta1.ComponentType, liveHashes map[string]int32, recorded []v1beta1.ComponentTrafficTarget) bool {
	live := make(map[string]struct{}, len(liveHashes))
	for h, n := range liveHashes {
		if h == "" || n <= 0 {
			continue
		}
		live[h] = struct{}{}
	}
	prefix := PerRevisionServiceName(isvcName, component, "")
	rec := make(map[string]struct{}, len(recorded))
	for _, t := range recorded {
		if len(t.RevisionName) <= len(prefix) || t.RevisionName[:len(prefix)] != prefix {
			continue
		}
		rec[t.RevisionName[len(prefix):]] = struct{}{}
	}
	if len(live) != len(rec) {
		return false
	}
	for h := range live {
		if _, ok := rec[h]; !ok {
			return false
		}
	}
	return true
}

// GCPerRevisionServices deletes both per-revision Services for one
// (component, revisionHash). Called when the revision is past
// retention AND no live pod carries the hash. NotFound is treated as
// success — the caller may invoke GC on stale entries.
func GCPerRevisionServices(ctx context.Context, c client.Client, namespace, isvcName string, component v1beta1.ComponentType, revisionHash string) error {
	if c == nil {
		return fmt.Errorf("GCPerRevisionServices: nil client")
	}
	names := PerRevisionServiceNames(isvcName, component, revisionHash)
	for _, name := range []string{names.RoutingName, names.HeadlessName} {
		svc := &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: namespace,
			},
		}
		if err := c.Delete(ctx, svc); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("delete per-revision service %s/%s: %w", namespace, name, err)
		}
	}
	return nil
}
