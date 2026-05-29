package types

// ConditionType is the workload-internal Type identifier stamped on
// metav1.Condition entries the workload reconciler writes through the
// adapter's WriteAggregateCondition / WriteInstanceCondition callbacks.
// The distinct Go type prevents accidental coupling with condition-type
// strings owned by other packages (e.g. omenative/status retains its
// own copies for legacy callers); call sites pass a ConditionType
// value, and the metav1.Condition.Type string field absorbs the
// implicit string conversion.
//
// Values match the legacy omenative/status condition-type strings
// byte-for-byte so existing operator dashboards and `kubectl get isvc
// -o jsonpath='...conditions[?(@.type=="…")]...'` queries keep matching
// after the workload extraction. The byte-equality is enforced by
// these typed constants being the single source-of-truth across the
// workload subpackages — drift can only occur inside this file, not
// across siblings.
type ConditionType string

// String returns the underlying string form. Convenience for call
// sites that need to interpolate the type in a free-form message.
func (t ConditionType) String() string { return string(t) }

// ConditionReason is the workload-internal Reason identifier stamped
// on metav1.Condition entries. Same anti-drift rationale as
// ConditionType.
type ConditionReason string

// String returns the underlying string form.
func (r ConditionReason) String() string { return string(r) }

// Workload-emitted condition types. Defined once in the workload root
// so every subpackage (gang/, future condition writers) shares the
// same identifiers; the legacy `omenative/status` package keeps its
// own copies (typed as plain strings) for callers still writing
// through the v1beta1-typed helpers.
const (
	// ConditionGangSchedulingUnavailable is True when the Component
	// has at least one multi-pod Instance but the cluster does not
	// have the scheduler-plugins
	// `scheduling.x-k8s.io/v1alpha1` PodGroup CRD installed. The
	// reconciler still creates pods — gang scheduling is a soft
	// requirement per OEP-0007 §"Gang Scheduling" — but partial-gang
	// placement is possible and the runtime may hang waiting for
	// missing peer pods. False when the CRD is present or the
	// Component is single-pod.
	ConditionGangSchedulingUnavailable ConditionType = "GangSchedulingUnavailable"
)

// Workload-emitted condition reasons. Same single-source-of-truth
// rationale as the ConditionType constants above.
const (
	// ReasonPodGroupCRDNotInstalled stamps the
	// GangSchedulingUnavailable condition with a precise diagnostic
	// — the cluster does not have the scheduler-plugins PodGroup CRD
	// installed.
	ReasonPodGroupCRDNotInstalled ConditionReason = "PodGroupCRDNotInstalled"

	// ReasonGangSchedulingAvailable stamps the condition Status=False
	// (the controller has gang scheduling and is using it, OR the
	// Component is single-pod and doesn't need it).
	ReasonGangSchedulingAvailable ConditionReason = "GangSchedulingAvailable"
)
