package omenative_perm_kind

import (
	"context"
	"fmt"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/tests/integration/kindfix"
	"github.com/ome-projects/ome/tests/integration/util"
)

const (
	runtimeCBM        = "ome-perm-noop"
	runtimeCSR        = "ome-perm-cpu"
	permStartupDelayS = 8 // fake-serving sleeps this long before Ready (widens transients)
)

// omeContainer builds the fake-serving container with the artificial startup
// delay AND a readiness probe gated on it. fake-serving sleeps
// permStartupDelayS seconds BEFORE binding :8080, so without a probe the kubelet
// reports ContainersReady the instant the process execs — defeating the delay.
// The readiness probe (httpGet /healthz, the only 200 route) makes
// container-readiness track the post-sleep bind, so the watch sees real
// surge/drain transitions (mirrors the playground CSR's startup+readiness
// probes; run.py reads the same containerStatuses[ome-container].ready signal).
func omeContainer(image string) corev1.Container {
	probe := func(period, failureThreshold int32) *corev1.Probe {
		return &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{
				Path: "/healthz", Port: intstr.FromInt(8080),
			}},
			PeriodSeconds:    period,
			FailureThreshold: failureThreshold,
		}
	}
	return corev1.Container{
		Name:  constants.MainContainerName,
		Image: image,
		Env:   []corev1.EnvVar{{Name: "STARTUP_DELAY_SEC", Value: fmt.Sprintf("%d", permStartupDelayS)}},
		Ports: []corev1.ContainerPort{{ContainerPort: 8080, Name: "http", Protocol: corev1.ProtocolTCP}},
		// startupProbe absorbs the sleep (failureThreshold*period > delay); only
		// once it succeeds does readiness begin reporting true.
		StartupProbe:   probe(1, 60),
		ReadinessProbe: probe(1, 2),
	}
}

// applyRuntime creates the shared CBM + CSR and stamps the model-Ready node
// label so the external manager can schedule pods. The create is idempotent so
// the parallel processes can each call it against the one shared cluster; runs
// in SynchronizedBeforeSuite.
func applyRuntime(ctx context.Context, g gomega.Gomega, c client.Client) {
	cbm := kindfix.StandardClusterBaseModel(runtimeCBM, "oci://n/test/b/models/o/perm-noop")
	csr := kindfix.StandardClusterServingRuntime(runtimeCSR)
	kindfix.ApplyModelRuntime(ctx, g, c, cbm, csr, kindfix.ApplyModelRuntimeOpts{IdempotentCreate: true})
}

// lc is one Component's lifecycle config. A nil *lc omits the Component.
type lc struct {
	n       int
	strat   v1beta1.UpdateStrategyType
	surge   *intstr.IntOrString
	unavail *intstr.IntOrString
}

// permCase is one rollout permutation — the Go analog of a pXX.yaml. Ceilings are
// EXPLICIT per-Component peak upper bounds (mirroring the playground header's
// ceil_e/ceil_d/ceil_r), not computed: the controller's surge math is the thing
// under test, so the bound is authored, not derived from it.
type permCase struct {
	id, desc            string
	eng, dec, rtr       *lc
	ceilE, ceilD, ceilR int

	// skip, when non-empty, marks a case as known-broken-pending-fix: the spec
	// ginkgo.Skip()s with this reason so the suite stays green while the gap is
	// tracked. Clear it to re-enable once the underlying behavior is fixed.
	skip string

	// Optional coordination group over members (legacy path only — the
	// IR-managed path does no coordination). Empty members == no group, so the
	// trigger bumps engine alone and decoder/router are negative controls.
	members      []v1beta1.ComponentType
	policy       v1beta1.CoordinationPolicy
	order        []v1beta1.ComponentType
	soak         string
	pacingType   v1beta1.CoordinationPacingType
	ratioTol     *int32
	groupSurge   *intstr.IntOrString
	groupUnavail *intstr.IntOrString
}

func (pc permCase) cfg(c v1beta1.ComponentType) *lc {
	switch c {
	case v1beta1.EngineComponent:
		return pc.eng
	case v1beta1.DecoderComponent:
		return pc.dec
	case v1beta1.RouterComponent:
		return pc.rtr
	}
	return nil
}

func (pc permCase) replicas(c v1beta1.ComponentType) int {
	if l := pc.cfg(c); l != nil {
		return l.n
	}
	return 0
}

func (pc permCase) ceil(c v1beta1.ComponentType) int {
	switch c {
	case v1beta1.EngineComponent:
		return pc.ceilE
	case v1beta1.DecoderComponent:
		return pc.ceilD
	case v1beta1.RouterComponent:
		return pc.ceilR
	}
	return 0
}

// servingFloor is the lowest ready count a Component may reach during the roll:
// N minus its max-unavailable budget. The per-Component lifecycle budget and
// (for group members) the group pacing budget both apply; the larger wins as a
// conservative lower bound.
func (pc permCase) servingFloor(c v1beta1.ComponentType) int {
	n := pc.replicas(c)
	u := 0
	if l := pc.cfg(c); l != nil {
		u = resolveIntOrPct(l.unavail, n)
	}
	if pc.groupUnavail != nil && pc.inMembers(c) {
		if gu := resolveIntOrPct(pc.groupUnavail, n); gu > u {
			u = gu
		}
	}
	if f := n - u; f > 0 {
		return f
	}
	return 0
}

func (pc permCase) inMembers(c v1beta1.ComponentType) bool {
	for _, m := range pc.members {
		if m == c {
			return true
		}
	}
	return false
}

// surgeExpected reports whether a Component is configured to surge new pods:
// SurgeThenDrain with a positive surge budget (per-Component or group). A roll
// that never created an extra pod for such a Component is a regression, so the
// runner asserts its peak actually exceeded N.
func (pc permCase) surgeExpected(c v1beta1.ComponentType) bool {
	l := pc.cfg(c)
	if l == nil || l.strat != v1beta1.UpdateStrategySurgeThenDrain {
		return false
	}
	n := pc.replicas(c)
	if resolveIntOrPct(l.surge, n) > 0 {
		return true
	}
	return pc.inMembers(c) && resolveIntOrPct(pc.groupSurge, n) > 0
}

// resolveIntOrPct turns an int-or-percent budget into a pod count against total.
// A percent rounds up (ceil), matching the controller's surge/unavailable math.
func resolveIntOrPct(v *intstr.IntOrString, total int) int {
	if v == nil {
		return 0
	}
	if v.Type == intstr.Int {
		if v.IntValue() < 0 {
			return 0
		}
		return v.IntValue()
	}
	pct := 0
	if _, err := fmt.Sscanf(v.StrVal, "%d%%", &pct); err != nil || pct <= 0 || total <= 0 {
		return 0
	}
	if pct > 100 {
		pct = 100
	}
	out := (total*pct + 99) / 100
	if out > total {
		out = total
	}
	return out
}

func (pc permCase) configured() []v1beta1.ComponentType {
	var out []v1beta1.ComponentType
	for _, c := range []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent, v1beta1.RouterComponent} {
		if pc.replicas(c) > 0 {
			out = append(out, c)
		}
	}
	return out
}

func ptr[T any](v T) *T { return &v }

// pi / ps build an int / percent IntOrString pointer for lifecycle budgets.
func pi(v int) *intstr.IntOrString    { x := intstr.FromInt(v); return &x }
func ps(v string) *intstr.IntOrString { x := intstr.FromString(v); return &x }

// buildPermISVC materializes the ISVC for a case: fake-serving containers with
// the startup delay + readiness probe, per-Component deploymentMode + lifecycle,
// and the optional coordination group. Thin per the playground style.
func buildPermISVC(ns string, pc permCase) *v1beta1.InferenceService {
	ext := func(c v1beta1.ComponentType) v1beta1.ComponentExtensionSpec {
		l := pc.cfg(c)
		n := l.n
		e := v1beta1.ComponentExtensionSpec{
			Annotations: map[string]string{constants.DeploymentMode: string(constants.OMENative)},
			MinReplicas: &n,
			MaxReplicas: n,
		}
		if l.strat != "" || l.surge != nil || l.unavail != nil {
			us := &v1beta1.UpdateStrategy{Type: l.strat}
			if l.surge != nil || l.unavail != nil {
				us.RollingUpdate = &v1beta1.RollingUpdate{MaxSurge: l.surge, MaxUnavailable: l.unavail}
			}
			e.Lifecycle = &v1beta1.LifecycleSpec{UpdateStrategy: us}
		}
		return e
	}
	pod := func() v1beta1.PodSpec {
		return v1beta1.PodSpec{Containers: []corev1.Container{omeContainer("fake-serving:v1")}}
	}

	isvc := &v1beta1.InferenceService{}
	isvc.Name = pc.id
	isvc.Namespace = ns
	// deploymentMode is set PER-COMPONENT (in ext()), not at top level — the
	// controller resolves it per-Component and does NOT read/copy a top-level value.
	isvc.Spec.Model = &v1beta1.ModelRef{Name: runtimeCBM, Kind: ptr("ClusterBaseModel")}
	isvc.Spec.Runtime = &v1beta1.ServingRuntimeRef{Name: runtimeCSR, Kind: ptr("ClusterServingRuntime")}
	if pc.eng != nil {
		isvc.Spec.Engine = &v1beta1.EngineSpec{ComponentExtensionSpec: ext(v1beta1.EngineComponent), PodSpec: pod()}
	}
	if pc.dec != nil {
		isvc.Spec.Decoder = &v1beta1.DecoderSpec{ComponentExtensionSpec: ext(v1beta1.DecoderComponent), PodSpec: pod()}
	}
	if pc.rtr != nil {
		isvc.Spec.Router = &v1beta1.RouterSpec{ComponentExtensionSpec: ext(v1beta1.RouterComponent), PodSpec: pod()}
	}
	if pc.policy != "" {
		g := v1beta1.RolloutCoordinationGroup{
			Components: pc.members,
			Policy:     pc.policy,
			Order:      pc.order,
		}
		if pc.soak != "" {
			if d, err := parseDur(pc.soak); err == nil {
				g.Soak = d
			}
		}
		if pc.pacingType != "" || pc.groupSurge != nil || pc.groupUnavail != nil {
			g.Pacing = &v1beta1.CoordinationPacing{
				Type:           pc.pacingType,
				MaxSurge:       pc.groupSurge,
				MaxUnavailable: pc.groupUnavail,
				RatioTolerance: pc.ratioTol,
			}
		}
		isvc.Spec.Rollout = &v1beta1.RolloutSpec{
			Coordination: &v1beta1.RolloutCoordinationSpec{Groups: []v1beta1.RolloutCoordinationGroup{g}},
		}
	}
	return isvc
}

// buildPermCases is the (growable) case list — faithful Go ports of the proven
// playground isvcs/pXX.yaml fixtures. Ceilings come straight from each YAML's
// header comment (ceil_e/ceil_d/ceil_r). Add a case by appending one literal.
func buildPermCases() []permCase {
	st := func(n int, surge, unavail *intstr.IntOrString) *lc {
		return &lc{n: n, strat: v1beta1.UpdateStrategySurgeThenDrain, surge: surge, unavail: unavail}
	}
	stD := func(n int) *lc { return st(n, pi(1), pi(0)) } // PD default: ST surge=1 unavail=0
	egDec := []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent}
	decEg := []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.EngineComponent}

	return []permCase{
		// --- engine-only lifecycle (path-agnostic; works IR-managed or legacy) ---
		{id: "p01", desc: "engine-only N=4 ST surge=1 unavail=0",
			eng: st(4, pi(1), pi(0)), ceilE: 5},
		{id: "p02", desc: "engine-only N=4 ST surge=2 unavail=0",
			eng: st(4, pi(2), pi(0)), ceilE: 6},
		{id: "p08", desc: "engine-only N=4 ST surge=100% unavail=0",
			eng: st(4, ps("100%"), pi(0)), ceilE: 8},
		{id: "p16", desc: "engine-only N=4 RecreatePod (drain-all-first, no surge)",
			eng: &lc{n: 4, strat: v1beta1.UpdateStrategyRecreatePod, surge: pi(0), unavail: ps("100%")}, ceilE: 4},
		{id: "p17", desc: "engine-only N=4 InPlaceIfPossible",
			eng: &lc{n: 4, strat: v1beta1.UpdateStrategyInPlaceIfPossible, surge: pi(1), unavail: pi(1)}, ceilE: 5},
		// p18 InPlaceOnly rejects the non-image (annotation) trigger and has zero
		// surge/unavail budget, so the roll can never make progress (it hangs).
		// That's the configuration's nature, not a regression — skipped.
		{id: "p18", desc: "engine-only N=4 InPlaceOnly",
			eng: &lc{n: 4, strat: v1beta1.UpdateStrategyInPlaceOnly, surge: pi(0), unavail: pi(0)}, ceilE: 4,
			skip: "InPlaceOnly rejects the annotation-only trigger with zero budget; roll cannot progress"},
		{id: "p22", desc: "engine-only N=8 ST surge=1 unavail=0",
			eng: st(8, pi(1), pi(0)), ceilE: 9},

		// --- PD coordination (legacy path) ---
		{id: "p26", desc: "PD e4 d4 r2 ST surge=1, NO group (engine-only bump; d+r isolated)",
			eng: stD(4), dec: stD(4), rtr: stD(2), ceilE: 5, ceilD: 5, ceilR: 3},
		{id: "p34", desc: "PD e4 d4 r2 Sequential order=[decoder,engine] soak=0s",
			eng: stD(4), dec: stD(4), rtr: stD(2), ceilE: 5, ceilD: 5, ceilR: 3,
			members: decEg, policy: v1beta1.CoordinationPolicySequential, order: decEg},
		{id: "p36", desc: "PD e4 d4 r2 Sequential order=[decoder,engine] soak=30s",
			eng: stD(4), dec: stD(4), rtr: stD(2), ceilE: 5, ceilD: 5, ceilR: 3,
			members: decEg, policy: v1beta1.CoordinationPolicySequential, order: decEg, soak: "30s"},
		{id: "p42", desc: "PD e4 d4 r2 RollingUpdate + RatioBalanced tol=25 unavail=1",
			eng: stD(4), dec: stD(4), rtr: stD(2), ceilE: 5, ceilD: 5, ceilR: 3,
			members: egDec, policy: v1beta1.CoordinationPolicyRollingUpdate,
			pacingType: v1beta1.CoordinationPacingRatioBalanced, ratioTol: ptr(int32(25)), groupUnavail: pi(1)},
		{id: "p51", desc: "PD RatioBalanced STRESS: engine RecreatePod unavail=3, decoder surges",
			eng: &lc{n: 4, strat: v1beta1.UpdateStrategyRecreatePod, surge: pi(0), unavail: pi(3)},
			dec: stD(4), rtr: stD(2), ceilE: 4, ceilD: 5, ceilR: 3,
			members: egDec, policy: v1beta1.CoordinationPolicyRollingUpdate,
			pacingType: v1beta1.CoordinationPacingRatioBalanced, ratioTol: ptr(int32(25)), groupUnavail: pi(3)},
	}
}

var _ = ginkgo.Describe("OMENative perm rollouts", ginkgo.Ordered, ginkgo.ContinueOnFailure, func() {
	var ns *corev1.Namespace
	ginkgo.BeforeEach(func() {
		var err error
		ns, err = util.CreateNamespaceFromPrefix(suiteCtx, k8sClient, "perm-")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
	})
	ginkgo.AfterEach(func() {
		_ = util.DeleteNamespace(suiteCtx, k8sClient, ns.Name)
	})

	entries := make([]ginkgo.TableEntry, 0, len(buildPermCases()))
	for _, pc := range buildPermCases() {
		entries = append(entries, ginkgo.Entry(fmt.Sprintf("%s: %s", pc.id, pc.desc), pc))
	}
	ginkgo.DescribeTable("rollout honors lifecycle + coordination", func(pc permCase) {
		if pc.skip != "" {
			ginkgo.Skip(pc.skip)
		}
		runPerm(ns.Name, pc)
	}, entries)
})
