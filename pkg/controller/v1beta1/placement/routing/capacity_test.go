package routing

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/types"
)

const testCapacityMaxResponseBytes int64 = 1 << 20

var capacityTestMap = types.NamespacedName{Name: "llama", Namespace: "team-a"}

func testCapacityConfig() CapacityConfig {
	return CapacityConfig{
		Path:    "/capacity",
		Method:  http.MethodGet,
		Format:  FormatReport,
		Period:  time.Second,
		Timeout: 500 * time.Millisecond,
		Samples: 10,
		Quorum:  2,
		MaxAge:  30 * time.Second,
	}
}

// singleSampleConfig applies each reading immediately (window of one, quorum of
// one), isolating tests that are about plumbing rather than aggregation.
func singleSampleConfig() CapacityConfig {
	c := testCapacityConfig()
	c.Samples, c.Quorum = 1, 1
	return c
}

func newTestPoller(cfg CapacityConfig) *CapacityPoller {
	executor, err := NewObserverExecutor(1, 1)
	if err != nil {
		panic(err)
	}
	return newTestPollerWithExecutor(cfg, executor)
}

func newTestPollerWithExecutor(cfg CapacityConfig, executor *ObserverExecutor) *CapacityPoller {
	poller, err := NewCapacityPoller(cfg, executor, NewObserverClient(), testCapacityMaxResponseBytes, logr.Discard())
	if err != nil {
		panic(err)
	}
	return poller
}

func testCapacityTarget(cluster, endpoint string) Target {
	return Target{
		OwnerUID: "owner-uid",
		Map:      capacityTestMap,
		Cluster:  cluster,
		URL:      endpoint,
	}
}

// capacityServer serves a fixed body, so each test states exactly the report
// shape it is exercising.
func capacityServer(t *testing.T, status int, body string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/capacity" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	return srv
}

func freshReport(servable int32) string {
	b, _ := json.Marshal(CapacityReport{Servable: servable, ObservedAt: time.Now()})
	return string(b)
}

func TestNewCapacityPollerRequiresRuntimeDependenciesAndLimit(t *testing.T) {
	executor, err := NewObserverExecutor(1, 1)
	if err != nil {
		t.Fatalf("NewObserverExecutor() error = %v", err)
	}
	tests := []struct {
		name       string
		executor   *ObserverExecutor
		client     *http.Client
		maxBytes   int64
		wantErrSub string
	}{
		{"nil executor", nil, NewObserverClient(), 1, "executor is nil"},
		{"nil client", executor, nil, 1, "client is nil"},
		{"zero response bound", executor, NewObserverClient(), 0, "must be positive"},
		{"negative response bound", executor, NewObserverClient(), -1, "must be positive"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewCapacityPoller(testCapacityConfig(), tt.executor, tt.client, tt.maxBytes, logr.Discard())
			if err == nil || !strings.Contains(err.Error(), tt.wantErrSub) {
				t.Fatalf("NewCapacityPoller() error = %v, want containing %q", err, tt.wantErrSub)
			}
		})
	}

	if _, err := NewCapacityPoller(CapacityConfig{}, executor, NewObserverClient(), 1, logr.Discard()); err != nil {
		t.Fatalf("disabled NewCapacityPoller() error = %v, want valid dependencies accepted", err)
	}
}

func TestCapacityPoller_AcceptsAFreshReport(t *testing.T) {
	srv := capacityServer(t, http.StatusOK, freshReport(3))
	p := newTestPoller(testCapacityConfig())

	got, reason := p.pollOne(context.Background(), testCapacityTarget("cluster-a", srv.URL))
	if got == nil {
		t.Fatalf("pollOne returned no report: %s", reason)
	}
	if *got != 3 {
		t.Fatalf("servable = %d, want 3", *got)
	}
	if reason != "" {
		t.Fatalf("reason = %q, want empty on an accepted report", reason)
	}
}

func TestCapacityPoller_AcceptsAReportOfZero(t *testing.T) {
	// Zero is a real signal ("I can serve nothing"), not an absent report. The
	// ceiling drives the home's weight to 0, which is the point.
	srv := capacityServer(t, http.StatusOK, freshReport(0))
	p := newTestPoller(testCapacityConfig())

	got, reason := p.pollOne(context.Background(), testCapacityTarget("cluster-a", srv.URL))
	if got == nil {
		t.Fatalf("a report of zero was discarded: %s", reason)
	}
	if *got != 0 {
		t.Fatalf("servable = %d, want 0", *got)
	}
}

func TestCapacityPoller_FailsOpenOnEveryBadAnswer(t *testing.T) {
	// Every one of these must leave the control-plane plan standing. Fail-closed
	// would turn a control-plane-to-data-plane blip into a fleet-wide cutoff,
	// and a malformed or stale number must never be able to black-hole a home.
	stale, _ := json.Marshal(CapacityReport{Servable: 2, ObservedAt: time.Now().Add(-time.Hour)})
	future, _ := json.Marshal(CapacityReport{Servable: 2, ObservedAt: time.Now().Add(time.Hour)})
	negative, _ := json.Marshal(CapacityReport{Servable: -1, ObservedAt: time.Now()})
	noStamp, _ := json.Marshal(CapacityReport{Servable: 2})

	tests := []struct {
		name   string
		status int
		body   string
	}{
		{"non-200 response", http.StatusServiceUnavailable, freshReport(2)},
		{"unparsable body", http.StatusOK, "not json"},
		{"stale report beyond maxAge", http.StatusOK, string(stale)},
		{"future report timestamp", http.StatusOK, string(future)},
		{"negative servable count", http.StatusOK, string(negative)},
		{"report with no observedAt stamp", http.StatusOK, string(noStamp)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := capacityServer(t, tt.status, tt.body)
			p := newTestPoller(testCapacityConfig())
			got, reason := p.pollOne(context.Background(), testCapacityTarget("cluster-a", srv.URL))
			if got != nil {
				t.Fatalf("report = %d, want nil (fail open to the plan)", *got)
			}
			if reason == "" {
				t.Fatal("fell open with no reason; an operator cannot tell this from agreement with the plan")
			}
		})
	}
}

func TestCapacityPoller_UnreachableEndpointFallsOpen(t *testing.T) {
	srv := capacityServer(t, http.StatusOK, freshReport(2))
	url := srv.URL
	srv.Close()

	p := newTestPoller(testCapacityConfig())
	got, reason := p.pollOne(context.Background(), testCapacityTarget("cluster-a", url))
	if got != nil {
		t.Fatalf("report = %d from an unreachable endpoint, want nil", *got)
	}
	if reason == "" {
		t.Fatal("unreachable endpoint produced no reason")
	}
}

func TestCapacityPoller_ReportedIsNilWhenPollingIsOff(t *testing.T) {
	off := newTestPoller(CapacityConfig{})
	tgt := testCapacityTarget("cluster-a", "https://cluster-a.example")
	if r := off.Reported(tgt); r != nil {
		t.Fatalf("Reported with polling off = %d, want nil", *r)
	}
	if reason := off.FellOpen(tgt); reason != "" {
		t.Fatalf("FellOpen with polling off = %q, want empty", reason)
	}
	var nilPoller *CapacityPoller
	if r := nilPoller.Reported(tgt); r != nil {
		t.Fatalf("Reported on nil poller = %d, want nil", *r)
	}
}

func TestCapacityPoller_NotifiesWhenCeilingOrFallbackReasonChanges(t *testing.T) {
	p := newTestPoller(singleSampleConfig())
	tgt := testCapacityTarget("cluster-a", "https://cluster-a.example")
	two, three := int32(2), int32(3)

	if !p.record(tgt, &two, "") {
		t.Fatal("first report should count as a change")
	}
	if p.record(tgt, &two, "") {
		t.Fatal("an unchanged report triggered a reconcile; weights depend on the number, not the poll")
	}
	if !p.record(tgt, &three, "") {
		t.Fatal("a changed report should trigger a reconcile")
	}
	// A home that stops reporting releases its ceiling, which changes weights
	// and so must notify.
	if !p.record(tgt, nil, "capacity endpoint unreachable") {
		t.Fatal("losing a report should trigger a reconcile")
	}
	if p.record(tgt, nil, "capacity endpoint unreachable") {
		t.Fatal("still-absent report triggered a redundant reconcile")
	}
	if !p.record(tgt, nil, "capacity report is stale") {
		t.Fatal("a changed fallback reason must trigger a reconcile even while the ceiling remains nil")
	}
	if reason := p.FellOpen(tgt); reason != "capacity report is stale" {
		t.Fatalf("FellOpen() = %q, want latest failure reason", reason)
	}
}

func TestCapacityPoller_FirstAcceptedSampleExplainsPendingQuorum(t *testing.T) {
	p := newTestPoller(testCapacityConfig())
	tgt := testCapacityTarget("cluster-a", "https://cluster-a.example")
	two := int32(2)

	if !p.record(tgt, &two, "") {
		t.Fatal("first below-quorum sample did not report its new fallback reason")
	}
	if got := p.Reported(tgt); got != nil {
		t.Fatalf("Reported() = %d, want nil before quorum", *got)
	}
	if reason := p.FellOpen(tgt); !strings.Contains(reason, "awaiting quorum (1/2 accepted samples)") {
		t.Fatalf("FellOpen() = %q, want a nonempty pending-quorum reason", reason)
	}
}

func TestCapacityPoller_StateIdentityIncludesOwnerMapClusterAndURL(t *testing.T) {
	p := newTestPoller(singleSampleConfig())
	tgt := testCapacityTarget("cluster-a", "https://cluster-a.example/base")
	two := int32(2)
	p.record(tgt, &two, "")

	tests := []struct {
		name   string
		mutate func(*Target)
	}{
		{"owner UID", func(other *Target) { other.OwnerUID = "replacement-owner" }},
		{"map namespace", func(other *Target) { other.Map.Namespace = "other-namespace" }},
		{"map name", func(other *Target) { other.Map.Name = "other-map" }},
		{"cluster", func(other *Target) { other.Cluster = "cluster-b" }},
		{"URL", func(other *Target) { other.URL = "https://other.example/base" }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			other := tgt
			tt.mutate(&other)
			if got := p.Reported(other); got != nil {
				t.Fatalf("Reported() for different %s = %d, want nil", tt.name, *got)
			}
		})
	}

	equivalent := tgt
	equivalent.URL = "https://cluster-a.example/base/"
	if got := p.Reported(equivalent); got == nil || *got != 2 {
		t.Fatalf("Reported() for canonically equivalent URL = %v, want 2", got)
	}
}

func TestCapacityPoller_LateGenerationCannotOverwriteCurrentState(t *testing.T) {
	p := newTestPoller(singleSampleConfig())
	tgt := testCapacityTarget("cluster-a", "https://cluster-a.example")
	first := p.prepare([]Target{tgt})
	second := p.prepare([]Target{tgt})
	if len(first) != 1 || len(second) != 1 {
		t.Fatalf("prepared work lengths = %d, %d; want 1, 1", len(first), len(second))
	}

	two, nine := int32(2), int32(9)
	if !p.recordCurrent(second[0].key, second[0].generation, &nine, "") {
		t.Fatal("current generation was not recorded")
	}
	if p.recordCurrent(first[0].key, first[0].generation, &two, "") {
		t.Fatal("late predecessor generation was accepted")
	}
	if got := p.Reported(tgt); got == nil || *got != 9 {
		t.Fatalf("Reported() after late predecessor = %v, want current value 9", got)
	}
}

func TestCapacityPoller_ForgetsHomesThatAreNoLongerTargets(t *testing.T) {
	p := newTestPoller(singleSampleConfig())
	tgt := testCapacityTarget("cluster-a", "https://cluster-a.example")
	two := int32(2)
	p.record(tgt, &two, "")
	if r := p.Reported(tgt); r == nil || *r != 2 {
		t.Fatalf("Reported = %v before forget, want 2", r)
	}
	p.forget(nil)
	if r := p.Reported(tgt); r != nil {
		t.Fatalf("Reported after forget = %d, want nil", *r)
	}
}

func TestCapacityPoller_ForgetRemovesOnlyOneTrafficMap(t *testing.T) {
	p := newTestPoller(singleSampleConfig())
	first := testCapacityTarget("cluster-a", "https://cluster-a.example")
	second := testCapacityTarget("cluster-b", "https://cluster-b.example")
	second.Map = types.NamespacedName{Namespace: "other", Name: "service"}
	one := int32(1)
	two := int32(2)
	p.record(first, &one, "")
	p.record(second, &two, "")

	p.Forget(first.Map)

	if got := p.Reported(first); got != nil {
		t.Fatalf("first Reported after Forget = %d, want nil", *got)
	}
	if got := p.Reported(second); got == nil || *got != two {
		t.Fatalf("second Reported after Forget = %v, want %d", got, two)
	}
}

func TestCapacityConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*CapacityConfig)
		wantErr bool
	}{
		{"fully configured is valid", func(*CapacityConfig) {}, false},
		{"disabled is valid", func(c *CapacityConfig) { *c = CapacityConfig{} }, false},
		{"relative path rejected", func(c *CapacityConfig) { c.Path = "capacity" }, true},
		{"missing method rejected", func(c *CapacityConfig) { c.Method = "" }, true},
		{"missing format rejected", func(c *CapacityConfig) { c.Format = "" }, true},
		{"unsafe method rejected", func(c *CapacityConfig) { c.Method = "DELETE" }, true},
		{"zero samples rejected", func(c *CapacityConfig) { c.Samples = 0 }, true},
		{"zero quorum rejected", func(c *CapacityConfig) { c.Quorum = 0 }, true},
		{"zero period rejected", func(c *CapacityConfig) { c.Period = 0 }, true},
		{"zero timeout rejected", func(c *CapacityConfig) { c.Timeout = 0 }, true},
		// Overlapping requests would pile up until the effective poll rate
		// silently stopped matching the configured one.
		{"timeout >= period rejected", func(c *CapacityConfig) { c.Timeout = c.Period }, true},
		// Without a bound the staleness guard can never fire.
		{"zero maxAge rejected", func(c *CapacityConfig) { c.MaxAge = 0 }, true},
		// Every report would age out before the next poll, flapping the ceiling.
		{"maxAge < period rejected", func(c *CapacityConfig) { c.MaxAge = c.Period / 2 }, true},
		{"maxAge shorter than quorum window rejected", func(c *CapacityConfig) { c.MaxAge = c.Period }, true},
		{"maxAge equal to quorum window accepted", func(c *CapacityConfig) { c.MaxAge = time.Duration(c.quorum()) * c.Period }, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := testCapacityConfig()
			tt.mutate(&cfg)
			err := cfg.Validate()
			if tt.wantErr && err == nil {
				t.Fatalf("Validate() = nil, want an error for %+v", cfg)
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("Validate() = %v, want nil", err)
			}
		})
	}
}

func TestProbeConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*ProbeConfig)
		wantErr bool
	}{
		{"fully configured is valid", func(*ProbeConfig) {}, false},
		{"disabled is valid", func(c *ProbeConfig) { *c = ProbeConfig{} }, false},
		{"relative path rejected", func(c *ProbeConfig) { c.Path = "health" }, true},
		{"missing method rejected", func(c *ProbeConfig) { c.Method = "" }, true},
		{"unsafe method rejected", func(c *ProbeConfig) { c.Method = "CONNECT" }, true},
		{"missing accept statuses rejected", func(c *ProbeConfig) { c.AcceptStatuses = nil }, true},
		{"missing gate statuses rejected", func(c *ProbeConfig) { c.GateStatuses = nil }, true},
		{"invalid accept status rejected", func(c *ProbeConfig) { c.AcceptStatuses = []int{99} }, true},
		{"invalid gate status rejected", func(c *ProbeConfig) { c.GateStatuses = []int{600} }, true},
		{"authentication failure cannot gate", func(c *ProbeConfig) { c.GateStatuses = []int{401} }, true},
		{"authorization failure cannot gate", func(c *ProbeConfig) { c.GateStatuses = []int{403} }, true},
		{"overload cannot gate", func(c *ProbeConfig) { c.GateStatuses = []int{429} }, true},
		// An ambiguous status has no defined verdict, so the policy would
		// depend on list order rather than on the operator's intent.
		{"status in both lists rejected", func(c *ProbeConfig) { c.GateStatuses = append(c.GateStatuses, 200) }, true},
		{"zero period rejected", func(c *ProbeConfig) { c.Period = 0 }, true},
		{"zero timeout rejected", func(c *ProbeConfig) { c.Timeout = 0 }, true},
		{"timeout >= period rejected", func(c *ProbeConfig) { c.Timeout = c.Period }, true},
		{"zero failure threshold rejected", func(c *ProbeConfig) { c.FailureThreshold = 0 }, true},
		{"zero success threshold rejected", func(c *ProbeConfig) { c.SuccessThreshold = 0 }, true},
		{"missing all-failed policy rejected", func(c *ProbeConfig) { c.AllFailedPolicy = "" }, true},
		{"unknown all-failed policy rejected", func(c *ProbeConfig) { c.AllFailedPolicy = "DropEverything" }, true},
		{"drain all-failed policy is valid", func(c *ProbeConfig) { c.AllFailedPolicy = AllFailedPolicyDrain }, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := testProbeConfig()
			tt.mutate(&cfg)
			err := cfg.Validate()
			if tt.wantErr && err == nil {
				t.Fatalf("Validate() = nil, want an error for %+v", cfg)
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("Validate() = %v, want nil", err)
			}
		})
	}
}

func TestConfig_ValidateHonorsInstallationGate(t *testing.T) {
	tests := []struct {
		name   string
		config Config
	}{
		{
			name: "invalid probe is inert",
			config: Config{Probe: func() ProbeConfig {
				probe := testProbeConfig()
				probe.Method = "DELETE"
				return probe
			}()},
		},
		{
			name: "unavailable capacity format is inert",
			config: Config{Capacity: func() CapacityConfig {
				capacity := testCapacityConfig()
				capacity.Format = CapacityFormat("Unavailable")
				return capacity
			}()},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			disabled := tt.config
			disabled.Enabled = false
			if err := disabled.Validate(); err != nil {
				t.Fatalf("disabled Validate() = %v, want nil", err)
			}

			enabled := tt.config
			enabled.Enabled = true
			if err := enabled.Validate(); err == nil {
				t.Fatal("enabled Validate() = nil, want staged configuration error")
			}
		})
	}
}

func TestAllocationSource(t *testing.T) {
	// Endpoint is claimed only when a report actually lowered the plan.
	// Attributing a control-plane number to the home would send an operator
	// looking at the wrong side of the system.
	two, nine := int32(2), int32(9)
	tests := []struct {
		name string
		home Home
		want string
	}{
		{"no report is control plane", Home{Allocated: 7}, "ControlPlane"},
		{"report above plan is control plane", Home{Allocated: 7, Reported: &nine}, "ControlPlane"},
		{"report equal to plan is control plane", Home{Allocated: 2, Reported: &two}, "ControlPlane"},
		{"report below plan is endpoint", Home{Allocated: 7, Reported: &two}, "Endpoint"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := string(allocationSource(tt.home)); got != tt.want {
				t.Fatalf("allocationSource() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestCapacityPoller_BodyIsBounded(t *testing.T) {
	// A home streaming an unbounded body must not be able to exhaust
	// control-plane memory, and truncation must not be mistaken for decoding.
	body := freshReport(2)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(body))
	}))
	defer srv.Close()

	executor, err := NewObserverExecutor(1, 1)
	if err != nil {
		t.Fatalf("NewObserverExecutor() error = %v", err)
	}
	p, err := NewCapacityPoller(testCapacityConfig(), executor, NewObserverClient(), int64(len(body)-1), logr.Discard())
	if err != nil {
		t.Fatalf("NewCapacityPoller() error = %v", err)
	}
	got, reason := p.pollOne(context.Background(), testCapacityTarget("cluster-a", srv.URL))
	if got != nil {
		t.Fatalf("oversized body produced a report %d, want nil", *got)
	}
	if !strings.Contains(reason, "exceeds configured limit") {
		t.Fatalf("oversized body reason = %q, want configured-limit error", reason)
	}
}

func TestCapacityPoller_AcceptsBodyAtConfiguredLimit(t *testing.T) {
	body := freshReport(2)
	srv := capacityServer(t, http.StatusOK, string(body))
	executor, err := NewObserverExecutor(1, 1)
	if err != nil {
		t.Fatalf("NewObserverExecutor() error = %v", err)
	}
	p, err := NewCapacityPoller(testCapacityConfig(), executor, NewObserverClient(), int64(len(body)), logr.Discard())
	if err != nil {
		t.Fatalf("NewCapacityPoller() error = %v", err)
	}

	got, reason := p.pollOne(context.Background(), testCapacityTarget("cluster-a", srv.URL))
	if got == nil || *got != 2 {
		t.Fatalf("report at exact limit = %v, want 2; reason = %q", got, reason)
	}
}

func TestCapacityPoller_PollAllRecordsPerHome(t *testing.T) {
	// Two homes with different answers must not contaminate each other.
	full := capacityServer(t, http.StatusOK, freshReport(8))
	scarce := capacityServer(t, http.StatusOK, freshReport(1))

	executor, _, _ := startTestObserverExecutor(t, 2, 2)
	p := newTestPollerWithExecutor(singleSampleConfig(), executor)
	p.Targets = func(context.Context) ([]Target, error) {
		return []Target{
			testCapacityTarget("cluster-a", full.URL),
			testCapacityTarget("cluster-b", scarce.URL),
		}, nil
	}
	var notified []types.NamespacedName
	p.OnChange = func(_ context.Context, nn types.NamespacedName) error {
		notified = append(notified, nn)
		return nil
	}

	if err := p.pollAll(context.Background()); err != nil {
		t.Fatalf("pollAll() error = %v", err)
	}

	if r := p.Reported(testCapacityTarget("cluster-a", full.URL)); r == nil || *r != 8 {
		t.Fatalf("cluster-a reported = %v, want 8", r)
	}
	if r := p.Reported(testCapacityTarget("cluster-b", scarce.URL)); r == nil || *r != 1 {
		t.Fatalf("cluster-b reported = %v, want 1", r)
	}
	// Both homes changed, but they share one map, so one reconcile covers them.
	if len(notified) != 1 {
		t.Fatalf("notified %d times (%v), want 1 for the shared map", len(notified), notified)
	}
}

func TestCapacityPoller_PollAllNotifiesForPendingQuorumFallback(t *testing.T) {
	srv := capacityServer(t, http.StatusOK, freshReport(3))
	executor, _, _ := startTestObserverExecutor(t, 1, 1)
	p := newTestPollerWithExecutor(testCapacityConfig(), executor)
	tgt := testCapacityTarget("cluster-a", srv.URL)
	p.Targets = func(context.Context) ([]Target, error) { return []Target{tgt}, nil }
	notified := 0
	p.OnChange = func(context.Context, types.NamespacedName) error {
		notified++
		return nil
	}

	if err := p.pollAll(context.Background()); err != nil {
		t.Fatalf("pollAll() error = %v", err)
	}
	if got := p.Reported(tgt); got != nil {
		t.Fatalf("Reported() = %d, want nil before quorum", *got)
	}
	if reason := p.FellOpen(tgt); reason == "" {
		t.Fatal("FellOpen() is empty after an accepted sample below quorum")
	}
	if notified != 1 {
		t.Fatalf("OnChange called %d times, want 1 for the new fallback reason", notified)
	}
}

func TestCapacityPoller_TargetDiscoveryFailureFallsOpenAndNotifies(t *testing.T) {
	srv := capacityServer(t, http.StatusOK, freshReport(3))
	executor, _, _ := startTestObserverExecutor(t, 1, 1)
	p := newTestPollerWithExecutor(singleSampleConfig(), executor)
	tgt := testCapacityTarget("cluster-a", srv.URL)
	p.Targets = func(context.Context) ([]Target, error) { return []Target{tgt}, nil }
	var notified []types.NamespacedName
	p.OnChange = func(_ context.Context, mapKey types.NamespacedName) error {
		notified = append(notified, mapKey)
		return nil
	}

	if err := p.pollAll(context.Background()); err != nil {
		t.Fatalf("initial pollAll() error = %v", err)
	}
	if got := p.Reported(tgt); got == nil || *got != 3 {
		t.Fatalf("initial Reported() = %v, want 3", got)
	}
	notified = nil
	p.Targets = func(context.Context) ([]Target, error) {
		return nil, errors.New("cache unavailable")
	}

	if err := p.pollAll(context.Background()); err != nil {
		t.Fatalf("failed-discovery pollAll() error = %v", err)
	}
	if got := p.Reported(tgt); got != nil {
		t.Fatalf("Reported() after target discovery failure = %d, want nil", *got)
	}
	if got := p.FellOpen(tgt); got != "capacity target discovery failed" {
		t.Fatalf("FellOpen() = %q, want target-discovery failure", got)
	}
	if !slices.Equal(notified, []types.NamespacedName{tgt.Map}) {
		t.Fatalf("notifications = %v, want %v", notified, []types.NamespacedName{tgt.Map})
	}
}

func TestCapacityPoller_RepeatedStaleReportDoesNotRenotify(t *testing.T) {
	report := CapacityReport{Servable: 3, ObservedAt: time.Now().Add(-time.Minute)}
	body, err := json.Marshal(report)
	if err != nil {
		t.Fatalf("marshal report: %v", err)
	}
	srv := capacityServer(t, http.StatusOK, string(body))
	executor, _, _ := startTestObserverExecutor(t, 1, 1)
	p := newTestPollerWithExecutor(singleSampleConfig(), executor)
	tgt := testCapacityTarget("cluster-a", srv.URL)
	p.Targets = func(context.Context) ([]Target, error) { return []Target{tgt}, nil }
	notified := 0
	p.OnChange = func(context.Context, types.NamespacedName) error {
		notified++
		return nil
	}

	if err := p.pollAll(context.Background()); err != nil {
		t.Fatalf("first pollAll() error = %v", err)
	}
	if err := p.pollAll(context.Background()); err != nil {
		t.Fatalf("second pollAll() error = %v", err)
	}
	if notified != 1 {
		t.Fatalf("OnChange called %d times, want once for a stable stale-report reason", notified)
	}
}

func TestCapacityPoller_PollAllSubmitsThroughExecutor(t *testing.T) {
	srv := capacityServer(t, http.StatusOK, freshReport(3))
	executor, err := NewObserverExecutor(1, 1)
	if err != nil {
		t.Fatalf("NewObserverExecutor() error = %v", err)
	}
	p := newTestPollerWithExecutor(singleSampleConfig(), executor)
	tgt := testCapacityTarget("cluster-a", srv.URL)
	p.Targets = func(context.Context) ([]Target, error) { return []Target{tgt}, nil }
	notified := 0
	p.OnChange = func(context.Context, types.NamespacedName) error {
		notified++
		return nil
	}

	if err := p.pollAll(context.Background()); err != nil {
		t.Fatalf("pollAll() error = %v", err)
	}
	if got := p.Reported(tgt); got != nil {
		t.Fatalf("Reported() = %d without a running executor, want nil", *got)
	}
	if reason := p.FellOpen(tgt); !strings.Contains(reason, ErrObserverExecutorNotRunning.Error()) {
		t.Fatalf("FellOpen() = %q, want executor submission failure", reason)
	}
	if notified != 1 {
		t.Fatalf("OnChange called %d times, want 1 for executor failure", notified)
	}
}

func TestCapacityPoller_PollAllDeliversEveryChangedMap(t *testing.T) {
	srv := capacityServer(t, http.StatusOK, freshReport(3))
	executor, _, _ := startTestObserverExecutor(t, 2, 2)
	p := newTestPollerWithExecutor(singleSampleConfig(), executor)
	first := testCapacityTarget("cluster-a", srv.URL)
	first.Map = types.NamespacedName{Namespace: "prod", Name: "a"}
	second := testCapacityTarget("cluster-b", srv.URL)
	second.Map = types.NamespacedName{Namespace: "prod", Name: "b"}
	p.Targets = func(context.Context) ([]Target, error) {
		return []Target{second, first}, nil
	}

	var notified []types.NamespacedName
	p.OnChange = func(_ context.Context, mapKey types.NamespacedName) error {
		notified = append(notified, mapKey)
		return nil
	}
	if err := p.pollAll(context.Background()); err != nil {
		t.Fatalf("pollAll() error = %v", err)
	}
	want := []types.NamespacedName{first.Map, second.Map}
	if !slices.Equal(notified, want) {
		t.Fatalf("notifications = %v, want every changed map in deterministic order %v", notified, want)
	}
}

func TestCapacityPoller_NotificationHonorsCancellation(t *testing.T) {
	srv := capacityServer(t, http.StatusOK, freshReport(3))
	executor, _, _ := startTestObserverExecutor(t, 2, 2)
	p := newTestPollerWithExecutor(singleSampleConfig(), executor)
	first := testCapacityTarget("cluster-a", srv.URL)
	first.Map = types.NamespacedName{Namespace: "prod", Name: "a"}
	second := testCapacityTarget("cluster-b", srv.URL)
	second.Map = types.NamespacedName{Namespace: "prod", Name: "b"}
	p.Targets = func(context.Context) ([]Target, error) {
		return []Target{first, second}, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	called := 0
	p.OnChange = func(notifyCtx context.Context, _ types.NamespacedName) error {
		called++
		cancel()
		return notifyCtx.Err()
	}
	err := p.pollAll(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("pollAll() error = %v, want context.Canceled", err)
	}
	if called != 1 {
		t.Fatalf("OnChange called %d times after cancellation, want 1", called)
	}
}

func TestCapacityConfig_ValidateFormats(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*CapacityConfig)
		wantErr bool
	}{
		{"format is required", func(c *CapacityConfig) { c.Format = "" }, true},
		{"explicit Report is valid", func(c *CapacityConfig) { c.Format = FormatReport }, false},
		// A format this build does not carry is a startup error, not a silent
		// fallback: ignoring it would drop the operator's intent and leave the
		// plan in force with no explanation.
		{"unregistered format rejected", func(c *CapacityConfig) { c.Format = "Bogus" }, true},
		// Report needs no format-specific settings, so an option here is either
		// a typo or a misunderstanding; either way it must not be ignored.
		{"options rejected for Report", func(c *CapacityConfig) {
			c.Options = map[string]string{"example": "1"}
		}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := testCapacityConfig()
			tt.mutate(&cfg)
			err := cfg.Validate()
			if tt.wantErr && err == nil {
				t.Fatalf("Validate() = nil, want error for %+v", cfg)
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("Validate() = %v, want nil", err)
			}
		})
	}
}

func TestCapacityFormatRegistry_DefaultBuildCarriesOnlyTheBuiltIn(t *testing.T) {
	// A build that registers no additional formats carries exactly the
	// built-in one. Any other name here means an out-of-tree format registered
	// itself in this test binary.
	got := RegisteredCapacityFormats()
	if len(got) != 1 || got[0] != string(FormatReport) {
		t.Fatalf("default build registered %v, want only %q", got, FormatReport)
	}
}

func TestRegisterCapacityFormat_RejectsDuplicatesAndIncompletePlugins(t *testing.T) {
	// Two packages silently competing for one name would make the active
	// decoder depend on import order.
	if err := RegisterCapacityFormat(FormatReport, CapacityFormatPlugin{Decode: decodeReport}); err == nil {
		t.Fatal("re-registering an existing format succeeded, want an error")
	}
	if err := RegisterCapacityFormat("NoDecoder", CapacityFormatPlugin{}); err == nil {
		t.Fatal("registering a plugin with no Decode succeeded, want an error")
	}
	if err := RegisterCapacityFormat("", CapacityFormatPlugin{Decode: decodeReport}); err == nil {
		t.Fatal("registering an empty name succeeded, want an error")
	}
}

// feed pushes readings through the poller and returns the applied ceiling, or
// nil when the plan still stands.
func feed(cfg CapacityConfig, readings ...int32) *int32 {
	p := newTestPoller(cfg)
	tgt := testCapacityTarget("cluster-a", "https://cluster-a.example")
	for _, r := range readings {
		v := r
		p.record(tgt, &v, "")
	}
	return p.Reported(tgt)
}

func TestCapacityPoller_QuorumToleratesAMisbehavingReporter(t *testing.T) {
	// The control plane polls one address per home, so consecutive readings may
	// come from different reporters behind a balancer. A strict minimum would
	// let whichever reporter is broken set the ceiling alone -- and with a
	// round-robin balancer and a window at least as long as the replica count,
	// hold it there permanently. The quorum is what makes a low reading need
	// corroboration.
	cfg := testCapacityConfig() // Samples 10, Quorum 2.
	tests := []struct {
		name     string
		readings []int32
		want     *int32
	}{
		{
			// One reporter returning zero while the rest are healthy must not
			// set the ceiling.
			name:     "a single zero among healthy readings is ignored",
			readings: []int32{0, 7, 7, 7, 7},
			want:     ptr(int32(7)),
		},
		{
			// Filtering only zeros would miss this: a partially-synced reporter
			// returns a small non-zero figure, not nothing.
			name:     "a single low non-zero reading is ignored",
			readings: []int32{2, 7, 7, 7, 7},
			want:     ptr(int32(7)),
		},
		{
			// Corroborated, so believed -- the pessimist knows something.
			name:     "two agreeing low readings are believed",
			readings: []int32{3, 3, 7, 7, 7},
			want:     ptr(int32(3)),
		},
		{
			// One broken reporter plus one genuine pessimist: drop the outlier,
			// keep the pessimist.
			name:     "a broken reporter does not mask a real pessimist",
			readings: []int32{0, 3, 7, 7, 7},
			want:     ptr(int32(3)),
		},
		{
			// A home that really can serve nothing must still be able to say so.
			name:     "a unanimous zero is believed",
			readings: []int32{0, 0, 0, 0},
			want:     ptr(int32(0)),
		},
		{
			// Believing a drop takes exactly Quorum readings -- stated, so the
			// lag is predictable rather than emergent.
			name:     "a sustained drop is applied once corroborated",
			readings: []int32{9, 9, 4, 4},
			want:     ptr(int32(4)),
		},
		{
			// Anti-any-responder: a lucky high reading must not undo a ceiling
			// that corroborated readings already lowered.
			name:     "one high reading cannot raise a corroborated ceiling",
			readings: []int32{4, 4, 9},
			want:     ptr(int32(4)),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := feed(cfg, tt.readings...)
			if got == nil || *got != *tt.want {
				g := "nil"
				if got != nil {
					g = fmt.Sprint(*got)
				}
				t.Fatalf("readings %v -> ceiling %s, want %d", tt.readings, g, *tt.want)
			}
		})
	}
}

func TestCapacityPoller_NoCeilingUntilCorroborated(t *testing.T) {
	// Before the quorum is reached the plan stands. A ceiling derived from
	// fewer readings than the quorum is exactly what the quorum prevents: the
	// first reading after startup could be the broken reporter's.
	cfg := testCapacityConfig()
	if got := feed(cfg, 0); got != nil {
		t.Fatalf("a lone reading set a ceiling of %d; the plan should still stand", *got)
	}
	if got := feed(cfg, 3); got != nil {
		t.Fatalf("a lone reading set a ceiling of %d; the plan should still stand", *got)
	}
}

func TestCapacityPoller_WindowAgesOutAStaleLow(t *testing.T) {
	// Bounded so a home recovers. An all-time minimum would pin it at its worst
	// reading forever.
	cfg := testCapacityConfig()
	cfg.Samples, cfg.Quorum = 3, 2
	if got := feed(cfg, 1, 1, 9, 9, 9); got == nil || *got != 9 {
		t.Fatalf("ceiling = %v after the low readings aged out, want 9", got)
	}
}

func TestCapacityPoller_FailedPollReleasesTheWindow(t *testing.T) {
	// Fail-open means the ceiling is released, so the window cannot survive --
	// retaining readings would keep constraining a home we can no longer see.
	cfg := testCapacityConfig()
	cfg.Samples, cfg.Quorum = 4, 2
	p := newTestPoller(cfg)
	tgt := testCapacityTarget("cluster-a", "https://cluster-a.example")
	for _, v := range []int32{2, 2} {
		x := v
		p.record(tgt, &x, "")
	}
	if got := p.Reported(tgt); got == nil || *got != 2 {
		t.Fatalf("ceiling = %v before the failure, want 2", got)
	}
	p.record(tgt, nil, "capacity endpoint unreachable")
	if got := p.Reported(tgt); got != nil {
		t.Fatalf("ceiling = %d after a failed poll, want nil (fail open)", *got)
	}
	// And the old low readings must not come back with the next success.
	nine := int32(9)
	p.record(tgt, &nine, "")
	if got := p.Reported(tgt); got != nil {
		t.Fatalf("ceiling = %d from one reading after recovery, want nil until corroborated", *got)
	}
	p.record(tgt, &nine, "")
	if got := p.Reported(tgt); got == nil || *got != 9 {
		t.Fatalf("ceiling = %v after recovery, want 9 (stale lows must not persist)", got)
	}
}

func TestCapacityConfig_QuorumAndSamplesValidation(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*CapacityConfig)
		wantErr bool
	}{
		{"configured values are valid", func(*CapacityConfig) {}, false},
		{"explicit values are valid", func(c *CapacityConfig) { c.Samples, c.Quorum = 5, 3 }, false},
		{"negative samples rejected", func(c *CapacityConfig) { c.Samples = -1 }, true},
		{"negative quorum rejected", func(c *CapacityConfig) { c.Quorum = -1 }, true},
		// Unreachable quorum means no reading could ever lower a ceiling, so the
		// input would silently do nothing.
		{"quorum above samples rejected", func(c *CapacityConfig) { c.Samples, c.Quorum = 3, 4 }, true},
		{"quorum equal to samples is valid", func(c *CapacityConfig) { c.Samples, c.Quorum = 3, 3 }, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := testCapacityConfig()
			tt.mutate(&cfg)
			err := cfg.Validate()
			if tt.wantErr && err == nil {
				t.Fatalf("Validate() = nil, want error for samples=%d quorum=%d", cfg.Samples, cfg.Quorum)
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("Validate() = %v, want nil", err)
			}
		})
	}
}
