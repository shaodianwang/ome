package routing

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/types"
)

func testCapacityConfig() CapacityConfig {
	return CapacityConfig{
		Path:    "/capacity",
		Method:  http.MethodGet,
		Period:  time.Second,
		Timeout: 500 * time.Millisecond,
		MaxAge:  30 * time.Second,
	}
}

func newTestPoller(cfg CapacityConfig) *CapacityPoller {
	return NewCapacityPoller(cfg, NewObserverClient(), logr.Discard())
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

func TestCapacityPoller_AcceptsAFreshReport(t *testing.T) {
	srv := capacityServer(t, http.StatusOK, freshReport(3))
	p := newTestPoller(testCapacityConfig())

	got, reason := p.pollOne(context.Background(), Target{Map: testMap, Cluster: "cluster-a", URL: srv.URL})
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

	got, reason := p.pollOne(context.Background(), Target{Map: testMap, Cluster: "cluster-a", URL: srv.URL})
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
	negative, _ := json.Marshal(CapacityReport{Servable: -1, ObservedAt: time.Now()})
	noStamp, _ := json.Marshal(CapacityReport{Servable: 2})

	tests := []struct {
		name   string
		status int
		body   string
	}{
		{"non-200 response", http.StatusServiceUnavailable, freshReport(2)},
		{"unparseable body", http.StatusOK, "not json"},
		{"stale report beyond maxAge", http.StatusOK, string(stale)},
		{"negative servable count", http.StatusOK, string(negative)},
		{"report with no observedAt stamp", http.StatusOK, string(noStamp)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := capacityServer(t, tt.status, tt.body)
			p := newTestPoller(testCapacityConfig())
			got, reason := p.pollOne(context.Background(), Target{Map: testMap, Cluster: "cluster-a", URL: srv.URL})
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
	got, reason := p.pollOne(context.Background(), Target{Map: testMap, Cluster: "cluster-a", URL: url})
	if got != nil {
		t.Fatalf("report = %d from an unreachable endpoint, want nil", *got)
	}
	if reason == "" {
		t.Fatal("unreachable endpoint produced no reason")
	}
}

func TestCapacityPoller_ReportedIsNilWhenPollingIsOff(t *testing.T) {
	off := newTestPoller(CapacityConfig{})
	if r := off.Reported(testMap, "cluster-a"); r != nil {
		t.Fatalf("Reported with polling off = %d, want nil", *r)
	}
	if reason := off.FellOpen(testMap, "cluster-a"); reason != "" {
		t.Fatalf("FellOpen with polling off = %q, want empty", reason)
	}
	var nilPoller *CapacityPoller
	if r := nilPoller.Reported(testMap, "cluster-a"); r != nil {
		t.Fatalf("Reported on nil poller = %d, want nil", *r)
	}
}

func TestCapacityPoller_NotifiesOnlyWhenTheCeilingChanges(t *testing.T) {
	p := newTestPoller(testCapacityConfig())
	tgt := Target{Map: testMap, Cluster: "cluster-a"}
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
}

func TestCapacityPoller_ForgetsHomesThatAreNoLongerTargets(t *testing.T) {
	p := newTestPoller(testCapacityConfig())
	tgt := Target{Map: testMap, Cluster: "cluster-a"}
	two := int32(2)
	p.record(tgt, &two, "")
	if r := p.Reported(testMap, "cluster-a"); r == nil || *r != 2 {
		t.Fatalf("Reported = %v before forget, want 2", r)
	}
	p.forget(nil)
	if r := p.Reported(testMap, "cluster-a"); r != nil {
		t.Fatalf("Reported after forget = %d, want nil", *r)
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
		{"zero period rejected", func(c *CapacityConfig) { c.Period = 0 }, true},
		{"zero timeout rejected", func(c *CapacityConfig) { c.Timeout = 0 }, true},
		// Overlapping requests would pile up until the effective poll rate
		// silently stopped matching the configured one.
		{"timeout >= period rejected", func(c *CapacityConfig) { c.Timeout = c.Period }, true},
		// Without a bound the staleness guard can never fire.
		{"zero maxAge rejected", func(c *CapacityConfig) { c.MaxAge = 0 }, true},
		// Every report would age out before the next poll, flapping the ceiling.
		{"maxAge < period rejected", func(c *CapacityConfig) { c.MaxAge = c.Period / 2 }, true},
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
		{"missing accept statuses rejected", func(c *ProbeConfig) { c.AcceptStatuses = nil }, true},
		{"missing gate statuses rejected", func(c *ProbeConfig) { c.GateStatuses = nil }, true},
		// An ambiguous status has no defined verdict, so the policy would
		// depend on list order rather than on the operator's intent.
		{"status in both lists rejected", func(c *ProbeConfig) { c.GateStatuses = append(c.GateStatuses, 200) }, true},
		{"zero period rejected", func(c *ProbeConfig) { c.Period = 0 }, true},
		{"zero timeout rejected", func(c *ProbeConfig) { c.Timeout = 0 }, true},
		{"timeout >= period rejected", func(c *ProbeConfig) { c.Timeout = c.Period }, true},
		{"zero failure threshold rejected", func(c *ProbeConfig) { c.FailureThreshold = 0 }, true},
		{"zero success threshold rejected", func(c *ProbeConfig) { c.SuccessThreshold = 0 }, true},
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
	// control-plane memory.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		chunk := make([]byte, 4096)
		for i := range chunk {
			chunk[i] = 'a'
		}
		for range 64 {
			if _, err := w.Write(chunk); err != nil {
				return
			}
		}
	}))
	defer srv.Close()

	p := newTestPoller(testCapacityConfig())
	got, reason := p.pollOne(context.Background(), Target{Map: testMap, Cluster: "cluster-a", URL: srv.URL})
	if got != nil {
		t.Fatalf("oversized body produced a report %d, want nil", *got)
	}
	if reason == "" {
		t.Fatal("oversized body produced no reason")
	}
}

func TestCapacityPoller_PollAllRecordsPerHome(t *testing.T) {
	// Two homes with different answers must not contaminate each other.
	full := capacityServer(t, http.StatusOK, freshReport(8))
	scarce := capacityServer(t, http.StatusOK, freshReport(1))

	p := newTestPoller(testCapacityConfig())
	p.Targets = func(context.Context) ([]Target, error) {
		return []Target{
			{Map: testMap, Cluster: "cluster-a", URL: full.URL},
			{Map: testMap, Cluster: "cluster-b", URL: scarce.URL},
		}, nil
	}
	var notified []types.NamespacedName
	p.OnChange = func(nn types.NamespacedName) { notified = append(notified, nn) }

	p.pollAll(context.Background())

	if r := p.Reported(testMap, "cluster-a"); r == nil || *r != 8 {
		t.Fatalf("cluster-a reported = %v, want 8", r)
	}
	if r := p.Reported(testMap, "cluster-b"); r == nil || *r != 1 {
		t.Fatalf("cluster-b reported = %v, want 1", r)
	}
	// Both homes changed, but they share one map, so one reconcile covers them.
	if len(notified) != 1 {
		t.Fatalf("notified %d times (%v), want 1 for the shared map", len(notified), notified)
	}
}

func TestCapacityConfig_ValidateFormats(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*CapacityConfig)
		wantErr bool
	}{
		{"default format is Report", func(c *CapacityConfig) { c.Format = "" }, false},
		{"explicit Report is valid", func(c *CapacityConfig) { c.Format = FormatReport }, false},
		// A format this build does not carry is a startup error, not a silent
		// fallback: ignoring it would drop the operator's intent and leave the
		// plan in force with no explanation.
		{"unregistered format rejected", func(c *CapacityConfig) { c.Format = "Bogus" }, true},
		// Report needs no format-specific settings, so an option here is either
		// a typo or a misunderstanding; either way it must not be ignored.
		{"options rejected for Report", func(c *CapacityConfig) {
			c.Options = map[string]string{"unitsPerReplica": "4"}
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
	// The deletion contract: a build without the optional tree registers
	// exactly the built-in format. If this fails under a plain `go test`, an
	// optional format has leaked into the default build.
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
