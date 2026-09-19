package routing

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"slices"
	"sort"
	"sync"
	"time"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/types"
)

// CapacityReport is the contract a home's capacity endpoint answers with.
//
// Servable is an ABSOLUTE count in servable units — usable prefill/decode
// pairs, or servable replicas for a non-disaggregated home. Deliberately not a
// utilization ratio: a saturated home and an idle one report the same fraction,
// which says nothing about how much either can serve. Deliberately not a flat
// sum of per-worker request slots either, since that counts prefill slots with
// no decode partner and so OVERSTATES capacity during exactly the rollout skew
// this input exists to correct.
//
// ObservedAt is when the home computed the figure, so a frozen reporter is
// detectable. Without it a stale read is indistinguishable from a fresh one and
// a wedged home would hold its weight forever.
type CapacityReport struct {
	Servable   int32     `json:"servable"`
	ObservedAt time.Time `json:"observedAt"`
}

// capacityState is the poller's running view of one home.
type capacityState struct {
	// generation identifies the polling pass allowed to update this state. A
	// completion from an older pass cannot overwrite a newer observation.
	generation uint64
	// window holds the most recent accepted readings, oldest first. The applied
	// ceiling is their Quorum-th smallest, so no single reading -- however low
	// -- can set it alone.
	window []int32
	// reported is the applied ceiling, or nil when the home has not answered
	// usably. Nil means the control-plane plan stands.
	reported *int32
	// reason explains why reported is nil, for the fail-open condition. It is
	// empty only while an applied ceiling exists.
	reason string
}

// capacityKey identifies the source of one capacity history. Owner and URL are
// part of the identity so a same-name replacement or endpoint change starts
// from an empty window instead of inheriting its predecessor's observations.
type capacityKey struct {
	ownerUID types.UID
	mapKey   types.NamespacedName
	cluster  string
	url      string
}

type capacityWork struct {
	target     Target
	key        capacityKey
	generation uint64
	future     *ObserverFuture
	reported   *int32
	reason     string
}

// observe folds one accepted reading into the window and returns the resulting
// ceiling -- the quorum-th smallest retained reading -- and whether one applies
// at all.
//
// Until the window holds quorum readings, none does: the plan stands. A ceiling
// derived from fewer readings than the quorum would be exactly the thing the
// quorum exists to prevent, since the first reading after startup or recovery
// could come from the one broken reporter and would set the ceiling with
// nothing to corroborate it.
func (st *capacityState) observe(v int32, samples, quorum int) (int32, bool) {
	st.window = append(st.window, v)
	if len(st.window) > samples {
		st.window = st.window[len(st.window)-samples:]
	}
	if len(st.window) < quorum {
		return 0, false
	}
	sorted := make([]int32, len(st.window))
	copy(sorted, st.window)
	slices.Sort(sorted)
	return sorted[quorum-1], true
}

// CapacityPoller asks each home what it can currently serve and turns the
// answer into a CEILING on that home's planned allocation.
//
// Only the home knows its realized prefill/decode pairing: the control plane
// computes MIN(ready_prefill, ready_decode), an upper bound on usable pairs
// rather than a count of them, and during a rollout the two components recover
// at different rates so the skew is widest exactly when the number matters
// most.
//
// It shares the prober's HTTP client: both outputs talk to the same homes with
// the same credentials and fail the same ways, so splitting them across two
// clients would mean two auth paths, two staleness semantics, and two ways for
// an operator to half-configure the feature. They stay independently
// enabled — neither implies the other.
//
// Every failure mode falls open to the plan. Fail-closed would turn a
// control-plane-to-data-plane network blip into a fleet-wide traffic cutoff,
// which is a far worse outcome than routing to a home that is briefly
// over-weighted.
type CapacityPoller struct {
	Config CapacityConfig
	Log    logr.Logger
	Client *http.Client
	// Executor owns the fixed worker and queue bounds shared by active routing
	// observations.
	Executor *ObserverExecutor
	// MaxResponseBytes is the configured upper bound for one response body.
	MaxResponseBytes int64

	// Targets supplies the homes to poll, shared with the prober's view of the
	// current routing tables.
	Targets func(context.Context) ([]Target, error)

	// OnChange delivers the affected TrafficMap when a home's reported ceiling
	// or fallback reason changes. It blocks until the notification is accepted
	// or ctx is cancelled; returning an error stops the poller.
	OnChange func(context.Context, types.NamespacedName) error

	mu         sync.RWMutex
	generation uint64
	state      map[capacityKey]*capacityState
}

// NewCapacityPoller builds a poller sharing the given HTTP client and bounded
// observer executor.
func NewCapacityPoller(
	cfg CapacityConfig,
	executor *ObserverExecutor,
	client *http.Client,
	maxResponseBytes int64,
	log logr.Logger,
) (*CapacityPoller, error) {
	if executor == nil {
		return nil, errors.New("capacity observer executor is nil")
	}
	if client == nil {
		return nil, errors.New("capacity HTTP client is nil")
	}
	if maxResponseBytes <= 0 {
		return nil, fmt.Errorf("capacity maximum response bytes must be positive, got %d", maxResponseBytes)
	}
	return &CapacityPoller{
		Config:           cfg,
		Log:              log,
		Client:           client,
		Executor:         executor,
		MaxResponseBytes: maxResponseBytes,
		state:            map[capacityKey]*capacityState{},
	}, nil
}

// Reported returns the home's last accepted servable count, or nil when the
// plan should stand — polling off, no answer yet, or the last answer was
// unusable. Nil is the fail-open value.
func (c *CapacityPoller) Reported(tgt Target) *int32 {
	if c == nil || !c.Config.IsEnabled() {
		return nil
	}
	k, err := c.keyFor(tgt)
	if err != nil {
		return nil
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	st, ok := c.state[k]
	if !ok || st.reported == nil {
		return nil
	}
	v := *st.reported
	return &v
}

// FellOpen reports why a home's report was not applied, or empty when it was
// applied or polling is off.
//
// Silent fallback is as bad as fail-closed for an operator: the weight looks
// the same whether the home agreed with the plan or the poll failed, and those
// have opposite remediations.
func (c *CapacityPoller) FellOpen(tgt Target) string {
	if c == nil || !c.Config.IsEnabled() {
		return ""
	}
	k, err := c.keyFor(tgt)
	if err != nil {
		return fmt.Sprintf("capacity target is invalid: %v", err)
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	st, ok := c.state[k]
	if !ok {
		return "no capacity report yet"
	}
	return st.reason
}

// Forget removes every capacity history for one TrafficMap. An observation
// already in flight retains its old generation and cannot recreate the state.
func (c *CapacityPoller) Forget(mapKey types.NamespacedName) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for key := range c.state {
		if key.mapKey == mapKey {
			delete(c.state, key)
		}
	}
}

// Start runs the poll loop until the context is cancelled, satisfying
// manager.Runnable. It returns immediately when capacity polling is not
// configured, so it can be wired unconditionally and stay inert by config.
func (c *CapacityPoller) Start(ctx context.Context) error {
	if !c.Config.IsEnabled() {
		c.Log.Info("endpoint-reported capacity disabled (no capacity path configured)")
		return nil
	}
	c.Log.Info("starting endpoint-reported capacity polling",
		"path", c.Config.Path, "period", c.Config.Period, "timeout", c.Config.Timeout,
		"maxAge", c.Config.MaxAge, "maxResponseBytes", c.MaxResponseBytes)

	t := time.NewTicker(c.Config.Period)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			if err := c.pollAll(ctx); err != nil {
				if ctx.Err() != nil {
					return nil
				}
				return err
			}
		}
	}
}

// NeedLeaderElection keeps polling on the elected leader only: extra replicas
// add load on the homes without adding signal, and only the leader writes.
func (c *CapacityPoller) NeedLeaderElection() bool { return true }

// pollAll polls every current target once and notifies each affected map after
// all accepted jobs have completed. Jobs run only in the shared bounded
// executor; this method creates no goroutine per target.
func (c *CapacityPoller) pollAll(ctx context.Context) error {
	targets, err := c.Targets(ctx)
	if err != nil {
		c.Log.Error(err, "listing capacity targets")
		return c.notifyChanges(ctx, c.failOpenAll("capacity target discovery failed"))
	}

	work := c.prepare(targets)
	for i := range work {
		item := &work[i]
		if c.Executor == nil {
			item.reason = "capacity observer executor is not configured"
			continue
		}
		future, submitErr := c.Executor.Submit(ctx, c.Config.Timeout, func(jobCtx context.Context) error {
			item.reported, item.reason = c.pollOne(jobCtx, item.target)
			return nil
		})
		if submitErr != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			item.reason = fmt.Sprintf("capacity observation could not be submitted: %v", submitErr)
			continue
		}
		item.future = future
	}

	changed := map[types.NamespacedName]struct{}{}
	for i := range work {
		item := &work[i]
		if item.future != nil {
			if waitErr := item.future.Wait(ctx); waitErr != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				if item.reason == "" {
					item.reason = fmt.Sprintf("capacity observation did not complete: %v", waitErr)
				}
				item.reported = nil
			}
		}
		if c.recordCurrent(item.key, item.generation, item.reported, item.reason) {
			changed[item.target.Map] = struct{}{}
		}
	}

	return c.notifyChanges(ctx, changed)
}

func (c *CapacityPoller) notifyChanges(
	ctx context.Context,
	changed map[types.NamespacedName]struct{},
) error {
	if c.OnChange == nil || len(changed) == 0 {
		return nil
	}
	maps := make([]types.NamespacedName, 0, len(changed))
	for mapKey := range changed {
		maps = append(maps, mapKey)
	}
	sort.Slice(maps, func(i, j int) bool { return maps[i].String() < maps[j].String() })
	for _, mapKey := range maps {
		if err := c.OnChange(ctx, mapKey); err != nil {
			return err
		}
	}
	return nil
}

func (c *CapacityPoller) failOpenAll(reason string) map[types.NamespacedName]struct{} {
	changed := map[types.NamespacedName]struct{}{}
	c.mu.Lock()
	defer c.mu.Unlock()
	for key, state := range c.state {
		if state.reported == nil && state.reason == reason && len(state.window) == 0 {
			continue
		}
		state.window = nil
		state.reported = nil
		state.reason = reason
		changed[key.mapKey] = struct{}{}
	}
	return changed
}

// pollOne fetches and validates one home's capacity report. It returns the
// accepted count, or nil plus the reason the plan stands instead.
func (c *CapacityPoller) pollOne(ctx context.Context, tgt Target) (*int32, string) {
	capacityURL, err := c.capacityURL(tgt)
	if err != nil {
		return nil, fmt.Sprintf("capacity request URL is invalid: %v", err)
	}
	req, err := http.NewRequestWithContext(ctx, c.Config.Method, capacityURL, nil)
	if err != nil {
		return nil, fmt.Sprintf("capacity request could not be built: %v", err)
	}
	resp, err := c.Client.Do(req)
	if err != nil {
		return nil, fmt.Sprintf("capacity endpoint unreachable: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Sprintf("capacity endpoint returned HTTP %d", resp.StatusCode)
	}
	body, err := readCapacityBody(resp.Body, c.MaxResponseBytes)
	if err != nil {
		return nil, fmt.Sprintf("reading capacity response: %v", err)
	}
	report, reason := c.Config.decode(body)
	if reason != "" {
		return nil, reason
	}
	if report.Servable < 0 {
		// Malformed, not an instruction to serve nothing. A parse or arithmetic
		// bug on the home must not be able to black-hole it.
		return nil, fmt.Sprintf("capacity report is negative (%d)", report.Servable)
	}
	now := time.Now()
	if report.ObservedAt.After(now) {
		return nil, "capacity report timestamp is in the future"
	}
	if age := now.Sub(report.ObservedAt); age > c.Config.MaxAge {
		return nil, fmt.Sprintf("capacity report is stale (older than configured maximum %s)", c.Config.MaxAge)
	}
	v := report.Servable
	return &v, ""
}

func readCapacityBody(body io.Reader, maxResponseBytes int64) ([]byte, error) {
	if maxResponseBytes <= 0 {
		return nil, fmt.Errorf("maximum response size must be positive")
	}
	readLimit := maxResponseBytes
	if readLimit < math.MaxInt64 {
		readLimit++
	}
	b, err := io.ReadAll(io.LimitReader(body, readLimit))
	if err != nil {
		return nil, err
	}
	if int64(len(b)) > maxResponseBytes {
		return nil, fmt.Errorf("response exceeds configured limit of %d bytes", maxResponseBytes)
	}
	return b, nil
}

func (c *CapacityPoller) capacityURL(tgt Target) (string, error) {
	endpoint, err := CanonicalEndpoint(tgt.URL)
	if err != nil {
		return "", err
	}
	capacityURL, err := url.JoinPath(endpoint, c.Config.Path)
	if err != nil {
		return "", fmt.Errorf("join endpoint and capacity path: %w", err)
	}
	return capacityURL, nil
}

func (c *CapacityPoller) keyFor(tgt Target) (capacityKey, error) {
	capacityURL, err := c.capacityURL(tgt)
	if err != nil {
		return capacityKey{}, err
	}
	return capacityKey{
		ownerUID: tgt.OwnerUID,
		mapKey:   tgt.Map,
		cluster:  tgt.Cluster,
		url:      capacityURL,
	}, nil
}

// decode turns a response body into a report in servable units by handing it to
// the configured format, or explains why it cannot.
func (c CapacityConfig) decode(body []byte) (CapacityReport, string) {
	name := c.resolvedFormat()
	plugin, ok := lookupCapacityFormat(name)
	if !ok {
		// Validate rejects this at startup, so reaching here means the registry
		// changed under us. Fall open rather than guess at the shape.
		return CapacityReport{}, fmt.Sprintf("capacity format %q is not registered in this build", name)
	}
	report, reason := plugin.Decode(body, c)
	if reason != "" {
		return CapacityReport{}, reason
	}
	if report.ObservedAt.IsZero() {
		// A format that does not stamp its own reading gets one at receipt, so
		// the staleness comparison below has something to work with. Note this
		// makes MaxAge inert for such a format: the value is always ~0 old.
		report.ObservedAt = time.Now()
	}
	return report, ""
}

// prepare establishes one generation for the current target snapshot. State
// not present in the snapshot is removed before any request completes, and a
// later snapshot supersedes all outstanding jobs from this one.
func (c *CapacityPoller) prepare(targets []Target) []capacityWork {
	work := make([]capacityWork, 0, len(targets))
	seen := make(map[capacityKey]struct{}, len(targets))
	for _, tgt := range targets {
		k, err := c.keyFor(tgt)
		if err != nil {
			c.Log.Error(err, "invalid capacity target", "trafficmap", tgt.Map.String(), "cluster", tgt.Cluster)
			continue
		}
		if _, ok := seen[k]; ok {
			continue
		}
		seen[k] = struct{}{}
		work = append(work, capacityWork{target: tgt, key: k})
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	c.generation++
	if c.generation == 0 {
		c.generation++
	}
	generation := c.generation
	for i := range work {
		st, ok := c.state[work[i].key]
		if !ok {
			st = &capacityState{}
			c.state[work[i].key] = st
		}
		st.generation = generation
		work[i].generation = generation
	}
	for k, st := range c.state {
		if st.generation != generation {
			delete(c.state, k)
		}
	}
	return work
}

// record stores a home's outcome without a polling generation. It supports
// synchronous callers and tests; executor jobs use recordCurrent so a late
// predecessor cannot overwrite current state.
func (c *CapacityPoller) record(tgt Target, reported *int32, reason string) bool {
	k, err := c.keyFor(tgt)
	if err != nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	st, ok := c.state[k]
	if !ok {
		st = &capacityState{}
		c.state[k] = st
	}
	return c.recordLocked(st, reported, reason)
}

func (c *CapacityPoller) recordCurrent(
	k capacityKey,
	generation uint64,
	reported *int32,
	reason string,
) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	st, ok := c.state[k]
	if !ok || st.generation != generation {
		return false
	}
	return c.recordLocked(st, reported, reason)
}

func (c *CapacityPoller) recordLocked(st *capacityState, reported *int32, reason string) bool {
	var applied *int32
	if reported != nil {
		if v, ok := st.observe(*reported, c.Config.samples(), c.Config.quorum()); ok {
			applied = &v
			reason = ""
		} else if reason == "" {
			reason = fmt.Sprintf("capacity report awaiting quorum (%d/%d accepted samples)",
				len(st.window), c.Config.quorum())
		}
	} else {
		// A failed poll falls open and releases the ceiling, so the window must
		// not survive it: retaining readings would keep constraining a home we
		// can no longer observe, which is the opposite of failing open.
		st.window = nil
		if reason == "" {
			reason = "capacity report unavailable"
		}
	}
	changed := !sameReport(st.reported, applied) || st.reason != reason
	st.reported = applied
	st.reason = reason
	return changed
}

// sameReport compares two optional counts, treating nil as its own value so a
// home that stops reporting is a change rather than a no-op.
func sameReport(a, b *int32) bool {
	switch {
	case a == nil && b == nil:
		return true
	case a == nil || b == nil:
		return false
	default:
		return *a == *b
	}
}

// forget drops state for homes that are no longer targets, so a removed home
// does not leak and a returning one is re-polled rather than inheriting a stale
// ceiling.
func (c *CapacityPoller) forget(targets []Target) {
	live := make(map[capacityKey]struct{}, len(targets))
	for _, t := range targets {
		k, err := c.keyFor(t)
		if err == nil {
			live[k] = struct{}{}
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for k := range c.state {
		if _, ok := live[k]; !ok {
			delete(c.state, k)
		}
	}
}
