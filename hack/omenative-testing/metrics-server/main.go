// Command canary-metrics is a tiny CPU-only workload for the OMENative
// canary-analysis e2e tests. It serves a Prometheus /metrics endpoint whose gate
// metric (canary_error_rate) is driven deterministically by env, so a test can
// force a canary to PASS (low rate), BREACH (high rate), or read INCONCLUSIVE
// (TEST_EMIT_METRICS=false omits the gate metric, so the query returns no
// series). It also emits real app_requests_total counters from a self-driving
// traffic loop, so the image is a genuine metrics server, not a static file.
//
// A running pod's rate can be changed without a restart through the control
// endpoint (/-/perm/error-rate): a harness sets a breach on the pods of one
// side of a rollout, for a bounded window or until the pod goes, and reads the
// rate back. The endpoint accepts its parameters on GET because the harness
// reaches a pod through the apiserver's pod proxy, which `kubectl get --raw`
// issues as GET.
//
// Stdlib only: the exposition format is hand-written, so the image has no module
// dependencies and builds to a static scratch binary.
package main

import (
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envFloat(key string, def float64) float64 {
	if f, err := strconv.ParseFloat(os.Getenv(key), 64); err == nil {
		return f
	}
	return def
}

func envInt(key string, def int) int {
	if n, err := strconv.Atoi(os.Getenv(key)); err == nil {
		return n
	}
	return def
}

// controlPath is the live rate override: GET with value=<rate> sets the rate
// every pod-side reading uses from then on, for=<seconds> bounds the override
// (the configured rate returns when it expires), clear=1 drops it, and a GET
// with no parameter reports the rate in force.
const controlPath = "/-/perm/error-rate"

// rateOverride is the live override of the configured error rate: the rate a
// control request set and, when it was bounded, the moment it expires. The
// zero value means no override.
type rateOverride struct {
	mu    sync.Mutex
	set   bool
	rate  float64
	until time.Time
}

// current returns the override rate while one is in force; the second value
// is false once it has expired or was never set.
func (o *rateOverride) current(now time.Time) (float64, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if !o.set {
		return 0, false
	}
	if !o.until.IsZero() && now.After(o.until) {
		o.set = false
		return 0, false
	}
	return o.rate, true
}

func (o *rateOverride) apply(rate float64, forS int, now time.Time) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.set, o.rate, o.until = true, rate, time.Time{}
	if forS > 0 {
		o.until = now.Add(time.Duration(forS) * time.Second)
	}
}

func (o *rateOverride) clear() {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.set = false
}

// controlHandler serves controlPath: it applies value=/for=/clear= and then
// reports, as key=value lines, the rate in force, the configured rate, whether
// an override is active and when it expires. An unparsable value is a 400.
func controlHandler(override *rateOverride, configured float64, effective func() float64) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		now := time.Now()
		if q.Get("clear") != "" {
			override.clear()
		}
		if v := q.Get("value"); v != "" {
			rate, err := strconv.ParseFloat(v, 64)
			if err != nil {
				http.Error(w, "value must be a number", http.StatusBadRequest)
				return
			}
			forS := 0
			if f := q.Get("for"); f != "" {
				n, err := strconv.Atoi(f)
				if err != nil || n < 0 {
					http.Error(w, "for must be a non-negative number of seconds", http.StatusBadRequest)
					return
				}
				forS = n
			}
			override.apply(rate, forS, now)
			log.Printf("error-rate override: %g for %ds", rate, forS)
		}
		w.Header().Set("Content-Type", "text/plain")
		_, active := override.current(now)
		fmt.Fprintf(w, "rate=%g\nconfigured=%g\noverride=%t\n", effective(), configured, active)
		override.mu.Lock()
		until := override.until
		override.mu.Unlock()
		if active && !until.IsZero() {
			fmt.Fprintf(w, "until=%s\n", until.UTC().Format(time.RFC3339))
		}
	}
}

// runProxy forwards every request to target after a fixed delay — a stand-in for
// a slow/hung Prometheus. perf/hung_prom.py points one canary's analysis source
// at this to prove a slow source can't stall the reconcile worker.
func runProxy(port, target string, delayMS int) {
	u, err := url.Parse(target)
	if err != nil {
		log.Fatalf("bad PROXY_TARGET %q: %v", target, err)
	}
	delay := time.Duration(delayMS) * time.Millisecond
	rp := httputil.NewSingleHostReverseProxy(u)
	log.Printf("delay-proxy -> %s (delay %v) on :%s", target, delay, port)
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if delay > 0 {
			time.Sleep(delay)
		}
		rp.ServeHTTP(w, r)
	})
	if err := http.ListenAndServe(":"+port, h); err != nil {
		log.Fatal(err)
	}
}

func main() {
	port := env("PORT", "8080")
	// Delay-proxy mode (hung-Prometheus test) takes precedence over the metrics
	// server when PROXY_TARGET is set.
	if target := os.Getenv("PROXY_TARGET"); target != "" {
		runProxy(port, target, envInt("PROXY_DELAY_MS", 0))
		return
	}
	// errorRate drives BOTH the gate gauge and the synthetic failure fraction, so
	// canary_error_rate and the app_requests_total error ratio stay consistent.
	errorRate := envFloat("TEST_ERROR_RATE", 0)
	latencyMS := envInt("TEST_LATENCY_MS", 5)
	rps := envInt("TEST_RPS", 20)
	// emit=false omits canary_error_rate entirely -> the analysis query returns no
	// series -> ErrNoData -> Inconclusive (the harness drives this path explicitly).
	emit := env("TEST_EMIT_METRICS", "true") != "false"
	startupDelay := envInt("TEST_STARTUP_DELAY", 15)
	// decayS>0: the canary is briefly bad then recovers — the gauge reports
	// errorRate for the first decayS seconds of serving, then 0. Drives the
	// transient-breach-then-recover (budget non-exhaustion) case without a template
	// change (which would otherwise retarget the canary).
	decayS := envInt("TEST_ERROR_RATE_DECAY_S", 0)

	// Artificial startup delay: don't listen until it elapses, so the pod's Ready
	// transition is slow enough for the harness pod-watcher to catch peak/drain
	// (same reason as the nginx fixture's startup sleep).
	if startupDelay > 0 {
		log.Printf("startup delay %ds (error_rate=%g emit=%v rps=%d decay=%ds)", startupDelay, errorRate, emit, rps, decayS)
		time.Sleep(time.Duration(startupDelay) * time.Second)
	}
	serveStart := time.Now()
	override := &rateOverride{}
	// currentRate applies the decay relative to serving start (≈ Ready), so the
	// recovery lines up with when the analysis begins sampling. A live override
	// wins over both while it is in force.
	currentRate := func() float64 {
		if rate, ok := override.current(time.Now()); ok {
			return rate
		}
		if decayS > 0 && time.Since(serveStart) > time.Duration(decayS)*time.Second {
			return 0
		}
		return errorRate
	}

	// Self-driving traffic: a real request loop that increments real counters at
	// ~rps, failing currentRate() of them. No external load generator needed.
	var okCount, errCount uint64
	if rps > 0 {
		go func() {
			tick := time.NewTicker(time.Second / time.Duration(rps))
			defer tick.Stop()
			rng := rand.New(rand.NewSource(time.Now().UnixNano()))
			for range tick.C {
				if rng.Float64() < currentRate() {
					atomic.AddUint64(&errCount, 1)
				} else {
					atomic.AddUint64(&okCount, 1)
				}
			}
		}()
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprintln(w, "ok")
	})
	mux.HandleFunc(controlPath, controlHandler(override, errorRate, currentRate))
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		if emit {
			fmt.Fprintln(w, "# HELP canary_error_rate Configured synthetic error rate (canary gate metric).")
			fmt.Fprintln(w, "# TYPE canary_error_rate gauge")
			fmt.Fprintf(w, "canary_error_rate %g\n", currentRate())
		}
		fmt.Fprintln(w, "# HELP app_requests_total Synthetic served requests by HTTP code.")
		fmt.Fprintln(w, "# TYPE app_requests_total counter")
		fmt.Fprintf(w, "app_requests_total{code=\"200\"} %d\n", atomic.LoadUint64(&okCount))
		fmt.Fprintf(w, "app_requests_total{code=\"500\"} %d\n", atomic.LoadUint64(&errCount))
		fmt.Fprintln(w, "# HELP app_request_latency_ms Configured synthetic request latency.")
		fmt.Fprintln(w, "# TYPE app_request_latency_ms gauge")
		fmt.Fprintf(w, "app_request_latency_ms %d\n", latencyMS)
	})

	log.Printf("canary-metrics listening on :%s", port)
	if err := http.ListenAndServe(":"+port, mux); err != nil {
		log.Fatal(err)
	}
}
