package analysis

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"time"

	promapi "github.com/prometheus/client_golang/api"
	promv1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
)

// ErrNoData is returned when a query produces no usable value — an empty result
// vector, a NaN (a 0/0 error-rate ratio before any traffic), or an infinity (an
// x/0 ratio). The reconciler maps it to INCONCLUSIVE ("can't tell"), never to a
// metric breach: "no requests yet" must not be read as "the canary is failing,"
// and a divide-by-zero +Inf must not be read as "infinitely bad."
var ErrNoData = errors.New("prometheus query returned no usable data")

// Querier runs an instant PromQL query and reduces it to a single scalar. It is
// the only component in this package that performs network I/O; everything layered
// above it (Evaluate) is pure and tested with a fake Querier.
type Querier interface {
	Query(ctx context.Context, promQL string) (float64, error)
}

// NewQuerier builds a Querier against serverAddress. A non-empty bearerToken is
// sent as "Authorization: Bearer <token>"; headers are sent verbatim on every
// request (e.g. "X-Scope-OrgID" for a multi-tenant Cortex/Thanos/Mimir
// front-end). With neither, requests are unauthenticated (the bundled
// Prometheus). TLS/mTLS is not yet supported.
func NewQuerier(serverAddress, bearerToken string, headers map[string]string) (Querier, error) {
	cfg := promapi.Config{Address: serverAddress}
	if bearerToken != "" || len(headers) > 0 {
		cfg.RoundTripper = &headerRoundTripper{token: bearerToken, headers: headers, base: promapi.DefaultRoundTripper}
	}
	client, err := promapi.NewClient(cfg)
	if err != nil {
		return nil, fmt.Errorf("build prometheus client for %q: %w", serverAddress, err)
	}
	return &promQuerier{api: promv1.NewAPI(client)}, nil
}

type promQuerier struct{ api promv1.API }

// Query runs an instant query at the current time and reduces the result. A
// transport error is returned verbatim (the caller maps it to INCONCLUSIVE);
// query warnings are ignored.
func (p *promQuerier) Query(ctx context.Context, promQL string) (float64, error) {
	val, _, err := p.api.Query(ctx, promQL, time.Now())
	if err != nil {
		return 0, err
	}
	return reduce(val)
}

// reduce collapses a Prometheus result to one scalar: a scalar passes through; a
// vector reduces to the MAX across series (the worst pod — the most conservative
// reading for a safety gate); a result with no usable value (empty, or all
// NaN/Inf) is ErrNoData; a string result is parsed as a float. Any other type is
// an error.
func reduce(v model.Value) (float64, error) {
	switch t := v.(type) {
	case *model.Scalar:
		f := float64(t.Value)
		if unusable(f) {
			return 0, ErrNoData
		}
		return f, nil
	case model.Vector:
		max := math.Inf(-1)
		found := false
		for _, s := range t {
			f := float64(s.Value)
			if unusable(f) {
				continue
			}
			found = true
			if f > max {
				max = f
			}
		}
		if !found {
			return 0, ErrNoData
		}
		return max, nil
	case *model.String:
		f, err := strconv.ParseFloat(t.Value, 64)
		if err != nil {
			return 0, fmt.Errorf("non-numeric string result %q", t.Value)
		}
		return f, nil
	default:
		return 0, fmt.Errorf("unsupported prometheus result type %T (expected scalar or vector)", v)
	}
}

// headerRoundTripper injects a static bearer token and/or custom headers. It
// clones the request so the shared transport never sees a caller request it
// mutated.
type headerRoundTripper struct {
	token   string
	headers map[string]string
	base    http.RoundTripper
}

func (h *headerRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	r := req.Clone(req.Context())
	if h.token != "" {
		r.Header.Set("Authorization", "Bearer "+h.token)
	}
	for k, v := range h.headers {
		r.Header.Set(k, v)
	}
	return h.base.RoundTrip(r)
}

// unusable reports whether a metric value cannot be compared to a threshold: a
// NaN (a 0/0 ratio) or an infinity (an x/0 ratio). Both mean "no usable signal,"
// not "infinitely bad" — treating +Inf as a breach would burn the failure budget
// on a divide-by-zero artifact — so both map to ErrNoData -> Inconclusive.
func unusable(f float64) bool {
	return math.IsNaN(f) || math.IsInf(f, 0)
}
