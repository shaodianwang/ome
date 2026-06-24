package analysis

import (
	"context"
	"errors"
	"math"
	"net/http"
	"testing"

	"github.com/prometheus/common/model"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

// fakeQuerier returns scripted values/errors keyed by the (rendered) query string.
type fakeQuerier struct {
	vals map[string]float64
	errs map[string]error
}

func (f *fakeQuerier) Query(_ context.Context, q string) (float64, error) {
	if err, ok := f.errs[q]; ok {
		return 0, err
	}
	if v, ok := f.vals[q]; ok {
		return v, nil
	}
	return 0, ErrNoData
}

func analysisWith(metrics ...v1beta1.AnalysisMetric) *v1beta1.RolloutAnalysis {
	return &v1beta1.RolloutAnalysis{FailureLimit: 1, Metrics: metrics}
}

func metric(name, query string, op v1beta1.ComparisonOperator, threshold string) v1beta1.AnalysisMetric {
	return v1beta1.AnalysisMetric{Name: name, Query: query, Operator: op, Threshold: threshold}
}

func TestEvaluate_Outcomes(t *testing.T) {
	tc := TemplateContext{CanaryService: "svc"}
	tests := []struct {
		name string
		a    *v1beta1.RolloutAnalysis
		q    *fakeQuerier
		want Outcome
	}{
		{
			name: "all pass",
			a:    analysisWith(metric("err", "q1", v1beta1.ComparisonLTE, "0.05")),
			q:    &fakeQuerier{vals: map[string]float64{"q1": 0.01}},
			want: Pass,
		},
		{
			name: "breach is fail",
			a:    analysisWith(metric("err", "q1", v1beta1.ComparisonLTE, "0.05")),
			q:    &fakeQuerier{vals: map[string]float64{"q1": 0.2}},
			want: Fail,
		},
		{
			name: "no data is inconclusive",
			a:    analysisWith(metric("err", "q1", v1beta1.ComparisonLTE, "0.05")),
			q:    &fakeQuerier{errs: map[string]error{"q1": ErrNoData}},
			want: Inconclusive,
		},
		{
			name: "fail beats inconclusive",
			a: analysisWith(
				metric("err", "q1", v1beta1.ComparisonLTE, "0.05"),
				metric("lat", "q2", v1beta1.ComparisonLTE, "100"),
			),
			q: &fakeQuerier{
				vals: map[string]float64{"q1": 0.2},     // breach
				errs: map[string]error{"q2": ErrNoData}, // inconclusive
			},
			want: Fail,
		},
		{
			name: "all pass multi-metric AND",
			a: analysisWith(
				metric("err", "q1", v1beta1.ComparisonLTE, "0.05"),
				metric("ok", "q2", v1beta1.ComparisonGTE, "0.99"),
			),
			q:    &fakeQuerier{vals: map[string]float64{"q1": 0.01, "q2": 0.999}},
			want: Pass,
		},
		{
			name: "template error is inconclusive",
			a:    analysisWith(metric("bad", "{{.Nope}}", v1beta1.ComparisonLTE, "1")),
			q:    &fakeQuerier{},
			want: Inconclusive,
		},
		{
			name: "non-numeric threshold is inconclusive",
			a:    analysisWith(metric("err", "q1", v1beta1.ComparisonLTE, "abc")),
			q:    &fakeQuerier{vals: map[string]float64{"q1": 0.01}},
			want: Inconclusive,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Evaluate(context.Background(), tt.q, tt.a, tc)
			if got.Outcome != tt.want {
				t.Fatalf("Outcome = %v, want %v (metrics=%+v)", got.Outcome, tt.want, got.Metrics)
			}
			if len(got.Metrics) != len(tt.a.Metrics) {
				t.Fatalf("len(Metrics) = %d, want %d", len(got.Metrics), len(tt.a.Metrics))
			}
		})
	}
}

func TestCompare(t *testing.T) {
	cases := []struct {
		op       v1beta1.ComparisonOperator
		val, thr float64
		want     bool
	}{
		{v1beta1.ComparisonLT, 1, 2, true},
		{v1beta1.ComparisonLT, 2, 2, false},
		{v1beta1.ComparisonLTE, 2, 2, true},
		{v1beta1.ComparisonGT, 3, 2, true},
		{v1beta1.ComparisonGT, 2, 2, false},
		{v1beta1.ComparisonGTE, 2, 2, true},
		{v1beta1.ComparisonOperator("BOGUS"), 1, 1, false},
	}
	for _, c := range cases {
		if got := compare(c.val, c.op, c.thr); got != c.want {
			t.Errorf("compare(%v,%s,%v) = %v, want %v", c.val, c.op, c.thr, got, c.want)
		}
	}
}

func TestReduce(t *testing.T) {
	if v, err := reduce(&model.Scalar{Value: 1.5}); err != nil || v != 1.5 {
		t.Errorf("scalar: got (%v,%v), want (1.5,nil)", v, err)
	}
	vec := model.Vector{{Value: 1}, {Value: 3}, {Value: 2}}
	if v, err := reduce(vec); err != nil || v != 3 {
		t.Errorf("vector max: got (%v,%v), want (3,nil)", v, err)
	}
	if _, err := reduce(model.Vector{}); !errors.Is(err, ErrNoData) {
		t.Errorf("empty vector: err = %v, want ErrNoData", err)
	}
	if _, err := reduce(&model.Scalar{Value: model.SampleValue(math.NaN())}); !errors.Is(err, ErrNoData) {
		t.Errorf("NaN scalar: err = %v, want ErrNoData", err)
	}
	allNaN := model.Vector{{Value: model.SampleValue(math.NaN())}}
	if _, err := reduce(allNaN); !errors.Is(err, ErrNoData) {
		t.Errorf("all-NaN vector: err = %v, want ErrNoData", err)
	}
	if _, err := reduce(&model.Scalar{Value: model.SampleValue(math.Inf(1))}); !errors.Is(err, ErrNoData) {
		t.Errorf("+Inf scalar: err = %v, want ErrNoData", err)
	}
	if _, err := reduce(model.Vector{{Value: model.SampleValue(math.Inf(1))}}); !errors.Is(err, ErrNoData) {
		t.Errorf("+Inf vector: err = %v, want ErrNoData", err)
	}
	// a +Inf series (a divide-by-zero artifact) is skipped, not taken as the max.
	mixed := model.Vector{{Value: 2}, {Value: model.SampleValue(math.Inf(1))}}
	if v, err := reduce(mixed); err != nil || v != 2 {
		t.Errorf("mixed +Inf vector: got (%v,%v), want (2,nil)", v, err)
	}
}

func TestRenderQuery(t *testing.T) {
	tc := TemplateContext{CanaryService: "svc-rev-abc", Namespace: "ns"}
	out, err := RenderQuery(`rate({service="{{.CanaryService}}"}[1m])`, tc)
	if err != nil {
		t.Fatal(err)
	}
	if want := `rate({service="svc-rev-abc"}[1m])`; out != want {
		t.Errorf("got %q, want %q", out, want)
	}
	if _, err := RenderQuery(`{{.DoesNotExist}}`, tc); err == nil {
		t.Error("expected error for unknown template field")
	}
}

// capturingRT records the request it last saw and returns a 200.
type capturingRT struct{ last *http.Request }

func (c *capturingRT) RoundTrip(r *http.Request) (*http.Response, error) {
	c.last = r
	return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody}, nil
}

func TestHeaderRoundTripper(t *testing.T) {
	cap := &capturingRT{}
	rt := &headerRoundTripper{token: "tok", headers: map[string]string{"X-Scope-OrgID": "team-a"}, base: cap}
	req, _ := http.NewRequest(http.MethodGet, "http://prom/api/v1/query", nil)

	if _, err := rt.RoundTrip(req); err != nil {
		t.Fatal(err)
	}
	if got := cap.last.Header.Get("Authorization"); got != "Bearer tok" {
		t.Errorf("Authorization = %q, want %q", got, "Bearer tok")
	}
	if got := cap.last.Header.Get("X-Scope-OrgID"); got != "team-a" {
		t.Errorf("X-Scope-OrgID = %q, want %q", got, "team-a")
	}
	// The caller's request must not be mutated — RoundTrip clones before setting.
	if req.Header.Get("Authorization") != "" {
		t.Error("original request was mutated (Authorization leaked onto caller request)")
	}
}
