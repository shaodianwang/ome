package irstatus

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

// instanceRepresentationSizes returns the serialized bytes of only the
// per-Instance representation in each candidate, which is the part of the
// status this codec owns, alongside the complete status sizes.
type representationSizes struct {
	denseRows, columnar         int
	denseStatus, columnarStatus int
	selected                    Encoding
}

func measure(t testing.TB, rows []v1beta1.OMENativeInstanceStatus) representationSizes {
	t.Helper()
	selection, err := SelectCandidate(logicalStatus(rows), testLimit)
	if err != nil {
		t.Fatalf("SelectCandidate() error = %v", err)
	}
	if selection.ColumnarV2 == nil {
		t.Fatalf("fixture is not eligible for ColumnarV2: %q", selection.IneligibleReason)
	}
	return representationSizes{
		denseRows:      len(mustJSON(t, selection.DenseV1.Status.InstanceStatuses)),
		columnar:       len(mustJSON(t, selection.ColumnarV2.Status.InstanceStatusColumns)),
		denseStatus:    selection.DenseV1.Size,
		columnarStatus: selection.ColumnarV2.Size,
		selected:       selection.Selected,
	}
}

func (s representationSizes) log(t testing.TB, name string) {
	t.Helper()
	t.Logf("%s: instance representation DenseV1 %d B, ColumnarV2 %d B (%.2f%% of dense); complete status DenseV1 %d B, ColumnarV2 %d B; selected %s",
		name, s.denseRows, s.columnar, 100*float64(s.columnar)/float64(s.denseRows), s.denseStatus, s.columnarStatus, s.selected)
}

// The uniform steady-state fixtures reproduce the modeled sizes: 322,891 and
// 808,891 dense bytes against 363 columnar bytes.
func TestUniformFixtureSizes(t *testing.T) {
	t.Parallel()
	for _, expected := range []struct{ rows, dense, columnar int }{
		{rows: 2000, dense: 322891, columnar: 363},
		{rows: 5000, dense: 808891, columnar: 363},
	} {
		sizes := measure(t, uniformRows(expected.rows))
		sizes.log(t, fmt.Sprintf("uniform %d rows", expected.rows))
		if sizes.denseRows != expected.dense || sizes.columnar != expected.columnar {
			t.Errorf("uniform %d rows: measured (%d, %d) B, recorded (%d, %d) B", expected.rows, sizes.denseRows, sizes.columnar, expected.dense, expected.columnar)
		}
		if sizes.selected != EncodingColumnarV2 || sizes.columnarStatus >= sizes.denseStatus {
			t.Errorf("uniform %d rows: selected %s with complete sizes %d vs %d", expected.rows, sizes.selected, sizes.columnarStatus, sizes.denseStatus)
		}
		if 10*sizes.columnar > sizes.denseRows {
			t.Errorf("uniform %d rows: columnar representation is more than 10%% of dense", expected.rows)
		}
	}
}

// Recorded measurements of the mass-failure fixture. The fixture is not tuned
// toward either representation; the numbers are what the codec produces.
const (
	massFailureDenseV1Bytes    = 2101171
	massFailureColumnarV2Bytes = 1667274
)

func TestMassFailureFixtureSizes(t *testing.T) {
	t.Parallel()
	rows := massFailureRows()
	phases := map[v1beta1.OMENativeInstancePhase]int{}
	revisions := map[string]int{}
	operations := 0
	for _, row := range rows {
		phases[row.Phase]++
		revisions[row.RunningRevision]++
		if row.Operation != nil {
			operations++
			if row.Operation.Type != v1beta1.InstanceOperationRestart {
				t.Fatalf("row %d carries a %s operation", row.Index, row.Operation.Type)
			}
		}
		if row.LastFailure == nil || row.PodCount > 1 {
			t.Fatalf("row %d is not a single-pod row with a preserved failure", row.Index)
		}
	}
	if len(rows) != 4000 || phases[v1beta1.OMENativeInstanceReady] != 2812 || phases[v1beta1.OMENativeInstanceRestarting] != 814 || phases[v1beta1.OMENativeInstanceFailed] != 374 {
		t.Fatalf("fixture shape = %d rows, phases %v", len(rows), phases)
	}
	if operations != 814+374 || len(revisions) != 2 || revisions[fixtureRevision] != 3999 || revisions[fixtureOtherRevision] != 1 {
		t.Fatalf("fixture shape = %d operations, revisions %v", operations, revisions)
	}
	if got := len(massFailureMessage); got < 130 || got > 150 {
		t.Fatalf("failure message is %d bytes, want about 140", got)
	}

	sizes := measure(t, rows)
	sizes.log(t, "mass failure 4000 rows")
	t.Logf("mass failure: failure message %d B, per-row dense %.1f B, per-row columnar %.1f B",
		len(massFailureMessage), float64(sizes.denseRows)/4000, float64(sizes.columnar)/4000)
	if sizes.denseRows != massFailureDenseV1Bytes || sizes.columnar != massFailureColumnarV2Bytes {
		t.Errorf("mass failure: measured (%d, %d) B, recorded (%d, %d) B", sizes.denseRows, sizes.columnar, massFailureDenseV1Bytes, massFailureColumnarV2Bytes)
	}
	want := selectEncoding(sizes.denseStatus, sizes.columnarStatus)
	if sizes.selected != want {
		t.Errorf("mass failure: selected %s, want %s for complete sizes %d vs %d", sizes.selected, want, sizes.denseStatus, sizes.columnarStatus)
	}
}

func TestRepresentativeFixtureSizes(t *testing.T) {
	t.Parallel()
	for _, n := range []int{2000, 5000} {
		sizes := measure(t, representativeRows(n))
		sizes.log(t, fmt.Sprintf("representative %d rows", n))
		if want := selectEncoding(sizes.denseStatus, sizes.columnarStatus); sizes.selected != want {
			t.Errorf("representative %d rows: selected %s, want %s", n, sizes.selected, want)
		}
		if 4*sizes.columnar > sizes.denseRows {
			t.Errorf("representative %d rows: columnar representation is more than 25%% of dense", n)
		}
	}
}

func benchmarkRows(name string, n int) []v1beta1.OMENativeInstanceStatus {
	switch name {
	case "uniform":
		return uniformRows(n)
	case "representative":
		return representativeRows(n)
	default:
		return massFailureRows()
	}
}

func benchmarkCases() []struct {
	name string
	rows []v1beta1.OMENativeInstanceStatus
} {
	return []struct {
		name string
		rows []v1beta1.OMENativeInstanceStatus
	}{
		{name: "uniform/2000", rows: benchmarkRows("uniform", 2000)},
		{name: "uniform/5000", rows: benchmarkRows("uniform", 5000)},
		{name: "representative/2000", rows: benchmarkRows("representative", 2000)},
		{name: "representative/5000", rows: benchmarkRows("representative", 5000)},
		{name: "massFailure/4000", rows: benchmarkRows("massFailure", 4000)},
	}
}

func BenchmarkEncodeColumns(b *testing.B) {
	for _, bc := range benchmarkCases() {
		b.Run(bc.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, err := EncodeColumns(bc.rows, testLimit); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkDecodeColumns(b *testing.B) {
	for _, bc := range benchmarkCases() {
		columns := mustEncode(b, bc.rows, testLimit)
		b.Run(bc.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, err := DecodeColumns(columns, testLimit); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// The DenseV1 encode baseline normalizes a write copy and serializes one
// complete dense status; the ColumnarV2 measurement builds both candidates,
// selects one, and serializes the selection.
func BenchmarkEncodeStatusDenseV1Baseline(b *testing.B) {
	for _, bc := range benchmarkCases() {
		logical := logicalStatus(bc.rows)
		b.Run(bc.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				write := logical.DeepCopy()
				ClearPodDerivedObservations(write.InstanceStatuses)
				if _, err := json.Marshal(write); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkEncodeStatusSelectCandidate(b *testing.B) {
	for _, bc := range benchmarkCases() {
		logical := logicalStatus(bc.rows)
		b.Run(bc.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				selection, err := SelectCandidate(logical, testLimit)
				if err != nil {
					b.Fatal(err)
				}
				if _, err := json.Marshal(selection.SelectedCandidate().Status); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// The decode baseline deserializes a complete dense status; the ColumnarV2
// measurement deserializes a complete columnar status and expands its rows.
func BenchmarkDecodeStatusDenseV1Baseline(b *testing.B) {
	for _, bc := range benchmarkCases() {
		data := mustJSON(b, logicalStatus(bc.rows))
		b.Run(bc.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				var status v1beta1.InferenceReplicaStatus
				if err := json.Unmarshal(data, &status); err != nil {
					b.Fatal(err)
				}
				if _, _, err := DecodeStatus(&status, testLimit); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkDecodeStatusColumnarV2(b *testing.B) {
	for _, bc := range benchmarkCases() {
		selection, err := SelectCandidate(logicalStatus(bc.rows), testLimit)
		if err != nil || selection.ColumnarV2 == nil {
			b.Fatalf("SelectCandidate() = %+v, %v", selection, err)
		}
		data := mustJSON(b, selection.ColumnarV2.Status)
		b.Run(bc.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				var status v1beta1.InferenceReplicaStatus
				if err := json.Unmarshal(data, &status); err != nil {
					b.Fatal(err)
				}
				if _, _, err := DecodeStatus(&status, testLimit); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
