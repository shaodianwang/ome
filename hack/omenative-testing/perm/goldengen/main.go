// Command goldengen writes the ColumnarV2 golden file that pins the harness's
// Python decoder to the Go codec: for each fixture, the columns the codec
// encodes and the dense rows it decodes them back to.
//
//	cd hack/omenative-testing/perm && go run ./goldengen
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/irstatus"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/irstatus/irstatustest"
)

// decodeBound is the row bound the fixtures are encoded under; every fixture
// below is far smaller.
const decodeBound = 20000

type golden struct {
	Name    string                            `json:"name"`
	Columns *v1beta1.InstanceStatusColumns    `json:"columns"`
	Rows    []v1beta1.OMENativeInstanceStatus `json:"rows"`
}

// defaultOutput is the golden file path relative to the perm directory; the
// drift test compares the committed file against a fresh generation.
const defaultOutput = "testdata/columnar_golden.json"

func main() {
	out := flag.String("out", defaultOutput, "path of the golden file to write")
	flag.Parse()

	data, err := render()
	if err != nil {
		fail("%v", err)
	}
	if err := os.WriteFile(*out, data, 0o644); err != nil {
		fail("write %s: %v", *out, err)
	}
	fmt.Printf("wrote %s\n", *out)
}

// render produces the golden file bytes: every fixture encoded by the codec
// and decoded back, serialized the way the API types marshal.
func render() ([]byte, error) {
	fixtures := []struct {
		name string
		rows []v1beta1.OMENativeInstanceStatus
	}{
		{"uniform-3", irstatustest.UniformRows(3)},
		{"uniform-50", irstatustest.UniformRows(50)},
		{"representative-40", irstatustest.RepresentativeRows(40)},
		{"full-feature", irstatustest.FullFeatureRows()},
		{"reversed-row-order", reversed(irstatustest.RepresentativeRows(12))},
	}

	goldens := make([]golden, 0, len(fixtures))
	for _, fixture := range fixtures {
		irstatus.ClearPodDerivedObservations(fixture.rows)
		columns, err := irstatus.EncodeColumns(fixture.rows, decodeBound)
		if err != nil {
			return nil, fmt.Errorf("encode %s: %w", fixture.name, err)
		}
		rows, err := irstatus.DecodeColumns(columns, decodeBound)
		if err != nil {
			return nil, fmt.Errorf("decode %s: %w", fixture.name, err)
		}
		goldens = append(goldens, golden{Name: fixture.name, Columns: columns, Rows: rows})
	}

	data, err := json.MarshalIndent(goldens, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("marshal: %w", err)
	}
	return append(data, '\n'), nil
}

func reversed(rows []v1beta1.OMENativeInstanceStatus) []v1beta1.OMENativeInstanceStatus {
	out := make([]v1beta1.OMENativeInstanceStatus, len(rows))
	for i := range rows {
		out[len(rows)-1-i] = rows[i]
	}
	return out
}

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "goldengen: "+format+"\n", args...)
	os.Exit(1)
}
