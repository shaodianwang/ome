package rolloutspec

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// canaryGridTestFile holds the canary executor's cell grid: one subtest per
// row, named by the row id the canary machine's cells cite as coverage.
const canaryGridTestFile = "pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/canary/reconciler_cells_test.go"

// canaryGridFunc is the table test that runs the grid; a citation is its
// name, a slash, and the row id.
const canaryGridFunc = "TestReconcileCells"

// canaryGridRow matches one authored row of the grid, {"canary/<State>/<event>", ...
var canaryGridRow = regexp.MustCompile(`\{"(canary/[^"]+)"`)

// TestCanaryCoverageCitesGridRows holds the canary machine's coverage to the
// grid it cites: every coverage.unit entry of the form
// TestReconcileCells/<row id> names a row the grid test declares. The
// coverage checker resolves such a name through its function only, so a
// renamed or deleted row would otherwise keep a cell claiming proof that
// no longer runs.
func TestCanaryCoverageCitesGridRows(t *testing.T) {
	root := repoRoot(t)
	src, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(canaryGridTestFile)))
	require.NoError(t, err)
	rows := map[string]bool{}
	for _, m := range canaryGridRow.FindAllStringSubmatch(string(src), -1) {
		rows[m[1]] = true
	}
	require.NotEmpty(t, rows, "no grid rows found in %s", canaryGridTestFile)

	m, err := Load("canary")
	require.NoError(t, err)
	cited := 0
	for i := range m.Cells {
		c := &m.Cells[i]
		if c.Coverage == nil {
			continue
		}
		for _, name := range c.Coverage.Unit {
			row, ok := strings.CutPrefix(name, canaryGridFunc+"/")
			if !ok {
				continue
			}
			cited++
			require.Truef(t, rows[row], "%s cites grid row %q, which %s does not declare", cellSelector(c), row, canaryGridTestFile)
		}
	}
	require.NotZero(t, cited, "the canary machine cites no grid row")
}
