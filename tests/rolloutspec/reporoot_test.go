package rolloutspec

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFindRepoRoot(t *testing.T) {
	root, err := FindRepoRoot()
	require.NoError(t, err)
	_, err = os.Stat(filepath.Join(root, "go.mod"))
	require.NoError(t, err, "root must contain go.mod")
	_, err = os.Stat(filepath.Join(root, "tests", "rolloutspec", "doc.go"))
	require.NoError(t, err, "root must be the repository root, not a nested module")
}
