package rolloutspec

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestAllMachinesWellFormed is the build gate for every embedded machine:
// a missing cell, a dangling reference, a moved deciding file, or a DECIDE
// in a frozen table fails here.
func TestAllMachinesWellFormed(t *testing.T) {
	root, err := FindRepoRoot()
	require.NoError(t, err)
	machines, err := LoadAll()
	require.NoError(t, err)
	require.NotEmpty(t, machines)
	for _, m := range machines {
		t.Run(m.Machine, func(t *testing.T) {
			for _, violation := range Validate(m, root) {
				t.Error(violation)
			}
		})
	}
}
