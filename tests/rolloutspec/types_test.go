package rolloutspec

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestVocabularies(t *testing.T) {
	assert.Equal(t, []string{WindowBefore, WindowBoundary, WindowAfter}, Windows)
	for _, o := range []string{"transition", "ignore", "defer", "reject", "escalate", "n/a", "DECIDE"} {
		assert.Contains(t, outcomes, o)
	}
	assert.NotContains(t, outcomes, "maybe")
	for _, s := range []string{"current", "review", "decided"} {
		assert.Contains(t, statuses, s)
	}
	for _, p := range []string{"unit", "perm", "kind", "sim"} {
		assert.Contains(t, placements, p)
	}
	assert.Len(t, outcomes, 7)
	assert.Len(t, Windows, 3)
	assert.Len(t, statuses, 3)
	assert.Len(t, placements, 4)
}
