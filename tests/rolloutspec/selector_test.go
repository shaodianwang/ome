package rolloutspec

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSelects(t *testing.T) {
	before := GridCell{State: "Idle", Event: "pod.ready", Variant: "source", Window: WindowBefore}
	boundary := GridCell{State: "Idle", Event: "pod.ready", Variant: "source", Window: WindowBoundary, Transition: "idle-to-working"}
	other := GridCell{State: "Idle", Event: "pod.ready", Variant: "target", Window: WindowBoundary, Transition: "other-transition"}

	assert.True(t, Selects(nil, nil, "", before), "empty selector matches any key")
	assert.True(t, Selects(nil, nil, "", boundary), "empty selector matches any key")

	assert.False(t, Selects(nil, nil, "idle-to-working", before), "transition-scoped selector never matches a before key")
	assert.True(t, Selects(nil, nil, "idle-to-working", boundary))
	assert.False(t, Selects(nil, nil, "idle-to-working", other), "transition-scoped selector requires the transition to match")

	assert.True(t, Selects([]string{"source"}, nil, "", before), "variant filter matches")
	assert.False(t, Selects([]string{"target"}, nil, "", before), "variant filter excludes a non-matching variant")

	assert.True(t, Selects(nil, []string{WindowBefore}, "", before), "window filter matches")
	assert.False(t, Selects(nil, []string{WindowBoundary}, "", before), "window filter excludes a non-matching window")
}
