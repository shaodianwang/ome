package rolloutspec

import "fmt"

// Selects reports whether an authored cell's selector (variants, windows,
// transition) matches one grid key. An empty list, or an empty transition,
// matches every value; a transition-scoped selector never matches a
// before-window key, since a before key's transition is always empty.
func Selects(variants, windows []string, transition string, key GridCell) bool {
	if transition != "" && (key.Window == WindowBefore || key.Transition != transition) {
		return false
	}
	return selectsValue(variants, key.Variant) && selectsValue(windows, key.Window)
}

// selectsValue treats an empty selector list as "all".
func selectsValue(list []string, v string) bool {
	if len(list) == 0 {
		return true
	}
	for _, x := range list {
		if x == v {
			return true
		}
	}
	return false
}

// selectorOf renders an authored cell's selector fields (windows, variants,
// transition) in the one form every message that names a cell uses, so a
// collision reported by Expand and a violation reported by Validate read
// identically.
func selectorOf(c *Cell) string {
	return fmt.Sprintf("windows=%v variants=%v transition=%s", c.Windows, c.Variants, c.Transition)
}

// cellSelector identifies an authored cell by its state/event coordinate and
// its windows/variants/transition selector, so a violation naming it is
// locatable across fragment files.
func cellSelector(c *Cell) string {
	return fmt.Sprintf("cell %s/%s %s", c.State, c.Event, selectorOf(c))
}
