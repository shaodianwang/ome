package quotatree

import (
	"fmt"
	"io"
	"os"
)

// ColorMode selects when to emit ANSI escapes.
type ColorMode string

const (
	// ColorAuto emits escapes only when the destination is a terminal and the
	// environment has not asked for plain output.
	ColorAuto ColorMode = "auto"
	// ColorAlways emits escapes unconditionally, for piping into a pager that
	// understands them.
	ColorAlways ColorMode = "always"
	// ColorNever emits none.
	ColorNever ColorMode = "never"
)

// ParseColorMode validates a --color value.
func ParseColorMode(s string) (ColorMode, error) {
	switch ColorMode(s) {
	case ColorAuto, ColorAlways, ColorNever:
		return ColorMode(s), nil
	default:
		return "", fmt.Errorf("invalid --color %q: want auto, always, or never", s)
	}
}

// style is one SGR parameter set, empty when styling is off.
type style string

const (
	styleNone   style = ""
	styleDim    style = "2"
	styleBold   style = "1"
	styleRed    style = "31"
	styleYellow style = "33"
	styleGreen  style = "32"
	styleCyan   style = "36"
)

// palette applies styles, or does not. Rendering always goes through it so the
// plain and colored paths cannot drift: width is computed on the unstyled text
// and the escape is wrapped around the padded cell.
type palette struct{ on bool }

// newPalette resolves the mode against the destination and the environment.
//
// NO_COLOR is honored for auto because a plugin inherits whatever pipeline the
// operator built; TERM=dumb is honored because emitting escapes into a terminal
// that cannot render them is worse than plain text. The terminal test is the
// character-device check rather than an ioctl, which keeps the plugin free of a
// terminal dependency and is accurate enough for a display default an operator
// can override.
func newPalette(mode ColorMode, out io.Writer) palette {
	switch mode {
	case ColorAlways:
		return palette{on: true}
	case ColorNever:
		return palette{on: false}
	}
	if _, set := os.LookupEnv("NO_COLOR"); set {
		return palette{on: false}
	}
	if term := os.Getenv("TERM"); term == "" || term == "dumb" {
		return palette{on: false}
	}
	f, ok := out.(*os.File)
	if !ok {
		return palette{on: false}
	}
	st, err := f.Stat()
	return palette{on: err == nil && st.Mode()&os.ModeCharDevice != 0}
}

func (p palette) apply(s style, text string) string {
	if !p.on || s == styleNone || text == "" {
		return text
	}
	return "\x1b[" + string(s) + "m" + text + "\x1b[0m"
}
