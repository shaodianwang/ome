// Command rolloutspec-grid renders one rollout state machine for review:
// the expanded (state x event x window) grid as JSON, the transitions as a
// Mermaid state diagram, or a narrative markdown document.
package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/ome-projects/ome/tests/rolloutspec"
)

// historyFlag collects a repeatable --history flag into an ordered list of
// sidecar file paths.
type historyFlag []string

func (h *historyFlag) String() string { return strings.Join(*h, ",") }

func (h *historyFlag) Set(v string) error {
	*h = append(*h, v)
	return nil
}

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "rolloutspec-grid:", err)
		os.Exit(1)
	}
}

// run parses flags, renders the requested machine into memory, and only
// then opens --out: a failed load or render must never leave a truncated or
// empty file behind.
func run(args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("rolloutspec-grid", flag.ContinueOnError)
	machine := fs.String("machine", "", "machine name to render (required)")
	format := fs.String("format", "json", "output format: json, markdown, or mermaid")
	out := fs.String("out", "", "output path (default stdout)")
	var history historyFlag
	fs.Var(&history, "history", "path to a history sidecar YAML (repeatable)")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if *machine == "" {
		return fmt.Errorf("--machine is required")
	}
	if *format != "json" && *format != "mermaid" && *format != "markdown" {
		return fmt.Errorf("unknown --format %q (want json, markdown, or mermaid)", *format)
	}

	m, err := rolloutspec.Load(*machine)
	if err != nil {
		return fmt.Errorf("load machine %q: %w", *machine, err)
	}

	var buf bytes.Buffer
	switch *format {
	case "json":
		err = emitJSONWithHistory(&buf, m, history)
	case "mermaid":
		err = emitMermaid(&buf, m)
	case "markdown":
		err = emitMarkdown(&buf, m)
	}
	if err != nil {
		return err
	}

	if *out == "" {
		_, err := stdout.Write(buf.Bytes())
		return err
	}
	f, err := os.Create(*out)
	if err != nil {
		return err
	}
	if _, err := f.Write(buf.Bytes()); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}
