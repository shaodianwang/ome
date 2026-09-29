//go:build meta_internal

package omebot

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func testDocs(t *testing.T) *Docs {
	t.Helper()
	base := t.TempDir()
	docs, oeps, outside := filepath.Join(base, "docs"), filepath.Join(base, "oeps"), filepath.Join(base, "outside")
	writeFile(t, filepath.Join(docs, "guide.md"), "# Guide\nRatioBalanced keeps ratios.\nplain line\n")
	writeFile(t, filepath.Join(docs, "sub", "values.yaml"), "strategy: ratiobalanced\n")
	writeFile(t, filepath.Join(docs, "code.go"), "// RatioBalanced in code is not docs\n")
	var many strings.Builder
	for i := 0; i < 30; i++ {
		fmt.Fprintf(&many, "match line %d\n", i)
	}
	writeFile(t, filepath.Join(oeps, "many.md"), many.String())
	writeFile(t, filepath.Join(outside, "secret.md"), "RatioBalanced secret\n")
	if err := os.Symlink(filepath.Join(outside, "secret.md"), filepath.Join(docs, "escape.md")); err != nil {
		t.Fatal(err)
	}
	return &Docs{Config: DocsConfig{
		Roots:        []DocsRoot{{Name: "docs", Path: docs}, {Name: "oeps", Path: oeps}},
		MaxMatches:   10,
		MaxReadBytes: 64,
	}, Limit: 4096}
}

func TestDocsSearch(t *testing.T) {
	d := testDocs(t)
	res, class := d.Run([]string{"search", "ratiobalanced"})
	if class != "" || res.ExitCode != 0 {
		t.Fatalf("class %q res %+v", class, res)
	}
	for _, want := range []string{"docs/guide.md:2: RatioBalanced keeps ratios.", "docs/sub/values.yaml:1: strategy: ratiobalanced"} {
		if !strings.Contains(res.Output, want) {
			t.Errorf("output lacks %q:\n%s", want, res.Output)
		}
	}
	for _, not := range []string{"code.go", "secret", "escape.md"} {
		if strings.Contains(res.Output, not) {
			t.Errorf("output contains %q:\n%s", not, res.Output)
		}
	}

	res, _ = d.Run([]string{"search", "match", "line", "--root", "oeps", "--max", "3"})
	if got := strings.Count(res.Output, "oeps/many.md:"); got != 3 || !strings.Contains(res.Output, "27 more matches not shown") {
		t.Errorf("--max 3: %d matches:\n%s", got, res.Output)
	}
	res, _ = d.Run([]string{"search", "match", "--max", "100"})
	if got := strings.Count(res.Output, "oeps/many.md:"); got != 10 {
		t.Errorf("--max above maxMatches: %d matches", got)
	}
	res, _ = d.Run([]string{"search", "--regexp", `^match line 2\d$`})
	if got := strings.Count(res.Output, "oeps/many.md:"); got != 10 {
		t.Errorf("regexp: %d matches:\n%s", got, res.Output)
	}
	if res, _ := d.Run([]string{"search", "nothing-matches-this"}); res.ExitCode != 1 {
		t.Errorf("no match should exit 1: %+v", res)
	}
}

func TestDocsRead(t *testing.T) {
	d := testDocs(t)
	res, class := d.Run([]string{"read", "docs/guide.md", "--from", "2", "--lines", "1"})
	if class != "" || res.ExitCode != 0 || !strings.Contains(res.Output, "RatioBalanced keeps ratios.") || strings.Contains(res.Output, "# Guide") {
		t.Fatalf("class %q res %+v", class, res)
	}
	res, _ = d.Run([]string{"read", "oeps/many.md"})
	if !res.Truncated || !strings.Contains(res.Output, "--from") {
		t.Fatalf("maxReadBytes not applied: %+v", res)
	}
}

func TestDocsRefusals(t *testing.T) {
	d := testDocs(t)
	for name, args := range map[string][]string{
		"no subcommand":   nil,
		"bad subcommand":  {"write", "docs/x.md"},
		"empty query":     {"search"},
		"unknown root":    {"search", "x", "--root", "nope"},
		"bad max":         {"search", "x", "--max", "zero"},
		"bad regexp":      {"search", "--regexp", "("},
		"dotdot":          {"read", "docs/../outside/secret.md"},
		"absolute":        {"read", "/etc/passwd"},
		"read unknown":    {"read", "nope/guide.md"},
		"symlink escape":  {"read", "docs/escape.md"},
		"directory":       {"read", "docs/sub"},
		"missing file":    {"read", "docs/none.md"},
		"bad from":        {"read", "docs/guide.md", "--from", "0"},
		"unknown flag":    {"read", "docs/guide.md", "--all"},
		"two paths":       {"read", "docs/guide.md", "docs/sub/values.yaml"},
		"non docs suffix": {"read", "docs/code.go"},
	} {
		wantClass := ErrBadArgs
		if name == "no subcommand" || name == "bad subcommand" {
			wantClass = ErrSubcommandNotAllowed
		}
		if res, class := d.Run(args); class != wantClass {
			t.Errorf("%s: class %q, want %q (%+v)", name, class, wantClass, res)
		}
	}
}
