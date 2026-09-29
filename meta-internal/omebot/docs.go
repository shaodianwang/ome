//go:build meta_internal

package omebot

import (
	"bufio"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
)

// docSuffixes are the file types the docs tool searches and reads.
var docSuffixes = []string{".md", ".yaml", ".yml"}

// Docs serves the built-in docs tool over read-only directory trees baked
// into the image. Every path is resolved and must stay inside its root.
type Docs struct {
	Config DocsConfig
	// Limit caps the output of one search.
	Limit int
}

// Run executes `docs search <query> [--root R] [--regexp] [--max N]` or
// `docs read <root>/<path> [--from L] [--lines N]`.
func (d *Docs) Run(args []string) (ToolResult, string) {
	if len(args) == 0 {
		return docsRefusal(ErrSubcommandNotAllowed, "docs needs one of: search, read")
	}
	switch args[0] {
	case "search":
		return d.search(args[1:])
	case "read":
		return d.read(args[1:])
	}
	return docsRefusal(ErrSubcommandNotAllowed, "docs needs one of: search, read")
}

func docsRefusal(class, format string, a ...any) (ToolResult, string) {
	return ToolResult{Tool: DocsToolName, ExitCode: -1, Error: fmt.Sprintf(format, a...)}, class
}

// docFlags splits args into positionals and the given value/bool flags.
func docFlags(args []string, valued, boolean []string) (pos []string, vals map[string]string, bools map[string]bool, err error) {
	vals, bools = map[string]string{}, map[string]bool{}
	for i := 0; i < len(args); i++ {
		a := args[i]
		if !strings.HasPrefix(a, "--") {
			pos = append(pos, a)
			continue
		}
		name, val, hasVal := strings.Cut(a, "=")
		switch {
		case contains(boolean, name) && !hasVal:
			bools[name] = true
		case contains(valued, name):
			if !hasVal {
				if i+1 >= len(args) {
					return nil, nil, nil, fmt.Errorf("%s needs a value", name)
				}
				i++
				val = args[i]
			}
			vals[name] = val
		default:
			return nil, nil, nil, fmt.Errorf("unknown flag %s", name)
		}
	}
	return pos, vals, bools, nil
}

func contains(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

func positiveInt(vals map[string]string, name string) (int, bool, error) {
	v, ok := vals[name]
	if !ok {
		return 0, false, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		return 0, false, fmt.Errorf("%s must be a positive integer", name)
	}
	return n, true, nil
}

func isDoc(path string) bool {
	for _, s := range docSuffixes {
		if strings.HasSuffix(path, s) {
			return true
		}
	}
	return false
}

func (d *Docs) root(name string) (DocsRoot, bool) {
	for _, r := range d.Config.Roots {
		if r.Name == name {
			return r, true
		}
	}
	return DocsRoot{}, false
}

func (d *Docs) search(args []string) (ToolResult, string) {
	pos, vals, bools, err := docFlags(args, []string{"--root", "--max"}, []string{"--regexp"})
	if err != nil {
		return docsRefusal(ErrBadArgs, "%v", err)
	}
	query := strings.Join(pos, " ")
	if strings.TrimSpace(query) == "" {
		return docsRefusal(ErrBadArgs, "docs search needs a query")
	}
	limit := d.Config.MaxMatches
	if n, ok, err := positiveInt(vals, "--max"); err != nil {
		return docsRefusal(ErrBadArgs, "%v", err)
	} else if ok && n < limit {
		limit = n
	}
	roots := d.Config.Roots
	if name, ok := vals["--root"]; ok {
		r, found := d.root(name)
		if !found {
			return docsRefusal(ErrBadArgs, "unknown docs root %q", name)
		}
		roots = []DocsRoot{r}
	}
	match := func(line string) bool { return strings.Contains(strings.ToLower(line), strings.ToLower(query)) }
	if bools["--regexp"] {
		re, err := regexp.Compile("(?i)" + query)
		if err != nil {
			return docsRefusal(ErrBadArgs, "bad regexp: %v", err)
		}
		match = re.MatchString
	}

	out := &cappedBuffer{limit: d.Limit}
	shown, total := 0, 0
	for _, r := range roots {
		base, err := filepath.EvalSymlinks(r.Path)
		if err != nil {
			continue
		}
		_ = filepath.WalkDir(base, func(path string, e fs.DirEntry, err error) error {
			// Symlinks are skipped so a search cannot leave the root.
			if err != nil || !e.Type().IsRegular() || !isDoc(path) {
				return nil
			}
			rel, _ := filepath.Rel(base, path)
			f, err := os.Open(path)
			if err != nil {
				return nil
			}
			defer f.Close()
			sc := bufio.NewScanner(f)
			for n := 1; sc.Scan(); n++ {
				if !match(sc.Text()) {
					continue
				}
				total++
				if shown < limit {
					shown++
					fmt.Fprintf(out, "%s/%s:%d: %s\n", r.Name, filepath.ToSlash(rel), n, strings.TrimSpace(sc.Text()))
				}
			}
			return nil
		})
	}
	if total == 0 {
		return ToolResult{Tool: DocsToolName, ExitCode: 1, Output: "no matches\n"}, ""
	}
	if total > shown {
		fmt.Fprintf(out, "… %d more matches not shown; narrow the query or pass --root\n", total-shown)
	}
	return ToolResult{Tool: DocsToolName, Output: out.String(), Truncated: out.truncated}, ""
}

// resolve maps <root>/<relpath> to a regular doc file inside that root.
func (d *Docs) resolve(ref string) (string, error) {
	name, rel, ok := strings.Cut(ref, "/")
	r, found := d.root(name)
	if !ok || !found {
		return "", fmt.Errorf("path must be <root>/<file>; roots: %s", strings.Join(d.rootNames(), ", "))
	}
	if rel == "" || filepath.IsAbs(rel) || !filepath.IsLocal(rel) {
		return "", errors.New("path must stay inside its root")
	}
	base, err := filepath.EvalSymlinks(r.Path)
	if err != nil {
		return "", fmt.Errorf("docs root %s unavailable", name)
	}
	full, err := filepath.EvalSymlinks(filepath.Join(base, rel))
	if err != nil {
		return "", fmt.Errorf("%s not found", ref)
	}
	if !strings.HasPrefix(full, base+string(filepath.Separator)) {
		return "", errors.New("path must stay inside its root")
	}
	st, err := os.Stat(full)
	if err != nil || !st.Mode().IsRegular() || !isDoc(full) {
		return "", fmt.Errorf("%s is not a readable doc (%s)", ref, strings.Join(docSuffixes, ", "))
	}
	return full, nil
}

func (d *Docs) rootNames() []string {
	names := make([]string, 0, len(d.Config.Roots))
	for _, r := range d.Config.Roots {
		names = append(names, r.Name)
	}
	return names
}

func (d *Docs) read(args []string) (ToolResult, string) {
	pos, vals, _, err := docFlags(args, []string{"--from", "--lines"}, nil)
	if err != nil {
		return docsRefusal(ErrBadArgs, "%v", err)
	}
	if len(pos) != 1 {
		return docsRefusal(ErrBadArgs, "docs read needs exactly one <root>/<file>")
	}
	from, hasFrom, err := positiveInt(vals, "--from")
	if err != nil {
		return docsRefusal(ErrBadArgs, "%v", err)
	}
	if !hasFrom {
		from = 1
	}
	lines, hasLines, err := positiveInt(vals, "--lines")
	if err != nil {
		return docsRefusal(ErrBadArgs, "%v", err)
	}
	path, err := d.resolve(pos[0])
	if err != nil {
		return docsRefusal(ErrBadArgs, "%v", err)
	}
	f, err := os.Open(path)
	if err != nil {
		return docsRefusal(ErrBadArgs, "%s not readable", pos[0])
	}
	defer f.Close()

	var b strings.Builder
	sc := bufio.NewScanner(f)
	n, last, truncated := 0, 0, false
	for sc.Scan() {
		n++
		if n < from || hasLines && n >= from+lines {
			continue
		}
		line := sc.Text() + "\n"
		if b.Len()+len(line) > d.Config.MaxReadBytes {
			truncated = true
			break
		}
		b.WriteString(line)
		last = n
	}
	if truncated {
		fmt.Fprintf(&b, "… truncated after line %d; continue with --from %d\n", last, last+1)
	}
	return ToolResult{Tool: DocsToolName, Output: b.String(), Truncated: truncated}, ""
}
