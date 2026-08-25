package quotatree

import (
	"fmt"
	"io"
	"sort"
	"strings"
	"unicode/utf8"
)

// Glyphs are the branch characters the tree is drawn with. Unicode by default;
// the ASCII set exists because a plugin's output lands in logs, tickets, and
// terminals whose encoding nobody controls.
type Glyphs struct {
	Tee      string // a child with siblings below it
	Last     string // the final child
	Vertical string // an ancestor's line continuing past this row
	Blank    string // that ancestor's line already closed
}

// UnicodeGlyphs draws with box-drawing characters.
var UnicodeGlyphs = Glyphs{Tee: "├─ ", Last: "└─ ", Vertical: "│  ", Blank: "   "}

// ASCIIGlyphs draws with characters safe in any encoding.
var ASCIIGlyphs = Glyphs{Tee: "|- ", Last: "`- ", Vertical: "|  ", Blank: "   "}

// RenderOptions controls the text renderer.
type RenderOptions struct {
	// Wide adds the columns that do not fit a default terminal: path,
	// borrowed, effective ceiling, capacity, namespaces, priority tier, and
	// every condition rather than only an unhealthy one.
	Wide bool

	// Glyphs draws the branches.
	Glyphs Glyphs

	// Color selects when to emit ANSI escapes.
	Color ColorMode
}

// unset is what a column shows when a node has no value for it. A dash rather
// than blank: blank reads as zero, and "this Cohort has no budget" and "this
// leaf is budgeted zero" are different states.
const unset = "-"

// Render writes the view as a tree of aligned columns.
//
// Column widths are computed over the unstyled text and the escape wrapped
// around the padded cell, so colored and plain output align identically.
func Render(w io.Writer, v *View, opts RenderOptions) error {
	if opts.Glyphs == (Glyphs{}) {
		opts.Glyphs = UnicodeGlyphs
	}
	p := newPalette(opts.Color, w)

	if v == nil || (v.Root == nil && len(v.Orphans) == 0) {
		_, err := fmt.Fprintln(w, "No AcceleratorQuota resources found.")
		return err
	}

	pairs := budgetPairs(v)
	// The (resource, flavor) columns earn their width only when the tree spans
	// more than one pair. With a single pair they say the same thing on every
	// row, so they move to a caption above the table.
	single := len(pairs) == 1
	layout := newLayout(opts.Wide, single)

	var rows []row
	if v.Root != nil {
		rows = append(rows, renderNode(v.Root, nil, layout, opts.Glyphs)...)
	}
	if len(v.Orphans) > 0 {
		rows = append(rows, layout.sectionRow("nodes with no position in the tree"))
		for i, o := range v.Orphans {
			rows = append(rows, renderNode(o, []bool{i == len(v.Orphans)-1}, layout, opts.Glyphs)...)
		}
	}

	var b strings.Builder
	if single {
		fmt.Fprintf(&b, "%s\n\n", p.apply(styleCaption, pairs[0].caption()))
	}
	writeRows(&b, append([]row{layout.header()}, rows...), p)
	writeFooter(&b, v, p)

	_, err := io.WriteString(w, b.String())
	return err
}

const styleCaption = styleCyan

// pair is one (resource, flavor) key.
type pair struct{ resourceName, flavor string }

func (p pair) caption() string {
	return fmt.Sprintf("%s on flavor %s", p.resourceName, p.flavor)
}

// budgetPairs is every (resource, flavor) key the view mentions, sorted.
func budgetPairs(v *View) []pair {
	seen := map[pair]bool{}
	eachNode(v, func(n *NodeView) {
		for _, b := range n.Budgets {
			seen[pair{b.ResourceName, b.ResourceFlavor}] = true
		}
		for _, c := range n.Capacity {
			seen[pair{c.ResourceName, c.ResourceFlavor}] = true
		}
	})
	out := make([]pair, 0, len(seen))
	for p := range seen {
		out = append(out, p)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].resourceName != out[j].resourceName {
			return out[i].resourceName < out[j].resourceName
		}
		return out[i].flavor < out[j].flavor
	})
	return out
}

// renderNode emits a node and its subtree. ancestorLast[i] reports whether the
// ancestor at depth i was the last of its siblings, which is what decides
// between a continuing line and a blank at that indent level.
//
// append is used on ancestorLast rather than a fresh slice per level, so the
// child slice is copied before recursing: sharing the backing array would let
// one branch overwrite the ancestry another branch is still drawing from.
func renderNode(n *NodeView, ancestorLast []bool, l layout, g Glyphs) []row {
	rows := l.nodeRows(n, treePrefix(ancestorLast, g), contPrefix(ancestorLast, len(n.Children) > 0, g))
	for i, c := range n.Children {
		childAncestry := make([]bool, len(ancestorLast), len(ancestorLast)+1)
		copy(childAncestry, ancestorLast)
		childAncestry = append(childAncestry, i == len(n.Children)-1)
		rows = append(rows, renderNode(c, childAncestry, l, g)...)
	}
	return rows
}

// treePrefix draws the branch art for a node's own row: the ancestors' open
// lines, then the tee or elbow joining this node to its parent.
func treePrefix(ancestorLast []bool, g Glyphs) string {
	if len(ancestorLast) == 0 {
		return ""
	}
	var b strings.Builder
	b.WriteString(stem(ancestorLast[:len(ancestorLast)-1], g))
	if ancestorLast[len(ancestorLast)-1] {
		b.WriteString(g.Last)
	} else {
		b.WriteString(g.Tee)
	}
	return b.String()
}

// contPrefix draws the art for a node's budget continuation rows. They sit
// between the node and its children, so every ancestor line that is still open
// stays open, and the node's own line does too when it has children below.
func contPrefix(ancestorLast []bool, hasChildren bool, g Glyphs) string {
	var b strings.Builder
	b.WriteString(stem(ancestorLast, g))
	if hasChildren {
		b.WriteString(g.Vertical)
	} else {
		b.WriteString(g.Blank)
	}
	return b.String()
}

// stem draws the ancestor lines: still open where that ancestor had siblings
// below it, blank where it was the last of its own.
func stem(ancestorLast []bool, g Glyphs) string {
	var b strings.Builder
	for _, last := range ancestorLast {
		if last {
			b.WriteString(g.Blank)
		} else {
			b.WriteString(g.Vertical)
		}
	}
	return b.String()
}

// cell is one column's content: an optional prefix carrying the branch art,
// then the value. The two are styled separately so the tree lines can recede
// while the name they lead to stands out — but both count toward the column
// width, which is measured on the unstyled runes so an escape never shifts a
// column.
type cell struct {
	prefix      string
	prefixStyle style
	text        string
	style       style
}

func (c cell) width() int {
	return utf8.RuneCountInString(c.prefix) + utf8.RuneCountInString(c.text)
}

// row is one output line. A row with span set is a full-width section heading
// rather than a set of columns.
type row struct {
	cells []cell
	span  string

	// style is the fallback for cells that declare none.
	style style

	// dim overrides every cell's own style. A row kept only as filter context
	// recedes as a whole; letting a green Ready survive inside it would make
	// the context read as an answer.
	dim bool
}

func writeRows(b *strings.Builder, rows []row, p palette) {
	widths := map[int]int{}
	for _, r := range rows {
		if r.span != "" {
			continue
		}
		for i, c := range r.cells {
			if n := c.width(); n > widths[i] {
				widths[i] = n
			}
		}
	}

	for _, r := range rows {
		if r.span != "" {
			fmt.Fprintf(b, "\n%s\n", p.apply(r.style, r.span))
			continue
		}
		var line strings.Builder
		for i, c := range r.cells {
			prefixStyle, textStyle := c.prefixStyle, c.style
			if textStyle == styleNone {
				textStyle = r.style
			}
			if r.dim {
				prefixStyle, textStyle = styleDim, styleDim
			}
			line.WriteString(p.apply(prefixStyle, c.prefix))
			line.WriteString(p.apply(textStyle, c.text))
			// Padding stays outside the escape: styling the gap between
			// columns would extend a background or underline across it.
			if i < len(r.cells)-1 {
				line.WriteString(strings.Repeat(" ", widths[i]-c.width()))
				line.WriteString("  ")
			}
		}
		fmt.Fprintln(b, strings.TrimRight(line.String(), " "))
	}
}

// writeFooter explains the marks the table used, and only those. A legend for a
// symbol that did not appear is noise.
func writeFooter(b *strings.Builder, v *View, p palette) {
	fromSpec := false
	eachNode(v, func(n *NodeView) {
		for _, bg := range n.Budgets {
			fromSpec = fromSpec || bg.FromSpec
		}
	})
	if fromSpec {
		fmt.Fprintf(b, "\n%s\n", p.apply(styleDim,
			"* nominal read from spec; status.budgets carries no resolved value for it"))
	}

	if len(v.Violations) == 0 {
		return
	}

	// A violation about a node the current filter hid is still worth knowing
	// about, but spelling it out would describe rows that are not on screen.
	// Report the visible ones in full and count the rest.
	visible := visibleNodes(v)
	var shown []ViolationView
	elsewhere := 0
	for _, vi := range v.Violations {
		if vi.Node == "" || visible[vi.Node] {
			shown = append(shown, vi)
			continue
		}
		elsewhere++
	}

	if len(shown) > 0 {
		fmt.Fprintf(b, "\n%s\n", p.apply(styleRed,
			fmt.Sprintf("%s violated:", plural(len(shown), "invariant"))))
		for _, vi := range shown {
			node := vi.Node
			if node == "" {
				node = "(tree)"
			}
			fmt.Fprintf(b, "  %s %s %s\n",
				p.apply(styleRed, "!"),
				p.apply(styleBold, node+":"),
				vi.Message)
			fmt.Fprintf(b, "    %s\n", p.apply(styleDim, "reason: "+vi.Reason))
		}
	}
	if elsewhere > 0 {
		if len(shown) == 0 {
			b.WriteString("\n")
		}
		fmt.Fprintf(b, "%s\n", p.apply(styleYellow, fmt.Sprintf(
			"%s violated outside this view; re-run without filters to see them",
			plural(elsewhere, "further invariant"))))
	}
}

func plural(n int, noun string) string {
	if n == 1 {
		return fmt.Sprintf("1 %s", noun)
	}
	return fmt.Sprintf("%d %ss", n, noun)
}
