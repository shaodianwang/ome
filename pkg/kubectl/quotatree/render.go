package quotatree

import (
	"fmt"
	"io"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"k8s.io/apimachinery/pkg/api/resource"
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

	// Split draws each budget's per-cluster breakdown as a tier of rows below
	// the node. Only the plane that authors a tree resolves a split, so on a
	// workload cluster there is none to draw and this changes nothing.
	Split bool

	// Color selects when to emit ANSI escapes.
	Color ColorMode

	// Now is what the AGE column is measured against. Zero means the wall
	// clock, which is what a caller wants; it is a field so a test can render a
	// fixture's ages without the output changing between two runs.
	Now time.Time
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
	if opts.Now.IsZero() {
		opts.Now = time.Now()
	}
	p := newPalette(opts.Color, w)

	if v == nil || (v.Root == nil && len(v.Clusters) == 0 && len(v.Orphans) == 0) {
		_, err := fmt.Fprintln(w, "No AcceleratorQuota resources found.")
		return err
	}

	pairs := budgetPairs(v)
	// The (resource, flavor) columns earn their width only when the tree spans
	// more than one pair. With a single pair they say the same thing on every
	// row, so they move to a caption above the table.
	single := len(pairs) == 1
	layout := newLayout(opts.Wide, single, opts.Split, opts.Now)

	var rows []row
	if v.Root != nil {
		rows = append(rows, renderNode(v.Root, nil, layout, opts.Glyphs)...)
	}
	for _, c := range v.Clusters {
		rows = append(rows, renderCluster(c, layout, opts.Glyphs)...)
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

// branches is the tree drawing for one node: the art joining it to its parent,
// and the art for the rows that sit at the tier below it — its budget
// continuation rows and its per-cluster rows.
type branches struct {
	// node joins this node to its parent.
	node string

	// stem is the ancestors' open lines, the indent every row at the tier
	// below this node starts from.
	stem   string
	glyphs Glyphs

	// children reports that the node has children of its own, which follow its
	// per-cluster rows at the same indent and so keep that line open past them.
	children bool
}

// cont draws a budget continuation row, which sits at the tier below the node
// and so keeps that line open whenever anything is still to come there.
func (b branches) cont(more bool) string {
	if more || b.children {
		return b.stem + b.glyphs.Vertical
	}
	return b.stem + b.glyphs.Blank
}

// tier draws a per-cluster row. Every budget's shares belong to one sibling
// list below the node, so the line closes on the last of them across all
// budgets, and only when no child follows.
func (b branches) tier(last bool) string {
	if last && !b.children {
		return b.stem + b.glyphs.Last
	}
	return b.stem + b.glyphs.Tee
}

// renderCluster emits one workload cluster and the slice of the tree it
// carries. The cluster is the top of its own tree, so it is drawn from no
// ancestry at all and its nodes hang off it as an ordinary first tier.
func renderCluster(c *ClusterView, l layout, g Glyphs) []row {
	rows := l.clusterRows(c)
	for i, n := range c.Nodes {
		rows = append(rows, renderNode(n, []bool{i == len(c.Nodes)-1}, l, g)...)
	}
	return rows
}

// renderNode emits a node and its subtree. ancestorLast[i] reports whether the
// ancestor at depth i was the last of its siblings, which is what decides
// between a continuing line and a blank at that indent level.
//
// append is used on ancestorLast rather than a fresh slice per level, so the
// child slice is copied before recursing: sharing the backing array would let
// one branch overwrite the ancestry another branch is still drawing from.
func renderNode(n *NodeView, ancestorLast []bool, l layout, g Glyphs) []row {
	rows := l.nodeRows(n, branches{
		node:     treePrefix(ancestorLast, g),
		stem:     stem(ancestorLast, g),
		glyphs:   g,
		children: len(n.Children) > 0,
	})
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
	// Said once rather than marked per row: on this axis nearly every row above
	// a leaf is a total, and a mark that lands on most of the table says
	// nothing. Only a ClusterQueue row is a figure the controller published.
	if len(v.Clusters) > 0 {
		fmt.Fprintf(b, "\n%s\n", p.apply(styleDim,
			"Cohort and WorkloadCluster rows total the leaf shares below them; "+
				"only a ClusterQueue row is a resolved budget"))
	}
	if fromSpec {
		fmt.Fprintf(b, "\n%s\n", p.apply(styleDim,
			"* nominal read from spec; status.budgets carries no resolved value for it"))
	}
	writeShortSplits(b, v, p)

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

// writeShortSplits reports the budgets whose per-cluster shares do not add up
// to the allowance they divide.
//
// No invariant covers this, so nothing else in the view says it: a node reports
// the total an admin authored whether or not the fleet was handed all of it,
// and the difference is capacity no cluster can admit against. Reported even
// when the tier itself is collapsed, because the gap is there either way.
func writeShortSplits(b *strings.Builder, v *View, p palette) {
	type short struct {
		node   string
		budget BudgetView
		total  resource.Quantity
	}
	var found []short
	eachNode(v, func(n *NodeView) {
		for _, bg := range n.Budgets {
			if total, whole := splitTotal(bg); !whole {
				found = append(found, short{node: n.Name, budget: bg, total: total})
			}
		}
	})
	if len(found) == 0 {
		return
	}

	// Neither "short" nor "over": the arithmetic can miss in both directions,
	// and the per-node lines below say which way each one went.
	heading := "1 budget split does not add up:"
	if len(found) > 1 {
		heading = fmt.Sprintf("%d budget splits do not add up:", len(found))
	}
	fmt.Fprintf(b, "\n%s\n", p.apply(styleRed, heading))
	for _, s := range found {
		fmt.Fprintf(b, "  %s %s %s on %s is %s but its per-cluster shares total %s\n",
			p.apply(styleRed, "!"),
			p.apply(styleBold, s.node+":"),
			s.budget.ResourceName, s.budget.ResourceFlavor,
			s.budget.Nominal.String(), s.total.String())
	}
}

func plural(n int, noun string) string {
	if n == 1 {
		return fmt.Sprintf("1 %s", noun)
	}
	return fmt.Sprintf("%d %ss", n, noun)
}
