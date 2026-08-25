package quotatree

import (
	"encoding/json"
	"fmt"
	"io"

	"sigs.k8s.io/yaml"
)

// OutputFormat selects how a View is written.
type OutputFormat string

const (
	// OutputTree is the default: an aligned tree with the budget and usage
	// columns that fit a normal terminal.
	OutputTree OutputFormat = "tree"
	// OutputWide is the tree with every column.
	OutputWide OutputFormat = "wide"
	// OutputJSON is the view as nested JSON.
	OutputJSON OutputFormat = "json"
	// OutputYAML is the view as YAML.
	OutputYAML OutputFormat = "yaml"
)

// ParseOutputFormat validates an -o value.
func ParseOutputFormat(s string) (OutputFormat, error) {
	switch OutputFormat(s) {
	case OutputTree, OutputWide, OutputJSON, OutputYAML:
		return OutputFormat(s), nil
	default:
		return "", fmt.Errorf("invalid -o %q: want tree, wide, json, or yaml", s)
	}
}

// Print writes v in the requested format.
//
// The machine formats emit the same view the tree renders, nested rather than
// flattened, so a script and an operator are reading one structure. Notably
// they keep the orphan list and the violations: a consumer that only reads
// .root would otherwise conclude a broken tree was healthy.
func Print(w io.Writer, v *View, format OutputFormat, opts RenderOptions) error {
	switch format {
	case OutputJSON:
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		return enc.Encode(emptyToZero(v))
	case OutputYAML:
		out, err := yaml.Marshal(emptyToZero(v))
		if err != nil {
			return fmt.Errorf("marshal quota tree to yaml: %w", err)
		}
		_, err = w.Write(out)
		return err
	case OutputWide:
		opts.Wide = true
		return Render(w, v, opts)
	case OutputTree, "":
		opts.Wide = false
		return Render(w, v, opts)
	default:
		return fmt.Errorf("invalid output format %q", format)
	}
}

// emptyToZero normalizes a nil view so the machine formats emit an object
// rather than "null". A caller polling for nodes must be able to tell "no
// AcceleratorQuotas exist" from "the request failed", and null reads as neither.
func emptyToZero(v *View) *View {
	if v == nil {
		return &View{}
	}
	return v
}
