// Package runtimeinheritance implements OEP-0012 ServingRuntime
// inheritance resolution.
package runtimeinheritance

import (
	"encoding/json"
	"fmt"

	"k8s.io/apimachinery/pkg/util/strategicpatch"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

// Merge overlays child onto parent (strategic-merge: child wins on
// any field it sets; parent fills the gaps).
func Merge(parent, child *v1beta1.ServingRuntimeSpec) (*v1beta1.ServingRuntimeSpec, error) {
	switch {
	case parent == nil && child == nil:
		return nil, nil
	case parent == nil:
		return child.DeepCopy(), nil
	case child == nil:
		return parent.DeepCopy(), nil
	}

	parentJSON, err := json.Marshal(parent)
	if err != nil {
		return nil, fmt.Errorf("marshal parent: %w", err)
	}
	childJSON, err := json.Marshal(child)
	if err != nil {
		return nil, fmt.Errorf("marshal child: %w", err)
	}
	// Several ServingRuntimeSpec fields (e.g. Containers) lack
	// omitempty, so a Go zero value marshals as `null` and strategic
	// merge would wipe the parent. Strip nulls to recover fill-only-nil.
	childJSON, err = stripJSONNulls(childJSON)
	if err != nil {
		return nil, fmt.Errorf("strip nulls from child: %w", err)
	}

	mergedJSON, err := strategicpatch.StrategicMergePatch(parentJSON, childJSON, v1beta1.ServingRuntimeSpec{})
	if err != nil {
		return nil, fmt.Errorf("strategic merge: %w", err)
	}

	merged := &v1beta1.ServingRuntimeSpec{}
	if err := json.Unmarshal(mergedJSON, merged); err != nil {
		return nil, fmt.Errorf("unmarshal merged: %w", err)
	}
	return merged, nil
}

// stripJSONNulls returns the JSON document with all null-valued object
// keys removed.
func stripJSONNulls(in []byte) ([]byte, error) {
	var v any
	if err := json.Unmarshal(in, &v); err != nil {
		return nil, err
	}
	cleaned := stripNullsValue(v)
	return json.Marshal(cleaned)
}

func stripNullsValue(v any) any {
	switch t := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, val := range t {
			if val == nil {
				continue
			}
			out[k] = stripNullsValue(val)
		}
		return out
	case []any:
		out := make([]any, len(t))
		for i, val := range t {
			out[i] = stripNullsValue(val)
		}
		return out
	default:
		return v
	}
}
