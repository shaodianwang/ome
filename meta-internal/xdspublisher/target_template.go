//go:build meta_internal

package xdspublisher

import (
	"fmt"
	"strings"
	"text/template"
)

var targetTemplateFuncs = template.FuncMap{
	"replaceAll":     templateReplaceAll,
	"mustTrimPrefix": templateMustTrimPrefix,
	"mustTrimSuffix": templateMustTrimSuffix,
}

func templateReplaceAll(old, replacement, value string) (string, error) {
	if old == "" {
		return "", fmt.Errorf("replaceAll old value must not be empty")
	}
	return strings.ReplaceAll(value, old, replacement), nil
}

func templateMustTrimPrefix(prefix, value string) (string, error) {
	if prefix == "" {
		return "", fmt.Errorf("mustTrimPrefix prefix must not be empty")
	}
	trimmed, found := strings.CutPrefix(value, prefix)
	if !found || trimmed == "" {
		return "", fmt.Errorf("%q must start with non-empty prefix %q and contain a value after it", value, prefix)
	}
	return trimmed, nil
}

func templateMustTrimSuffix(suffix, value string) (string, error) {
	if suffix == "" {
		return "", fmt.Errorf("mustTrimSuffix suffix must not be empty")
	}
	trimmed, found := strings.CutSuffix(value, suffix)
	if !found || trimmed == "" {
		return "", fmt.Errorf("%q must end with non-empty suffix %q and contain a value before it", value, suffix)
	}
	return trimmed, nil
}
