package rolloutspec

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
)

// FindRepoRoot returns the nearest ancestor of this source file that
// contains a go.mod. Validate resolves decided_in paths against it so a
// moved deciding function breaks the build instead of silently detaching
// the table from the code.
//
// FindRepoRoot is usable only from tests and tooling: it relies on
// runtime.Caller returning an absolute source path, which does not hold
// in a -trimpath build.
func FindRepoRoot() (string, error) {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		return "", errors.New("rolloutspec: cannot locate package source file")
	}
	if !filepath.IsAbs(file) {
		return "", fmt.Errorf("rolloutspec: caller returned a non-absolute path %q; FindRepoRoot is usable only from tests and tooling", file)
	}
	dir := filepath.Dir(file)
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("rolloutspec: no go.mod above %s", filepath.Dir(file))
		}
		dir = parent
	}
}
