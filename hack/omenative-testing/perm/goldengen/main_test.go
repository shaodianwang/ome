package main

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

// The committed golden file must be what the current codec and fixtures
// produce. A codec or fixture change without a regeneration would leave the
// harness's Python decoder tested against a stale contract.
func TestCommittedGoldenIsCurrent(t *testing.T) {
	want, err := render()
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	path := filepath.Join("..", defaultOutput)
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("%s is stale: run `go run ./goldengen` from hack/omenative-testing/perm and commit the result", path)
	}
}
