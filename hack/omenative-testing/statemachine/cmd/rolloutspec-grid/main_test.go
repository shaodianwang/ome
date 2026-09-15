package main

import (
	"bytes"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRunHelpReturnsNilAfterUsage(t *testing.T) {
	var buf bytes.Buffer
	err := run([]string{"--help"}, &buf)
	require.NoError(t, err)
}

func TestRunRejectsUnknownFormatBeforeLoadingMachine(t *testing.T) {
	var buf bytes.Buffer
	// _fixture exists, so an error here can only come from format validation,
	// not a failed machine load.
	err := run([]string{"--machine", "_fixture", "--format", "yaml"}, &buf)
	require.Error(t, err)
	require.Contains(t, err.Error(), `unknown --format "yaml"`)
}

func TestRunDoesNotCreateOutputFileOnFailure(t *testing.T) {
	out := filepath.Join(t.TempDir(), "should-not-exist.json")
	var buf bytes.Buffer
	err := run([]string{"--machine", "does-not-exist", "--format", "json", "--out", out}, &buf)
	require.Error(t, err)
	require.NoFileExists(t, out)
}
