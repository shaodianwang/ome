package rolloutspec

import (
	"io/fs"
	"testing"
	"testing/fstest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// reverseDirFS wraps an fs.FS and returns ReadDir entries in reverse name
// order. A test built on it only passes if the caller sorts the entries
// itself, rather than relying on an FS that already happens to.
type reverseDirFS struct {
	inner fs.FS
}

func (r reverseDirFS) Open(name string) (fs.File, error) {
	return r.inner.Open(name)
}

func (r reverseDirFS) ReadDir(name string) ([]fs.DirEntry, error) {
	entries, err := fs.ReadDir(r.inner, name)
	if err != nil {
		return nil, err
	}
	reversed := make([]fs.DirEntry, len(entries))
	for i, e := range entries {
		reversed[len(entries)-1-i] = e
	}
	return reversed, nil
}

func TestLoadFixture(t *testing.T) {
	m, err := Load("_fixture")
	require.NoError(t, err)
	assert.Equal(t, "_fixture", m.Machine)
	assert.True(t, m.Frozen)
	assert.Len(t, m.States, 2)
	assert.Len(t, m.Events, 2)
	assert.Len(t, m.Transitions, 2)
	assert.Len(t, m.Cells, 6)
	assert.Equal(t, "no operation is in flight while Idle", m.States[0].NotApplicable["timer.deadline"])
	assert.Equal(t, &OpRef{Type: "Update", Step: "Surge"}, m.States[1].Op)
	assert.Len(t, m.Invariants, 2)
	assert.Equal(t, "ready.count.matches.pods", m.Invariants[0].ID)
	assert.NotEmpty(t, m.Invariants[0].Statement)
	assert.Equal(t, []string{"ready.count.matches.pods"}, m.Cells[0].Invariants)
}

func TestLoadMachineFileInvariants(t *testing.T) {
	m := &Machine{}
	require.NoError(t, mergeFragment(m, "machine.yaml", []byte(
		"machine: demo\ninvariants:\n  - {id: a.b, statement: a holds}\n")))
	assert.Equal(t, []Invariant{{ID: "a.b", Statement: "a holds"}}, m.Invariants)
}

func TestLoadMachineFileRejectsUnknownInvariantField(t *testing.T) {
	m := &Machine{}
	err := mergeFragment(m, "machine.yaml", []byte(
		"machine: demo\ninvariants:\n  - {id: a.b, claim: a holds}\n"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "claim")
}

func TestLoadAllIncludesFixture(t *testing.T) {
	ms, err := LoadAll()
	require.NoError(t, err)
	var names []string
	for _, m := range ms {
		names = append(names, m.Machine)
	}
	assert.Contains(t, names, "_fixture")
}

func TestLoadFromErrors(t *testing.T) {
	cases := map[string]struct {
		files fstest.MapFS
		name  string // defaults to "x" when empty
		want  string
	}{
		"name containing slash is rejected": {
			files: fstest.MapFS{},
			name:  "a/b",
			want:  `invalid machine name "a/b"`,
		},
		"name starting with dot is rejected": {
			files: fstest.MapFS{},
			name:  ".hidden",
			want:  `invalid machine name ".hidden"`,
		},
		"missing machine.yaml": {
			files: fstest.MapFS{"machines/x/states.yaml": {Data: []byte("states: []\n")}},
			want:  "missing machine.yaml",
		},
		"missing states.yaml": {
			files: fstest.MapFS{
				"machines/x/machine.yaml": {Data: []byte("machine: x\nscope: s\n")},
				"machines/x/events.yaml":  {Data: []byte("events: []\n")},
			},
			want: "missing states.yaml",
		},
		"name mismatch": {
			// "y" is a bare YAML 1.1 boolean literal (like on/off/yes/no);
			// use a value outside that reserved set so it round-trips as
			// the string it's written as.
			files: fstest.MapFS{
				"machines/x/machine.yaml": {Data: []byte("machine: yy\nscope: s\n")},
				"machines/x/states.yaml":  {Data: []byte("states: []\n")},
				"machines/x/events.yaml":  {Data: []byte("events: []\n")},
			},
			want: "declares machine \"yy\"",
		},
		"unknown fragment": {
			files: fstest.MapFS{
				"machines/x/machine.yaml": {Data: []byte("machine: x\nscope: s\n")},
				"machines/x/extra.yaml":   {Data: []byte("cells: []\n")},
			},
			want: "unrecognized fragment",
		},
		"misplaced key is rejected strictly": {
			files: fstest.MapFS{
				"machines/x/machine.yaml": {Data: []byte("machine: x\nscope: s\n")},
				"machines/x/states.yaml":  {Data: []byte("cells: []\n")},
			},
			want: "unknown field",
		},
		"subdirectory is an error": {
			files: fstest.MapFS{
				"machines/x/machine.yaml":       {Data: []byte("machine: x\nscope: s\n")},
				"machines/x/states.yaml":        {Data: []byte("states: []\n")},
				"machines/x/events.yaml":        {Data: []byte("events: []\n")},
				"machines/x/nested/states.yaml": {Data: []byte("states: []\n")},
			},
			want: "unexpected entry",
		},
		"non-yaml file is an error": {
			files: fstest.MapFS{
				"machines/x/machine.yaml": {Data: []byte("machine: x\nscope: s\n")},
				"machines/x/states.yaml":  {Data: []byte("states: []\n")},
				"machines/x/events.yaml":  {Data: []byte("events: []\n")},
				"machines/x/states.yml":   {Data: []byte("states: []\n")},
			},
			want: "unexpected entry",
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			loadName := tc.name
			if loadName == "" {
				loadName = "x"
			}
			_, err := loadFrom(tc.files, loadName)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.want)
		})
	}
}

func TestLoadCellsConcatenateInNameOrder(t *testing.T) {
	files := fstest.MapFS{
		"machines/x/machine.yaml": {Data: []byte("machine: x\nscope: s\n")},
		"machines/x/states.yaml":  {Data: []byte("states: []\n")},
		"machines/x/events.yaml":  {Data: []byte("events: []\n")},
		"machines/x/cells-b.yaml": {Data: []byte("cells:\n  - state: second\n    event: e\n    outcome: ignore\n    status: current\n")},
		"machines/x/cells-a.yaml": {Data: []byte("cells:\n  - state: first\n    event: e\n    outcome: ignore\n    status: current\n")},
	}
	// Wrapped in reverseDirFS so this test only passes if loadFrom sorts
	// entries itself; fstest.MapFS's own ReadDir already returns them in
	// name order, which would let a removed sort.Slice call go unnoticed.
	m, err := loadFrom(reverseDirFS{inner: files}, "x")
	require.NoError(t, err)
	require.Len(t, m.Cells, 2)
	assert.Equal(t, "first", m.Cells[0].State)
	assert.Equal(t, "second", m.Cells[1].State)
}

func TestLoadZeroCellsFilesIsAccepted(t *testing.T) {
	files := fstest.MapFS{
		"machines/x/machine.yaml": {Data: []byte("machine: x\nscope: s\n")},
		"machines/x/states.yaml":  {Data: []byte("states: []\n")},
		"machines/x/events.yaml":  {Data: []byte("events: []\n")},
	}
	m, err := loadFrom(files, "x")
	require.NoError(t, err)
	assert.Empty(t, m.Cells)
}
