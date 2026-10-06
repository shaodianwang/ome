package rolloutspec

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCoverageFloorHolds is the ratchet: every real machine carries a floor
// file, and its uncovered and undecided row counts are at or under the
// floor. A count above it fails and names the rows behind the number.
func TestCoverageFloorHolds(t *testing.T) {
	floors, err := CoverageFloors()
	require.NoError(t, err)
	machines, err := LoadAll()
	require.NoError(t, err)

	withFloor := map[string]bool{}
	for _, name := range floors {
		withFloor[name] = true
	}
	known := map[string]bool{}
	for _, m := range machines {
		known[m.Machine] = true
		// Underscore-prefixed directories are the package's own fixtures.
		if !strings.HasPrefix(m.Machine, "_") {
			assert.Truef(t, withFloor[m.Machine], "machine %s has no %s%s", m.Machine, m.Machine, coverageFloorSuffix)
		}
	}
	for _, name := range floors {
		assert.Truef(t, known[name], "%s%s names no machine directory", name, coverageFloorSuffix)
	}

	for _, m := range machines {
		if !withFloor[m.Machine] {
			continue
		}
		t.Run(m.Machine, func(t *testing.T) {
			floor, err := LoadCoverageFloor(m.Machine)
			require.NoError(t, err)
			count := CountCoverage(m)
			t.Logf("%s: %d uncovered (floor %d), %d undecided (floor %d), set on %s",
				m.Machine, len(count.Uncovered), floor.Uncovered, len(count.Undecided), floor.Undecided, floor.SetOn)
			for _, msg := range floor.Breaches(count) {
				t.Error(msg)
			}
		})
	}
}

// The fixture's rows are all decided and covered, so every count starts
// at zero and each mutation moves exactly the number it should.
func TestCountCoverage(t *testing.T) {
	m := fixture(t)
	count := CountCoverage(m)
	assert.Empty(t, count.Uncovered)
	assert.Empty(t, count.Undecided)

	m.Cells[0].Coverage = nil
	m.Cells[1].Coverage = &Coverage{}
	m.Cells[2].Status = StatusCurrent
	m.Cells[3].Status = StatusReview
	count = CountCoverage(m)
	assert.Len(t, count.Uncovered, 2)
	assert.Len(t, count.Undecided, 2)
	assert.Equal(t, "Idle", count.Uncovered[0].State)
}

// An n/a row has nothing to exercise, so it is not uncovered even without
// coverage, matching the validator's exemption.
func TestCountCoverageExemptsNA(t *testing.T) {
	m := fixture(t)
	m.Cells[0].Outcome = OutcomeNA
	m.Cells[0].Reason = "cannot occur"
	m.Cells[0].Coverage = nil
	assert.Empty(t, CountCoverage(m).Uncovered)
}

func TestCoverageFloorBreaches(t *testing.T) {
	m := fixture(t)
	m.Cells[0].Coverage = nil
	m.Cells[0].Status = StatusCurrent
	count := CountCoverage(m)

	floor := &CoverageFloor{Uncovered: 1, Undecided: 1, SetOn: "2026-01-01"}
	assert.Empty(t, floor.Breaches(count), "equal is clean")

	floor = &CoverageFloor{Uncovered: 5, Undecided: 5, SetOn: "2026-01-01"}
	assert.Empty(t, floor.Breaches(count), "lower is clean")

	floor = &CoverageFloor{Uncovered: 0, Undecided: 0, SetOn: "2026-01-01"}
	got := floor.Breaches(count)
	require.Len(t, got, 2)
	assert.Contains(t, got[0], "1 uncovered rows, floor 0 (+1)")
	assert.Contains(t, got[0], "  Idle: 1\n")
	assert.Contains(t, got[0], "cell Idle/pod.ready windows=[before] variants=[source] transition=")
	assert.Contains(t, got[1], "1 undecided rows, floor 0 (+1)")
}

func TestParseCoverageFloor(t *testing.T) {
	f, err := parseCoverageFloor([]byte("uncovered: 3\nundecided: 7\nset_on: \"2026-01-01\"\n"))
	require.NoError(t, err)
	assert.Equal(t, &CoverageFloor{Uncovered: 3, Undecided: 7, SetOn: "2026-01-01"}, f)

	for name, data := range map[string]string{
		"unknown key":   "uncovered: 3\nundecided: 7\nset_on: \"2026-01-01\"\nextra: 1\n",
		"negative":      "uncovered: -1\nundecided: 7\nset_on: \"2026-01-01\"\n",
		"missing date":  "uncovered: 3\nundecided: 7\n",
		"not a number":  "uncovered: many\nundecided: 7\nset_on: \"2026-01-01\"\n",
		"not a mapping": "- 3\n",
	} {
		t.Run(name, func(t *testing.T) {
			_, err := parseCoverageFloor([]byte(data))
			assert.Error(t, err)
		})
	}
}

func TestLoadCoverageFloorMissing(t *testing.T) {
	_, err := LoadCoverageFloor("no-such-machine")
	assert.Error(t, err)
}
