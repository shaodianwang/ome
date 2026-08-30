package framework

import (
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/onsi/ginkgo/v2"
	uberzap "go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
	ctrl "sigs.k8s.io/controller-runtime"
	crzap "sigs.k8s.io/controller-runtime/pkg/log/zap"
)

// testLogLevelEnv selects the controller-runtime log verbosity for a
// suite. Integer zapcore levels: negative for verbose (debug = -1),
// 0 for info, 1 for warn, 2 for error. Absent → defaultTestLogLevel.
const testLogLevelEnv = "TEST_LOG_LEVEL"

// defaultTestLogLevel keeps suite output readable: only error-and-above
// reach GinkgoWriter unless TEST_LOG_LEVEL lowers the bar. The
// write-conflict gate (VerifyNoWriteConflicts) observes at ErrorLevel
// regardless, so the gate never depends on this knob.
const defaultTestLogLevel = zapcore.ErrorLevel

// setupObservedLogger wires the controller-runtime logger exactly once
// per test binary and returns the in-memory observer that captures
// every entry at or above the configured level. Only the first
// ctrl.SetLogger call takes effect — controller-runtime fulfils the
// root sink's promise once and drops later calls — so a second
// observer installed this way would silently receive nothing. The
// OnceValue makes repeated framework.Init() calls hand back the one
// live observer.
//
// The returned core is teed: entries still flow to GinkgoWriter (so
// -ginkgo.v failure reports show them) AND into the observer (so
// Teardown can assert on them). Output level honors TEST_LOG_LEVEL,
// but the observer is always created at the more-verbose of the
// configured level and ErrorLevel, guaranteeing the write-conflict
// gate sees error logs even when the human-facing level is raised.
var setupObservedLogger = sync.OnceValue(func() *observer.ObservedLogs {
	outputLevel := resolveTestLogLevel()

	// Observe at whichever is more verbose so the gate always captures
	// >=Error entries even if a suite raises TEST_LOG_LEVEL above it.
	observeLevel := outputLevel
	if zapcore.ErrorLevel < observeLevel {
		observeLevel = zapcore.ErrorLevel
	}
	logsObserver, observedLogs := observer.New(observeLevel)

	// WrapCore tees the suite's normal GinkgoWriter core with the
	// observer core; both receive every enabled entry.
	teeObserver := uberzap.WrapCore(func(core zapcore.Core) zapcore.Core {
		return zapcore.NewTee(core, logsObserver)
	})

	ctrl.SetLogger(crzap.New(
		crzap.UseDevMode(true),
		crzap.WriteTo(ginkgo.GinkgoWriter),
		crzap.Level(outputLevel),
		crzap.RawZapOpts(teeObserver),
	))
	return observedLogs
})

// SetupTestLogger wires the controller-runtime logger for an
// integration suite and returns the observer the write-conflict gate
// reads in Teardown. Defaults to error-only output (suite logs stay
// readable). Override the human-facing verbosity via TEST_LOG_LEVEL
// using integer zapcore levels — negative for verbose (debug = -1),
// 0 for info, 1 for warn, 2 for error.
//
//	TEST_LOG_LEVEL=-1 go test ./tests/integration/...   # full debug trace
//	TEST_LOG_LEVEL=0  go test ./tests/integration/...   # info+
//	(default)         go test ./tests/integration/...   # error only
//
// Output writes to ginkgo.GinkgoWriter so log lines surface in
// per-spec failure reports / -ginkgo.v output rather than being
// dumped to stdout for every passing spec. Calling it more than once
// per test binary is safe — the underlying setup runs exactly once.
func SetupTestLogger() *observer.ObservedLogs {
	return setupObservedLogger()
}

// resolveTestLogLevel parses TEST_LOG_LEVEL or falls back to the
// default. A malformed value is treated as unset rather than failing
// suite startup.
func resolveTestLogLevel() zapcore.Level {
	if v, ok := os.LookupEnv(testLogLevelEnv); ok {
		if parsed, err := strconv.Atoi(strings.TrimSpace(v)); err == nil {
			return zapcore.Level(parsed)
		}
	}
	return defaultTestLogLevel
}
