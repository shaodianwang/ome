package framework

import (
	"os"
	"strconv"

	"github.com/onsi/ginkgo/v2"
	"go.uber.org/zap/zapcore"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

// SetupTestLogger wires the controller-runtime logger for an
// integration suite. Defaults to ErrorLevel (suite output stays
// readable). Override via the TEST_LOG_LEVEL env var using integer
// zapcore levels — negative for verbose (debug = -1), 0 for info,
// 1 for warn, 2 for error.
//
//	TEST_LOG_LEVEL=-1 go test ./tests/integration/...   # full debug trace
//	TEST_LOG_LEVEL=0  go test ./tests/integration/...   # info+
//	(default)         go test ./tests/integration/...   # error only
//
// Output writes to ginkgo.GinkgoWriter so log lines surface in
// per-spec failure reports / -ginkgo.v output rather than being
// dumped to stdout for every passing spec.
func SetupTestLogger() {
	level := zapcore.ErrorLevel
	if v, ok := os.LookupEnv("TEST_LOG_LEVEL"); ok {
		if parsed, err := strconv.Atoi(v); err == nil {
			level = zapcore.Level(parsed)
		}
	}
	ctrl.SetLogger(zap.New(
		zap.UseDevMode(true),
		zap.WriteTo(ginkgo.GinkgoWriter),
		zap.Level(level),
	))
}
