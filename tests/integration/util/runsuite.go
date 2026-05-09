package util

import (
	"testing"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

// RunSuite is the one-line entrypoint Ginkgo-based integration suites
// use from their TestXxx(t *testing.T) function. Wires Gomega's
// failure handler into Ginkgo and starts the suite.
//
// Example:
//
//	func TestBaseModelController(t *testing.T) {
//	    util.RunSuite(t, "BaseModel Controller Integration Suite")
//	}
//
// Mirrors Kueue's test/util/util.go RunSuite (the same shape every
// suite_test.go opens with).
func RunSuite(t *testing.T, suiteName string) {
	t.Helper()
	gomega.RegisterFailHandler(ginkgo.Fail)
	ginkgo.RunSpecs(t, suiteName)
}
