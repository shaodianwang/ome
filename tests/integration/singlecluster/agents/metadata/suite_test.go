// Package metadata hosts integration tests for the metadata-extraction
// agent (internal/ome-agent/model-metadata) driven against envtest via
// the shared framework.
package metadata

import (
	"context"
	"testing"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/constants"

	"github.com/ome-projects/ome/tests/integration/framework"
	"github.com/ome-projects/ome/tests/integration/util"
)

func TestMetadataAgent(t *testing.T) {
	util.RunSuite(t, "Metadata Agent Integration Suite")
}

// Suite-wide state populated by BeforeSuite, used by every Describe.
var (
	suiteCtx    context.Context
	suiteCancel context.CancelFunc
	suiteFwk    *framework.Framework
	k8sClient   client.Client
)

var _ = ginkgo.BeforeSuite(func() {
	s, err := framework.NewSuite(&framework.Framework{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	suiteFwk = s.Fwk
	k8sClient = s.Client
	suiteCtx, suiteCancel = context.WithCancel(s.Ctx)

	// Pre-create the OME namespace so the agent's writes land somewhere.
	gomega.Expect(k8sClient.Create(suiteCtx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: constants.OMENamespace},
	})).To(gomega.Or(gomega.BeNil(), gomega.MatchError(gomega.ContainSubstring("already exists"))))
})

var _ = ginkgo.AfterSuite(func() {
	if suiteCancel != nil {
		suiteCancel()
	}
	if suiteFwk != nil {
		_ = suiteFwk.Teardown()
	}
})
