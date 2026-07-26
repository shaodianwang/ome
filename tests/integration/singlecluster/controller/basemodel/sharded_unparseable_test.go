package basemodel

import (
	"context"
	"fmt"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/types"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/basemodel/backends/sharded"
	v1beta1testing "github.com/ome-projects/ome/pkg/utils/testing/v1beta1"

	"github.com/ome-projects/ome/tests/integration/util"
)

// Pins the provider contract: providers signal "this source layout is
// structurally unparseable, retrying won't help" via
// sharded.ErrSourceUnparseable; the BaseModel controller must transition
// the model to state=Failed with reason=MetadataUnparseable instead of
// looping forever in state=Importing reason=MetadataPending. Catches the
// regression where the controller treats every InspectSource error as
// transient (the historical behavior — wedged ISVCs with no operator signal).
var _ = ginkgo.Describe("Sharded BaseModel with unparseable source", ginkgo.Ordered, func() {

	ginkgo.It("ErrSourceUnparseable from InspectSource → state=Failed, reason=MetadataUnparseable", func() {
		// Source is reachable (CheckSource succeeds with default
		// Reachable=true), but InspectSource fails with a wrapped
		// ErrSourceUnparseable — mirroring the real cluster_cache
		// "missing config.json" path.
		suiteFakeMC.CheckSourceFunc = func(ctx context.Context, source sharded.Source) (*sharded.SourceStatus, error) {
			return &sharded.SourceStatus{Reachable: true, Reason: v1beta1.ModelConditionReasonSourceReachable}, nil
		}
		suiteFakeMC.InspectSourceFunc = func(ctx context.Context, source sharded.Source) (*sharded.SourceInspection, error) {
			return nil, fmt.Errorf("source missing config.json: %w", sharded.ErrSourceUnparseable)
		}
		ginkgo.DeferCleanup(func() {
			suiteFakeMC.CheckSourceFunc = nil
			suiteFakeMC.InspectSourceFunc = nil
		})

		cbm := v1beta1testing.MakeClusterBaseModel("sharded-unparseable").
			StorageURI("s3://bucket/no-config-model").
			Distribution(v1beta1.DistributionSharded).
			ModelFormat("safetensors").Obj()
		Expect(k8sClient.Create(suiteCtx, cbm)).To(Succeed())
		ginkgo.DeferCleanup(func() { _ = k8sClient.Delete(suiteCtx, cbm) })

		Eventually(func(g Gomega) {
			got := &v1beta1.ClusterBaseModel{}
			g.Expect(k8sClient.Get(suiteCtx, types.NamespacedName{Name: "sharded-unparseable"}, got)).To(Succeed())

			// state must be Failed, NOT Importing — the controller has to
			// surface "this is permanent" or operators stare at Importing
			// for hours waiting for a model that will never become Ready.
			g.Expect(got.Status.State).To(Equal(v1beta1.LifeCycleStateFailed),
				"unparseable source must flip state=Failed; staying in Importing hides the permanent failure")

			// SourceReachable=True (we successfully reached the source),
			// but Ready=False with the new terminal reason.
			source := meta.FindStatusCondition(got.Status.Conditions, v1beta1.ModelConditionSourceReachable)
			g.Expect(source).NotTo(BeNil())
			g.Expect(string(source.Status)).To(Equal("True"))

			ready := meta.FindStatusCondition(got.Status.Conditions, v1beta1.ModelConditionReady)
			g.Expect(ready).NotTo(BeNil())
			g.Expect(string(ready.Status)).To(Equal("False"))
			g.Expect(ready.Reason).To(Equal(v1beta1.ModelConditionReasonMetadataUnparseable),
				"new reason constant for the terminal-permanent case (distinct from MetadataPending which is transient/retried)")
			g.Expect(ready.Message).To(ContainSubstring("config.json"),
				"original provider error must propagate so the operator can fix the source")
		}).WithTimeout(util.LongTimeout).WithPolling(util.PollInterval).Should(Succeed())

		// Sanity: the fake's InspectSource was called at least once.
		// Without this the test could spuriously pass if the controller
		// somehow short-circuited before reaching InspectSource.
		Expect(len(suiteFakeMC.InspectSourceCalls())).To(BeNumerically(">=", 1))
	})
})
