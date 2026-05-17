package sharded

import (
	"context"
	"fmt"
	"testing"

	"github.com/go-logr/logr"
	"github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

// stubClient is the in-package test double for Client. We don't use
// the sibling fake/ package because backends/sharded/fake imports
// backends/sharded — bringing it into the sharded package's own test
// binary would cycle.
type stubClient struct {
	checkSourceFunc   func(context.Context, Source) (*SourceStatus, error)
	inspectSourceFunc func(context.Context, Source) (*SourceInspection, error)
}

func (s *stubClient) CheckSource(ctx context.Context, src Source) (*SourceStatus, error) {
	return s.checkSourceFunc(ctx, src)
}

func (s *stubClient) InspectSource(ctx context.Context, src Source) (*SourceInspection, error) {
	return s.inspectSourceFunc(ctx, src)
}

// TestReconcileMetadataMarksFailedOnUnparseableSource pins the
// behavior change: providers that return ErrSourceUnparseable must
// drive the ClusterBaseModel to state=Failed with reason
// MetadataUnparseable and an empty ctrl.Result (no 5s requeue loop).
// The previous behavior was to retry indefinitely as MetadataPending,
// which silently wedged any ISVC pointing at an unparseable model.
func TestReconcileMetadataMarksFailedOnUnparseableSource(t *testing.T) {
	g := gomega.NewGomegaWithT(t)

	scheme := runtime.NewScheme()
	g.Expect(v1beta1.AddToScheme(scheme)).To(gomega.Succeed())

	cbm := &v1beta1.ClusterBaseModel{
		ObjectMeta: metav1.ObjectMeta{Name: "unparseable-cbm", Generation: 1},
		Spec: v1beta1.BaseModelSpec{
			Storage: &v1beta1.StorageSpec{StorageUri: ptr.To("s3://bucket/no-config")},
		},
	}
	kubeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cbm).
		WithStatusSubresource(cbm).
		Build()

	cacheClient := &stubClient{
		inspectSourceFunc: func(ctx context.Context, source Source) (*SourceInspection, error) {
			return nil, fmt.Errorf("source missing config.json: %w", ErrSourceUnparseable)
		},
	}

	source := Source{URI: "s3://bucket/no-config"}
	inspection, result, err := reconcileModelMetadataSpec(
		context.Background(), kubeClient, logr.Discard(), cacheClient,
		source, cbm, "ClusterBaseModel", ProviderClusterCache,
	)

	g.Expect(err).NotTo(gomega.HaveOccurred(),
		"unparseable should NOT bubble up as a controller error — that'd cause controller-runtime backoff retries; we want a terminal status write and no requeue")
	g.Expect(inspection).To(gomega.BeNil(),
		"caller must not receive a partial inspection on the unparseable path")
	g.Expect(result.Requeue).To(gomega.BeFalse(),
		"terminal state must not auto-requeue; operator action (spec change) re-triggers via the informer")
	g.Expect(result.RequeueAfter).To(gomega.BeZero(),
		"terminal state must not auto-requeue at the 5s poll interval — that's the pending path's contract, not failed")

	got := &v1beta1.ClusterBaseModel{}
	g.Expect(kubeClient.Get(context.Background(), client.ObjectKeyFromObject(cbm), got)).To(gomega.Succeed())
	g.Expect(got.Status.State).To(gomega.Equal(v1beta1.LifeCycleStateFailed),
		"unparseable source must transition the ClusterBaseModel to state=Failed; staying in Importing hides the permanent error from operators")

	ready := findCondition(got.Status.Conditions, v1beta1.ModelConditionReady)
	g.Expect(ready).NotTo(gomega.BeNil())
	g.Expect(ready.Status).To(gomega.Equal(metav1.ConditionFalse))
	g.Expect(ready.Reason).To(gomega.Equal(v1beta1.ModelConditionReasonMetadataUnparseable))
	g.Expect(ready.Message).To(gomega.ContainSubstring("config.json"),
		"original provider error must propagate into the condition message so the operator can fix the source")
}

// TestApplyMetadataUnparseableIsIdempotent asserts the second call with the
// same generation + message is a no-op (returns false), so the reconcile loop
// doesn't keep writing identical status updates after the first failure.
func TestApplyMetadataUnparseableIsIdempotent(t *testing.T) {
	g := gomega.NewGomegaWithT(t)

	status := &v1beta1.ModelStatusSpec{}
	first := applyMetadataUnparseable(1, status, "config.json missing")
	g.Expect(first).To(gomega.BeTrue(), "first call must transition state and write the condition")
	g.Expect(status.State).To(gomega.Equal(v1beta1.LifeCycleStateFailed))

	second := applyMetadataUnparseable(1, status, "config.json missing")
	g.Expect(second).To(gomega.BeFalse(),
		"repeated apply with same generation+message must return false so updateModelStatusWithRetry skips status.Update — prevents a tight reconcile loop on a terminal state")
}

func findCondition(conditions []metav1.Condition, t string) *metav1.Condition {
	for i := range conditions {
		if conditions[i].Type == t {
			return &conditions[i]
		}
	}
	return nil
}
