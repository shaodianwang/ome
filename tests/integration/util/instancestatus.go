package util

import (
	"context"

	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/irstatus"
)

// DecodedInstanceStatuses returns the logical per-Instance rows of ir in
// dense order, whichever representation the stored object carries. A
// ColumnarV2 payload expands under the seeded bound; a stored object that
// does not decode yields an error and no rows.
func DecodedInstanceStatuses(ir *v1beta1.InferenceReplica) ([]v1beta1.OMENativeInstanceStatus, error) {
	if ir == nil {
		return nil, nil
	}
	rows, _, err := irstatus.DecodeStatus(&ir.Status, SeededMaxDecodedInstances)
	return rows, err
}

// GetInstanceStatuses fetches the InferenceReplica at key through reads and
// returns its decoded rows. A fetch error is returned before any decode and
// keeps its API error identity.
func GetInstanceStatuses(ctx context.Context, reads client.Reader, key client.ObjectKey) ([]v1beta1.OMENativeInstanceStatus, error) {
	ir := &v1beta1.InferenceReplica{}
	if err := reads.Get(ctx, key, ir); err != nil {
		return nil, err
	}
	return DecodedInstanceStatuses(ir)
}

// InstallDenseInstanceStatuses makes rows the DenseV1 representation of
// status in memory. The marker and columns are cleared alongside so the
// status carries exactly one representation.
func InstallDenseInstanceStatuses(status *v1beta1.InferenceReplicaStatus, rows []v1beta1.OMENativeInstanceStatus) {
	status.InstanceStatuses = rows
	status.InstanceStatusEncoding = nil
	status.InstanceStatusColumns = nil
}

// WriteDenseInstanceStatuses installs rows on ir as DenseV1 and commits the
// status subresource in the same update. A manager that targets ColumnarV2
// converts the stored object on its next reconcile.
func WriteDenseInstanceStatuses(ctx context.Context, c client.Client, ir *v1beta1.InferenceReplica, rows []v1beta1.OMENativeInstanceStatus) error {
	InstallDenseInstanceStatuses(&ir.Status, rows)
	return c.Status().Update(ctx, ir)
}
