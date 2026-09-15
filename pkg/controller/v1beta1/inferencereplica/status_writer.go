package inferencereplica

import (
	"context"
	"errors"
	"fmt"

	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/irstatus"
)

// ErrColumnarV2StatusWrite reports a status write refused because the object
// was read from a ColumnarV2 representation. This writer persists DenseV1
// only; rewriting a ColumnarV2 object would convert it, and conversion is not
// a write this manager performs.
var ErrColumnarV2StatusWrite = errors.New("InferenceReplica status write refused: the object is stored as ColumnarV2 and this manager writes DenseV1 only")

// updateInferenceReplicaStatus is the single InferenceReplica status writer.
// source is the encoding the object was decoded from; only a DenseV1 source
// may be written, and the written object carries the dense rows alone. It
// omits ReadyPodCount, ScheduledPodCount, and NodesOccupied because workload
// decisions derive them from current Pods.
func updateInferenceReplicaStatus(ctx context.Context, c client.Client, ir *v1beta1.InferenceReplica, source irstatus.Encoding) error {
	if source != irstatus.EncodingDenseV1 {
		return fmt.Errorf("%s/%s (source %q): %w", ir.Namespace, ir.Name, source, ErrColumnarV2StatusWrite)
	}
	if ir.Status.InstanceStatusEncoding != nil || ir.Status.InstanceStatusColumns != nil {
		return fmt.Errorf("%s/%s (object carries a ColumnarV2 marker or columns): %w", ir.Namespace, ir.Name, ErrColumnarV2StatusWrite)
	}
	clearPodDerivedInstanceObservations(ir)
	return c.Status().Update(ctx, ir)
}

func clearPodDerivedInstanceObservations(ir *v1beta1.InferenceReplica) {
	if ir == nil {
		return
	}
	for i := range ir.Status.InstanceStatuses {
		ir.Status.InstanceStatuses[i].ReadyPodCount = 0
		ir.Status.InstanceStatuses[i].ScheduledPodCount = 0
		ir.Status.InstanceStatuses[i].NodesOccupied = nil
	}
}
