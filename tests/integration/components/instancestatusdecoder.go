package components

import (
	"k8s.io/client-go/kubernetes"

	"github.com/ome-projects/ome/pkg/controller/v1beta1/controllerconfig"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/irstatus"
)

// InstanceStatusDecoderFromConfig derives the InferenceReplica per-Instance
// status decoder from the seeded inferenceservice-config exactly as the
// manager binary does: the omenativeStatus block's maxDecodedInstances is the
// ColumnarV2 row bound. A missing ConfigMap or block is an error, as at
// manager startup; a zero decoder would fail closed on every ColumnarV2
// object and surface only as stuck rollouts, so no setup may run without the
// block. Every in-process manager seeds it through util.SeedDefaultConfigMaps.
func InstanceStatusDecoderFromConfig(clientset kubernetes.Interface) (irstatus.Decoder, error) {
	cfg, err := controllerconfig.NewOMENativeStatusConfig(clientset)
	if err != nil {
		return irstatus.Decoder{}, err
	}
	return irstatus.NewDecoder(cfg.DecodeBound()), nil
}
