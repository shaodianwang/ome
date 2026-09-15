package components

import (
	"context"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/controllerconfig"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/irstatus"
)

// InstanceStatusDecoderFromConfig derives the InferenceReplica per-Instance
// status decoder from the seeded inferenceservice-config the same way the
// manager binary does: the omenativeStatus block's maxDecodedInstances is the
// ColumnarV2 row bound. A missing ConfigMap or block yields the zero decoder,
// which decodes DenseV1 unchanged and fails closed on any ColumnarV2 payload,
// so suites that seed their own minimal ConfigMap keep their behavior; a
// present but invalid block is an error, exactly as at manager startup.
func InstanceStatusDecoderFromConfig(ctx context.Context, clientset kubernetes.Interface) (irstatus.Decoder, error) {
	configMap, err := clientset.CoreV1().ConfigMaps(constants.OMENamespace).Get(ctx, constants.InferenceServiceConfigMapName, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return irstatus.Decoder{}, nil
	}
	if err != nil {
		return irstatus.Decoder{}, fmt.Errorf("read %s/%s: %w", constants.OMENamespace, constants.InferenceServiceConfigMapName, err)
	}
	if _, ok := configMap.Data[controllerconfig.OMENativeStatusConfigName]; !ok {
		return irstatus.Decoder{}, nil
	}
	cfg, err := controllerconfig.ParseOMENativeStatusConfig(configMap)
	if err != nil {
		return irstatus.Decoder{}, err
	}
	return irstatus.NewDecoder(cfg.DecodeBound()), nil
}
