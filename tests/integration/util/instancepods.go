package util

import (
	"context"
	"fmt"
	"slices"
	"strconv"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
)

// MaterializeSinglePodInstances gives simulated admission rows owned Pods on a
// bare member API. Readiness requires a kubelet and is not simulated here.
func MaterializeSinglePodInstances(ctx context.Context, c client.Client, ir *v1beta1.InferenceReplica, rows []v1beta1.OMENativeInstanceStatus) ([]v1beta1.OMENativeInstanceStatus, error) {
	if ir == nil || ir.UID == "" || ir.ParentName() == "" || len(ir.Spec.Runners) != 1 || ir.Spec.Runners[0].Size != 1 {
		return nil, fmt.Errorf("simulated Pods require an identified single-Pod component")
	}
	out := slices.Clone(rows)
	for i := range out {
		row := &out[i]
		row.RunningRevision = ir.Name + "-fixture"
		row.PodCount = 1
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Namespace: ir.Namespace, Name: fmt.Sprintf("%s-%d", ir.Name, row.Index),
				OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(ir, v1beta1.SchemeGroupVersion.WithKind("InferenceReplica"))},
				Labels: map[string]string{
					constants.InferenceServicePodLabelKey: ir.ParentName(),
					query.LabelInstanceIdx:                strconv.FormatInt(int64(row.Index), 10),
					query.LabelInstanceIncarnation:        strconv.FormatInt(row.Incarnation, 10),
					query.LabelPodOrdinal:                 strconv.FormatInt(int64(row.ActiveOrdinal), 10),
					query.LabelRevisionHash:               query.RevisionHashFromControllerRevisionName(row.RunningRevision),
				}},
			Spec: *ir.Spec.Runners[0].Template.Spec.DeepCopy(),
		}
		if err := c.Create(ctx, pod); err != nil {
			if !apierrors.IsAlreadyExists(err) {
				return nil, err
			}
			live := &corev1.Pod{}
			if err := c.Get(ctx, client.ObjectKeyFromObject(pod), live); err != nil {
				return nil, err
			}
			if !metav1.IsControlledBy(live, ir) || !live.DeletionTimestamp.IsZero() {
				return nil, fmt.Errorf("simulation Pod %q has an unavailable owner", live.Name)
			}
			for key, value := range pod.Labels {
				if live.Labels[key] != value {
					return nil, fmt.Errorf("simulation Pod %q has a different instance identity", live.Name)
				}
			}
		}
	}
	return out, nil
}
