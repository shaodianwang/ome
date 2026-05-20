package omenative

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
)

// BuildHeadlessService produces the per-Component headless Service that
// gives every OMENative-managed pod a stable FQDN of the form
//
//	<pod-name>.<isvc>-<component>-headless.<namespace>.svc.<cluster-domain>
//
// Properties:
//   - ClusterIP=None — no kube-proxy load-balancing; DNS lookups return
//     per-pod A records.
//   - PublishNotReadyAddresses=true — peer DNS resolves during gang init
//     before all pods are Ready, so workers can discover each other while
//     the controller-owned ome.io/serving gate is still False.
//   - Selector scoped to ome.io/managed-by=OMENative so the Service does
//     not pick up legacy LWS-managed or RawDeployment pods that share the
//     bare component label.
//   - One OwnerReference back to the parent InferenceService; deletion of
//     the ISVC cascades to this Service.
//
// Pure compute, no I/O.
func BuildHeadlessService(isvc *v1beta1.InferenceService, component v1beta1.ComponentType) (*corev1.Service, error) {
	if isvc == nil {
		return nil, fmt.Errorf("nil InferenceService")
	}

	selector := map[string]string{
		constants.InferenceServicePodLabelKey: isvc.Name,
		constants.OMEComponentLabel:           string(component),
		labelManagedBy:                        managedByOMENative,
	}

	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      headlessServiceName(isvc.Name, component),
			Namespace: isvc.Namespace,
			Labels:    selector,
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(isvc, isvcGroupVersionKind),
			},
		},
		Spec: corev1.ServiceSpec{
			ClusterIP:                corev1.ClusterIPNone,
			PublishNotReadyAddresses: true,
			Selector:                 selector,
			// Ports omitted by design: this Service exists for DNS-based
			// peer discovery, not for kube-proxy routing. Each runtime
			// resolves <pod-name>.<this-service>... and dials whatever
			// port it cares about. Adding a placeholder port would imply
			// load-balanceable endpoints, which is the opposite of what
			// the headless contract is for.
			Type: corev1.ServiceTypeClusterIP,
		},
	}, nil
}
