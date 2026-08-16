package raw

import (
	"context"
	"testing"

	kedav1 "github.com/kedacore/keda/v2/apis/keda/v1alpha1"
	monitoringv1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	ctrlclientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/controllerconfig"
	isvcutils "github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/utils"
)

// ingressConfigMap builds the inferenceservice-config ConfigMap the ingress
// config loader reads, with the given urlScheme (empty ⇒ omitted so the loader
// applies its own default).
func ingressConfigMap(urlScheme string) *corev1.ConfigMap {
	ingress := `{"ingressGateway":"knative-serving/knative-ingress-gateway","ingressService":"istio-ingressgateway.istio-system.svc.cluster.local","ingressDomain":"example.com","domainTemplate":"{{.Name}}.{{.Namespace}}.{{.IngressDomain}}"`
	if urlScheme != "" {
		ingress += `,"urlScheme":"` + urlScheme + `"`
	}
	ingress += `}`
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      constants.InferenceServiceConfigMapName,
			Namespace: constants.OMENamespace,
		},
		Data: map[string]string{
			controllerconfig.IngressConfigKeyName: ingress,
		},
	}
}

// TestCreateRawURL_HonorsUrlScheme is the urlScheme guard: the raw
// per-component status URL must carry the configured urlScheme, not a hardcoded
// http.
func TestCreateRawURL_HonorsUrlScheme(t *testing.T) {
	for _, tc := range []struct {
		name       string
		urlScheme  string
		wantScheme string
	}{
		{name: "https is honored", urlScheme: "https", wantScheme: "https"},
		{name: "http is honored", urlScheme: "http", wantScheme: "http"},
		{name: "empty falls back to loader default", urlScheme: "", wantScheme: controllerconfig.DefaultUrlScheme},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clientset := fake.NewSimpleClientset(ingressConfigMap(tc.urlScheme))
			meta := metav1.ObjectMeta{Name: "my-isvc", Namespace: "my-ns"}

			url, err := createRawURL(clientset, meta)
			require.NoError(t, err)
			assert.Equal(t, tc.wantScheme, url.Scheme)
			assert.Equal(t, "my-isvc.my-ns.example.com", url.Host)
		})
	}
}

func TestRawKubeReconciler_ReconcilesBaseResourcesWithoutAutoscaler(t *testing.T) {
	scheme := runtime.NewScheme()
	for _, addToScheme := range []func(*runtime.Scheme) error{
		v1beta1.AddToScheme,
		appsv1.AddToScheme,
		autoscalingv2.AddToScheme,
		corev1.AddToScheme,
		policyv1.AddToScheme,
		kedav1.AddToScheme,
		monitoringv1.AddToScheme,
	} {
		require.NoError(t, addToScheme(scheme))
	}

	cl := ctrlclientfake.NewClientBuilder().WithScheme(scheme).Build()
	clientset := fake.NewSimpleClientset(ingressConfigMap("https"))
	minReplicas := 1
	componentMeta := metav1.ObjectMeta{
		Name:      "raw-base-only",
		Namespace: "default",
		Annotations: map[string]string{
			constants.AutoscalerClass: string(constants.AutoscalerClassHPA),
		},
	}
	spec := &v1beta1.InferenceServiceSpec{
		Engine: &v1beta1.EngineSpec{
			ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
				MinReplicas: &minReplicas,
				MaxReplicas: 3,
			},
		},
	}
	podSpec := &corev1.PodSpec{
		Containers: []corev1.Container{{Name: constants.MainContainerName, Image: "example.com/runtime:test"}},
	}

	reconciler, err := NewRawKubeReconciler(cl, clientset, scheme, componentMeta, spec, podSpec)
	require.NoError(t, err)
	_, err = reconciler.Reconcile()
	require.NoError(t, err)

	hpa := &autoscalingv2.HorizontalPodAutoscaler{}
	err = cl.Get(context.Background(), types.NamespacedName{Namespace: componentMeta.Namespace, Name: componentMeta.Name}, hpa)
	assert.True(t, apierrors.IsNotFound(err), "RawKubeReconciler must leave autoscaler dispatch to the common reconciler")

	so := &kedav1.ScaledObject{}
	err = cl.Get(context.Background(), types.NamespacedName{
		Namespace: componentMeta.Namespace,
		Name:      isvcutils.GetScaledObjectName(componentMeta.Name),
	}, so)
	assert.True(t, apierrors.IsNotFound(err), "RawKubeReconciler must not create a ScaledObject")
}
