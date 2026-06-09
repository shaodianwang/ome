package components

import (
	"context"
	"testing"

	policyv1 "k8s.io/api/policy/v1"

	kedav1 "github.com/kedacore/keda/v2/apis/keda/v1alpha1"
	"github.com/onsi/gomega"
	monitoringv1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlclientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/controllerconfig"
)

func TestDecoderReconcile(t *testing.T) {
	g := gomega.NewGomegaWithT(t)

	// Create scheme
	scheme := runtime.NewScheme()
	g.Expect(v1beta1.AddToScheme(scheme)).NotTo(gomega.HaveOccurred())
	g.Expect(v1.AddToScheme(scheme)).NotTo(gomega.HaveOccurred())
	g.Expect(appsv1.AddToScheme(scheme)).NotTo(gomega.HaveOccurred())
	g.Expect(kedav1.AddToScheme(scheme)).NotTo(gomega.HaveOccurred())
	g.Expect(autoscalingv2.AddToScheme(scheme)).NotTo(gomega.HaveOccurred())
	g.Expect(policyv1.AddToScheme(scheme)).NotTo(gomega.HaveOccurred())
	g.Expect(monitoringv1.AddToScheme(scheme)).NotTo(gomega.HaveOccurred())

	tests := []struct {
		name           string
		deploymentMode constants.DeploymentModeType
		baseModel      *v1beta1.BaseModelSpec
		baseModelMeta  *metav1.ObjectMeta
		decoderSpec    *v1beta1.DecoderSpec
		runtime        *v1beta1.ServingRuntimeSpec
		runtimeName    string
		isvc           *v1beta1.InferenceService
		validate       func(*testing.T, client.Client, *v1beta1.InferenceService)
		wantErr        bool
		setupMocks     func(client.Client, kubernetes.Interface)
	}{
		{
			name:           "Raw deployment for PD-disaggregated decoder",
			deploymentMode: constants.RawDeployment,
			baseModel: &v1beta1.BaseModelSpec{
				ModelFormat: v1beta1.ModelFormat{
					Name: "safetensors",
				},
				Storage: &v1beta1.StorageSpec{
					Path: stringPtr("/mnt/models/model1"),
				},
			},
			baseModelMeta: &metav1.ObjectMeta{
				Name:      "base-model-1",
				Namespace: "default",
			},
			decoderSpec: &v1beta1.DecoderSpec{
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
					MinReplicas: intPtr(1),
					MaxReplicas: 3,
				},
				PodSpec: v1beta1.PodSpec{
					Containers: []v1.Container{
						{
							Name:  "decoder-container",
							Image: "decoder:latest",
							Resources: v1.ResourceRequirements{
								Requests: v1.ResourceList{
									v1.ResourceCPU:    resource.MustParse("1"),
									v1.ResourceMemory: resource.MustParse("2Gi"),
								},
							},
						},
					},
				},
			},
			runtime:     &v1beta1.ServingRuntimeSpec{},
			runtimeName: "pd-runtime",
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pd-isvc",
					Namespace: "default",
				},
				Spec: v1beta1.InferenceServiceSpec{
					Model: &v1beta1.ModelRef{},
				},
			},
			setupMocks: func(c client.Client, cs kubernetes.Interface) {
				// Create inferenceservice config
				cm := &v1.ConfigMap{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "inferenceservice-config",
						Namespace: "ome",
					},
					Data: map[string]string{
						"config": "{}",
					},
				}
				_, err := cs.CoreV1().ConfigMaps("ome").Create(context.TODO(), cm, metav1.CreateOptions{})
				g.Expect(err).NotTo(gomega.HaveOccurred())
			},
			validate: func(t *testing.T, c client.Client, isvc *v1beta1.InferenceService) {
				// Check deployment was created
				deployment := &appsv1.Deployment{}
				err := c.Get(context.TODO(), types.NamespacedName{
					Name:      "test-pd-isvc-decoder",
					Namespace: "default",
				}, deployment)
				g.Expect(err).NotTo(gomega.HaveOccurred())
				g.Expect(deployment.Spec.Template.Spec.Containers[0].Image).To(gomega.Equal("decoder:latest"))

				// Check node selector was added for the base model
				g.Expect(deployment.Spec.Template.Spec.NodeSelector).NotTo(gomega.BeNil())
				value, found := deployment.Spec.Template.Spec.NodeSelector["models.ome.io/default.basemodel.base-model-1"]
				g.Expect(found).To(gomega.BeTrue(), "Model node selector not found")
				g.Expect(value).To(gomega.Equal("Ready"))

				// Check service was created
				service := &v1.Service{}
				err = c.Get(context.TODO(), types.NamespacedName{
					Name:      "test-pd-isvc-decoder",
					Namespace: "default",
				}, service)
				g.Expect(err).NotTo(gomega.HaveOccurred())
			},
		},
		{
			name:           "ClusterBaseModel decoder with node selector",
			deploymentMode: constants.RawDeployment,
			baseModel: &v1beta1.BaseModelSpec{
				ModelFormat: v1beta1.ModelFormat{
					Name: "safetensors",
				},
				Storage: &v1beta1.StorageSpec{
					Path: stringPtr("/mnt/models/cluster-model"),
				},
			},
			baseModelMeta: &metav1.ObjectMeta{
				Name: "cluster-decoder-model",
				// No namespace for ClusterBaseModel
			},
			decoderSpec: &v1beta1.DecoderSpec{
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
					MinReplicas: intPtr(1),
					MaxReplicas: 2,
				},
				PodSpec: v1beta1.PodSpec{
					Containers: []v1.Container{
						{
							Name:  "decoder",
							Image: "decoder:latest",
						},
					},
				},
			},
			runtime:     &v1beta1.ServingRuntimeSpec{},
			runtimeName: "decoder-runtime",
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-cluster-decoder",
					Namespace: "default",
				},
				Spec: v1beta1.InferenceServiceSpec{
					Model: &v1beta1.ModelRef{},
				},
			},
			setupMocks: func(c client.Client, cs kubernetes.Interface) {
				// Create inferenceservice config
				cm := &v1.ConfigMap{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "inferenceservice-config",
						Namespace: "ome",
					},
					Data: map[string]string{
						"config": "{}",
					},
				}
				_, err := cs.CoreV1().ConfigMaps("ome").Create(context.TODO(), cm, metav1.CreateOptions{})
				g.Expect(err).NotTo(gomega.HaveOccurred())
			},
			validate: func(t *testing.T, c client.Client, isvc *v1beta1.InferenceService) {
				// Check deployment was created
				deployment := &appsv1.Deployment{}
				err := c.Get(context.TODO(), types.NamespacedName{
					Name:      "test-cluster-decoder-decoder",
					Namespace: "default",
				}, deployment)
				g.Expect(err).NotTo(gomega.HaveOccurred())

				// Check node selector for ClusterBaseModel (no namespace in label)
				g.Expect(deployment.Spec.Template.Spec.NodeSelector).NotTo(gomega.BeNil())
				value, found := deployment.Spec.Template.Spec.NodeSelector["models.ome.io/clusterbasemodel.cluster-decoder-model"]
				g.Expect(found).To(gomega.BeTrue(), "Model node selector not found")
				g.Expect(value).To(gomega.Equal("Ready"))
			},
		},
		{
			name:           "Decoder with nil spec should error",
			deploymentMode: constants.RawDeployment,
			decoderSpec:    nil,
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-nil-decoder",
					Namespace: "default",
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create fake client
			c := ctrlclientfake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(tt.isvc).
				Build()

			// Create fake clientset
			clientset := fake.NewClientset()

			// Setup mocks if needed
			if tt.setupMocks != nil {
				tt.setupMocks(c, clientset)
			}

			// Create inference service config
			isvcConfig := &controllerconfig.InferenceServicesConfig{}

			// Create decoder component
			decoder := NewDecoder(
				c,
				clientset,
				nil, // apiReader
				nil, // expectations
				nil, // recorder
				scheme,
				isvcConfig,
				tt.deploymentMode,
				tt.baseModel,
				tt.baseModelMeta,
				tt.decoderSpec,
				tt.runtime,
				tt.runtimeName,
				nil, // supportedModelFormat
				nil, // overlays
			)

			// Reconcile
			result, err := decoder.Reconcile(context.TODO(), tt.isvc)

			if tt.wantErr {
				g.Expect(err).To(gomega.HaveOccurred())
			} else {
				g.Expect(err).NotTo(gomega.HaveOccurred())
				g.Expect(result).To(gomega.Equal(ctrl.Result{}))

				// Run validations
				if tt.validate != nil {
					tt.validate(t, c, tt.isvc)
				}
			}
		})
	}
}

func TestDecoderReconcileObjectMeta(t *testing.T) {
	g := gomega.NewGomegaWithT(t)

	tests := []struct {
		name                string
		isvc                *v1beta1.InferenceService
		decoderSpec         *v1beta1.DecoderSpec
		baseModel           *v1beta1.BaseModelSpec
		baseModelMeta       *metav1.ObjectMeta
		runtimeName         string
		expectedAnnotations map[string]string
		expectedLabels      map[string]string
		expectedName        string
	}{
		{
			name: "Basic decoder object metadata",
			isvc: &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-isvc",
					Namespace: "default",
					Annotations: map[string]string{
						"custom-annotation": "value",
					},
					Labels: map[string]string{
						"custom-label": "value",
					},
				},
			},
			decoderSpec: &v1beta1.DecoderSpec{
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
					Annotations: map[string]string{
						"decoder-annotation": "decoder-value",
					},
					Labels: map[string]string{
						"decoder-label": "decoder-value",
					},
				},
			},
			baseModel: &v1beta1.BaseModelSpec{
				ModelFormat: v1beta1.ModelFormat{
					Name:    "safetensors",
					Version: stringPtr("1.0"),
				},
				Storage: &v1beta1.StorageSpec{
					Path: stringPtr("/mnt/models/decoder"),
				},
				ModelExtensionSpec: v1beta1.ModelExtensionSpec{
					Vendor: stringPtr("meta"),
				},
			},
			baseModelMeta: &metav1.ObjectMeta{
				Name: "base-model",
				Annotations: map[string]string{
					constants.ModelCategoryAnnotation: "LARGE",
				},
			},
			runtimeName: "test-runtime",
			expectedAnnotations: map[string]string{
				"custom-annotation":                    "value",
				"decoder-annotation":                   "decoder-value",
				constants.BaseModelName:                "base-model",
				constants.BaseModelFormat:              "safetensors",
				constants.BaseModelFormatVersion:       "1.0",
				constants.BaseModelVendorAnnotationKey: "meta",
				constants.ServingRuntimeKeyName:        "test-runtime",
			},
			expectedLabels: map[string]string{
				"custom-label":                                  "value",
				"decoder-label":                                 "decoder-value",
				constants.InferenceServicePodLabelKey:           "test-isvc",
				constants.OMEComponentLabel:                     "decoder",
				constants.ServingRuntimeLabelKey:                "test-runtime",
				constants.InferenceServiceBaseModelNameLabelKey: "base-model",
				constants.InferenceServiceBaseModelSizeLabelKey: "LARGE",
				constants.BaseModelTypeLabelKey:                 "Serving",
				constants.BaseModelVendorLabelKey:               "meta",
				constants.FTServingLabelKey:                     "false",
			},
			expectedName: "test-isvc-decoder",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create scheme
			scheme := runtime.NewScheme()
			g.Expect(v1.AddToScheme(scheme)).NotTo(gomega.HaveOccurred())
			clientset := fake.NewClientset()
			c := ctrlclientfake.NewClientBuilder().WithScheme(scheme).Build()

			// Create decoder using the constructor
			decoder := NewDecoder(
				c,
				clientset,
				nil, // apiReader
				nil, // expectations
				nil, // recorder
				scheme,
				&controllerconfig.InferenceServicesConfig{},
				constants.RawDeployment,
				tt.baseModel,
				tt.baseModelMeta,
				tt.decoderSpec,
				nil, // runtime
				tt.runtimeName,
				nil, // supportedModelFormat
				nil, // overlays
			).(*Decoder)

			// Test reconcileObjectMeta
			objectMeta, err := decoder.reconcileObjectMeta(context.TODO(), tt.isvc)
			g.Expect(err).NotTo(gomega.HaveOccurred())

			// Validate name
			g.Expect(objectMeta.Name).To(gomega.Equal(tt.expectedName))
			g.Expect(objectMeta.Namespace).To(gomega.Equal("default"))

			// Validate annotations
			for k, v := range tt.expectedAnnotations {
				g.Expect(objectMeta.Annotations).To(gomega.HaveKeyWithValue(k, v))
			}

			// Validate labels
			for k, v := range tt.expectedLabels {
				g.Expect(objectMeta.Labels).To(gomega.HaveKeyWithValue(k, v))
			}
		})
	}
}

func TestDecoderWorkerPodSpec(t *testing.T) {
	g := gomega.NewGomegaWithT(t)

	tests := []struct {
		name               string
		decoderSpec        *v1beta1.DecoderSpec
		expectedError      bool
		expectedContainers int
		validatePodSpec    func(*v1.PodSpec)
	}{
		{
			name: "Worker with leader runner",
			decoderSpec: &v1beta1.DecoderSpec{
				Leader: &v1beta1.LeaderSpec{
					Runner: &v1beta1.RunnerSpec{
						Container: v1.Container{
							Name:  "leader-runner",
							Image: "leader-runtime:latest",
						},
					},
				},
				Worker: &v1beta1.WorkerSpec{
					Size: intPtr(2),
					PodSpec: v1beta1.PodSpec{
						Containers: []v1.Container{
							{
								Name:  "worker-container",
								Image: "worker:latest",
							},
						},
					},
				},
			},
			expectedContainers: 1,
			validatePodSpec: func(ps *v1.PodSpec) {
				// Should have worker container (leader runner is not merged into worker pod spec)
				found := false
				for _, c := range ps.Containers {
					if c.Name == "worker-container" {
						found = true
						g.Expect(c.Image).To(gomega.Equal("worker:latest"))
					}
				}
				g.Expect(found).To(gomega.BeTrue())
			},
		},
		{
			name:        "No worker spec returns nil",
			decoderSpec: &v1beta1.DecoderSpec{},
			validatePodSpec: func(ps *v1.PodSpec) {
				g.Expect(ps).To(gomega.BeNil())
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isvc := &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-isvc",
					Namespace: "default",
				},
			}

			objectMeta := &metav1.ObjectMeta{}

			// Create scheme
			scheme := runtime.NewScheme()
			g.Expect(v1.AddToScheme(scheme)).NotTo(gomega.HaveOccurred())
			clientset := fake.NewClientset()
			c := ctrlclientfake.NewClientBuilder().WithScheme(scheme).Build()

			// Create decoder using the constructor
			decoder := NewDecoder(
				c,
				clientset,
				nil, // apiReader
				nil, // expectations
				nil, // recorder
				scheme,
				&controllerconfig.InferenceServicesConfig{},
				constants.RawDeployment,
				nil, // baseModel
				nil, // baseModelMeta
				tt.decoderSpec,
				nil, // runtime
				"",  // runtimeName
				nil, // supportedModelFormat
				nil, // overlays
			).(*Decoder)

			podSpec, err := decoder.reconcileWorkerPodSpec(isvc, objectMeta)

			if tt.expectedError {
				g.Expect(err).To(gomega.HaveOccurred())
			} else {
				g.Expect(err).NotTo(gomega.HaveOccurred())
				if tt.validatePodSpec != nil {
					tt.validatePodSpec(podSpec)
				}
			}
		})
	}
}

func TestDecoderComponentConfig(t *testing.T) {
	g := gomega.NewGomegaWithT(t)

	tests := []struct {
		name                    string
		decoderSpec             *v1beta1.DecoderSpec
		expectedComponentType   v1beta1.ComponentType
		expectedServiceSuffix   string
		expectedValidationError bool
	}{
		{
			name: "Valid decoder spec",
			decoderSpec: &v1beta1.DecoderSpec{
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
					MinReplicas: intPtr(1),
					MaxReplicas: 3,
				},
				PodSpec: v1beta1.PodSpec{
					Containers: []v1.Container{
						{
							Name:  "decoder-container",
							Image: "decoder:latest",
						},
					},
				},
			},
			expectedComponentType:   v1beta1.DecoderComponent,
			expectedServiceSuffix:   "-decoder",
			expectedValidationError: false,
		},
		{
			name:                    "Nil decoder spec",
			decoderSpec:             nil,
			expectedComponentType:   v1beta1.DecoderComponent,
			expectedServiceSuffix:   "-decoder",
			expectedValidationError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create scheme
			scheme := runtime.NewScheme()
			g.Expect(v1.AddToScheme(scheme)).NotTo(gomega.HaveOccurred())
			clientset := fake.NewClientset()
			c := ctrlclientfake.NewClientBuilder().WithScheme(scheme).Build()

			// Create decoder using the constructor
			decoder := NewDecoder(
				c,
				clientset,
				nil, // apiReader
				nil, // expectations
				nil, // recorder
				scheme,
				&controllerconfig.InferenceServicesConfig{},
				constants.RawDeployment,
				nil, // baseModel
				nil, // baseModelMeta
				tt.decoderSpec,
				nil, // runtime
				"test-runtime",
				nil, // supportedModelFormat
				nil, // overlays
			).(*Decoder)

			// Test GetComponentType
			componentType := decoder.GetComponentType()
			g.Expect(componentType).To(gomega.Equal(tt.expectedComponentType))

			// Test GetServiceSuffix
			serviceSuffix := decoder.GetServiceSuffix()
			g.Expect(serviceSuffix).To(gomega.Equal(tt.expectedServiceSuffix))

			// Test GetComponentSpec
			componentSpec := decoder.GetComponentSpec()
			if tt.decoderSpec != nil {
				g.Expect(componentSpec).NotTo(gomega.BeNil())
				g.Expect(componentSpec).To(gomega.Equal(&tt.decoderSpec.ComponentExtensionSpec))
			} else {
				g.Expect(componentSpec).To(gomega.BeNil())
			}

			// Test ValidateSpec
			err := decoder.ValidateSpec()
			if tt.expectedValidationError {
				g.Expect(err).To(gomega.HaveOccurred())
				g.Expect(err.Error()).To(gomega.ContainSubstring("decoder spec is nil"))
			} else {
				g.Expect(err).NotTo(gomega.HaveOccurred())
			}
		})
	}
}

func TestDecoderSetupMocks(t *testing.T) {
	g := gomega.NewGomegaWithT(t)

	tests := []struct {
		name           string
		setupMocks     func(client.Client, kubernetes.Interface)
		expectedError  bool
		expectedConfig string
	}{
		{
			name: "Setup inferenceservice config",
			setupMocks: func(c client.Client, cs kubernetes.Interface) {
				// Create inferenceservice config
				cm := &v1.ConfigMap{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "inferenceservice-config",
						Namespace: "ome",
					},
					Data: map[string]string{
						"config": "{}",
					},
				}
				_, err := cs.CoreV1().ConfigMaps("ome").Create(context.TODO(), cm, metav1.CreateOptions{})
				g.Expect(err).NotTo(gomega.HaveOccurred())
			},
			expectedError:  false,
			expectedConfig: "{}",
		},
		{
			name:           "Setup error",
			setupMocks:     func(c client.Client, cs kubernetes.Interface) {},
			expectedError:  true,
			expectedConfig: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create fake client
			scheme := runtime.NewScheme()
			// Add v1 to scheme for ConfigMap
			g.Expect(v1.AddToScheme(scheme)).NotTo(gomega.HaveOccurred())

			c := ctrlclientfake.NewClientBuilder().
				WithScheme(scheme).
				Build()

			// Create fake clientset
			clientset := fake.NewClientset()

			// Setup mocks
			tt.setupMocks(c, clientset)

			// Get inferenceservice config from clientset
			cm, err := clientset.CoreV1().ConfigMaps("ome").Get(context.TODO(), "inferenceservice-config", metav1.GetOptions{})

			if tt.expectedError {
				g.Expect(err).To(gomega.HaveOccurred())
			} else {
				g.Expect(err).NotTo(gomega.HaveOccurred())
				g.Expect(cm.Data["config"]).To(gomega.Equal(tt.expectedConfig))
			}
		})
	}
}

// TestDecoderUsesLeaderTemplate pins the structural predicate that drives
// pod-template selection in decoder.reconcilePodSpec. Template selection
// keys on the presence of a Leader block, NOT on the dispatch mode: a
// multi-pod OMENative decoder (Leader/Worker set) must source the Leader
// template, otherwise reconcilePodSpec collapses onto the empty top-level
// runner and fails with "no containers found in pod spec and no runner
// spec provided", short-circuiting the component loop and leaving the
// decoder (and router) InferenceReplicas un-projected.
// decoderUsesLeaderTemplate decouples template selection from dispatch-mode
// classification, matching engineUsesLeaderTemplate.
func TestDecoderUsesLeaderTemplate(t *testing.T) {
	g := gomega.NewGomegaWithT(t)

	tests := []struct {
		name     string
		spec     *v1beta1.DecoderSpec
		expected bool
	}{
		{
			name:     "nil spec returns false",
			spec:     nil,
			expected: false,
		},
		{
			name:     "empty spec — no leader — false",
			spec:     &v1beta1.DecoderSpec{},
			expected: false,
		},
		{
			name: "leader set — true (regardless of mode)",
			spec: &v1beta1.DecoderSpec{
				Leader: &v1beta1.LeaderSpec{
					PodSpec: v1beta1.PodSpec{
						Containers: []v1.Container{{Name: "leader"}},
					},
				},
			},
			expected: true,
		},
		{
			name: "leader + worker set — true (multi-pod OMENative shape)",
			spec: &v1beta1.DecoderSpec{
				Leader: &v1beta1.LeaderSpec{
					PodSpec: v1beta1.PodSpec{
						Containers: []v1.Container{{Name: "leader"}},
					},
				},
				Worker: &v1beta1.WorkerSpec{
					Size: intPtr(2),
				},
			},
			expected: true,
		},
		{
			name: "worker only — false (template still from top-level)",
			spec: &v1beta1.DecoderSpec{
				Worker: &v1beta1.WorkerSpec{
					Size: intPtr(2),
				},
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g.Expect(decoderUsesLeaderTemplate(tt.spec)).To(gomega.Equal(tt.expected))
		})
	}
}
