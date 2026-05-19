package basemodel

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlclientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/basemodel/backends/sharded"
	shardedfake "github.com/ome-projects/ome/pkg/controller/v1beta1/basemodel/backends/sharded/fake"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/basemodel/shared"
)

func TestBaseModelReconcile(t *testing.T) {
	g := gomega.NewGomegaWithT(t)

	// Create scheme
	scheme := runtime.NewScheme()
	g.Expect(v1beta1.AddToScheme(scheme)).NotTo(gomega.HaveOccurred())
	g.Expect(corev1.AddToScheme(scheme)).NotTo(gomega.HaveOccurred())
	// batchv1 needed for cleanupStalePVCArtifacts which lists Jobs on
	// every non-PVC reconcile (idempotent no-op when none exist; the
	// List itself still requires the scheme).
	g.Expect(batchv1.AddToScheme(scheme)).NotTo(gomega.HaveOccurred())

	tests := []struct {
		name       string
		baseModel  *v1beta1.BaseModel
		setupMocks func(client.Client)
		validate   func(*testing.T, client.Client, *v1beta1.BaseModel, ctrl.Result, error)
		wantErr    bool
	}{
		{
			name: "New BaseModel gets finalizer added",
			baseModel: &v1beta1.BaseModel{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-model",
					Namespace: "default",
				},
				Spec: v1beta1.BaseModelSpec{
					ModelFormat: v1beta1.ModelFormat{
						Name: "safetensors",
					},
					Storage: &v1beta1.StorageSpec{
						StorageUri: stringPtr("oci://bucket/model"),
					},
				},
			},
			setupMocks: func(c client.Client) {
				// No setup needed for this test
			},
			validate: func(t *testing.T, c client.Client, baseModel *v1beta1.BaseModel, result ctrl.Result, reconcileErr error) {
				// Fetch the updated BaseModel
				updated := &v1beta1.BaseModel{}
				err := c.Get(context.TODO(), types.NamespacedName{
					Name:      baseModel.Name,
					Namespace: baseModel.Namespace,
				}, updated)
				g.Expect(err).NotTo(gomega.HaveOccurred())

				// Verify finalizer was added
				g.Expect(updated.Finalizers).To(gomega.ContainElement(constants.BaseModelFinalizer))
			},
		},
		{
			name: "Sharded BaseModel reports missing model cache provider and ignores ConfigMaps",
			baseModel: &v1beta1.BaseModel{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "sharded-model",
					Namespace:  "default",
					Finalizers: []string{constants.BaseModelFinalizer},
				},
				Spec: v1beta1.BaseModelSpec{
					ModelFormat: v1beta1.ModelFormat{
						Name: "safetensors",
					},
					Storage: &v1beta1.StorageSpec{
						StorageUri: stringPtr("oci://bucket/sharded-model"),
					},
					Distribution: distributionPtr(v1beta1.DistributionSharded),
				},
			},
			setupMocks: func(c client.Client) {
				node := &corev1.Node{
					ObjectMeta: metav1.ObjectMeta{
						Name: "worker-node-1",
					},
				}
				err := c.Create(context.TODO(), node)
				g.Expect(err).NotTo(gomega.HaveOccurred())

				modelEntry := shared.ModelEntry{
					Status: shared.ModelStatusReady,
				}
				entryData, _ := json.Marshal(modelEntry)

				configMap := &corev1.ConfigMap{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "worker-node-1",
						Namespace: constants.OMENamespace,
						Labels: map[string]string{
							constants.ModelStatusConfigMapLabel: "true",
						},
					},
					Data: map[string]string{
						"default.basemodel.sharded-model": string(entryData),
					},
				}
				err = c.Create(context.TODO(), configMap)
				g.Expect(err).NotTo(gomega.HaveOccurred())
			},
			validate: func(t *testing.T, c client.Client, baseModel *v1beta1.BaseModel, result ctrl.Result, reconcileErr error) {
				updated := &v1beta1.BaseModel{}
				err := c.Get(context.TODO(), types.NamespacedName{
					Name:      baseModel.Name,
					Namespace: baseModel.Namespace,
				}, updated)
				g.Expect(err).NotTo(gomega.HaveOccurred())

				g.Expect(result).To(gomega.Equal(ctrl.Result{}))
				validateProviderNotConfiguredShardedStatus(t, updated.Status, "oci://bucket/sharded-model")
			},
		},
		{
			name: "BaseModel with ConfigMap status updates to Ready",
			baseModel: &v1beta1.BaseModel{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "ready-model",
					Namespace:  "default",
					Finalizers: []string{constants.BaseModelFinalizer},
				},
				Spec: v1beta1.BaseModelSpec{
					ModelFormat: v1beta1.ModelFormat{
						Name: "pytorch",
					},
				},
			},
			setupMocks: func(c client.Client) {
				// Create ome namespace
				omeNamespace := &corev1.Namespace{
					ObjectMeta: metav1.ObjectMeta{
						Name: constants.OMENamespace,
					},
				}
				err := c.Create(context.TODO(), omeNamespace)
				g.Expect(err).NotTo(gomega.HaveOccurred())

				// Create node
				node := &corev1.Node{
					ObjectMeta: metav1.ObjectMeta{
						Name: "worker-node-1",
					},
				}
				err = c.Create(context.TODO(), node)
				g.Expect(err).NotTo(gomega.HaveOccurred())

				// Create ConfigMap with Ready status
				modelEntry := shared.ModelEntry{
					Status: shared.ModelStatusReady,
					Config: &shared.ModelConfig{
						ModelType:         "gpt2",
						ModelArchitecture: "GPT2LMHeadModel",
						MaxTokens:         2048,
					},
				}
				entryData, _ := json.Marshal(modelEntry)

				configMap := &corev1.ConfigMap{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "worker-node-1",
						Namespace: constants.OMENamespace,
						Labels: map[string]string{
							constants.ModelStatusConfigMapLabel: "true",
						},
					},
					Data: map[string]string{
						"default.basemodel.ready-model": string(entryData),
					},
				}
				err = c.Create(context.TODO(), configMap)
				g.Expect(err).NotTo(gomega.HaveOccurred())
			},
			validate: func(t *testing.T, c client.Client, baseModel *v1beta1.BaseModel, result ctrl.Result, reconcileErr error) {
				// Fetch the updated BaseModel
				updated := &v1beta1.BaseModel{}
				err := c.Get(context.TODO(), types.NamespacedName{
					Name:      baseModel.Name,
					Namespace: baseModel.Namespace,
				}, updated)
				g.Expect(err).NotTo(gomega.HaveOccurred())

				// Verify status was updated
				g.Expect(updated.Status.State).To(gomega.Equal(v1beta1.LifeCycleStateReady))
				g.Expect(updated.Status.NodesReady).To(gomega.ContainElement("worker-node-1"))
				g.Expect(updated.Status.NodesFailed).To(gomega.BeEmpty())

				// Verify spec was updated with config
				g.Expect(updated.Spec.ModelType).ToNot(gomega.BeNil())
				g.Expect(*updated.Spec.ModelType).To(gomega.Equal("gpt2"))
				g.Expect(updated.Spec.ModelArchitecture).ToNot(gomega.BeNil())
				g.Expect(*updated.Spec.ModelArchitecture).To(gomega.Equal("GPT2LMHeadModel"))
				g.Expect(updated.Spec.MaxTokens).ToNot(gomega.BeNil())
				g.Expect(*updated.Spec.MaxTokens).To(gomega.Equal(int32(2048)))
			},
		},
		{
			name: "BaseModel with multiple nodes - mixed status",
			baseModel: &v1beta1.BaseModel{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "mixed-status-model",
					Namespace:  "test-ns",
					Finalizers: []string{constants.BaseModelFinalizer},
				},
				Spec: v1beta1.BaseModelSpec{
					ModelFormat: v1beta1.ModelFormat{
						Name: "onnx",
					},
				},
			},
			setupMocks: func(c client.Client) {
				// Create ome namespace
				omeNamespace := &corev1.Namespace{
					ObjectMeta: metav1.ObjectMeta{
						Name: constants.OMENamespace,
					},
				}
				err := c.Create(context.TODO(), omeNamespace)
				g.Expect(err).NotTo(gomega.HaveOccurred())

				// Create nodes
				for _, nodeName := range []string{"node-1", "node-2", "node-3"} {
					node := &corev1.Node{
						ObjectMeta: metav1.ObjectMeta{
							Name: nodeName,
						},
					}
					err := c.Create(context.TODO(), node)
					g.Expect(err).NotTo(gomega.HaveOccurred())
				}

				// Create ConfigMaps with different statuses
				statuses := map[string]shared.ModelStatus{
					"node-1": shared.ModelStatusReady,
					"node-2": shared.ModelStatusFailed,
					"node-3": shared.ModelStatusUpdating,
				}

				for nodeName, status := range statuses {
					modelEntry := shared.ModelEntry{
						Status: status,
					}
					entryData, _ := json.Marshal(modelEntry)

					configMap := &corev1.ConfigMap{
						ObjectMeta: metav1.ObjectMeta{
							Name:      nodeName,
							Namespace: constants.OMENamespace,
							Labels: map[string]string{
								constants.ModelStatusConfigMapLabel: "true",
							},
						},
						Data: map[string]string{
							"test-ns.basemodel.mixed-status-model": string(entryData),
						},
					}
					err := c.Create(context.TODO(), configMap)
					g.Expect(err).NotTo(gomega.HaveOccurred())
				}
			},
			validate: func(t *testing.T, c client.Client, baseModel *v1beta1.BaseModel, result ctrl.Result, reconcileErr error) {
				updated := &v1beta1.BaseModel{}
				err := c.Get(context.TODO(), types.NamespacedName{
					Name:      baseModel.Name,
					Namespace: baseModel.Namespace,
				}, updated)
				g.Expect(err).NotTo(gomega.HaveOccurred())

				// Should be Ready because at least one node is ready
				g.Expect(updated.Status.State).To(gomega.Equal(v1beta1.LifeCycleStateReady))
				g.Expect(updated.Status.NodesReady).To(gomega.ContainElement("node-1"))
				g.Expect(updated.Status.NodesFailed).To(gomega.ContainElement("node-2"))
				g.Expect(updated.Status.NodesReady).To(gomega.HaveLen(1))
				g.Expect(updated.Status.NodesFailed).To(gomega.HaveLen(1))
			},
		},
		{
			name: "BaseModel deletion removes finalizer when no ConfigMaps exist",
			baseModel: &v1beta1.BaseModel{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "delete-me-no-configmaps",
					Namespace:         "default",
					Finalizers:        []string{constants.BaseModelFinalizer},
					DeletionTimestamp: &metav1.Time{Time: time.Now()},
				},
				Spec: v1beta1.BaseModelSpec{
					ModelFormat: v1beta1.ModelFormat{
						Name: "tensorflow",
					},
				},
			},
			setupMocks: func(c client.Client) {
				// Create ome namespace but no ConfigMaps
				omeNamespace := &corev1.Namespace{
					ObjectMeta: metav1.ObjectMeta{
						Name: constants.OMENamespace,
					},
				}
				err := c.Create(context.TODO(), omeNamespace)
				g.Expect(err).NotTo(gomega.HaveOccurred())
			},
			validate: func(t *testing.T, c client.Client, baseModel *v1beta1.BaseModel, result ctrl.Result, reconcileErr error) {
				updated := &v1beta1.BaseModel{}
				err := c.Get(context.TODO(), types.NamespacedName{
					Name:      baseModel.Name,
					Namespace: baseModel.Namespace,
				}, updated)

				// The object should exist (fake client behavior) but finalizer should be removed
				if err == nil {
					// Verify finalizer was removed
					g.Expect(updated.Finalizers).NotTo(gomega.ContainElement(constants.BaseModelFinalizer))
				} else {
					// If object is not found, that's also acceptable as it means deletion completed
					g.Expect(errors.IsNotFound(err)).To(gomega.BeTrue())
				}
			},
		},
		{
			name: "BaseModel deletion waits when ConfigMap entries exist but are not deleted",
			baseModel: &v1beta1.BaseModel{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "deletion-waiting-model",
					Namespace:         "default",
					Finalizers:        []string{constants.BaseModelFinalizer},
					DeletionTimestamp: &metav1.Time{Time: time.Now()},
				},
				Spec: v1beta1.BaseModelSpec{
					ModelFormat: v1beta1.ModelFormat{
						Name: "pytorch",
					},
				},
			},
			setupMocks: func(c client.Client) {
				// Create ome namespace
				omeNamespace := &corev1.Namespace{
					ObjectMeta: metav1.ObjectMeta{
						Name: constants.OMENamespace,
					},
				}
				err := c.Create(context.TODO(), omeNamespace)
				g.Expect(err).NotTo(gomega.HaveOccurred())

				// Create nodes
				for _, nodeName := range []string{"node-1", "node-2"} {
					node := &corev1.Node{
						ObjectMeta: metav1.ObjectMeta{
							Name: nodeName,
						},
					}
					err := c.Create(context.TODO(), node)
					g.Expect(err).NotTo(gomega.HaveOccurred())
				}

				// Create ConfigMaps with entries not yet deleted
				for _, nodeName := range []string{"node-1", "node-2"} {
					modelEntry := shared.ModelEntry{
						Status: shared.ModelStatusReady, // Not marked for deletion
					}
					entryData, _ := json.Marshal(modelEntry)

					configMap := &corev1.ConfigMap{
						ObjectMeta: metav1.ObjectMeta{
							Name:      nodeName,
							Namespace: constants.OMENamespace,
							Labels: map[string]string{
								constants.ModelStatusConfigMapLabel: "true",
							},
						},
						Data: map[string]string{
							"default.basemodel.deletion-waiting-model": string(entryData),
						},
					}
					err := c.Create(context.TODO(), configMap)
					g.Expect(err).NotTo(gomega.HaveOccurred())
				}
			},
			validate: func(t *testing.T, c client.Client, baseModel *v1beta1.BaseModel, result ctrl.Result, reconcileErr error) {
				updated := &v1beta1.BaseModel{}
				err := c.Get(context.TODO(), types.NamespacedName{
					Name:      baseModel.Name,
					Namespace: baseModel.Namespace,
				}, updated)
				g.Expect(err).NotTo(gomega.HaveOccurred())

				// Finalizer should still be present since deletion is waiting for ConfigMaps to be cleared
				g.Expect(updated.Finalizers).To(gomega.ContainElement(constants.BaseModelFinalizer))

				// The reconciler should have set a requeue delay when waiting for ConfigMaps to be cleared
				g.Expect(result.RequeueAfter).To(gomega.Equal(time.Second * 30))
			},
		},
		{
			name: "BaseModel deletion removes finalizer when ConfigMap entries are marked as deleted",
			baseModel: &v1beta1.BaseModel{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "deletion-complete-model",
					Namespace:         "default",
					Finalizers:        []string{constants.BaseModelFinalizer},
					DeletionTimestamp: &metav1.Time{Time: time.Now()},
				},
				Spec: v1beta1.BaseModelSpec{
					ModelFormat: v1beta1.ModelFormat{
						Name: "onnx",
					},
				},
			},
			setupMocks: func(c client.Client) {
				// Create ome namespace
				omeNamespace := &corev1.Namespace{
					ObjectMeta: metav1.ObjectMeta{
						Name: constants.OMENamespace,
					},
				}
				err := c.Create(context.TODO(), omeNamespace)
				g.Expect(err).NotTo(gomega.HaveOccurred())

				// Create nodes
				for _, nodeName := range []string{"node-1", "node-2"} {
					node := &corev1.Node{
						ObjectMeta: metav1.ObjectMeta{
							Name: nodeName,
						},
					}
					err := c.Create(context.TODO(), node)
					g.Expect(err).NotTo(gomega.HaveOccurred())
				}

				// Create ConfigMaps with entries marked as deleted
				for _, nodeName := range []string{"node-1", "node-2"} {
					modelEntry := shared.ModelEntry{
						Status: shared.ModelStatusDeleted, // Marked for deletion
					}
					entryData, _ := json.Marshal(modelEntry)

					configMap := &corev1.ConfigMap{
						ObjectMeta: metav1.ObjectMeta{
							Name:      nodeName,
							Namespace: constants.OMENamespace,
							Labels: map[string]string{
								constants.ModelStatusConfigMapLabel: "true",
							},
						},
						Data: map[string]string{
							"default.basemodel.deletion-complete-model": string(entryData),
						},
					}
					err := c.Create(context.TODO(), configMap)
					g.Expect(err).NotTo(gomega.HaveOccurred())
				}
			},
			validate: func(t *testing.T, c client.Client, baseModel *v1beta1.BaseModel, result ctrl.Result, reconcileErr error) {
				updated := &v1beta1.BaseModel{}
				err := c.Get(context.TODO(), types.NamespacedName{
					Name:      baseModel.Name,
					Namespace: baseModel.Namespace,
				}, updated)

				// Finalizer should be removed since all entries are marked as deleted
				if err == nil {
					g.Expect(updated.Finalizers).NotTo(gomega.ContainElement(constants.BaseModelFinalizer))
				} else {
					g.Expect(errors.IsNotFound(err)).To(gomega.BeTrue())
				}
			},
		},
		{
			name: "BaseModel deletion with mix of deleted and active entries waits",
			baseModel: &v1beta1.BaseModel{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "mixed-deletion-model",
					Namespace:         "default",
					Finalizers:        []string{constants.BaseModelFinalizer},
					DeletionTimestamp: &metav1.Time{Time: time.Now()},
				},
				Spec: v1beta1.BaseModelSpec{
					ModelFormat: v1beta1.ModelFormat{
						Name: "safetensors",
					},
				},
			},
			setupMocks: func(c client.Client) {
				// Create ome namespace
				omeNamespace := &corev1.Namespace{
					ObjectMeta: metav1.ObjectMeta{
						Name: constants.OMENamespace,
					},
				}
				err := c.Create(context.TODO(), omeNamespace)
				g.Expect(err).NotTo(gomega.HaveOccurred())

				// Create nodes
				for _, nodeName := range []string{"node-1", "node-2", "node-3"} {
					node := &corev1.Node{
						ObjectMeta: metav1.ObjectMeta{
							Name: nodeName,
						},
					}
					err := c.Create(context.TODO(), node)
					g.Expect(err).NotTo(gomega.HaveOccurred())
				}

				// Create ConfigMaps with mixed deletion status
				statuses := map[string]shared.ModelStatus{
					"node-1": shared.ModelStatusDeleted, // Marked for deletion
					"node-2": shared.ModelStatusReady,   // Not deleted
					"node-3": shared.ModelStatusFailed,  // Not deleted
				}

				for nodeName, status := range statuses {
					modelEntry := shared.ModelEntry{
						Status: status,
					}
					entryData, _ := json.Marshal(modelEntry)

					configMap := &corev1.ConfigMap{
						ObjectMeta: metav1.ObjectMeta{
							Name:      nodeName,
							Namespace: constants.OMENamespace,
							Labels: map[string]string{
								constants.ModelStatusConfigMapLabel: "true",
							},
						},
						Data: map[string]string{
							"default.basemodel.mixed-deletion-model": string(entryData),
						},
					}
					err := c.Create(context.TODO(), configMap)
					g.Expect(err).NotTo(gomega.HaveOccurred())
				}
			},
			validate: func(t *testing.T, c client.Client, baseModel *v1beta1.BaseModel, result ctrl.Result, reconcileErr error) {
				updated := &v1beta1.BaseModel{}
				err := c.Get(context.TODO(), types.NamespacedName{
					Name:      baseModel.Name,
					Namespace: baseModel.Namespace,
				}, updated)
				g.Expect(err).NotTo(gomega.HaveOccurred())

				// Finalizer should still be present since not all entries are marked for deletion
				g.Expect(updated.Finalizers).To(gomega.ContainElement(constants.BaseModelFinalizer))
			},
		},
		{
			name: "BaseModel with deleted node ignores ConfigMap",
			baseModel: &v1beta1.BaseModel{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "deleted-node-model",
					Namespace:  "default",
					Finalizers: []string{constants.BaseModelFinalizer},
				},
				Spec: v1beta1.BaseModelSpec{
					ModelFormat: v1beta1.ModelFormat{
						Name: "huggingface",
					},
				},
			},
			setupMocks: func(c client.Client) {
				// Create ConfigMap for non-existent node
				modelEntry := shared.ModelEntry{
					Status: shared.ModelStatusReady,
				}
				entryData, _ := json.Marshal(modelEntry)

				configMap := &corev1.ConfigMap{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "deleted-node",
						Namespace: constants.OMENamespace,
						Labels: map[string]string{
							constants.ModelStatusConfigMapLabel: "true",
						},
					},
					Data: map[string]string{
						"default.basemodel.deleted-node-model": string(entryData),
					},
				}
				err := c.Create(context.TODO(), configMap)
				g.Expect(err).NotTo(gomega.HaveOccurred())
			},
			validate: func(t *testing.T, c client.Client, baseModel *v1beta1.BaseModel, result ctrl.Result, reconcileErr error) {
				updated := &v1beta1.BaseModel{}
				err := c.Get(context.TODO(), types.NamespacedName{
					Name:      baseModel.Name,
					Namespace: baseModel.Namespace,
				}, updated)
				g.Expect(err).NotTo(gomega.HaveOccurred())

				// Should remain in InTransit state (no valid nodes)
				g.Expect(updated.Status.State).To(gomega.Equal(v1beta1.LifeCycleStateInTransit))
				g.Expect(updated.Status.NodesReady).To(gomega.BeEmpty())
				g.Expect(updated.Status.NodesFailed).To(gomega.BeEmpty())
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Create client
			c := ctrlclientfake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(tt.baseModel).
				WithStatusSubresource(tt.baseModel).
				Build()

			// Setup test mocks
			tt.setupMocks(c)

			// Run reconciliation
			reconciler := &BaseModelReconciler{
				Client: c,
				Scheme: c.Scheme(),
			}

			result, err := reconciler.Reconcile(context.TODO(), ctrl.Request{
				NamespacedName: types.NamespacedName{
					Namespace: tt.baseModel.Namespace,
					Name:      tt.baseModel.Name,
				},
			})
			if tt.wantErr {
				g.Expect(err).To(gomega.HaveOccurred())
			} else {
				g.Expect(err).NotTo(gomega.HaveOccurred())
			}

			// Run validation
			tt.validate(t, c, tt.baseModel, result, err)
		})
	}
}

func TestClusterBaseModelReconcile(t *testing.T) {
	g := gomega.NewGomegaWithT(t)

	// Create scheme
	scheme := runtime.NewScheme()
	g.Expect(v1beta1.AddToScheme(scheme)).NotTo(gomega.HaveOccurred())
	g.Expect(corev1.AddToScheme(scheme)).NotTo(gomega.HaveOccurred())
	// batchv1 needed for cleanupStalePVCArtifacts which lists Jobs on
	// every non-PVC reconcile (idempotent no-op when none exist; the
	// List itself still requires the scheme).
	g.Expect(batchv1.AddToScheme(scheme)).NotTo(gomega.HaveOccurred())

	tests := []struct {
		name             string
		clusterBaseModel *v1beta1.ClusterBaseModel
		setupMocks       func(client.Client)
		validate         func(*testing.T, client.Client, *v1beta1.ClusterBaseModel)
		wantErr          bool
	}{
		{
			name: "New ClusterBaseModel gets finalizer",
			clusterBaseModel: &v1beta1.ClusterBaseModel{
				ObjectMeta: metav1.ObjectMeta{
					Name: "cluster-model",
				},
				Spec: v1beta1.BaseModelSpec{
					ModelFormat: v1beta1.ModelFormat{
						Name: "gguf",
					},
				},
			},
			setupMocks: func(c client.Client) {},
			validate: func(t *testing.T, c client.Client, clusterBaseModel *v1beta1.ClusterBaseModel) {
				updated := &v1beta1.ClusterBaseModel{}
				err := c.Get(context.TODO(), types.NamespacedName{
					Name: clusterBaseModel.Name,
				}, updated)
				g.Expect(err).NotTo(gomega.HaveOccurred())
				g.Expect(updated.Finalizers).To(gomega.ContainElement(constants.ClusterBaseModelFinalizer))
			},
		},
		{
			name: "Sharded ClusterBaseModel reports missing model cache provider and ignores ConfigMaps",
			clusterBaseModel: &v1beta1.ClusterBaseModel{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "sharded-cluster-model",
					Finalizers: []string{constants.ClusterBaseModelFinalizer},
				},
				Spec: v1beta1.BaseModelSpec{
					ModelFormat: v1beta1.ModelFormat{
						Name: "safetensors",
					},
					Storage: &v1beta1.StorageSpec{
						StorageUri: stringPtr("s3://bucket/sharded-cluster-model"),
					},
					Distribution: distributionPtr(v1beta1.DistributionSharded),
				},
			},
			setupMocks: func(c client.Client) {
				node := &corev1.Node{
					ObjectMeta: metav1.ObjectMeta{
						Name: "cluster-node-1",
					},
				}
				err := c.Create(context.TODO(), node)
				g.Expect(err).NotTo(gomega.HaveOccurred())

				modelEntry := shared.ModelEntry{
					Status: shared.ModelStatusReady,
				}
				entryData, _ := json.Marshal(modelEntry)

				configMap := &corev1.ConfigMap{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "cluster-node-1",
						Namespace: constants.OMENamespace,
						Labels: map[string]string{
							constants.ModelStatusConfigMapLabel: "true",
						},
					},
					Data: map[string]string{
						"clusterbasemodel.sharded-cluster-model": string(entryData),
					},
				}
				err = c.Create(context.TODO(), configMap)
				g.Expect(err).NotTo(gomega.HaveOccurred())
			},
			validate: func(t *testing.T, c client.Client, clusterBaseModel *v1beta1.ClusterBaseModel) {
				updated := &v1beta1.ClusterBaseModel{}
				err := c.Get(context.TODO(), types.NamespacedName{
					Name: clusterBaseModel.Name,
				}, updated)
				g.Expect(err).NotTo(gomega.HaveOccurred())

				validateProviderNotConfiguredShardedStatus(t, updated.Status, "s3://bucket/sharded-cluster-model")
			},
		},
		{
			name: "ClusterBaseModel status update from multiple nodes",
			clusterBaseModel: &v1beta1.ClusterBaseModel{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "multi-node-cluster-model",
					Finalizers: []string{constants.ClusterBaseModelFinalizer},
				},
				Spec: v1beta1.BaseModelSpec{
					ModelFormat: v1beta1.ModelFormat{
						Name: "vllm",
					},
				},
			},
			setupMocks: func(c client.Client) {
				// Create ome namespace
				omeNamespace := &corev1.Namespace{
					ObjectMeta: metav1.ObjectMeta{
						Name: constants.OMENamespace,
					},
				}
				err := c.Create(context.TODO(), omeNamespace)
				g.Expect(err).NotTo(gomega.HaveOccurred())

				// Create nodes
				for i := 1; i <= 3; i++ {
					node := &corev1.Node{
						ObjectMeta: metav1.ObjectMeta{
							Name: fmt.Sprintf("cluster-node-%d", i),
						},
					}
					err := c.Create(context.TODO(), node)
					g.Expect(err).NotTo(gomega.HaveOccurred())
				}

				// Create ConfigMaps - all ready
				for i := 1; i <= 3; i++ {
					modelEntry := shared.ModelEntry{
						Status: shared.ModelStatusReady,
						Config: &shared.ModelConfig{
							ModelFramework: map[string]string{
								"name":    "transformers",
								"version": "4.21.0",
							},
						},
					}
					entryData, _ := json.Marshal(modelEntry)

					configMap := &corev1.ConfigMap{
						ObjectMeta: metav1.ObjectMeta{
							Name:      fmt.Sprintf("cluster-node-%d", i),
							Namespace: constants.OMENamespace,
							Labels: map[string]string{
								constants.ModelStatusConfigMapLabel: "true",
							},
						},
						Data: map[string]string{
							"clusterbasemodel.multi-node-cluster-model": string(entryData),
						},
					}
					err := c.Create(context.TODO(), configMap)
					g.Expect(err).NotTo(gomega.HaveOccurred())
				}
			},
			validate: func(t *testing.T, c client.Client, clusterBaseModel *v1beta1.ClusterBaseModel) {
				updated := &v1beta1.ClusterBaseModel{}
				err := c.Get(context.TODO(), types.NamespacedName{
					Name: clusterBaseModel.Name,
				}, updated)
				g.Expect(err).NotTo(gomega.HaveOccurred())

				g.Expect(updated.Status.State).To(gomega.Equal(v1beta1.LifeCycleStateReady))
				g.Expect(updated.Status.NodesReady).To(gomega.HaveLen(3))
				g.Expect(updated.Status.NodesReady).To(gomega.ContainElements("cluster-node-1", "cluster-node-2", "cluster-node-3"))

				// Verify spec updates from config
				g.Expect(updated.Spec.ModelFramework).ToNot(gomega.BeNil())
				g.Expect(updated.Spec.ModelFramework.Name).To(gomega.Equal("transformers"))
				g.Expect(updated.Spec.ModelFramework.Version).ToNot(gomega.BeNil())
				g.Expect(*updated.Spec.ModelFramework.Version).To(gomega.Equal("4.21.0"))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := ctrlclientfake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(tt.clusterBaseModel).
				WithStatusSubresource(tt.clusterBaseModel).
				Build()

			if tt.setupMocks != nil {
				tt.setupMocks(c)
			}

			reconciler := &ClusterBaseModelReconciler{
				Client: c,
				Log:    ctrl.Log.WithName("test"),
				Scheme: scheme,
			}

			req := ctrl.Request{
				NamespacedName: types.NamespacedName{
					Name: tt.clusterBaseModel.Name,
				},
			}
			result, err := reconciler.Reconcile(context.TODO(), req)

			if tt.wantErr {
				g.Expect(err).To(gomega.HaveOccurred())
			} else {
				g.Expect(err).NotTo(gomega.HaveOccurred())
				g.Expect(result).To(gomega.Equal(ctrl.Result{}))

				if tt.validate != nil {
					tt.validate(t, c, tt.clusterBaseModel)
				}
			}
		})
	}
}

// TestBaseModelReconcileWithModelCache covers the lazy-fetch flow:
// (1) CheckSource → SourceReachable=True with 5s requeue, (2) metadata
// parse after source reachable → Ready=True, (3) identity changes
// (URI or backend) re-init the cache fields and re-run CheckSource.
// Prewarm is intentionally absent — cluster_cache daemons pull bytes
// on demand at first inference; there is no separate placement step.
func TestBaseModelReconcileWithModelCache(t *testing.T) {
	g := gomega.NewGomegaWithT(t)

	scheme := runtime.NewScheme()
	g.Expect(v1beta1.AddToScheme(scheme)).NotTo(gomega.HaveOccurred())
	g.Expect(corev1.AddToScheme(scheme)).NotTo(gomega.HaveOccurred())
	g.Expect(batchv1.AddToScheme(scheme)).NotTo(gomega.HaveOccurred())

	tests := []struct {
		name        string
		status      v1beta1.ModelStatusSpec
		modelCache  *shardedfake.Client
		validate    func(*testing.T, client.Client, *shardedfake.Client, ctrl.Result)
		wantRequeue time.Duration
	}{
		{
			name: "source check success sets SourceReachable and requeues",
			modelCache: &shardedfake.Client{
				CheckSourceFunc: func(ctx context.Context, source sharded.Source) (*sharded.SourceStatus, error) {
					return &sharded.SourceStatus{Reachable: true}, nil
				},
			},
			wantRequeue: 5 * time.Second,
			validate: func(t *testing.T, c client.Client, cache *shardedfake.Client, result ctrl.Result) {
				updated := getBaseModelForTest(t, c, "default", "cached-model")

				sourceReachable := meta.FindStatusCondition(updated.Status.Conditions, v1beta1.ModelConditionSourceReachable)
				g.Expect(sourceReachable).ToNot(gomega.BeNil())
				g.Expect(sourceReachable.Status).To(gomega.Equal(metav1.ConditionTrue))
				g.Expect(sourceReachable.Reason).To(gomega.Equal(v1beta1.ModelConditionReasonSourceReachable))
				g.Expect(updated.Status.Cache.Backend).To(gomega.Equal(sharded.ProviderClusterCache))
				g.Expect(cache.CheckSourceCalls()).To(gomega.Equal([]sharded.Source{{URI: "s3://bucket/cached-model"}}))
				g.Expect(cache.InspectSourceCalls()).To(gomega.BeEmpty())
				g.Expect(result.RequeueAfter).To(gomega.Equal(5 * time.Second))
			},
		},
		{
			name: "metadata parse succeeds and marks Ready after SourceReachable",
			status: v1beta1.ModelStatusSpec{
				Conditions: []metav1.Condition{
					{
						Type:   v1beta1.ModelConditionSourceReachable,
						Status: metav1.ConditionTrue,
						Reason: v1beta1.ModelConditionReasonSourceReachable,
					},
				},
			},
			modelCache: &shardedfake.Client{
				InspectSourceFunc: func(ctx context.Context, source sharded.Source) (*sharded.SourceInspection, error) {
					return shardedInspectionForTest(t, source), nil
				},
			},
			validate: func(t *testing.T, c client.Client, cache *shardedfake.Client, result ctrl.Result) {
				updated := getBaseModelForTest(t, c, "default", "cached-model")

				g.Expect(updated.Status.State).To(gomega.Equal(v1beta1.LifeCycleStateReady))
				ready := meta.FindStatusCondition(updated.Status.Conditions, v1beta1.ModelConditionReady)
				g.Expect(ready).ToNot(gomega.BeNil())
				g.Expect(ready.Status).To(gomega.Equal(metav1.ConditionTrue))
				g.Expect(ready.Reason).To(gomega.Equal(v1beta1.ModelConditionReasonMetadataParsed))
				g.Expect(updated.Spec.ModelArchitecture).ToNot(gomega.BeNil())
				g.Expect(*updated.Spec.ModelArchitecture).To(gomega.Equal("PhiModel"))
				g.Expect(cache.CheckSourceCalls()).To(gomega.BeEmpty())
				g.Expect(cache.InspectSourceCalls()).To(gomega.Equal([]sharded.Source{{URI: "s3://bucket/cached-model"}}))
				g.Expect(result).To(gomega.Equal(ctrl.Result{}))
			},
		},
		{
			name: "source URI change resets cache identity and re-runs CheckSource",
			status: v1beta1.ModelStatusSpec{
				State: v1beta1.LifeCycleStateImporting,
				Cache: &v1beta1.ModelCacheStatus{
					Backend:   sharded.ProviderClusterCache,
					SourceUri: "s3://bucket/old-model",
				},
				Conditions: []metav1.Condition{
					{
						Type:   v1beta1.ModelConditionSourceReachable,
						Status: metav1.ConditionTrue,
						Reason: v1beta1.ModelConditionReasonSourceReachable,
					},
				},
			},
			modelCache:  &shardedfake.Client{},
			wantRequeue: 5 * time.Second,
			validate: func(t *testing.T, c client.Client, cache *shardedfake.Client, result ctrl.Result) {
				updated := getBaseModelForTest(t, c, "default", "cached-model")

				g.Expect(updated.Status.Cache.SourceUri).To(gomega.Equal("s3://bucket/cached-model"))
				sourceReachable := meta.FindStatusCondition(updated.Status.Conditions, v1beta1.ModelConditionSourceReachable)
				g.Expect(sourceReachable).ToNot(gomega.BeNil())
				g.Expect(sourceReachable.Status).To(gomega.Equal(metav1.ConditionUnknown))
				g.Expect(sourceReachable.Reason).To(gomega.Equal(v1beta1.ModelConditionReasonNotChecked))
				g.Expect(cache.CheckSourceCalls()).To(gomega.BeEmpty(),
					"identity-change reset returns before CheckSource; the next reconcile fires it")
				g.Expect(result.RequeueAfter).To(gomega.Equal(5 * time.Second))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			baseModel := &v1beta1.BaseModel{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "cached-model",
					Namespace:  "default",
					Finalizers: []string{constants.BaseModelFinalizer},
				},
				Spec: v1beta1.BaseModelSpec{
					ModelFormat:  v1beta1.ModelFormat{Name: "safetensors"},
					Storage:      &v1beta1.StorageSpec{StorageUri: stringPtr("s3://bucket/cached-model")},
					Distribution: distributionPtr(v1beta1.DistributionSharded),
				},
				Status: tt.status,
			}

			c := ctrlclientfake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(baseModel).
				WithStatusSubresource(baseModel).
				Build()

			reconciler := &BaseModelReconciler{
				Client:     c,
				Scheme:     c.Scheme(),
				ModelCache: sharded.NewProvider(sharded.ProviderClusterCache, tt.modelCache),
			}

			result, err := reconciler.Reconcile(context.TODO(), ctrl.Request{
				NamespacedName: types.NamespacedName{
					Namespace: baseModel.Namespace,
					Name:      baseModel.Name,
				},
			})
			g.Expect(err).NotTo(gomega.HaveOccurred())
			g.Expect(result.RequeueAfter).To(gomega.Equal(tt.wantRequeue))

			tt.validate(t, c, tt.modelCache, result)
		})
	}
}

// TestSharded_URIChange_ResetsCacheAndReRunsCheckSource pins the
// URI-change → reset → re-CheckSource chain. Without prewarm in the
// flow, the user-visible regression is "spec URI changes but
// status.cache.sourceUri stays stale" — this drives Reconcile until
// that update propagates.
func TestSharded_URIChange_ResetsCacheAndReRunsCheckSource(t *testing.T) {
	g := gomega.NewGomegaWithT(t)

	scheme := runtime.NewScheme()
	g.Expect(v1beta1.AddToScheme(scheme)).NotTo(gomega.HaveOccurred())
	g.Expect(corev1.AddToScheme(scheme)).NotTo(gomega.HaveOccurred())
	g.Expect(batchv1.AddToScheme(scheme)).NotTo(gomega.HaveOccurred())

	cacheClient := &shardedfake.Client{
		CheckSourceFunc: func(_ context.Context, _ sharded.Source) (*sharded.SourceStatus, error) {
			return &sharded.SourceStatus{Reachable: true, Reason: "SourceReachable"}, nil
		},
		InspectSourceFunc: func(_ context.Context, src sharded.Source) (*sharded.SourceInspection, error) {
			return &sharded.SourceInspection{
				Objects: []sharded.Object{{Source: src, Path: src.URI + "/config.json"}},
				Metadata: sharded.ModelMetadataInputs{
					ConfigJSON: []byte(`{"model_type":"phi","architectures":["PhiForCausalLM"]}`),
				},
			}, nil
		},
	}

	const v1URI = "oci://n/test/b/models/o/llama-v1"
	const v2URI = "oci://n/test/b/models/o/llama-v2"

	cbm := &v1beta1.ClusterBaseModel{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "uri-change-cbm",
			Finalizers: []string{constants.ClusterBaseModelFinalizer},
		},
		Spec: v1beta1.BaseModelSpec{
			ModelFormat:  v1beta1.ModelFormat{Name: "safetensors"},
			Storage:      &v1beta1.StorageSpec{StorageUri: stringPtr(v1URI)},
			Distribution: distributionPtr(v1beta1.DistributionSharded),
		},
	}
	kubeClient := ctrlclientfake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cbm).
		WithStatusSubresource(cbm).
		Build()

	reconciler := &ClusterBaseModelReconciler{
		Client:     kubeClient,
		Scheme:     kubeClient.Scheme(),
		ModelCache: sharded.NewProvider("test_cache", cacheClient),
	}

	reconcileOnce := func() {
		_, err := reconciler.Reconcile(context.TODO(), ctrl.Request{
			NamespacedName: types.NamespacedName{Name: cbm.Name},
		})
		g.Expect(err).NotTo(gomega.HaveOccurred())
	}
	get := func() *v1beta1.ClusterBaseModel {
		got := &v1beta1.ClusterBaseModel{}
		g.Expect(kubeClient.Get(context.TODO(), types.NamespacedName{Name: cbm.Name}, got)).To(gomega.Succeed())
		return got
	}

	// Phase 1: drive reconciles until CheckSource runs against v1 and
	// status records that URI. Upper bound of 10 iterations is generous;
	// in practice it converges in 2 (init → CheckSource).
	for i := 0; i < 10; i++ {
		reconcileOnce()
		if got := get(); got.Status.Cache != nil && got.Status.Cache.SourceUri == v1URI {
			break
		}
	}
	g.Expect(get().Status.Cache.SourceUri).To(gomega.Equal(v1URI))
	g.Expect(cacheClient.CheckSourceCalls()).To(gomega.HaveLen(1))
	g.Expect(cacheClient.CheckSourceCalls()[0].URI).To(gomega.Equal(v1URI))

	// Mutate spec to v2 — the user-visible event that should reset cache identity.
	{
		got := get()
		got.Spec.Storage.StorageUri = stringPtr(v2URI)
		g.Expect(kubeClient.Update(context.TODO(), got)).To(gomega.Succeed())
	}

	// Phase 2: drive reconciles until the fresh CheckSource fires for v2.
	for i := 0; i < 10 && len(cacheClient.CheckSourceCalls()) < 2; i++ {
		reconcileOnce()
	}
	g.Expect(cacheClient.CheckSourceCalls()).To(gomega.HaveLen(2),
		"URI change must trigger a fresh CheckSource")
	g.Expect(cacheClient.CheckSourceCalls()[1].URI).To(gomega.Equal(v2URI),
		"fresh CheckSource must use the new URI")
	g.Expect(get().Status.Cache.SourceUri).To(gomega.Equal(v2URI))
}

func TestUpdateSpecWithConfig(t *testing.T) {
	g := gomega.NewGomegaWithT(t)

	tests := []struct {
		name         string
		initialSpec  *v1beta1.BaseModelSpec
		config       *shared.ModelConfig
		expectUpdate bool
		validateSpec func(*v1beta1.BaseModelSpec)
	}{
		{
			name: "Complete config update",
			initialSpec: &v1beta1.BaseModelSpec{
				ModelFormat: v1beta1.ModelFormat{
					Name: "", // Empty so it can be updated
				},
			},
			config: &shared.ModelConfig{
				ModelType:          "llama",
				ModelArchitecture:  "LlamaForCausalLM",
				Quantization:       "nvfp4",
				ModelParameterSize: "7B",
				ModelCapabilities:  []string{"TEXT_GENERATION", "CHAT"},
				ModelFramework: map[string]string{
					"name":    "transformers",
					"version": "4.21.0",
				},
				ModelFormat: map[string]string{
					"name":    "safetensors",
					"version": "0.3.0",
				},
				MaxTokens: 4096,
			},
			expectUpdate: true,
			validateSpec: func(spec *v1beta1.BaseModelSpec) {
				g.Expect(spec.ModelType).ToNot(gomega.BeNil())
				g.Expect(*spec.ModelType).To(gomega.Equal("llama"))
				g.Expect(spec.ModelArchitecture).ToNot(gomega.BeNil())
				g.Expect(*spec.ModelArchitecture).To(gomega.Equal("LlamaForCausalLM"))
				g.Expect(spec.Quantization).ToNot(gomega.BeNil())
				g.Expect(*spec.Quantization).To(gomega.Equal(v1beta1.ModelQuantization("nvfp4")))
				g.Expect(spec.ModelParameterSize).ToNot(gomega.BeNil())
				g.Expect(*spec.ModelParameterSize).To(gomega.Equal("7B"))
				g.Expect(spec.ModelCapabilities).To(gomega.Equal([]string{"TEXT_GENERATION", "CHAT"}))
				g.Expect(spec.ModelFramework).ToNot(gomega.BeNil())
				g.Expect(spec.ModelFramework.Name).To(gomega.Equal("transformers"))
				g.Expect(*spec.ModelFramework.Version).To(gomega.Equal("4.21.0"))
				g.Expect(spec.ModelFormat.Name).To(gomega.Equal("safetensors"))
				g.Expect(*spec.ModelFormat.Version).To(gomega.Equal("0.3.0"))
				g.Expect(spec.MaxTokens).ToNot(gomega.BeNil())
				g.Expect(*spec.MaxTokens).To(gomega.Equal(int32(4096)))
			},
		},
		{
			name: "No update when fields already set",
			initialSpec: &v1beta1.BaseModelSpec{
				ModelType:         stringPtr("existing-type"),
				ModelArchitecture: stringPtr("existing-arch"),
				Quantization:      quantPtr("existing-quant"),
				ModelFormat: v1beta1.ModelFormat{
					Name:    "existing-format",
					Version: stringPtr("1.0.0"),
				},
				MaxTokens: int32Ptr(2048),
			},
			config: &shared.ModelConfig{
				ModelType:         "new-type",
				ModelArchitecture: "new-arch",
				Quantization:      "new-quant",
				ModelFormat: map[string]string{
					"name":    "new-format",
					"version": "2.0.0",
				},
				MaxTokens: 4096,
			},
			expectUpdate: false,
			validateSpec: func(spec *v1beta1.BaseModelSpec) {
				// Values should remain unchanged
				g.Expect(*spec.ModelType).To(gomega.Equal("existing-type"))
				g.Expect(*spec.ModelArchitecture).To(gomega.Equal("existing-arch"))
				g.Expect(*spec.Quantization).To(gomega.Equal(v1beta1.ModelQuantization("existing-quant")))
				g.Expect(spec.ModelFormat.Name).To(gomega.Equal("existing-format"))
				g.Expect(*spec.ModelFormat.Version).To(gomega.Equal("1.0.0"))
				g.Expect(*spec.MaxTokens).To(gomega.Equal(int32(2048)))
			},
		},
		{
			name:         "Nil inputs return false",
			initialSpec:  nil,
			config:       &shared.ModelConfig{},
			expectUpdate: false,
		},
		{
			name: "Nil config returns false",
			initialSpec: &v1beta1.BaseModelSpec{
				ModelFormat: v1beta1.ModelFormat{Name: "test"},
			},
			config:       nil,
			expectUpdate: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			updated := shared.UpdateSpecWithConfig(tt.initialSpec, tt.config)
			g.Expect(updated).To(gomega.Equal(tt.expectUpdate))

			if tt.validateSpec != nil && tt.initialSpec != nil {
				tt.validateSpec(tt.initialSpec)
			}
		})
	}
}

func TestShardedBaseModelDeletionShortCircuitsConfigMapWait(t *testing.T) {
	g := gomega.NewGomegaWithT(t)

	scheme := runtime.NewScheme()
	g.Expect(v1beta1.AddToScheme(scheme)).NotTo(gomega.HaveOccurred())
	g.Expect(corev1.AddToScheme(scheme)).NotTo(gomega.HaveOccurred())
	g.Expect(batchv1.AddToScheme(scheme)).NotTo(gomega.HaveOccurred())

	t.Run("Sharded BaseModel with stale per-node entries: finalizer removed, entries scrubbed", func(t *testing.T) {
		baseModel := &v1beta1.BaseModel{
			ObjectMeta: metav1.ObjectMeta{
				Name:              "sharded-delete-me",
				Namespace:         "default",
				Finalizers:        []string{constants.BaseModelFinalizer},
				DeletionTimestamp: &metav1.Time{Time: time.Now()},
			},
			Spec: v1beta1.BaseModelSpec{
				ModelFormat:  v1beta1.ModelFormat{Name: "safetensors"},
				Storage:      &v1beta1.StorageSpec{StorageUri: stringPtr("s3://b/m")},
				Distribution: distributionPtr(v1beta1.DistributionSharded),
			},
		}

		c := ctrlclientfake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(baseModel).
			Build()

		// Simulate the production scenario: many nodes left a stale entry
		// from a prior PerNode lifecycle (or the model was always Sharded
		// and model-agent never marked them deleted — either way the wait
		// never converged before this fix).
		modelKey := constants.GetModelConfigMapKey("default", "sharded-delete-me", false)
		entry, _ := json.Marshal(shared.ModelEntry{Status: shared.ModelStatusReady})
		for i := 1; i <= 5; i++ {
			node := fmt.Sprintf("node-%d", i)
			cm := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      node,
					Namespace: constants.OMENamespace,
					Labels:    map[string]string{constants.ModelStatusConfigMapLabel: "true"},
				},
				Data: map[string]string{modelKey: string(entry)},
			}
			g.Expect(c.Create(context.TODO(), cm)).NotTo(gomega.HaveOccurred())
		}

		reconciler := &BaseModelReconciler{Client: c, Scheme: scheme}
		result, err := reconciler.Reconcile(context.TODO(), ctrl.Request{
			NamespacedName: types.NamespacedName{Name: baseModel.Name, Namespace: baseModel.Namespace},
		})
		g.Expect(err).NotTo(gomega.HaveOccurred())
		g.Expect(result.RequeueAfter).To(gomega.Equal(time.Duration(0)), "must NOT requeue waiting on per-node handshake")

		// Finalizer must be gone (object may be GC'd by the fake client).
		updated := &v1beta1.BaseModel{}
		getErr := c.Get(context.TODO(), types.NamespacedName{Name: baseModel.Name, Namespace: baseModel.Namespace}, updated)
		if getErr == nil {
			g.Expect(updated.Finalizers).NotTo(gomega.ContainElement(constants.BaseModelFinalizer))
		} else {
			g.Expect(errors.IsNotFound(getErr)).To(gomega.BeTrue())
		}

		// Stale entries must be scrubbed so a re-creation with the same name
		// doesn't inherit phantom "ready" status.
		for i := 1; i <= 5; i++ {
			cm := &corev1.ConfigMap{}
			err := c.Get(context.TODO(), types.NamespacedName{Name: fmt.Sprintf("node-%d", i), Namespace: constants.OMENamespace}, cm)
			g.Expect(err).NotTo(gomega.HaveOccurred())
			_, exists := cm.Data[modelKey]
			g.Expect(exists).To(gomega.BeFalse(), "stale per-node entry for the deleted Sharded model must be scrubbed on node-%d", i)
		}
	})

	t.Run("Sharded ClusterBaseModel with stale per-node entries: finalizer removed, entries scrubbed", func(t *testing.T) {
		cbm := &v1beta1.ClusterBaseModel{
			ObjectMeta: metav1.ObjectMeta{
				Name:              "sharded-cbm-delete-me",
				Finalizers:        []string{constants.ClusterBaseModelFinalizer},
				DeletionTimestamp: &metav1.Time{Time: time.Now()},
			},
			Spec: v1beta1.BaseModelSpec{
				ModelFormat:  v1beta1.ModelFormat{Name: "safetensors"},
				Storage:      &v1beta1.StorageSpec{StorageUri: stringPtr("s3://b/m")},
				Distribution: distributionPtr(v1beta1.DistributionSharded),
			},
		}

		c := ctrlclientfake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(cbm).
			Build()

		modelKey := constants.GetModelConfigMapKey("", "sharded-cbm-delete-me", true)
		entry, _ := json.Marshal(shared.ModelEntry{Status: shared.ModelStatusReady})
		for i := 1; i <= 3; i++ {
			node := fmt.Sprintf("cluster-node-%d", i)
			cm := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      node,
					Namespace: constants.OMENamespace,
					Labels:    map[string]string{constants.ModelStatusConfigMapLabel: "true"},
				},
				Data: map[string]string{modelKey: string(entry)},
			}
			g.Expect(c.Create(context.TODO(), cm)).NotTo(gomega.HaveOccurred())
		}

		reconciler := &ClusterBaseModelReconciler{Client: c, Scheme: scheme}
		result, err := reconciler.Reconcile(context.TODO(), ctrl.Request{
			NamespacedName: types.NamespacedName{Name: cbm.Name},
		})
		g.Expect(err).NotTo(gomega.HaveOccurred())
		g.Expect(result.RequeueAfter).To(gomega.Equal(time.Duration(0)))

		updated := &v1beta1.ClusterBaseModel{}
		getErr := c.Get(context.TODO(), types.NamespacedName{Name: cbm.Name}, updated)
		if getErr == nil {
			g.Expect(updated.Finalizers).NotTo(gomega.ContainElement(constants.ClusterBaseModelFinalizer))
		} else {
			g.Expect(errors.IsNotFound(getErr)).To(gomega.BeTrue())
		}

		for i := 1; i <= 3; i++ {
			cm := &corev1.ConfigMap{}
			err := c.Get(context.TODO(), types.NamespacedName{Name: fmt.Sprintf("cluster-node-%d", i), Namespace: constants.OMENamespace}, cm)
			g.Expect(err).NotTo(gomega.HaveOccurred())
			_, exists := cm.Data[modelKey]
			g.Expect(exists).To(gomega.BeFalse(), "stale per-node entry must be scrubbed on cluster-node-%d", i)
		}
	})

	t.Run("Sharded BaseModel with no leftover entries: finalizer removed immediately", func(t *testing.T) {
		// The always-Sharded case (cluster_cache from day one) — there
		// should never have been per-node entries, and the scrub must be a
		// no-op rather than a failure.
		baseModel := &v1beta1.BaseModel{
			ObjectMeta: metav1.ObjectMeta{
				Name:              "pristine-sharded",
				Namespace:         "default",
				Finalizers:        []string{constants.BaseModelFinalizer},
				DeletionTimestamp: &metav1.Time{Time: time.Now()},
			},
			Spec: v1beta1.BaseModelSpec{
				ModelFormat:  v1beta1.ModelFormat{Name: "safetensors"},
				Storage:      &v1beta1.StorageSpec{StorageUri: stringPtr("s3://b/m")},
				Distribution: distributionPtr(v1beta1.DistributionSharded),
			},
		}

		c := ctrlclientfake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(baseModel).
			Build()

		reconciler := &BaseModelReconciler{Client: c, Scheme: scheme}
		result, err := reconciler.Reconcile(context.TODO(), ctrl.Request{
			NamespacedName: types.NamespacedName{Name: baseModel.Name, Namespace: baseModel.Namespace},
		})
		g.Expect(err).NotTo(gomega.HaveOccurred())
		g.Expect(result.RequeueAfter).To(gomega.Equal(time.Duration(0)))

		updated := &v1beta1.BaseModel{}
		getErr := c.Get(context.TODO(), types.NamespacedName{Name: baseModel.Name, Namespace: baseModel.Namespace}, updated)
		if getErr == nil {
			g.Expect(updated.Finalizers).NotTo(gomega.ContainElement(constants.BaseModelFinalizer))
		} else {
			g.Expect(errors.IsNotFound(getErr)).To(gomega.BeTrue())
		}
	})

	t.Run("Non-Sharded BaseModel deletion still goes through per-node handshake", func(t *testing.T) {
		// Regression guard: the short-circuit must not bypass the normal
		// flow for PerNode-distribution models.
		baseModel := &v1beta1.BaseModel{
			ObjectMeta: metav1.ObjectMeta{
				Name:              "pernode-delete-me",
				Namespace:         "default",
				Finalizers:        []string{constants.BaseModelFinalizer},
				DeletionTimestamp: &metav1.Time{Time: time.Now()},
			},
			Spec: v1beta1.BaseModelSpec{
				ModelFormat: v1beta1.ModelFormat{Name: "safetensors"},
				Storage:     &v1beta1.StorageSpec{StorageUri: stringPtr("s3://b/m")},
				// No Distribution → defaults to PerNode (nil treated as
				// non-Sharded by isShardedDistribution).
			},
		}

		c := ctrlclientfake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(baseModel).
			Build()

		// Stale entry NOT marked deleted — should keep finalizer + requeue.
		modelKey := constants.GetModelConfigMapKey("default", "pernode-delete-me", false)
		entry, _ := json.Marshal(shared.ModelEntry{Status: shared.ModelStatusReady})
		cm := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "node-1",
				Namespace: constants.OMENamespace,
				Labels:    map[string]string{constants.ModelStatusConfigMapLabel: "true"},
			},
			Data: map[string]string{modelKey: string(entry)},
		}
		g.Expect(c.Create(context.TODO(), cm)).NotTo(gomega.HaveOccurred())

		reconciler := &BaseModelReconciler{Client: c, Scheme: scheme}
		result, err := reconciler.Reconcile(context.TODO(), ctrl.Request{
			NamespacedName: types.NamespacedName{Name: baseModel.Name, Namespace: baseModel.Namespace},
		})
		g.Expect(err).NotTo(gomega.HaveOccurred())
		g.Expect(result.RequeueAfter).To(gomega.Equal(30*time.Second), "PerNode deletion still waits for model-agent")

		updated := &v1beta1.BaseModel{}
		g.Expect(c.Get(context.TODO(), types.NamespacedName{Name: baseModel.Name, Namespace: baseModel.Namespace}, updated)).NotTo(gomega.HaveOccurred())
		g.Expect(updated.Finalizers).To(gomega.ContainElement(constants.BaseModelFinalizer))

		// Entry must NOT be scrubbed by the per-node path — model-agent owns
		// the lifecycle for these.
		afterCM := &corev1.ConfigMap{}
		g.Expect(c.Get(context.TODO(), types.NamespacedName{Name: "node-1", Namespace: constants.OMENamespace}, afterCM)).NotTo(gomega.HaveOccurred())
		_, exists := afterCM.Data[modelKey]
		g.Expect(exists).To(gomega.BeTrue(), "PerNode path must not scrub — that's model-agent's job")
	})
}

// Helper functions
func stringPtr(s string) *string {
	return &s
}

func quantPtr(q v1beta1.ModelQuantization) *v1beta1.ModelQuantization {
	return &q
}

func int32Ptr(i int32) *int32 {
	return &i
}

func distributionPtr(distribution v1beta1.Distribution) *v1beta1.Distribution {
	return &distribution
}

func tinyPhiConfigForTest(t *testing.T) []byte {
	t.Helper()

	data, err := os.ReadFile("../../../modelconfig/testdata/tiny-random-PhiModel/config.json")
	if err != nil {
		t.Fatalf("failed to read tiny phi config fixture: %v", err)
	}
	return data
}

func shardedInspectionForTest(t *testing.T, source sharded.Source) *sharded.SourceInspection {
	t.Helper()

	safetensorsData := safetensorsHeaderForTest(`{"weight":{"dtype":"F32","shape":[2,3],"data_offsets":[0,24]}}`)
	weightPath := source.URI + "/model-00001-of-00001.safetensors"
	return &sharded.SourceInspection{
		Objects: []sharded.Object{
			{
				Source:    source,
				Path:      weightPath,
				SizeBytes: int64(len(safetensorsData)) + 1024,
			},
		},
		Metadata: sharded.ModelMetadataInputs{
			ConfigJSON: tinyPhiConfigForTest(t),
			SafetensorHeaders: []sharded.SafetensorHeader{
				{
					Path:       weightPath,
					SizeBytes:  int64(len(safetensorsData)) + 1024,
					HeaderJSON: safetensorsHeaderJSONForTest(safetensorsData),
				},
			},
		},
	}
}

// safetensorsHeaderLenFieldBytes mirrors the safetensors 8-byte
// little-endian header-length prefix used by the parser. Local helper
// — the sharded sub-package has its own copy for its tests.
const safetensorsHeaderLenFieldBytes = 8

func safetensorsHeaderForTest(header string) []byte {
	headerBytes := []byte(header)
	data := make([]byte, safetensorsHeaderLenFieldBytes+len(headerBytes))
	binary.LittleEndian.PutUint64(data[:safetensorsHeaderLenFieldBytes], uint64(len(headerBytes)))
	copy(data[safetensorsHeaderLenFieldBytes:], headerBytes)
	return data
}

func safetensorsHeaderJSONForTest(data []byte) []byte {
	length := binary.LittleEndian.Uint64(data[:safetensorsHeaderLenFieldBytes])
	return data[safetensorsHeaderLenFieldBytes : safetensorsHeaderLenFieldBytes+int(length)]
}

func getBaseModelForTest(t *testing.T, c client.Client, namespace, name string) *v1beta1.BaseModel {
	t.Helper()
	g := gomega.NewGomegaWithT(t)

	baseModel := &v1beta1.BaseModel{}
	err := c.Get(context.TODO(), types.NamespacedName{Namespace: namespace, Name: name}, baseModel)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	return baseModel
}

func validateProviderNotConfiguredShardedStatus(t *testing.T, status v1beta1.ModelStatusSpec, sourceURI string) {
	t.Helper()
	g := gomega.NewGomegaWithT(t)

	g.Expect(status.State).To(gomega.Equal(v1beta1.LifeCycleStateImporting))
	g.Expect(status.NodesReady).To(gomega.BeEmpty())
	g.Expect(status.NodesFailed).To(gomega.BeEmpty())
	g.Expect(status.Cache).ToNot(gomega.BeNil())
	g.Expect(status.Cache.Backend).To(gomega.BeEmpty())
	g.Expect(status.Cache.SourceUri).To(gomega.Equal(sourceURI))

	sourceReachable := meta.FindStatusCondition(status.Conditions, v1beta1.ModelConditionSourceReachable)
	g.Expect(sourceReachable).ToNot(gomega.BeNil())
	g.Expect(sourceReachable.Status).To(gomega.Equal(metav1.ConditionUnknown))
	g.Expect(sourceReachable.Reason).To(gomega.Equal(v1beta1.ModelConditionReasonProviderNotConfigured))

	ready := meta.FindStatusCondition(status.Conditions, v1beta1.ModelConditionReady)
	g.Expect(ready).ToNot(gomega.BeNil())
	g.Expect(ready.Status).To(gomega.Equal(metav1.ConditionFalse))
	g.Expect(ready.Reason).To(gomega.Equal(v1beta1.ModelConditionReasonProviderNotConfigured))
}
