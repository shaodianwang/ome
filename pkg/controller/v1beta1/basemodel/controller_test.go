package basemodel

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"sync/atomic"
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
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/modelagent"
	"github.com/ome-projects/ome/pkg/modelcache"
	modelcachefake "github.com/ome-projects/ome/pkg/modelcache/fake"
	"github.com/ome-projects/ome/pkg/modelcache/providers/clustercache"
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

				modelEntry := modelagent.ModelEntry{
					Status: modelagent.ModelStatusReady,
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
				modelEntry := modelagent.ModelEntry{
					Status: modelagent.ModelStatusReady,
					Config: &modelagent.ModelConfig{
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
				statuses := map[string]modelagent.ModelStatus{
					"node-1": modelagent.ModelStatusReady,
					"node-2": modelagent.ModelStatusFailed,
					"node-3": modelagent.ModelStatusUpdating,
				}

				for nodeName, status := range statuses {
					modelEntry := modelagent.ModelEntry{
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
					modelEntry := modelagent.ModelEntry{
						Status: modelagent.ModelStatusReady, // Not marked for deletion
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
					modelEntry := modelagent.ModelEntry{
						Status: modelagent.ModelStatusDeleted, // Marked for deletion
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
				statuses := map[string]modelagent.ModelStatus{
					"node-1": modelagent.ModelStatusDeleted, // Marked for deletion
					"node-2": modelagent.ModelStatusReady,   // Not deleted
					"node-3": modelagent.ModelStatusFailed,  // Not deleted
				}

				for nodeName, status := range statuses {
					modelEntry := modelagent.ModelEntry{
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
				modelEntry := modelagent.ModelEntry{
					Status: modelagent.ModelStatusReady,
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

				modelEntry := modelagent.ModelEntry{
					Status: modelagent.ModelStatusReady,
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
					modelEntry := modelagent.ModelEntry{
						Status: modelagent.ModelStatusReady,
						Config: &modelagent.ModelConfig{
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

func TestBaseModelReconcileWithModelCache(t *testing.T) {
	g := gomega.NewGomegaWithT(t)

	scheme := runtime.NewScheme()
	g.Expect(v1beta1.AddToScheme(scheme)).NotTo(gomega.HaveOccurred())
	g.Expect(corev1.AddToScheme(scheme)).NotTo(gomega.HaveOccurred())
	// batchv1 needed for cleanupStalePVCArtifacts which lists Jobs on
	// every non-PVC reconcile (idempotent no-op when none exist; the
	// List itself still requires the scheme).
	g.Expect(batchv1.AddToScheme(scheme)).NotTo(gomega.HaveOccurred())

	totalBytes := int64(100)
	cachedBytes := int64(40)
	totalChunks := int32(10)
	cachedChunks := int32(4)

	tests := []struct {
		name         string
		status       v1beta1.ModelStatusSpec
		providerName string
		modelCache   *modelcachefake.Client
		validate     func(*testing.T, client.Client, *modelcachefake.Client, ctrl.Result)
		wantRequeue  time.Duration
	}{
		{
			name:         "source check success sets SourceReachable",
			providerName: "test_cache",
			modelCache: &modelcachefake.Client{
				CheckSourceFunc: func(ctx context.Context, source modelcache.Source) (*modelcache.SourceStatus, error) {
					return &modelcache.SourceStatus{Reachable: true}, nil
				},
			},
			wantRequeue: 5 * time.Second,
			validate: func(t *testing.T, c client.Client, cache *modelcachefake.Client, result ctrl.Result) {
				updated := getBaseModelForTest(t, c, "default", "cached-model")

				sourceReachable := meta.FindStatusCondition(updated.Status.Conditions, v1beta1.ModelConditionSourceReachable)
				g.Expect(sourceReachable).ToNot(gomega.BeNil())
				g.Expect(sourceReachable.Status).To(gomega.Equal(metav1.ConditionTrue))
				g.Expect(sourceReachable.Reason).To(gomega.Equal(v1beta1.ModelConditionReasonSourceReachable))
				g.Expect(updated.Status.Cache.Backend).To(gomega.Equal("test_cache"))
				g.Expect(updated.Status.Cache.PrewarmID).To(gomega.BeEmpty())
				g.Expect(cache.CheckSourceCalls()).To(gomega.Equal([]modelcache.Source{{URI: "s3://bucket/cached-model"}}))
				g.Expect(cache.StartPrewarmCalls()).To(gomega.BeEmpty())
				g.Expect(result.RequeueAfter).To(gomega.Equal(5 * time.Second))
			},
		},
		{
			name: "starts prewarm after source is reachable",
			status: v1beta1.ModelStatusSpec{
				Conditions: []metav1.Condition{
					{
						Type:   v1beta1.ModelConditionSourceReachable,
						Status: metav1.ConditionTrue,
						Reason: v1beta1.ModelConditionReasonSourceReachable,
					},
				},
			},
			modelCache: &modelcachefake.Client{
				InspectSourceFunc: func(ctx context.Context, source modelcache.Source) (*modelcache.SourceInspection, error) {
					return shardedInspectionForTest(t, source), nil
				},
				StartPrewarmFunc: func(ctx context.Context, req modelcache.PrewarmRequest) (*modelcache.PrewarmOperation, error) {
					return &modelcache.PrewarmOperation{ID: "op-1"}, nil
				},
			},
			wantRequeue: 5 * time.Second,
			validate: func(t *testing.T, c client.Client, cache *modelcachefake.Client, result ctrl.Result) {
				updated := getBaseModelForTest(t, c, "default", "cached-model")

				g.Expect(updated.Status.Cache).ToNot(gomega.BeNil())
				g.Expect(updated.Status.Cache.PrewarmID).To(gomega.Equal("op-1"))
				cached := meta.FindStatusCondition(updated.Status.Conditions, v1beta1.ModelConditionCached)
				g.Expect(cached).ToNot(gomega.BeNil())
				g.Expect(cached.Status).To(gomega.Equal(metav1.ConditionFalse))
				g.Expect(cached.Reason).To(gomega.Equal(v1beta1.ModelConditionReasonPrewarmPending))
				g.Expect(cache.CheckSourceCalls()).To(gomega.BeEmpty())
				g.Expect(cache.InspectSourceCalls()).To(gomega.Equal([]modelcache.Source{{URI: "s3://bucket/cached-model"}}))
				g.Expect(cache.StartPrewarmCalls()).To(gomega.HaveLen(1))
				g.Expect(cache.StartPrewarmCalls()[0].Source.URI).To(gomega.Equal("s3://bucket/cached-model"))
				g.Expect(cache.StartPrewarmCalls()[0].Objects).To(gomega.Equal([]modelcache.ObjectRef{{
					Source: modelcache.Source{URI: "s3://bucket/cached-model"},
					Path:   "s3://bucket/cached-model/model-00001-of-00001.safetensors",
				}}))
				g.Expect(cache.StartPrewarmCalls()[0].IdempotencyKey).To(gomega.HavePrefix("BaseModel/default/cached-model/0/"))
				g.Expect(result.RequeueAfter).To(gomega.Equal(5 * time.Second))
			},
		},
		{
			name:        "source change resets stale prewarm status",
			status:      shardedCachedStatusForSource("s3://bucket/old-model"),
			modelCache:  &modelcachefake.Client{},
			wantRequeue: 5 * time.Second,
			validate: func(t *testing.T, c client.Client, cache *modelcachefake.Client, result ctrl.Result) {
				updated := getBaseModelForTest(t, c, "default", "cached-model")

				g.Expect(updated.Status.Cache).ToNot(gomega.BeNil())
				g.Expect(updated.Status.Cache.SourceUri).To(gomega.Equal("s3://bucket/cached-model"))
				g.Expect(updated.Status.Cache.PrewarmID).To(gomega.BeEmpty())
				g.Expect(updated.Status.Cache.TotalBytes).To(gomega.BeNil())
				g.Expect(updated.Status.Cache.CachedBytes).To(gomega.BeNil())
				g.Expect(updated.Status.Cache.TotalChunks).To(gomega.BeNil())
				g.Expect(updated.Status.Cache.CachedChunks).To(gomega.BeNil())

				sourceReachable := meta.FindStatusCondition(updated.Status.Conditions, v1beta1.ModelConditionSourceReachable)
				g.Expect(sourceReachable).ToNot(gomega.BeNil())
				g.Expect(sourceReachable.Status).To(gomega.Equal(metav1.ConditionUnknown))
				g.Expect(sourceReachable.Reason).To(gomega.Equal(v1beta1.ModelConditionReasonNotChecked))
				cached := meta.FindStatusCondition(updated.Status.Conditions, v1beta1.ModelConditionCached)
				g.Expect(cached).ToNot(gomega.BeNil())
				g.Expect(cached.Status).To(gomega.Equal(metav1.ConditionFalse))
				g.Expect(cached.Reason).To(gomega.Equal(v1beta1.ModelConditionReasonPrewarmPending))
				g.Expect(cache.CheckSourceCalls()).To(gomega.BeEmpty())
				g.Expect(cache.StartPrewarmCalls()).To(gomega.BeEmpty())
				g.Expect(cache.GetPrewarmStatusCalls()).To(gomega.BeEmpty())
				g.Expect(result.RequeueAfter).To(gomega.Equal(5 * time.Second))
			},
		},
		{
			name:         "provider change resets stale prewarm status",
			status:       shardedCachedStatusForSource("s3://bucket/cached-model"),
			providerName: "test_cache",
			modelCache:   &modelcachefake.Client{},
			wantRequeue:  5 * time.Second,
			validate: func(t *testing.T, c client.Client, cache *modelcachefake.Client, result ctrl.Result) {
				updated := getBaseModelForTest(t, c, "default", "cached-model")

				g.Expect(updated.Status.Cache).ToNot(gomega.BeNil())
				g.Expect(updated.Status.Cache.Backend).To(gomega.Equal("test_cache"))
				g.Expect(updated.Status.Cache.SourceUri).To(gomega.Equal("s3://bucket/cached-model"))
				g.Expect(updated.Status.Cache.PrewarmID).To(gomega.BeEmpty())
				g.Expect(updated.Status.Cache.TotalBytes).To(gomega.BeNil())
				g.Expect(updated.Status.Cache.CachedBytes).To(gomega.BeNil())
				g.Expect(updated.Status.Cache.TotalChunks).To(gomega.BeNil())
				g.Expect(updated.Status.Cache.CachedChunks).To(gomega.BeNil())

				sourceReachable := meta.FindStatusCondition(updated.Status.Conditions, v1beta1.ModelConditionSourceReachable)
				g.Expect(sourceReachable).ToNot(gomega.BeNil())
				g.Expect(sourceReachable.Status).To(gomega.Equal(metav1.ConditionUnknown))
				g.Expect(sourceReachable.Reason).To(gomega.Equal(v1beta1.ModelConditionReasonNotChecked))
				cached := meta.FindStatusCondition(updated.Status.Conditions, v1beta1.ModelConditionCached)
				g.Expect(cached).ToNot(gomega.BeNil())
				g.Expect(cached.Status).To(gomega.Equal(metav1.ConditionFalse))
				g.Expect(cached.Reason).To(gomega.Equal(v1beta1.ModelConditionReasonPrewarmPending))
				g.Expect(cache.CheckSourceCalls()).To(gomega.BeEmpty())
				g.Expect(cache.StartPrewarmCalls()).To(gomega.BeEmpty())
				g.Expect(cache.GetPrewarmStatusCalls()).To(gomega.BeEmpty())
				g.Expect(result.RequeueAfter).To(gomega.Equal(5 * time.Second))
			},
		},
		{
			name:   "polls running prewarm and writes progress",
			status: shardedStatusWithPrewarmID("op-1"),
			modelCache: &modelcachefake.Client{
				GetPrewarmStatusFunc: func(ctx context.Context, operation modelcache.PrewarmOperation) (*modelcache.PrewarmStatus, error) {
					return &modelcache.PrewarmStatus{
						Operation: operation,
						Phase:     modelcache.PrewarmPhaseRunning,
						Progress: modelcache.PrewarmProgress{
							TotalBytes:   &totalBytes,
							CachedBytes:  &cachedBytes,
							TotalChunks:  &totalChunks,
							CachedChunks: &cachedChunks,
						},
					}, nil
				},
			},
			wantRequeue: 5 * time.Second,
			validate: func(t *testing.T, c client.Client, cache *modelcachefake.Client, result ctrl.Result) {
				updated := getBaseModelForTest(t, c, "default", "cached-model")

				cached := meta.FindStatusCondition(updated.Status.Conditions, v1beta1.ModelConditionCached)
				g.Expect(cached).ToNot(gomega.BeNil())
				g.Expect(cached.Status).To(gomega.Equal(metav1.ConditionFalse))
				g.Expect(cached.Reason).To(gomega.Equal(v1beta1.ModelConditionReasonPrewarmRunning))
				g.Expect(*updated.Status.Cache.TotalBytes).To(gomega.Equal(totalBytes))
				g.Expect(*updated.Status.Cache.CachedBytes).To(gomega.Equal(cachedBytes))
				g.Expect(*updated.Status.Cache.TotalChunks).To(gomega.Equal(totalChunks))
				g.Expect(*updated.Status.Cache.CachedChunks).To(gomega.Equal(cachedChunks))
				g.Expect(cache.GetPrewarmStatusCalls()).To(gomega.Equal([]modelcache.PrewarmOperation{{ID: "op-1"}}))
				g.Expect(result.RequeueAfter).To(gomega.Equal(5 * time.Second))
			},
		},
		{
			name:   "prewarm success waits for metadata inputs when unavailable",
			status: shardedStatusWithPrewarmID("op-1"),
			modelCache: &modelcachefake.Client{
				GetPrewarmStatusFunc: func(ctx context.Context, operation modelcache.PrewarmOperation) (*modelcache.PrewarmStatus, error) {
					return &modelcache.PrewarmStatus{
						Operation: operation,
						Phase:     modelcache.PrewarmPhaseSucceeded,
					}, nil
				},
			},
			wantRequeue: 5 * time.Second,
			validate: func(t *testing.T, c client.Client, cache *modelcachefake.Client, result ctrl.Result) {
				updated := getBaseModelForTest(t, c, "default", "cached-model")

				g.Expect(updated.Status.State).To(gomega.Equal(v1beta1.LifeCycleStateImporting))
				cached := meta.FindStatusCondition(updated.Status.Conditions, v1beta1.ModelConditionCached)
				g.Expect(cached).ToNot(gomega.BeNil())
				g.Expect(cached.Status).To(gomega.Equal(metav1.ConditionTrue))
				g.Expect(cached.Reason).To(gomega.Equal(v1beta1.ModelConditionReasonPrewarmSucceeded))
				ready := meta.FindStatusCondition(updated.Status.Conditions, v1beta1.ModelConditionReady)
				g.Expect(ready).ToNot(gomega.BeNil())
				g.Expect(ready.Status).To(gomega.Equal(metav1.ConditionFalse))
				g.Expect(ready.Reason).To(gomega.Equal(v1beta1.ModelConditionReasonMetadataPending))
				g.Expect(ready.Message).To(gomega.ContainSubstring("metadata"))
				g.Expect(result.RequeueAfter).To(gomega.Equal(5 * time.Second))
			},
		},
		{
			name:   "prewarm success parses metadata and marks Ready",
			status: shardedStatusWithPrewarmID("op-1"),
			modelCache: &modelcachefake.Client{
				GetPrewarmStatusFunc: func(ctx context.Context, operation modelcache.PrewarmOperation) (*modelcache.PrewarmStatus, error) {
					return &modelcache.PrewarmStatus{
						Operation: operation,
						Phase:     modelcache.PrewarmPhaseSucceeded,
					}, nil
				},
				InspectSourceFunc: func(ctx context.Context, source modelcache.Source) (*modelcache.SourceInspection, error) {
					return shardedInspectionForTest(t, source), nil
				},
			},
			validate: func(t *testing.T, c client.Client, cache *modelcachefake.Client, result ctrl.Result) {
				updated := getBaseModelForTest(t, c, "default", "cached-model")

				g.Expect(updated.Status.State).To(gomega.Equal(v1beta1.LifeCycleStateReady))
				ready := meta.FindStatusCondition(updated.Status.Conditions, v1beta1.ModelConditionReady)
				g.Expect(ready).ToNot(gomega.BeNil())
				g.Expect(ready.Status).To(gomega.Equal(metav1.ConditionTrue))
				g.Expect(ready.Reason).To(gomega.Equal(v1beta1.ModelConditionReasonMetadataParsed))
				g.Expect(updated.Spec.ModelArchitecture).ToNot(gomega.BeNil())
				g.Expect(*updated.Spec.ModelArchitecture).To(gomega.Equal("PhiModel"))
				g.Expect(updated.Spec.ModelParameterSize).ToNot(gomega.BeNil())
				g.Expect(cache.InspectSourceCalls()).To(gomega.Equal([]modelcache.Source{{URI: "s3://bucket/cached-model"}}))
				g.Expect(result).To(gomega.Equal(ctrl.Result{}))
			},
		},
		{
			name:        "prewarm failure within retry budget retries with backoff",
			status:      shardedStatusWithPrewarmAttempts("op-1", 1),
			wantRequeue: 30 * time.Second,
			modelCache: &modelcachefake.Client{
				GetPrewarmStatusFunc: func(ctx context.Context, operation modelcache.PrewarmOperation) (*modelcache.PrewarmStatus, error) {
					return &modelcache.PrewarmStatus{
						Operation: operation,
						Phase:     modelcache.PrewarmPhaseFailed,
						Reason:    "ClusterCacheFetchFailed",
						Message:   "cgroup pressure 2GB/2GB",
					}, nil
				},
			},
			validate: func(t *testing.T, c client.Client, cache *modelcachefake.Client, result ctrl.Result) {
				updated := getBaseModelForTest(t, c, "default", "cached-model")

				g.Expect(updated.Status.State).To(gomega.Equal(v1beta1.LifeCycleStateImporting))
				g.Expect(updated.Status.Cache.PrewarmID).To(gomega.BeEmpty())
				cached := meta.FindStatusCondition(updated.Status.Conditions, v1beta1.ModelConditionCached)
				g.Expect(cached).ToNot(gomega.BeNil())
				g.Expect(cached.Status).To(gomega.Equal(metav1.ConditionFalse))
				g.Expect(cached.Reason).To(gomega.Equal(v1beta1.ModelConditionReasonPrewarmFailed))
				g.Expect(cached.Message).To(gomega.ContainSubstring("attempt 1/10, will retry"))
				g.Expect(cached.Message).To(gomega.ContainSubstring("ClusterCacheFetchFailed"))
				g.Expect(cached.Message).To(gomega.ContainSubstring("cgroup pressure"))
				ready := meta.FindStatusCondition(updated.Status.Conditions, v1beta1.ModelConditionReady)
				g.Expect(ready).ToNot(gomega.BeNil())
				g.Expect(ready.Reason).To(gomega.Equal(v1beta1.ModelConditionReasonCacheNotReady))
				g.Expect(result.RequeueAfter).To(gomega.Equal(30 * time.Second))
			},
		},
		{
			name:        "prewarm failure at retry budget cap becomes exhausted with long-tail retry",
			status:      shardedStatusWithPrewarmAttempts("op-final", 10),
			wantRequeue: time.Hour,
			modelCache: &modelcachefake.Client{
				GetPrewarmStatusFunc: func(ctx context.Context, operation modelcache.PrewarmOperation) (*modelcache.PrewarmStatus, error) {
					return &modelcache.PrewarmStatus{
						Operation: operation,
						Phase:     modelcache.PrewarmPhaseFailed,
						Reason:    "ClusterCacheFetchFailed",
						Message:   "cgroup pressure 2GB/2GB",
					}, nil
				},
			},
			validate: func(t *testing.T, c client.Client, cache *modelcachefake.Client, result ctrl.Result) {
				updated := getBaseModelForTest(t, c, "default", "cached-model")

				g.Expect(updated.Status.State).To(gomega.Equal(v1beta1.LifeCycleStateFailed))
				g.Expect(updated.Status.Cache.PrewarmID).To(gomega.BeEmpty())
				cached := meta.FindStatusCondition(updated.Status.Conditions, v1beta1.ModelConditionCached)
				g.Expect(cached).ToNot(gomega.BeNil())
				g.Expect(cached.Reason).To(gomega.Equal(v1beta1.ModelConditionReasonPrewarmExhausted))
				g.Expect(cached.Message).To(gomega.ContainSubstring("slow-retrying every 1h0m0s"))
				g.Expect(cached.Message).To(gomega.ContainSubstring("cgroup pressure"))
				ready := meta.FindStatusCondition(updated.Status.Conditions, v1beta1.ModelConditionReady)
				g.Expect(ready).ToNot(gomega.BeNil())
				g.Expect(ready.Reason).To(gomega.Equal(v1beta1.ModelConditionReasonPrewarmExhausted))
				g.Expect(result.RequeueAfter).To(gomega.Equal(time.Hour))
			},
		},
		{
			name:   "prewarm canceled is terminal regardless of attempts",
			status: shardedStatusWithPrewarmAttempts("op-canceled", 1),
			modelCache: &modelcachefake.Client{
				GetPrewarmStatusFunc: func(ctx context.Context, operation modelcache.PrewarmOperation) (*modelcache.PrewarmStatus, error) {
					return &modelcache.PrewarmStatus{
						Operation: operation,
						Phase:     modelcache.PrewarmPhaseCanceled,
						Reason:    "OperatorCanceled",
						Message:   "operator canceled the prewarm",
					}, nil
				},
			},
			validate: func(t *testing.T, c client.Client, cache *modelcachefake.Client, result ctrl.Result) {
				updated := getBaseModelForTest(t, c, "default", "cached-model")

				g.Expect(updated.Status.State).To(gomega.Equal(v1beta1.LifeCycleStateFailed))
				g.Expect(result).To(gomega.Equal(ctrl.Result{}))
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
					ModelFormat: v1beta1.ModelFormat{
						Name: "safetensors",
					},
					Storage: &v1beta1.StorageSpec{
						StorageUri: stringPtr("s3://bucket/cached-model"),
					},
					Distribution: distributionPtr(v1beta1.DistributionSharded),
				},
				Status: tt.status,
			}

			c := ctrlclientfake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(baseModel).
				WithStatusSubresource(baseModel).
				Build()

			providerName := tt.providerName
			if providerName == "" {
				providerName = modelcache.ProviderClusterCache
			}
			reconciler := &BaseModelReconciler{
				Client:     c,
				Scheme:     c.Scheme(),
				ModelCache: modelcache.NewProvider(providerName, tt.modelCache),
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

// TestSharded_URIChange_ResetsCacheAndFiresFreshPrewarm exercises the
// URI-change → reset → fresh-StartPrewarm flow directly via the
// ClusterBaseModel reconciler with an in-memory fake k8s client and the
// modelcache fake. It replaces an integration spec
// (tests/integration/.../sharded_test.go) that depended on the
// controller-runtime work queue + watch + informer cache to converge
// within a fixed wall-clock budget — under heavy CI suite contention
// that budget kept being insufficient. This unit test asserts the same
// invariant deterministically by driving Reconcile in a tight loop.
func TestSharded_URIChange_ResetsCacheAndFiresFreshPrewarm(t *testing.T) {
	g := gomega.NewGomegaWithT(t)

	scheme := runtime.NewScheme()
	g.Expect(v1beta1.AddToScheme(scheme)).NotTo(gomega.HaveOccurred())
	g.Expect(corev1.AddToScheme(scheme)).NotTo(gomega.HaveOccurred())
	g.Expect(batchv1.AddToScheme(scheme)).NotTo(gomega.HaveOccurred())

	// Per-call unique PrewarmID so a fresh StartPrewarm is observable as
	// a different PrewarmID on the CBM status — a counter alone races
	// with conflict-retries, but PrewarmID changes only on a successful
	// applyPrewarmStarted that wrote to status.
	var prewarmSeq int32
	cacheClient := &modelcachefake.Client{
		CheckSourceFunc: func(_ context.Context, _ modelcache.Source) (*modelcache.SourceStatus, error) {
			return &modelcache.SourceStatus{Reachable: true, Reason: "SourceReachable"}, nil
		},
		InspectSourceFunc: func(_ context.Context, src modelcache.Source) (*modelcache.SourceInspection, error) {
			return &modelcache.SourceInspection{
				Objects: []modelcache.Object{{Source: src, Path: src.URI + "/config.json"}},
				Metadata: modelcache.ModelMetadataInputs{
					ConfigJSON: []byte(`{"model_type":"phi","architectures":["PhiForCausalLM"]}`),
				},
			}, nil
		},
		StartPrewarmFunc: func(_ context.Context, _ modelcache.PrewarmRequest) (*modelcache.PrewarmOperation, error) {
			n := atomic.AddInt32(&prewarmSeq, 1)
			return &modelcache.PrewarmOperation{ID: fmt.Sprintf("op-%d", n)}, nil
		},
		// Stay in Running; we don't want PrewarmPhaseSucceeded to push the
		// status into Cached=True before the URI-change reset path runs,
		// because Cached=True short-circuits the StartPrewarm branch.
		GetPrewarmStatusFunc: func(_ context.Context, op modelcache.PrewarmOperation) (*modelcache.PrewarmStatus, error) {
			return &modelcache.PrewarmStatus{Operation: op, Phase: modelcache.PrewarmPhaseRunning}, nil
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
		ModelCache: modelcache.NewProvider("test_cache", cacheClient),
	}

	reconcile := func() {
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

	// Phase 1: drive reconciles until the initial StartPrewarm fires
	// (URI=v1) and PrewarmID is recorded on status. The chain is reset
	// → CheckSource → StartPrewarm; an upper bound of 10 iterations is
	// generous (each iteration is a single in-process Reconcile call).
	for i := 0; i < 10 && atomic.LoadInt32(&prewarmSeq) < 1; i++ {
		reconcile()
	}
	g.Expect(int(atomic.LoadInt32(&prewarmSeq))).To(gomega.Equal(1), "initial StartPrewarm should have fired")
	g.Expect(cacheClient.StartPrewarmCalls()[0].Source.URI).To(gomega.Equal(v1URI))
	firstPrewarmID := get().Status.Cache.PrewarmID
	g.Expect(firstPrewarmID).NotTo(gomega.BeEmpty())

	// Mutate spec to v2 — the user-visible event that should reset cache identity.
	{
		got := get()
		got.Spec.Storage.StorageUri = stringPtr(v2URI)
		g.Expect(kubeClient.Update(context.TODO(), got)).To(gomega.Succeed())
	}

	// Phase 2: drive reconciles until the fresh StartPrewarm fires for
	// v2. If the controller's URI-change → reset → re-prewarm chain
	// regresses (e.g. cacheIdentityChanged stops firing on URI swap, or
	// resetShardedCacheProgress stops clearing PrewarmID), prewarmSeq
	// stays at 1 and the assertion below fails with a clear signal.
	for i := 0; i < 10 && atomic.LoadInt32(&prewarmSeq) < 2; i++ {
		reconcile()
	}
	g.Expect(int(atomic.LoadInt32(&prewarmSeq))).To(gomega.Equal(2), "URI change must trigger a fresh StartPrewarm")
	g.Expect(cacheClient.StartPrewarmCalls()[1].Source.URI).To(gomega.Equal(v2URI),
		"fresh StartPrewarm must use the new URI")

	final := get()
	g.Expect(final.Status.Cache.SourceUri).To(gomega.Equal(v2URI))
	g.Expect(final.Status.Cache.PrewarmID).NotTo(gomega.Equal(firstPrewarmID),
		"PrewarmID should have been reset and re-populated, not unchanged from the v1 prewarm")
}

func TestBaseModelReconcileWithClusterCacheProvider(t *testing.T) {
	g := gomega.NewGomegaWithT(t)

	scheme := runtime.NewScheme()
	g.Expect(v1beta1.AddToScheme(scheme)).NotTo(gomega.HaveOccurred())
	g.Expect(corev1.AddToScheme(scheme)).NotTo(gomega.HaveOccurred())
	// batchv1 needed for cleanupStalePVCArtifacts which lists Jobs on
	// every non-PVC reconcile (idempotent no-op when none exist; the
	// List itself still requires the scheme).
	g.Expect(batchv1.AddToScheme(scheme)).NotTo(gomega.HaveOccurred())

	const sourceURI = "s3://bucket/cached-model"
	configPath := sourceURI + "/config.json"
	weightPath := sourceURI + "/model-00001-of-00001.safetensors"
	fetchPaths := make(chan []string, 4)
	fileMetadataPaths := make(chan string, 4)
	prefetchOwnedChunksPaths := make(chan []string, 4)
	configData := tinyPhiConfigForTest(t)
	safetensorsData := safetensorsHeaderForTest(`{"weight":{"dtype":"F32","shape":[2,3],"data_offsets":[0,24]}}`)
	configMTime := uint64(123)
	weightMTime := uint64(124)
	chunkSize := uint64(16)
	var serverAddr string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/file_metadata":
			if r.Method != http.MethodGet {
				http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
				return
			}
			fileMetadataPaths <- r.URL.Query().Get("path")
			writeJSONForControllerTest(t, w, map[string]any{
				"root": sourceURI,
				"files": []map[string]any{
					{"path": configPath, "relative_path": "config.json", "size": len(configData), "mtime": configMTime},
					{"path": weightPath, "relative_path": "model-00001-of-00001.safetensors", "size": len(safetensorsData), "mtime": weightMTime},
				},
			})
		case "/ring_info":
			if r.Method != http.MethodGet {
				http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
				return
			}
			writeJSONForControllerTest(t, w, map[string]any{"nodes": []string{serverAddr}})
		case "/packed_byte_ranges_by_key":
			if r.Method != http.MethodPost {
				http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
				return
			}
			var req struct {
				Ranges []struct {
					Path        string `json:"path"`
					ChunkOffset uint64 `json:"chunk_offset"`
					RangeOffset uint64 `json:"range_offset"`
					Length      uint64 `json:"length"`
				} `json:"ranges"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			for _, rangeSpec := range req.Ranges {
				data := map[string][]byte{
					configPath: configData,
					weightPath: safetensorsData,
				}[rangeSpec.Path]
				start := rangeSpec.ChunkOffset + rangeSpec.RangeOffset
				end := start + rangeSpec.Length
				if end > uint64(len(data)) {
					http.Error(w, "range out of bounds", http.StatusBadRequest)
					return
				}
				_, _ = w.Write(data[start:end])
			}
		case "/fetch":
			if r.Method != http.MethodPost {
				http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
				return
			}
			var req struct {
				Paths []string `json:"paths"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			fetchPaths <- req.Paths
			results := make([]map[string]any, 0, len(req.Paths))
			for _, fetchPath := range req.Paths {
				size := len(configData)
				mtime := configMTime
				if fetchPath == weightPath {
					size = len(safetensorsData)
					mtime = weightMTime
				}
				results = append(results, map[string]any{
					"path":          fetchPath,
					"status":        "warmed",
					"total_size":    size,
					"mtime":         mtime,
					"chunk_size":    chunkSize,
					"total_chunks":  1,
					"cached_chunks": 0,
					"warmed_chunks": 1,
				})
			}
			writeJSONForControllerTest(t, w, map[string]any{"results": results})
		case "/prefetch_owned_chunks":
			if r.Method != http.MethodPost {
				http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
				return
			}
			var req struct {
				Chunks []struct {
					Path   string `json:"path"`
					Offset uint64 `json:"offset"`
					MTime  uint64 `json:"mtime"`
					Length uint64 `json:"length"`
				} `json:"chunks"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			paths := make([]string, 0, len(req.Chunks))
			seen := map[string]struct{}{}
			for _, c := range req.Chunks {
				if _, ok := seen[c.Path]; ok {
					continue
				}
				seen[c.Path] = struct{}{}
				paths = append(paths, c.Path)
			}
			prefetchOwnedChunksPaths <- paths
			writeJSONForControllerTest(t, w, map[string]any{
				"total_chunks":  len(req.Chunks),
				"cached_chunks": 0,
				"warmed_chunks": len(req.Chunks),
				"errors":        []string{},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	serverAddr = server.Listener.Addr().String()

	cacheClient, err := clustercache.NewClient(modelcache.ProviderConfig{
		Endpoint: server.URL,
		Options: map[string]string{
			"timeout":             "5s",
			"operation_retention": "1m",
		},
	})
	g.Expect(err).NotTo(gomega.HaveOccurred())
	defer func() {
		closeable, ok := cacheClient.(interface{ Close() error })
		g.Expect(ok).To(gomega.BeTrue())
		g.Expect(closeable.Close()).NotTo(gomega.HaveOccurred())
	}()

	baseModel := &v1beta1.BaseModel{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "cached-model",
			Namespace:  "default",
			Finalizers: []string{constants.BaseModelFinalizer},
		},
		Spec: v1beta1.BaseModelSpec{
			ModelFormat: v1beta1.ModelFormat{Name: "safetensors"},
			Storage: &v1beta1.StorageSpec{
				StorageUri: stringPtr(sourceURI),
			},
			Distribution: distributionPtr(v1beta1.DistributionSharded),
		},
	}
	kubeClient := ctrlclientfake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(baseModel).
		WithStatusSubresource(baseModel).
		Build()

	reconciler := &BaseModelReconciler{
		Client:     kubeClient,
		Scheme:     kubeClient.Scheme(),
		ModelCache: modelcache.NewProvider(modelcache.ProviderClusterCache, cacheClient),
	}
	reconcile := func() ctrl.Result {
		result, err := reconciler.Reconcile(context.TODO(), ctrl.Request{
			NamespacedName: types.NamespacedName{
				Namespace: baseModel.Namespace,
				Name:      baseModel.Name,
			},
		})
		g.Expect(err).NotTo(gomega.HaveOccurred())
		return result
	}

	result := reconcile()
	g.Expect(result.RequeueAfter).To(gomega.Equal(5 * time.Second))
	g.Eventually(fileMetadataPaths, time.Second).Should(gomega.Receive(gomega.Equal(sourceURI)))
	updated := getBaseModelForTest(t, kubeClient, "default", "cached-model")
	sourceReachable := meta.FindStatusCondition(updated.Status.Conditions, v1beta1.ModelConditionSourceReachable)
	g.Expect(sourceReachable).ToNot(gomega.BeNil())
	g.Expect(sourceReachable.Status).To(gomega.Equal(metav1.ConditionTrue))

	result = reconcile()
	g.Expect(result.RequeueAfter).To(gomega.Equal(5 * time.Second))
	// /fetch is still used for chunk_size discovery, single-path.
	g.Eventually(fetchPaths, time.Second).Should(gomega.Receive(gomega.Equal([]string{configPath})))
	// The new owner-routed prewarm POSTs to /prefetch_owned_chunks (not /fetch).
	// With a single-node ring (serverAddr is the only node), all chunks for
	// both files route to that one owner in one POST.
	g.Eventually(prefetchOwnedChunksPaths, time.Second).Should(gomega.Receive(gomega.ConsistOf(configPath, weightPath)))
	updated = getBaseModelForTest(t, kubeClient, "default", "cached-model")
	g.Expect(updated.Status.Cache).ToNot(gomega.BeNil())
	g.Expect(updated.Status.Cache.PrewarmID).ToNot(gomega.BeEmpty())
	prewarmID := updated.Status.Cache.PrewarmID

	g.Eventually(func() modelcache.PrewarmPhase {
		status, err := cacheClient.GetPrewarmStatus(context.TODO(), modelcache.PrewarmOperation{ID: prewarmID})
		if err != nil {
			return ""
		}
		return status.Phase
	}, time.Second, 10*time.Millisecond).Should(gomega.Equal(modelcache.PrewarmPhaseSucceeded))

	result = reconcile()
	g.Expect(result).To(gomega.Equal(ctrl.Result{}))
	updated = getBaseModelForTest(t, kubeClient, "default", "cached-model")
	cached := meta.FindStatusCondition(updated.Status.Conditions, v1beta1.ModelConditionCached)
	g.Expect(cached).ToNot(gomega.BeNil())
	g.Expect(cached.Status).To(gomega.Equal(metav1.ConditionTrue))
	g.Expect(cached.Reason).To(gomega.Equal(v1beta1.ModelConditionReasonPrewarmSucceeded))
	ready := meta.FindStatusCondition(updated.Status.Conditions, v1beta1.ModelConditionReady)
	g.Expect(ready).ToNot(gomega.BeNil())
	g.Expect(ready.Status).To(gomega.Equal(metav1.ConditionTrue))
	g.Expect(ready.Reason).To(gomega.Equal(v1beta1.ModelConditionReasonMetadataParsed))
	g.Expect(updated.Spec.ModelArchitecture).ToNot(gomega.BeNil())
	g.Expect(*updated.Spec.ModelArchitecture).To(gomega.Equal("PhiModel"))
	g.Expect(updated.Status.Cache.Backend).To(gomega.Equal(modelcache.ProviderClusterCache))
	g.Expect(updated.Status.Cache.SourceUri).To(gomega.Equal(sourceURI))
	// The /prefetch_owned_chunks response doesn't include byte counts —
	// only chunk counts (api.rs:58-62: OwnedChunkPrefetchResponse).
	// TotalBytes/CachedBytes stay nil; TotalChunks/CachedChunks sum across owners.
	g.Expect(updated.Status.Cache.TotalBytes).To(gomega.BeNil(), "owner-routed prewarm doesn't surface byte progress")
	g.Expect(updated.Status.Cache.CachedBytes).To(gomega.BeNil(), "owner-routed prewarm doesn't surface byte progress")
	expectedChunks := int32(chunksForSize(uint64(len(configData)), chunkSize) +
		chunksForSize(uint64(len(safetensorsData)), chunkSize))
	g.Expect(*updated.Status.Cache.TotalChunks).To(gomega.Equal(expectedChunks))
	g.Expect(*updated.Status.Cache.CachedChunks).To(gomega.Equal(expectedChunks))
}

// chunksForSize returns the number of chunks a file of size `size`
// occupies given chunk_size (matches client-side computation in
// pkg/modelcache/providers/clustercache/prefetch.go).
func chunksForSize(size, chunkSize uint64) int {
	if size == 0 || chunkSize == 0 {
		return 0
	}
	return int((size + chunkSize - 1) / chunkSize)
}

func TestMapConfigMapToModelRequests(t *testing.T) {
	g := gomega.NewGomegaWithT(t)
	logger := ctrl.Log.WithName("test")

	tests := []struct {
		name          string
		configMap     *corev1.ConfigMap
		keyPrefix     string
		isNamespaced  bool
		expectedCount int
		expectedFirst *types.NamespacedName
	}{
		{
			name: "BaseModel mapping",
			configMap: &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-node",
					Namespace: constants.OMENamespace,
				},
				Data: map[string]string{
					"default.basemodel.my-model":     `{"status":"Ready"}`,
					"test-ns.basemodel.other-model":  `{"status":"Failed"}`,
					"clusterbasemodel.cluster-model": `{"status":"Ready"}`, // Should be ignored
				},
			},
			keyPrefix:     "basemodel",
			isNamespaced:  true,
			expectedCount: 2,
			expectedFirst: nil, // Don't check specific order since map iteration is random
		},
		{
			name: "ClusterBaseModel mapping",
			configMap: &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "cluster-node",
					Namespace: constants.OMENamespace,
				},
				Data: map[string]string{
					"clusterbasemodel.global-model":    `{"status":"Ready"}`,
					"clusterbasemodel.multi.part.name": `{"status":"InTransit"}`,
					"basemodel.default.local-model":    `{"status":"Ready"}`, // Should be ignored
				},
			},
			keyPrefix:     "clusterbasemodel",
			isNamespaced:  false,
			expectedCount: 2,
			expectedFirst: nil, // Don't check specific order since map iteration is random
		},
		{
			name: "Invalid JSON data",
			configMap: &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "invalid-node",
					Namespace: constants.OMENamespace,
				},
				Data: map[string]string{
					"default.basemodel.broken-model": `{invalid json}`,
					"default.basemodel.valid-model":  `{"status":"Ready"}`,
				},
			},
			keyPrefix:     "basemodel",
			isNamespaced:  true,
			expectedCount: 1, // Only valid entry should be processed
			expectedFirst: &types.NamespacedName{
				Namespace: "default",
				Name:      "valid-model",
			},
		},
		{
			name:          "Non-ConfigMap object",
			configMap:     nil, // Will pass a different object type
			keyPrefix:     "basemodel",
			isNamespaced:  true,
			expectedCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var requests []reconcile.Request

			if tt.configMap != nil {
				if tt.isNamespaced {
					// Test BaseModel mapping
					reconciler := &BaseModelReconciler{Log: logger}
					requests = reconciler.mapConfigMapToBaseModels(tt.configMap)
				} else {
					// Test ClusterBaseModel mapping
					reconciler := &ClusterBaseModelReconciler{Log: logger}
					requests = reconciler.mapConfigMapToClusterBaseModels(tt.configMap)
				}
			} else {
				// Pass a non-ConfigMap object
				reconciler := &BaseModelReconciler{Log: logger}
				requests = reconciler.mapConfigMapToBaseModels(&corev1.Pod{})
			}

			g.Expect(requests).To(gomega.HaveLen(tt.expectedCount))

			if tt.expectedFirst != nil && len(requests) > 0 {
				g.Expect(requests[0].NamespacedName).To(gomega.Equal(*tt.expectedFirst))
			} else if tt.expectedCount > 0 {
				// Instead of checking order, verify that all expected requests are present
				// For BaseModel mapping case
				if tt.name == "BaseModel mapping" {
					foundDefault := false
					foundTestNs := false

					for _, req := range requests {
						if req.NamespacedName.Namespace == "default" && req.NamespacedName.Name == "my-model" {
							foundDefault = true
						}
						if req.NamespacedName.Namespace == "test-ns" && req.NamespacedName.Name == "other-model" {
							foundTestNs = true
						}
					}

					g.Expect(foundDefault).To(gomega.BeTrue(), "Should find default.my-model")
					g.Expect(foundTestNs).To(gomega.BeTrue(), "Should find test-ns.other-model")
				}
			}
		})
	}
}

func TestUpdateSpecWithConfig(t *testing.T) {
	g := gomega.NewGomegaWithT(t)

	tests := []struct {
		name         string
		initialSpec  *v1beta1.BaseModelSpec
		config       *modelagent.ModelConfig
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
			config: &modelagent.ModelConfig{
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
			config: &modelagent.ModelConfig{
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
			config:       &modelagent.ModelConfig{},
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
			updated := updateSpecWithConfig(tt.initialSpec, tt.config)
			g.Expect(updated).To(gomega.Equal(tt.expectUpdate))

			if tt.validateSpec != nil && tt.initialSpec != nil {
				tt.validateSpec(tt.initialSpec)
			}
		})
	}
}

func TestCalculateLifecycleState(t *testing.T) {
	g := gomega.NewGomegaWithT(t)

	tests := []struct {
		name          string
		nodesReady    []string
		nodesFailed   []string
		expectedState v1beta1.LifeCycleState
	}{
		{
			name:          "Ready state with ready nodes",
			nodesReady:    []string{"node1", "node2"},
			nodesFailed:   []string{},
			expectedState: v1beta1.LifeCycleStateReady,
		},
		{
			name:          "Ready state with mixed nodes",
			nodesReady:    []string{"node1"},
			nodesFailed:   []string{"node2"},
			expectedState: v1beta1.LifeCycleStateReady,
		},
		{
			name:          "Failed state with only failed nodes",
			nodesReady:    []string{},
			nodesFailed:   []string{"node1", "node2"},
			expectedState: v1beta1.LifeCycleStateFailed,
		},
		{
			name:          "InTransit state with no nodes",
			nodesReady:    []string{},
			nodesFailed:   []string{},
			expectedState: v1beta1.LifeCycleStateInTransit,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			state := calculateLifecycleState(tt.nodesReady, tt.nodesFailed)
			g.Expect(state).To(gomega.Equal(tt.expectedState))
		})
	}
}

func TestCreateModelStatusConfigMapPredicate(t *testing.T) {
	g := gomega.NewGomegaWithT(t)

	predicate := createModelStatusConfigMapPredicate()

	tests := []struct {
		name     string
		obj      client.Object
		expected bool
	}{
		{
			name: "Valid model status ConfigMap",
			obj: &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-node",
					Namespace: constants.OMENamespace,
					Labels: map[string]string{
						constants.ModelStatusConfigMapLabel: "true",
					},
				},
			},
			expected: true,
		},
		{
			name: "ConfigMap in wrong namespace",
			obj: &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-node",
					Namespace: "wrong-namespace",
					Labels: map[string]string{
						constants.ModelStatusConfigMapLabel: "true",
					},
				},
			},
			expected: false,
		},
		{
			name: "ConfigMap without label",
			obj: &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-node",
					Namespace: constants.OMENamespace,
				},
			},
			expected: false,
		},
		{
			name: "ConfigMap with wrong label value",
			obj: &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-node",
					Namespace: constants.OMENamespace,
					Labels: map[string]string{
						constants.ModelStatusConfigMapLabel: "false",
					},
				},
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test CreateFunc
			result := predicate.Create(event.TypedCreateEvent[client.Object]{Object: tt.obj})
			g.Expect(result).To(gomega.Equal(tt.expected))

			// Test UpdateFunc
			result = predicate.Update(event.TypedUpdateEvent[client.Object]{ObjectNew: tt.obj})
			g.Expect(result).To(gomega.Equal(tt.expected))

			// Test DeleteFunc
			result = predicate.Delete(event.TypedDeleteEvent[client.Object]{Object: tt.obj})
			g.Expect(result).To(gomega.Equal(tt.expected))
		})
	}
}

func TestAddToSlice(t *testing.T) {
	g := gomega.NewGomegaWithT(t)

	tests := []struct {
		name     string
		slice    []string
		item     string
		expected []string
	}{
		{
			name:     "Add to empty slice",
			slice:    []string{},
			item:     "item1",
			expected: []string{"item1"},
		},
		{
			name:     "Add new item",
			slice:    []string{"item1", "item2"},
			item:     "item3",
			expected: []string{"item1", "item2", "item3"},
		},
		{
			name:     "Don't add existing item",
			slice:    []string{"item1", "item2"},
			item:     "item1",
			expected: []string{"item1", "item2"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := addToSlice(tt.slice, tt.item)
			g.Expect(result).To(gomega.Equal(tt.expected))
		})
	}
}

func TestCreateNodeDeletionPredicate(t *testing.T) {
	g := gomega.NewGomegaWithT(t)

	pred := createNodeDeletionPredicate()

	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-node",
		},
	}

	// CreateFunc should return false
	createResult := pred.Create(event.TypedCreateEvent[client.Object]{Object: node})
	g.Expect(createResult).To(gomega.BeFalse(), "CreateFunc should return false")

	// UpdateFunc should return false
	updateResult := pred.Update(event.TypedUpdateEvent[client.Object]{ObjectNew: node, ObjectOld: node})
	g.Expect(updateResult).To(gomega.BeFalse(), "UpdateFunc should return false")

	// DeleteFunc should return true
	deleteResult := pred.Delete(event.TypedDeleteEvent[client.Object]{Object: node})
	g.Expect(deleteResult).To(gomega.BeTrue(), "DeleteFunc should return true")
}

func TestHandleNodeDeletion(t *testing.T) {
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
		nodeName   string
		setupMocks func(client.Client)
		validate   func(*testing.T, client.Client, string)
	}{
		{
			name:     "Node deletion cleans up associated ConfigMap",
			nodeName: "node-with-configmap",
			setupMocks: func(c client.Client) {
				// Create ome namespace
				omeNamespace := &corev1.Namespace{
					ObjectMeta: metav1.ObjectMeta{
						Name: constants.OMENamespace,
					},
				}
				err := c.Create(context.TODO(), omeNamespace)
				g.Expect(err).NotTo(gomega.HaveOccurred())

				// Create a model status ConfigMap for this node
				configMap := &corev1.ConfigMap{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "node-with-configmap",
						Namespace: constants.OMENamespace,
						Labels: map[string]string{
							constants.ModelStatusConfigMapLabel: "true",
						},
					},
					Data: map[string]string{
						"clusterbasemodel.test-model": `{"status":"Ready"}`,
					},
				}
				err = c.Create(context.TODO(), configMap)
				g.Expect(err).NotTo(gomega.HaveOccurred())
			},
			validate: func(t *testing.T, c client.Client, nodeName string) {
				// ConfigMap should be deleted
				configMap := &corev1.ConfigMap{}
				err := c.Get(context.TODO(), types.NamespacedName{
					Namespace: constants.OMENamespace,
					Name:      nodeName,
				}, configMap)
				g.Expect(errors.IsNotFound(err)).To(gomega.BeTrue(), "ConfigMap should be deleted")
			},
		},
		{
			name:     "Node deletion with no ConfigMap does nothing",
			nodeName: "node-without-configmap",
			setupMocks: func(c client.Client) {
				// Create ome namespace but no ConfigMap
				omeNamespace := &corev1.Namespace{
					ObjectMeta: metav1.ObjectMeta{
						Name: constants.OMENamespace,
					},
				}
				err := c.Create(context.TODO(), omeNamespace)
				g.Expect(err).NotTo(gomega.HaveOccurred())
			},
			validate: func(t *testing.T, c client.Client, nodeName string) {
				// No ConfigMap to check - just ensure no error occurred
				// The function should silently skip
			},
		},
		{
			name:     "Node deletion skips non-model-status ConfigMap",
			nodeName: "node-with-other-configmap",
			setupMocks: func(c client.Client) {
				// Create ome namespace
				omeNamespace := &corev1.Namespace{
					ObjectMeta: metav1.ObjectMeta{
						Name: constants.OMENamespace,
					},
				}
				err := c.Create(context.TODO(), omeNamespace)
				g.Expect(err).NotTo(gomega.HaveOccurred())

				// Create a ConfigMap without model status label
				configMap := &corev1.ConfigMap{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "node-with-other-configmap",
						Namespace: constants.OMENamespace,
						// No model status label
					},
					Data: map[string]string{
						"some-key": "some-value",
					},
				}
				err = c.Create(context.TODO(), configMap)
				g.Expect(err).NotTo(gomega.HaveOccurred())
			},
			validate: func(t *testing.T, c client.Client, nodeName string) {
				// ConfigMap should NOT be deleted (it's not a model status ConfigMap)
				configMap := &corev1.ConfigMap{}
				err := c.Get(context.TODO(), types.NamespacedName{
					Namespace: constants.OMENamespace,
					Name:      nodeName,
				}, configMap)
				g.Expect(err).NotTo(gomega.HaveOccurred(), "ConfigMap should still exist")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := ctrlclientfake.NewClientBuilder().
				WithScheme(scheme).
				Build()

			tt.setupMocks(c)

			log := ctrl.Log.WithName("test")

			// Create the node object that was "deleted"
			deletedNode := &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name: tt.nodeName,
				},
			}

			// Call handleNodeDeletion (shared function)
			requests := handleNodeDeletion(context.TODO(), c, log, deletedNode)

			// Should return nil (no reconcile requests needed)
			g.Expect(requests).To(gomega.BeNil())

			// Validate the result
			tt.validate(t, c, tt.nodeName)
		})
	}
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

func shardedStatusWithPrewarmID(prewarmID string) v1beta1.ModelStatusSpec {
	return shardedStatusWithPrewarmAttempts(prewarmID, 1)
}

func shardedStatusWithPrewarmAttempts(prewarmID string, attempts int32) v1beta1.ModelStatusSpec {
	return v1beta1.ModelStatusSpec{
		Cache: &v1beta1.ModelCacheStatus{
			Backend:         modelcache.ProviderClusterCache,
			SourceUri:       "s3://bucket/cached-model",
			PrewarmID:       prewarmID,
			PrewarmAttempts: &attempts,
		},
		Conditions: []metav1.Condition{
			{
				Type:   v1beta1.ModelConditionSourceReachable,
				Status: metav1.ConditionTrue,
				Reason: v1beta1.ModelConditionReasonSourceReachable,
			},
		},
	}
}

func shardedCachedStatusForSource(sourceURI string) v1beta1.ModelStatusSpec {
	totalBytes := int64(100)
	cachedBytes := int64(100)
	totalChunks := int32(10)
	cachedChunks := int32(10)

	return v1beta1.ModelStatusSpec{
		State: v1beta1.LifeCycleStateImporting,
		Cache: &v1beta1.ModelCacheStatus{
			Backend:      modelcache.ProviderClusterCache,
			SourceUri:    sourceURI,
			PrewarmID:    "op-old",
			TotalBytes:   &totalBytes,
			CachedBytes:  &cachedBytes,
			TotalChunks:  &totalChunks,
			CachedChunks: &cachedChunks,
		},
		Conditions: []metav1.Condition{
			{
				Type:   v1beta1.ModelConditionSourceReachable,
				Status: metav1.ConditionTrue,
				Reason: v1beta1.ModelConditionReasonSourceReachable,
			},
			{
				Type:   v1beta1.ModelConditionCached,
				Status: metav1.ConditionTrue,
				Reason: v1beta1.ModelConditionReasonPrewarmSucceeded,
			},
			{
				Type:   v1beta1.ModelConditionReady,
				Status: metav1.ConditionFalse,
				Reason: v1beta1.ModelConditionReasonMetadataPending,
			},
		},
	}
}

func shardedInspectionForTest(t *testing.T, source modelcache.Source) *modelcache.SourceInspection {
	t.Helper()

	safetensorsData := safetensorsHeaderForTest(`{"weight":{"dtype":"F32","shape":[2,3],"data_offsets":[0,24]}}`)
	weightPath := source.URI + "/model-00001-of-00001.safetensors"
	return &modelcache.SourceInspection{
		Objects: []modelcache.Object{
			{
				Source:    source,
				Path:      weightPath,
				SizeBytes: int64(len(safetensorsData)) + 1024,
			},
		},
		Metadata: modelcache.ModelMetadataInputs{
			ConfigJSON: tinyPhiConfigForTest(t),
			SafetensorHeaders: []modelcache.SafetensorHeader{
				{
					Path:       weightPath,
					SizeBytes:  int64(len(safetensorsData)) + 1024,
					HeaderJSON: safetensorsHeaderJSONForTest(safetensorsData),
				},
			},
		},
	}
}

func getBaseModelForTest(t *testing.T, c client.Client, namespace, name string) *v1beta1.BaseModel {
	t.Helper()
	g := gomega.NewGomegaWithT(t)

	baseModel := &v1beta1.BaseModel{}
	err := c.Get(context.TODO(), types.NamespacedName{Namespace: namespace, Name: name}, baseModel)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	return baseModel
}

func writeJSONForControllerTest(t *testing.T, w http.ResponseWriter, value any) {
	t.Helper()

	w.Header().Set("Content-Type", "application/json")
	gomega.NewGomegaWithT(t).Expect(json.NewEncoder(w).Encode(value)).NotTo(gomega.HaveOccurred())
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

	cached := meta.FindStatusCondition(status.Conditions, v1beta1.ModelConditionCached)
	g.Expect(cached).ToNot(gomega.BeNil())
	g.Expect(cached.Status).To(gomega.Equal(metav1.ConditionFalse))
	g.Expect(cached.Reason).To(gomega.Equal(v1beta1.ModelConditionReasonProviderNotConfigured))

	ready := meta.FindStatusCondition(status.Conditions, v1beta1.ModelConditionReady)
	g.Expect(ready).ToNot(gomega.BeNil())
	g.Expect(ready.Status).To(gomega.Equal(metav1.ConditionFalse))
	g.Expect(ready.Reason).To(gomega.Equal(v1beta1.ModelConditionReasonProviderNotConfigured))
}
