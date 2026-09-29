// Package inferencereplicatpuslice hosts the envtest suite for the
// InferenceReplica controller's TPU slice provisioning. Only the IR
// reconciler runs, against a stub of the provider's Slice CRD: no slice
// provider, no scheduler, no kubelet. The specs create InferenceReplica
// objects directly and act as the provider themselves, reporting a
// slice's state by writing its status. Pods never bind to a node.
package inferencereplicatpuslice

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/controllerconfig"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
	"github.com/ome-projects/ome/tests/integration/components"
	"github.com/ome-projects/ome/tests/integration/framework"
	"github.com/ome-projects/ome/tests/integration/util"
)

// Ready-condition reasons the specs report for a slice. Only the first two
// are configured ready states.
const (
	stateActive   = "ACTIVE"
	stateDegraded = "ACTIVE_DEGRADED"
	stateCreating = "CREATING"
)

// Accelerator label values of the suite's two node pools.
const (
	onDemandAccelerator = "tpu7x"
	staticAccelerator   = "v6e"
)

// sliceConfig is the tpuSliceProvisioning block the suite writes into
// inferenceservice-config. Both accelerators are configured, but only the
// onDemandAccelerator pool carries the provision-only label: the
// staticAccelerator pool's slices are static.
var sliceConfig = controllerconfig.TPUSliceProvisioningConfig{
	ChipResource: "example.com/tpu",
	NodeLabels: controllerconfig.TPUSliceNodeLabels{
		Accelerator: "example.com/tpu-accelerator",
		Topology:    "example.com/tpu-topology",
		Slice:       "example.com/tpu-slice",
	},
	ProvisionOnly: controllerconfig.TPUSliceLabel{Key: "example.com/tpu-provisioning", Value: "on-demand"},
	Accelerators: map[string]controllerconfig.TPUSliceAccelerator{
		onDemandAccelerator: {SliceType: "tpu7x", ChipsPerHost: 4, Topologies: []string{"2x2x1", metricsTopology, "2x2x4"}},
		staticAccelerator:   {SliceType: "v6e", ChipsPerHost: 4, Topologies: []string{"2x2"}},
	},
	Slice: controllerconfig.TPUSliceObject{
		OwnerKindLabel: "example.com/slice-owner-kind",
		OwnerNameLabel: "example.com/slice-owner-name",
		Annotations:    map[string]string{"example.com/slice-scheduler": "on-demand"},
		ReadyStates:    []string{stateActive, stateDegraded},
	},
}

var (
	suiteCtx    context.Context
	suiteCancel context.CancelFunc
	suiteFwk    *framework.Framework
	k8sClient   client.Client
)

func TestInferenceReplicaTPUSlice(t *testing.T) {
	util.RunSuite(t, "InferenceReplica TPU Slice Provisioning Integration Suite")
}

var _ = ginkgo.BeforeSuite(func() {
	framework.SetupTestLogger()

	s, err := framework.NewSuite(&framework.Framework{
		OptionalCRDs: []framework.OptionalCRDBundle{framework.GKESlice},
	})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	suiteFwk = s.Fwk
	k8sClient = s.Client
	suiteCtx, suiteCancel = context.WithCancel(s.Ctx)

	ginkgo.By("creating a node of each pool")
	for _, node := range []*corev1.Node{
		{ObjectMeta: metav1.ObjectMeta{Name: "on-demand-node", Labels: map[string]string{
			sliceConfig.NodeLabels.Accelerator: onDemandAccelerator,
			sliceConfig.ProvisionOnly.Key:      sliceConfig.ProvisionOnly.Value,
		}}},
		{ObjectMeta: metav1.ObjectMeta{Name: "static-node", Labels: map[string]string{
			sliceConfig.NodeLabels.Accelerator: staticAccelerator,
			sliceConfig.NodeLabels.Topology:    "2x2",
		}}},
	} {
		gomega.Expect(k8sClient.Create(suiteCtx, node)).To(gomega.Succeed())
	}

	// The configuration is read once, when the reconciler is set up, so it
	// must be in place before the manager starts.
	ginkgo.By("seeding inferenceservice-config with the tpuSliceProvisioning block")
	gomega.Expect(util.SeedDefaultConfigMaps(suiteCtx, k8sClient)).To(gomega.Succeed())
	block, err := json.Marshal(sliceConfig)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	configMap := &corev1.ConfigMap{}
	gomega.Expect(k8sClient.Get(suiteCtx, client.ObjectKey{
		Namespace: constants.OMENamespace,
		Name:      constants.InferenceServiceConfigMapName,
	}, configMap)).To(gomega.Succeed())
	configMap.Data[controllerconfig.TPUSliceProvisioningConfigName] = string(block)
	gomega.Expect(k8sClient.Update(suiteCtx, configMap)).To(gomega.Succeed())

	ginkgo.By("registering the InferenceReplica reconciler")
	mgrSetup := func(ctx context.Context, mgr ctrl.Manager) error {
		// The workload pipeline lists pods through this cache field
		// index (mirrors the production manager wiring).
		if err := query.RegisterOMENativePodIndex(ctx, mgr.GetFieldIndexer()); err != nil {
			return err
		}
		return components.SetupInferenceReplica(ctx, mgr)
	}
	gomega.Expect(suiteFwk.StartManager(suiteCtx, s.Cfg, mgrSetup)).To(gomega.Succeed())
})

var _ = ginkgo.AfterSuite(func() {
	if suiteCancel != nil {
		suiteCancel()
	}
	if suiteFwk != nil {
		_ = suiteFwk.StopManager(context.Background())
		_ = suiteFwk.Teardown()
	}
})
