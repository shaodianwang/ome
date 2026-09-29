package util_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	authenticationv1 "k8s.io/api/authentication/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/controllerconfig"
	workloadtypes "github.com/ome-projects/ome/pkg/controller/v1beta1/workload/types"
	"github.com/ome-projects/ome/tests/integration/framework"
	"github.com/ome-projects/ome/tests/integration/util"
)

// TestUtil is the Ginkgo entrypoint for self-tests of the integration
// util package.
func TestUtil(t *testing.T) {
	util.RunSuite(t, "Integration Util Self-Test Suite")
}

// Suite-wide envtest shared across specs (boots once, ~5s).
var (
	suiteCtx    context.Context
	suiteFwk    *framework.Framework
	suiteClient client.Client
)

var _ = ginkgo.BeforeSuite(func() {
	s, err := framework.NewSuite(&framework.Framework{})
	Expect(err).NotTo(HaveOccurred())
	suiteFwk = s.Fwk
	suiteClient = s.Client
	suiteCtx = s.Ctx
})

var _ = ginkgo.AfterSuite(func() {
	if suiteFwk != nil {
		_ = suiteFwk.Teardown()
	}
})

var _ = ginkgo.Describe("util.CreateNamespaceFromPrefix", func() {
	ginkgo.It("two consecutive calls produce different names with the same prefix", func() {
		a, err := util.CreateNamespaceFromPrefix(suiteCtx, suiteClient, "ns-uniq-")
		Expect(err).NotTo(HaveOccurred())
		b, err := util.CreateNamespaceFromPrefix(suiteCtx, suiteClient, "ns-uniq-")
		Expect(err).NotTo(HaveOccurred())
		Expect(a.Name).NotTo(Equal(b.Name), "GenerateName must produce unique names")

		ginkgo.DeferCleanup(func() {
			_ = util.DeleteNamespace(suiteCtx, suiteClient, a.Name)
			_ = util.DeleteNamespace(suiteCtx, suiteClient, b.Name)
		})
	})
})

var _ = ginkgo.Describe("util.SeedDefaultConfigMaps", func() {
	ginkgo.It("creates inferenceservice-config in the OME namespace", func() {
		Expect(util.SeedDefaultConfigMaps(suiteCtx, suiteClient)).To(Succeed())

		ginkgo.By("idempotent: a second call returns nil without erroring")
		Expect(util.SeedDefaultConfigMaps(suiteCtx, suiteClient)).To(Succeed())

		ginkgo.By("the helpers in pkg/controller/v1beta1/controllerconfig parse the seeded JSON")
		// We don't call NewIngressConfig directly here (it uses
		// kubernetes.Interface, not controller-runtime client). The
		// real validation of these JSON shapes happens in the ISVC
		// suite that consumes them. Here we only assert the ConfigMap
		// exists with the right keys.
		cm := &corev1.ConfigMap{}
		Expect(suiteClient.Get(suiteCtx,
			types.NamespacedName{Namespace: "ome", Name: "inferenceservice-config"},
			cm)).To(Succeed())
		Expect(cm.Data).To(HaveKey("ingress"))
		Expect(cm.Data).To(HaveKey("deploy"))
		Expect(cm.Data).To(HaveKey("omeAgent"))
		Expect(cm.Data).To(HaveKey("lifecycle"))
		// The lifecycle block is asserted through the parsers the manager
		// itself uses, not against a literal: every knob here is
		// config-only (the binary carries no default), so what matters is
		// that the seed round-trips to the chart values the suites expect.
		lifecycle := &controllerconfig.LifecycleConfig{}
		Expect(json.Unmarshal([]byte(cm.Data["lifecycle"]), lifecycle)).To(Succeed())
		Expect(lifecycle.ToScaleUpPodBatchSize()).To(HaveValue(Equal(int32(100))))
		Expect(lifecycle.ToScaleDownPodBatchSize()).To(HaveValue(Equal(int32(100))))
		Expect(lifecycle.ToRevisionHistoryLimit()).To(HaveValue(Equal(int32(10))))
		Expect(lifecycle.ToRetryBlockHistoryLimit()).To(HaveValue(Equal(int32(3))))
		Expect(lifecycle.GangScheduleTimeout.ToClamp()).To(HaveValue(Equal(workloadtypes.GangScheduleTimeoutClamp{
			Min: time.Minute, Max: 10 * time.Minute,
		})))
		Expect(lifecycle.Audit.ToPolicy()).To(HaveValue(Equal(workloadtypes.MigrationAuditPolicy{
			MaxInFlight: 3, MaxPerWindow: 10, Window: time.Hour,
		})))
		Expect(lifecycle.Requeue.ToIntervals()).To(Equal(workloadtypes.RequeueIntervals{
			Operation: 5 * time.Second, Gate: 3 * time.Second,
		}))
		// The rollout block must round-trip the chart defaults: the canary
		// capacity-gate escalation timeout has no in-code fallback, so the
		// seed is what keeps suite behavior chart-equivalent.
		Expect(cm.Data).To(HaveKey("rollout"))
		Expect(cm.Data["rollout"]).To(MatchJSON(`{"maxPinnedPlanBytes":16384,"defaultReadyTimeout":"15m","canaryRequeue":"10s","canaryParkedRequeue":"5m"}`))
		// The omenativeStatus block is required by the InferenceReplica
		// reconciler at startup; the seed targets the chart default.
		Expect(cm.Data).To(HaveKey("omenativeStatus"))
		Expect(cm.Data["omenativeStatus"]).To(MatchJSON(`{"instanceStatusEncoding":"ColumnarV2","maxDecodedInstances":20000}`))
		// The deploy block must round-trip the chart-equivalent replica and
		// update-strategy defaults the reconciler fills on the merged
		// component spec. Without the updateStrategy block an OMENative
		// component runs with no per-Component rollout budget, since the
		// binary carries none.
		Expect(cm.Data["deploy"]).To(MatchJSON(`{
			"defaultDeploymentMode": "RawDeployment",
			"replicas": {
				"defaultMinReplicas": 1,
				"defaultMaxReplicas": {"engine": 3, "decoder": 3, "router": 2}
			},
			"updateStrategy": {
				"router":  {"type": "SurgeThenDrain", "maxSurge": "25%", "maxUnavailable": "25%"},
				"engine":  {"type": "SurgeThenDrain", "maxSurge": "25%", "maxUnavailable": "25%"},
				"decoder": {"type": "SurgeThenDrain", "maxSurge": "25%", "maxUnavailable": "25%"}
			}
		}`))
		Expect(cm.Data).To(HaveKeyWithValue(controllerconfig.InferenceReplicaConfigName, util.InferenceReplicaConfigJSON()))
	})

	// The InferenceReplica webhook admits spec writes on projected replicas only
	// from the controller identity, and the in-process manager writes as the
	// envtest admin (system:masters) or the kind admin (kubeadm:cluster-admins).
	ginkgo.It("seeds an inferenceReplica block the webhook loader reads as the harness admin identities", func() {
		clientset := fake.NewSimpleClientset(&corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: constants.InferenceServiceConfigMapName, Namespace: constants.OMENamespace},
			Data:       map[string]string{controllerconfig.InferenceReplicaConfigName: util.InferenceReplicaConfigJSON()},
		})
		cfg, err := controllerconfig.NewInferenceReplicaConfigCached(nil, clientset)
		Expect(err).NotTo(HaveOccurred())
		identity := cfg.Identity()
		Expect(identity.Matches(authenticationv1.UserInfo{
			Username: "admin", Groups: []string{"system:masters", "system:authenticated"},
		})).To(BeTrue(), "the envtest admin")
		Expect(identity.Matches(authenticationv1.UserInfo{
			Username: "kubernetes-admin", Groups: []string{"kubeadm:cluster-admins", "system:authenticated"},
		})).To(BeTrue(), "the kind admin")
		Expect(identity.Matches(authenticationv1.UserInfo{
			Username: "example-user", Groups: []string{"system:authenticated"},
		})).To(BeFalse(), "an ordinary authenticated caller")
	})
})

var _ = ginkgo.Describe("util.DeleteNamespace", func() {

	ginkgo.It("runs registered cleanups with the right namespace argument", func() {
		ns, err := util.CreateNamespaceFromPrefix(suiteCtx, suiteClient, "ns-cleanup-")
		Expect(err).NotTo(HaveOccurred())

		var observed atomic.Value
		util.RegisterCleanup(func(ctx context.Context, c client.Client, namespace string) error {
			if namespace == ns.Name {
				observed.Store(namespace)
			}
			return nil
		})

		Expect(util.DeleteNamespace(suiteCtx, suiteClient, ns.Name)).To(Succeed())
		got, _ := observed.Load().(string)
		Expect(got).To(Equal(ns.Name), "cleanup hook must have observed our namespace")
	})

	ginkgo.It("propagates a CleanupFunc error via errors.Is", func() {
		ns, err := util.CreateNamespaceFromPrefix(suiteCtx, suiteClient, "ns-cleanuperr-")
		Expect(err).NotTo(HaveOccurred())

		target := errors.New("ns-cleanuperr-injected")
		util.RegisterCleanup(func(ctx context.Context, c client.Client, namespace string) error {
			if namespace == ns.Name {
				return target
			}
			return nil
		})

		err = util.DeleteNamespace(suiteCtx, suiteClient, ns.Name)
		Expect(err).To(HaveOccurred())
		Expect(errors.Is(err, target)).To(BeTrue(), "wrapped sentinel must surface")
	})

	ginkgo.It("an empty namespace name is a no-op (foot-gun guard for AfterEach with unset state)", func() {
		Expect(util.DeleteNamespace(context.Background(), suiteClient, "")).To(Succeed())
	})
})
