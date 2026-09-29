// Package inferencereplica hosts the envtest suite for the InferenceReplica
// validating webhook: the ownership rules per form (projected by an
// InferenceService, or standalone) and per requester identity. Only the
// webhook runs; no controller.
package inferencereplica

import (
	"context"
	"testing"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/controllerconfig"
	"github.com/ome-projects/ome/tests/integration/components"
	"github.com/ome-projects/ome/tests/integration/framework"
	"github.com/ome-projects/ome/tests/integration/util"
)

// plainUser is an identity with cluster-admin RBAC that is not the
// controller, so every denial in this suite comes from the webhook.
const plainUser = "example-user"

var (
	suiteCtx    context.Context
	suiteCancel context.CancelFunc
	suiteFwk    *framework.Framework
	// k8sClient acts as the envtest admin (group system:masters), which the
	// seeded config names as the controller identity.
	k8sClient client.Client
	// userClient impersonates plainUser.
	userClient client.Client
)

func TestInferenceReplicaWebhook(t *testing.T) {
	util.RunSuite(t, "InferenceReplica Webhook Integration Suite")
}

var _ = ginkgo.BeforeSuite(func() {
	framework.SetupTestLogger()

	s, err := framework.NewSuite(&framework.Framework{
		WebhookPath: "tests/integration/singlecluster/webhook/inferencereplica/testdata",
	})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	suiteFwk = s.Fwk
	k8sClient = s.Client
	suiteCtx, suiteCancel = context.WithCancel(s.Ctx)

	ginkgo.By("seeding inferenceservice-config with the controller identity")
	gomega.Expect(k8sClient.Create(suiteCtx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: constants.OMENamespace},
	})).To(gomega.Succeed())
	gomega.Expect(k8sClient.Create(suiteCtx, &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: constants.InferenceServiceConfigMapName, Namespace: constants.OMENamespace},
		Data:       map[string]string{controllerconfig.InferenceReplicaConfigName: util.InferenceReplicaConfigJSON()},
	})).To(gomega.Succeed())

	ginkgo.By("granting the plain user cluster-admin so only the webhook can deny it")
	gomega.Expect(k8sClient.Create(suiteCtx, &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: plainUser + "-admin"},
		RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: "cluster-admin"},
		Subjects:   []rbacv1.Subject{{APIGroup: rbacv1.GroupName, Kind: rbacv1.UserKind, Name: plainUser}},
	})).To(gomega.Succeed())
	userCfg := rest.CopyConfig(s.Cfg)
	userCfg.Impersonate = rest.ImpersonationConfig{UserName: plainUser, Groups: []string{"system:authenticated"}}
	userClient, err = client.New(userCfg, client.Options{Scheme: k8sClient.Scheme()})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())

	mgrSetup := func(ctx context.Context, mgr ctrl.Manager) error {
		return components.SetupInferenceReplicaWebhook(mgr)
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
