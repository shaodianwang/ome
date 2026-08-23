package framework

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	configapi "sigs.k8s.io/kueue/apis/config/v1beta2"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	qcache "sigs.k8s.io/kueue/pkg/cache/queue"
	schdcache "sigs.k8s.io/kueue/pkg/cache/scheduler"
	"sigs.k8s.io/kueue/pkg/constants"
	"sigs.k8s.io/kueue/pkg/controller/core"
	coreindexer "sigs.k8s.io/kueue/pkg/controller/core/indexer"
	"sigs.k8s.io/kueue/pkg/controller/jobframework"
	"sigs.k8s.io/kueue/pkg/controller/jobs/pod"
	"sigs.k8s.io/kueue/pkg/scheduler"
	preemptexpectations "sigs.k8s.io/kueue/pkg/scheduler/preemption/expectations"
	utiltesting "sigs.k8s.io/kueue/pkg/util/testing/v1beta2"
	"sigs.k8s.io/kueue/pkg/webhooks"
)

const kueueModulePath = "sigs.k8s.io/kueue"

// StartKueue wires Kueue's full admission stack onto the supplied
// manager and starts its scheduler: core controllers + indexers, the
// pod-integration reconciler + webhook, the core webhooks, and the
// scheduler goroutine. After StartKueue returns, a queue-labeled pod
// (one carrying the kueue.x-k8s.io/queue-name label) created in a
// non-excluded namespace is GATED by the pod webhook and then UNGATED
// once the scheduler admits it against a ClusterQueue's quota.
//
// Prerequisites the caller owns:
//   - The manager's scheme has the Kueue types registered (use the
//     framework.Kueue OptionalCRDBundle, which contributes
//     kueue.AddToScheme to the client-side scheme; configapi.AddToScheme
//     is registered here on the manager's scheme so the default
//     Configuration can be built).
//   - The manager has a webhook server configured (envtest's
//     WebhookInstallOptions host/port/certdir) and the Kueue pod webhook
//     manifests are installed on the apiserver. The Kueue webhook
//     manifests are NOT in dep-crds (those are CRDs only); they live in
//     the kueue module's config/components/webhook — see kueue_test.go
//     for how the smoke test points envtest at them.
//
// The wiring mirrors Kueue's own cmd/kueue manager setup, reduced to the
// pod integration; the option set is load-bearing and documented inline
// below.
func StartKueue(ctx context.Context, mgr ctrl.Manager) error {
	// configapi types must be on the manager scheme so the default
	// Configuration can be materialized via scheme.Default below.
	if err := configapi.AddToScheme(mgr.GetScheme()); err != nil {
		return fmt.Errorf("StartKueue: register config scheme: %w", err)
	}

	if err := coreindexer.Setup(ctx, mgr.GetFieldIndexer()); err != nil {
		return fmt.Errorf("StartKueue: core indexer setup: %w", err)
	}
	if err := pod.SetupIndexes(ctx, mgr.GetFieldIndexer()); err != nil {
		return fmt.Errorf("StartKueue: pod indexes setup: %w", err)
	}

	jobframework.EnableIntegration(pod.FrameworkName)

	// The pod webhook gates every pod in a namespace this selector matches;
	// the system namespaces are excluded so the harness never gates Kueue's
	// or the control plane's own pods.
	nsSelector := &metav1.LabelSelector{
		MatchExpressions: []metav1.LabelSelectorRequirement{{
			Key:      "kubernetes.io/metadata.name",
			Operator: metav1.LabelSelectorOpNotIn,
			Values:   []string{"kube-system", "kueue-system"},
		}},
	}
	mjnsSelector, err := metav1.LabelSelectorAsSelector(nsSelector)
	if err != nil {
		return fmt.Errorf("StartKueue: managed-jobs ns selector: %w", err)
	}

	// The scheduler cache and the queue manager are a pair: the cache holds
	// admitted usage, the queues hold what is waiting. The requeuer is the
	// Runnable that re-drives workloads parked as inadmissible, so it has to
	// be handed to the manager or a workload that misses quota once is never
	// retried when quota is released.
	cCache := schdcache.New(mgr.GetClient())
	requeuer := qcache.NewRequeuer()
	if err := mgr.Add(requeuer); err != nil {
		return fmt.Errorf("StartKueue: add inadmissible-workload requeuer: %w", err)
	}
	preemptionExpectations := preemptexpectations.New()
	queues := qcache.NewManager(mgr.GetClient(), cCache, requeuer,
		qcache.WithPreemptionExpectations(preemptionExpectations))

	opts := []jobframework.Option{
		// Without an explicit queue-name label, pods are left alone —
		// the harness drives admission by labeling pods, not by managing
		// every workload.
		jobframework.WithManageJobsWithoutQueueName(false),
		jobframework.WithManagedJobsNamespaceSelector(mjnsSelector),
		jobframework.WithCache(cCache),
		jobframework.WithQueues(queues),
	}

	podRec, err := pod.NewReconciler(ctx, mgr.GetClient(), mgr.GetFieldIndexer(),
		mgr.GetEventRecorderFor(constants.JobControllerName), opts...)
	if err != nil {
		return fmt.Errorf("StartKueue: pod reconciler: %w", err)
	}
	if err := podRec.SetupWithManager(mgr); err != nil {
		return fmt.Errorf("StartKueue: pod reconciler setup: %w", err)
	}

	conf := &configapi.Configuration{}
	mgr.GetScheme().Default(conf)
	// A nil RoleTracker is Kueue's standalone mode: every controller acts as
	// the active one. The harness runs a single manager without leader
	// election, so there is no follower to demote.
	if _, err := core.SetupControllers(mgr, queues, cCache, conf, nil, preemptionExpectations); err != nil {
		return fmt.Errorf("StartKueue: core.SetupControllers: %w", err)
	}

	if err := pod.SetupWebhook(mgr, opts...); err != nil {
		return fmt.Errorf("StartKueue: pod webhook setup: %w", err)
	}
	if _, err := webhooks.Setup(mgr, nil); err != nil {
		return fmt.Errorf("StartKueue: webhooks.Setup: %w", err)
	}

	sched := scheduler.New(queues, cCache, mgr.GetClient(),
		mgr.GetEventRecorderFor(constants.AdmissionName),
		scheduler.WithPreemptionExpectations(preemptionExpectations))
	if err := sched.Start(ctx); err != nil {
		return fmt.Errorf("StartKueue: scheduler start: %w", err)
	}
	return nil
}

// ApplyKueueQuota creates the minimal Kueue objects that make a
// LocalQueue admit pods: a cluster-scoped ResourceFlavor "default", a
// ClusterQueue named queueName+"-cq" granting the given CPU quota on
// that flavor, and a LocalQueue named queueName in namespace bound to
// that ClusterQueue. Pods labeled with queueName are then admitted up to
// the quota.
//
// cpu is a resource.Quantity string (e.g. "1" admits a 500m pod; "0"
// blocks any pod with a non-zero CPU request). Idempotent: an
// AlreadyExists on any object is treated as success, so the same quota
// can be re-applied across specs.
func ApplyKueueQuota(ctx context.Context, c client.Client, namespace, queueName, cpu string) error {
	cqName := queueName + "-cq"

	flavor := utiltesting.MakeResourceFlavor("default").Obj()
	if err := createIgnoreExists(ctx, c, flavor); err != nil {
		return fmt.Errorf("ApplyKueueQuota: resource flavor: %w", err)
	}

	cq := utiltesting.MakeClusterQueue(cqName).
		ResourceGroup(*utiltesting.MakeFlavorQuotas("default").Resource(corev1.ResourceCPU, cpu).Obj()).
		Obj()
	if err := upsertClusterQueueQuota(ctx, c, cq); err != nil {
		return fmt.Errorf("ApplyKueueQuota: cluster queue: %w", err)
	}

	lq := utiltesting.MakeLocalQueue(queueName, namespace).ClusterQueue(cqName).Obj()
	if err := createIgnoreExists(ctx, c, lq); err != nil {
		return fmt.Errorf("ApplyKueueQuota: local queue: %w", err)
	}
	return nil
}

// KueueWebhookDir resolves the Kueue pod-webhook manifest directory in
// the read-only module cache. StartKueue's pod webhook is only installed
// on envtest if these manifests are applied, so suites that run StartKueue
// pass this into Framework.ExtraWebhookPaths. The Kueue webhook manifests
// are NOT vendored into dep-crds (that dir is CRDs-only).
//
// The directory is resolved with `go list -m`, the same way the kueue-crds
// Makefile target locates the CRDs, so a go.mod kueue bump needs no edit here
// and a module cache holding several Kueue versions cannot serve manifests
// from the wrong one. It returns ("", error) when the module dir cannot be
// located.
func KueueWebhookDir() (string, error) {
	out, err := exec.Command("go", "list", "-m", "-f", "{{.Dir}}", kueueModulePath).Output()
	if err != nil {
		return "", fmt.Errorf("KueueWebhookDir: go list -m %s: %w", kueueModulePath, err)
	}
	moduleDir := strings.TrimSpace(string(out))
	if moduleDir == "" {
		return "", fmt.Errorf("KueueWebhookDir: %s has no module dir; run `go mod download`", kueueModulePath)
	}
	whDir := filepath.Join(moduleDir, "config", "components", "webhook")
	if _, err := os.Stat(whDir); err != nil {
		return "", fmt.Errorf("KueueWebhookDir: %s: %w", whDir, err)
	}
	return whDir, nil
}

// createIgnoreExists creates obj, treating AlreadyExists as success so
// quota helpers are idempotent across specs.
func createIgnoreExists(ctx context.Context, c client.Client, obj client.Object) error {
	if err := c.Create(ctx, obj); err != nil && !apierrors.IsAlreadyExists(err) {
		return err
	}
	return nil
}

// upsertClusterQueueQuota creates the ClusterQueue or, if it already exists,
// updates its resource groups to the desired quota. Unlike createIgnoreExists,
// re-applying with a DIFFERENT cpu actually RAISES (or lowers) the quota — the
// "release quota" step of the re-placement / all-components specs depends on this
// (a plain create-ignore-exists would silently keep the original, smaller quota).
// The update retries on the benign 409 the Kueue LocalQueue/CQ controller emits
// when it reconciles status concurrently.
func upsertClusterQueueQuota(ctx context.Context, c client.Client, desired *kueue.ClusterQueue) error {
	if err := c.Create(ctx, desired); err == nil || !apierrors.IsAlreadyExists(err) {
		return err
	}
	for {
		existing := &kueue.ClusterQueue{}
		if err := c.Get(ctx, client.ObjectKeyFromObject(desired), existing); err != nil {
			return err
		}
		existing.Spec.ResourceGroups = desired.Spec.ResourceGroups
		err := c.Update(ctx, existing)
		if err == nil {
			return nil
		}
		if !apierrors.IsConflict(err) {
			return err
		}
		// Conflict: re-read and retry.
	}
}
