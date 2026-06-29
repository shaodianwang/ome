package framework

import (
	"context"
	"fmt"
	"go/build"
	"os"
	"path/filepath"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	configapi "sigs.k8s.io/kueue/apis/config/v1beta1"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta1"
	"sigs.k8s.io/kueue/pkg/cache"
	"sigs.k8s.io/kueue/pkg/constants"
	"sigs.k8s.io/kueue/pkg/controller/core"
	coreindexer "sigs.k8s.io/kueue/pkg/controller/core/indexer"
	"sigs.k8s.io/kueue/pkg/controller/jobframework"
	"sigs.k8s.io/kueue/pkg/controller/jobs/pod"
	"sigs.k8s.io/kueue/pkg/queue"
	"sigs.k8s.io/kueue/pkg/scheduler"
	utiltesting "sigs.k8s.io/kueue/pkg/util/testing"
	"sigs.k8s.io/kueue/pkg/webhooks"
)

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
//     kueue.AddToScheme + kueuealpha.AddToScheme to the client-side
//     scheme; configapi.AddToScheme is registered here on the manager's
//     scheme so the default Configuration can be built).
//   - The manager has a webhook server configured (envtest's
//     WebhookInstallOptions host/port/certdir) and the Kueue pod webhook
//     manifests are installed on the apiserver. The Kueue webhook
//     manifests are NOT in dep-crds (those are CRDs only); they live in
//     the kueue module's config/components/webhook — see kueue_test.go
//     for how the smoke test points envtest at them.
//
// The wiring mirrors kueue@v0.10.6's pod integration suite managerSetup;
// the option set is load-bearing and documented inline below.
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

	// The pod webhook gates a pod only if it matches the integration's
	// podSelector AND namespaceSelector. A nil podSelector becomes
	// labels.Nothing() (matches nothing) -> no gate, so PodSelector MUST
	// be a non-nil EMPTY selector to gate all pods.
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

	opts := []jobframework.Option{
		// Without an explicit queue-name label, pods are left alone —
		// the harness drives admission by labeling pods, not by managing
		// every workload.
		jobframework.WithManageJobsWithoutQueueName(false),
		jobframework.WithManagedJobsNamespaceSelector(mjnsSelector),
		jobframework.WithIntegrationOptions(corev1.SchemeGroupVersion.WithKind("Pod").String(), &configapi.PodIntegrationOptions{
			PodSelector:       &metav1.LabelSelector{}, // non-nil empty = all pods; nil silently disables gating
			NamespaceSelector: nsSelector,
		}),
	}

	podRec := pod.NewReconciler(mgr.GetClient(), mgr.GetEventRecorderFor(constants.JobControllerName), opts...)
	if err := podRec.SetupWithManager(mgr); err != nil {
		return fmt.Errorf("StartKueue: pod reconciler setup: %w", err)
	}

	cCache := cache.New(mgr.GetClient())
	queues := queue.NewManager(mgr.GetClient(), cCache)
	opts = append(opts, jobframework.WithQueues(queues))

	conf := &configapi.Configuration{}
	mgr.GetScheme().Default(conf)
	if _, err := core.SetupControllers(mgr, queues, cCache, conf); err != nil {
		return fmt.Errorf("StartKueue: core.SetupControllers: %w", err)
	}

	if err := pod.SetupWebhook(mgr, opts...); err != nil {
		return fmt.Errorf("StartKueue: pod webhook setup: %w", err)
	}
	if _, err := webhooks.Setup(mgr); err != nil {
		return fmt.Errorf("StartKueue: webhooks.Setup: %w", err)
	}

	sched := scheduler.New(queues, cCache, mgr.GetClient(), mgr.GetEventRecorderFor(constants.AdmissionName))
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
// The module version is resolved by globbing the cache (sigs.k8s.io/kueue@*)
// rather than hardcoding it, so a go.mod kueue bump needs no edit here. It
// returns ("", error) when the module dir cannot be located.
func KueueWebhookDir() (string, error) {
	mc := os.Getenv("GOMODCACHE")
	if mc == "" {
		mc = filepath.Join(build.Default.GOPATH, "pkg", "mod")
	}
	matches, err := filepath.Glob(filepath.Join(mc, "sigs.k8s.io", "kueue@*"))
	if err != nil {
		return "", fmt.Errorf("KueueWebhookDir: glob module cache: %w", err)
	}
	if len(matches) == 0 {
		return "", fmt.Errorf("KueueWebhookDir: no sigs.k8s.io/kueue@* in module cache %s", mc)
	}
	// Prefer a dir that actually has the webhook manifests; on a multi-version
	// cache this skips a stale entry.
	for _, dir := range matches {
		whDir := filepath.Join(dir, "config", "components", "webhook")
		if _, statErr := os.Stat(whDir); statErr == nil {
			return whDir, nil
		}
	}
	return "", fmt.Errorf("KueueWebhookDir: no config/components/webhook under any of %v", matches)
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
