package kindfix

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	admissionv1 "k8s.io/api/admissionregistration/v1"
	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/tests/integration/kindutil"
)

// KueueManagedLabel marks the namespaces Kueue is allowed to manage.
//
// Every OME-derived pod carries a Kueue queue-name label, so an installed Kueue
// would otherwise gate the workload of every spec in every suite — including the
// ones that grant no quota and would simply hang. Admission is therefore opt-in
// per namespace: a spec that wants gating labels its namespace, everything else
// is untouched.
//
// Two things enforce that, and only the second actually prevents gating:
// managedJobsNamespaceSelector in the rendered Configuration, and the webhook
// namespaceSelector rewritten by scopeWebhooksToManagedNamespaces. See there for
// why the Configuration alone is not enough.
//
// Kept in step with KUEUE_MANAGED_LABEL in the Makefile by InstallKueue, which
// refuses to install a manifest that does not mention it.
const KueueManagedLabel = "ome.io/kueue-managed"

// kueueInstallDir holds the manifest rendered by `make kueue-install`, relative
// to the repo root. Gitignored and vendored like the other dep-crds, because it
// is derived from the Kueue version in kueueVersionFile.
const kueueInstallDir = "tests/integration/dep-crds/kueue-install"

// kueueVersionFile pins the Kueue version the fleet runs, relative to the repo
// root.
const kueueVersionFile = ".kueue-crd-version"

// kueueRolloutTimeout bounds the wait for the controller to become Available.
// Generous: the node has to unpack the image before the container starts.
const kueueRolloutTimeout = 3 * time.Minute

// InstallKueue brings Kueue up inside a KIND cluster: it loads the controller
// image the node cannot pull, applies the rendered install manifest, and waits
// until the deployment is Available and its webhook answers.
//
// Kueue has to RUN, not merely be registered — pods are gated by its mutating
// webhook, so its CRDs alone prove nothing about admission. That is the whole
// reason this exists rather than another entry in OMECRDPaths.
func InstallKueue(ctx context.Context, cfg *rest.Config, clusterName string) error {
	root, err := repoRoot()
	if err != nil {
		return err
	}
	manifest := filepath.Join(root, kueueInstallDir, "kueue.yaml")
	raw, err := os.ReadFile(manifest) //nolint:gosec // fixed, repo-relative path
	if err != nil {
		return fmt.Errorf("InstallKueue: read %s (run `make kueue-install`): %w", manifest, err)
	}
	// Drift guard: the namespace opt-in only works if the rendered selector and
	// KueueManagedLabel agree. They are produced by different tools (Makefile
	// sed, Go constant), so check rather than assume.
	if !strings.Contains(string(raw), KueueManagedLabel) {
		return fmt.Errorf(
			"InstallKueue: %s does not mention %q — the rendered managedJobsNamespaceSelector "+
				"and KueueManagedLabel have drifted; re-run `make kueue-install`",
			manifest, KueueManagedLabel)
	}

	image, err := kueueImage(root)
	if err != nil {
		return err
	}
	// Drift guard: the image comes from the committed pin, the manifest from a
	// gitignored render that survives a branch switch. One left from an earlier
	// pin deploys a Kueue the node was never handed.
	if !strings.Contains(string(raw), "image: "+image) {
		return fmt.Errorf(
			"InstallKueue: %s does not deploy %s — the rendered manifest predates the "+
				"current %s; re-run `make kueue-install`", manifest, image, kueueVersionFile)
	}

	if err := kindutil.LoadImage(ctx, clusterName, image); err != nil {
		return fmt.Errorf("InstallKueue: load %s into %s: %w", image, clusterName, err)
	}
	if err := kindutil.ApplyManifest(ctx, cfg, manifest); err != nil {
		return fmt.Errorf("InstallKueue: apply on %s: %w", clusterName, err)
	}
	if err := scopeWebhooksToManagedNamespaces(ctx, cfg); err != nil {
		return fmt.Errorf("InstallKueue: scope webhooks on %s: %w", clusterName, err)
	}
	return waitForKueueReady(ctx, cfg, clusterName)
}

// kueueImage pairs the pinned Kueue version with the repository the manifest
// was rendered against. Taken from the pin rather than the rendered output, so
// the caller can reject a manifest that disagrees.
func kueueImage(root string) (string, error) {
	raw, err := os.ReadFile(filepath.Join(root, kueueVersionFile)) //nolint:gosec // fixed, repo-relative path
	if err != nil {
		return "", fmt.Errorf("InstallKueue: read %s: %w", kueueVersionFile, err)
	}
	version := strings.TrimSpace(string(raw))
	if version == "" {
		return "", fmt.Errorf("InstallKueue: %s is empty", kueueVersionFile)
	}
	repo := os.Getenv("KUEUE_IMAGE_REPO")
	if repo == "" {
		repo = defaultKueueImageRepo
	}
	return repo + ":" + version, nil
}

// defaultKueueImageRepo mirrors KUEUE_IMAGE_REPO in the Makefile; override via
// that environment variable when a mirror is required.
const defaultKueueImageRepo = "registry.k8s.io/kueue/kueue"

// waitForKueueReady blocks until the controller reports an available replica
// and its webhook answers. Until it does, the webhook endpoint is not serving
// and any pod created in a managed namespace is admitted ungated — which would
// make an admission spec pass for entirely the wrong reason.
func waitForKueueReady(ctx context.Context, cfg *rest.Config, clusterName string) error {
	c, err := client.New(cfg, client.Options{Scheme: kindutil.BaseScheme()})
	if err != nil {
		return fmt.Errorf("InstallKueue: client for %s: %w", clusterName, err)
	}
	key := types.NamespacedName{Namespace: "kueue-system", Name: "kueue-controller-manager"}
	deadline := time.Now().Add(kueueRolloutTimeout)
	for {
		dep := &appsv1.Deployment{}
		if err := c.Get(ctx, key, dep); err == nil && dep.Status.AvailableReplicas > 0 {
			return waitForKueueWebhook(ctx, c, clusterName)
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("InstallKueue: %s not Available on %s within %s",
				key.Name, clusterName, kueueRolloutTimeout)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
}

// waitForKueueWebhook blocks until Kueue's webhook answers. An available
// replica is not enough: the webhook server binds its port only once its
// serving certificate is in place, which trails the readiness probe, and an
// object created in that gap fails with the API server unable to call the
// webhook. The probe is a server dry-run create of a ResourceFlavor, a
// cluster-scoped kind the narrowed namespace selector never skips, so it
// persists nothing and exercises the same path a spec's first Kueue object
// takes.
func waitForKueueWebhook(ctx context.Context, c client.Client, clusterName string) error {
	deadline := time.Now().Add(kueueRolloutTimeout)
	for {
		probe := &unstructured.Unstructured{}
		probe.SetGroupVersionKind(schema.GroupVersionKind{Group: "kueue.x-k8s.io", Version: "v1beta2", Kind: "ResourceFlavor"})
		probe.SetName("kindfix-webhook-probe")
		err := c.Create(ctx, probe, client.DryRunAll)
		if err == nil || apierrors.IsAlreadyExists(err) {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("InstallKueue: webhook on %s not answering within %s: %w", clusterName, kueueRolloutTimeout, err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
}

// repoRoot walks up from the working directory to the module root.
func repoRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("kindfix: no go.mod above %s", dir)
		}
		dir = parent
	}
}

// scopeWebhooksToManagedNamespaces narrows every Kueue webhook to namespaces
// carrying KueueManagedLabel.
//
// managedJobsNamespaceSelector in the Configuration is NOT sufficient: Kueue's
// pod webhook ships with a namespaceSelector that merely excludes kube-system
// and kueue-system, so it intercepts and gates pods everywhere else regardless
// of what the Configuration says. Since every OME-derived pod carries a
// queue-name label, leaving that as-is gates the workload of every spec in the
// suite — including the ones that grant no quota, which then hang until they
// time out.
//
// Narrowing the webhook itself is what actually makes admission opt-in: outside
// a labelled namespace Kueue is never consulted, so those pods are created
// exactly as they were before Kueue existed.
func scopeWebhooksToManagedNamespaces(ctx context.Context, cfg *rest.Config) error {
	c, err := client.New(cfg, client.Options{Scheme: kindutil.BaseScheme()})
	if err != nil {
		return fmt.Errorf("client: %w", err)
	}
	selector := metav1.LabelSelector{MatchLabels: map[string]string{KueueManagedLabel: "true"}}

	mutating := &admissionv1.MutatingWebhookConfiguration{}
	if err := c.Get(ctx, types.NamespacedName{Name: "kueue-mutating-webhook-configuration"}, mutating); err != nil {
		return fmt.Errorf("get mutating webhook config: %w", err)
	}
	for i := range mutating.Webhooks {
		mutating.Webhooks[i].NamespaceSelector = selector.DeepCopy()
	}
	if err := c.Update(ctx, mutating); err != nil {
		return fmt.Errorf("update mutating webhook config: %w", err)
	}

	validating := &admissionv1.ValidatingWebhookConfiguration{}
	if err := c.Get(ctx, types.NamespacedName{Name: "kueue-validating-webhook-configuration"}, validating); err != nil {
		return fmt.Errorf("get validating webhook config: %w", err)
	}
	for i := range validating.Webhooks {
		validating.Webhooks[i].NamespaceSelector = selector.DeepCopy()
	}
	if err := c.Update(ctx, validating); err != nil {
		return fmt.Errorf("update validating webhook config: %w", err)
	}
	return nil
}
