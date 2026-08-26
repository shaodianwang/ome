package kindutil

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ApplyManifest applies a multi-document YAML file to a cluster — the whole
// install, not just CRDs, so a suite can stand up a dependency that has to be
// RUNNING rather than merely registered.
//
// Documents are applied in two passes. Namespaces and CustomResourceDefinitions
// go first and CRDs are awaited as established; everything else follows. Without
// that split a single-pass apply races itself: a namespaced object can reach the
// apiserver before its namespace exists, and a custom resource before the type
// is served.
//
// Objects already present are updated rather than recreated, so re-running a
// suite against a reused cluster is a no-op instead of a conflict.
func ApplyManifest(ctx context.Context, cfg *rest.Config, path string) error {
	docs, err := decodeManifest(path)
	if err != nil {
		return err
	}

	scheme, err := crdScheme()
	if err != nil {
		return fmt.Errorf("ApplyManifest: scheme: %w", err)
	}
	c, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		return fmt.Errorf("ApplyManifest: client: %w", err)
	}

	var first, rest []*unstructured.Unstructured
	for _, doc := range docs {
		switch doc.GetKind() {
		case "Namespace", "CustomResourceDefinition":
			first = append(first, doc)
		default:
			rest = append(rest, doc)
		}
	}

	for _, doc := range first {
		if err := upsert(ctx, c, doc); err != nil {
			return err
		}
	}
	for _, doc := range first {
		if doc.GetKind() != "CustomResourceDefinition" {
			continue
		}
		if err := waitForCRDEstablished(ctx, c, doc.GetName()); err != nil {
			return fmt.Errorf("ApplyManifest: %w", err)
		}
	}
	for _, doc := range rest {
		if err := upsert(ctx, c, doc); err != nil {
			return err
		}
	}
	return nil
}

// upsert applies one document server-side.
//
// Server-side apply rather than get-modify-update because these manifests
// install controllers that then own fields in their own objects — Kueue's cert
// controller injects caBundle into its webhook configurations, for instance. A
// read-modify-write both races that controller (conflict on every re-apply) and
// would clobber the injected value if it won. Field-ownership merging leaves
// what the installed controller owns alone.
func upsert(ctx context.Context, c client.Client, desired *unstructured.Unstructured) error {
	obj := desired.DeepCopy()
	if err := c.Patch(ctx, obj, client.Apply,
		client.FieldOwner(manifestFieldOwner), client.ForceOwnership); err != nil {
		return fmt.Errorf("ApplyManifest: apply %s %s/%s: %w",
			desired.GetKind(), desired.GetNamespace(), desired.GetName(), err)
	}
	return nil
}

// manifestFieldOwner identifies this applier in server-side apply metadata, so
// a conflict names the test harness rather than an anonymous writer.
const manifestFieldOwner = "ome-kind-suite"

// decodeManifest reads every YAML document in a file, skipping empty ones.
func decodeManifest(path string) ([]*unstructured.Unstructured, error) {
	f, err := os.Open(filepath.Clean(path))
	if err != nil {
		return nil, fmt.Errorf("ApplyManifest: open %s: %w", path, err)
	}
	defer func() { _ = f.Close() }()

	reader := yaml.NewYAMLOrJSONDecoder(f, 4096)
	var docs []*unstructured.Unstructured
	for {
		obj := &unstructured.Unstructured{}
		err := reader.Decode(obj)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("ApplyManifest: decode %s: %w", path, err)
		}
		if len(obj.Object) == 0 || obj.GetKind() == "" {
			continue
		}
		docs = append(docs, obj)
	}
	if len(docs) == 0 {
		return nil, fmt.Errorf("ApplyManifest: %s has no documents", path)
	}
	return docs, nil
}
