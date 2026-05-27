package kindutil

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	sigsyaml "sigs.k8s.io/yaml"
)

// defaultCRDPath is the OME repo's directory of generated CRDs.
// Relative to repo root.
const defaultCRDPath = "config/crd/full"

// crdEstablishedTimeout caps how long ApplyCRDs waits for each CRD
// to reach Established + NamesAccepted before returning. CRDs that
// take longer indicate either an apiserver-side issue or a schema
// validation rejection.
const crdEstablishedTimeout = 60 * time.Second

// ApplyCRDs reads every YAML file under each path (recursively),
// decodes the CRD documents, applies them, and waits for each to
// reach Established + NamesAccepted. Idempotent: existing CRDs are
// updated rather than re-created.
//
// Paths may be relative to the repo root (the test process's
// CWD-walks-up-to-go.mod root); absolute paths are honored as-is.
func ApplyCRDs(ctx context.Context, cfg *rest.Config, paths []string) error {
	if len(paths) == 0 {
		paths = []string{defaultCRDPath}
	}
	scheme, err := crdScheme()
	if err != nil {
		return fmt.Errorf("ApplyCRDs: build scheme: %w", err)
	}
	c, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		return fmt.Errorf("ApplyCRDs: new client: %w", err)
	}

	var applied []string
	for _, p := range paths {
		abs, err := resolveContextDir(p)
		if err != nil {
			return fmt.Errorf("ApplyCRDs: resolve %s: %w", p, err)
		}
		names, err := applyCRDDir(ctx, c, abs)
		if err != nil {
			return fmt.Errorf("ApplyCRDs: apply %s: %w", p, err)
		}
		applied = append(applied, names...)
	}

	for _, name := range applied {
		if err := waitForCRDEstablished(ctx, c, name); err != nil {
			return fmt.Errorf("ApplyCRDs: wait for %s established: %w", name, err)
		}
	}
	return nil
}

// applyCRDDir reads every *.yaml / *.yml file under dir and applies
// the embedded CRD documents (skipping non-CRD docs silently).
// Returns the names of every CRD seen so the caller can wait on
// Established.
func applyCRDDir(ctx context.Context, c client.Client, dir string) ([]string, error) {
	var names []string
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if ext := strings.ToLower(filepath.Ext(path)); ext != ".yaml" && ext != ".yml" {
			return nil
		}
		f, err := os.Open(path)
		if err != nil {
			return err
		}
		defer f.Close()
		got, err := applyCRDStream(ctx, c, f)
		if err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
		names = append(names, got...)
		return nil
	})
	return names, err
}

// applyCRDStream decodes multi-document YAML from r, picks out
// CRDs, and applies each. Returns the names applied.
func applyCRDStream(ctx context.Context, c client.Client, r io.Reader) ([]string, error) {
	dec := yaml.NewYAMLOrJSONDecoder(r, 4096)
	var names []string
	for {
		var raw map[string]interface{}
		if err := dec.Decode(&raw); err != nil {
			if err == io.EOF {
				return names, nil
			}
			return names, fmt.Errorf("decode YAML doc: %w", err)
		}
		if raw == nil {
			continue
		}
		kind, _ := raw["kind"].(string)
		apiVersion, _ := raw["apiVersion"].(string)
		if kind != "CustomResourceDefinition" || !strings.HasPrefix(apiVersion, "apiextensions.k8s.io/") {
			continue
		}
		// Round-trip through YAML→struct via Marshal so the
		// untyped map upgrades into a properly-shaped CRD object.
		var buf bytes.Buffer
		if err := yamlEncode(&buf, raw); err != nil {
			return names, fmt.Errorf("re-encode CRD doc: %w", err)
		}
		var crd apiextensionsv1.CustomResourceDefinition
		if err := yaml.NewYAMLOrJSONDecoder(&buf, 4096).Decode(&crd); err != nil {
			return names, fmt.Errorf("decode CRD: %w", err)
		}
		if err := upsertCRD(ctx, c, &crd); err != nil {
			return names, fmt.Errorf("upsert %s: %w", crd.Name, err)
		}
		names = append(names, crd.Name)
	}
}

// upsertCRD creates the CRD if absent, updates spec+labels+annotations
// when present. Status and ResourceVersion are read from the live
// object so the update is API-server-accepted.
func upsertCRD(ctx context.Context, c client.Client, crd *apiextensionsv1.CustomResourceDefinition) error {
	existing := &apiextensionsv1.CustomResourceDefinition{}
	err := c.Get(ctx, client.ObjectKey{Name: crd.Name}, existing)
	if apierrors.IsNotFound(err) {
		return c.Create(ctx, crd)
	}
	if err != nil {
		return err
	}
	existing.Spec = crd.Spec
	existing.Labels = crd.Labels
	existing.Annotations = crd.Annotations
	return c.Update(ctx, existing)
}

// waitForCRDEstablished polls the named CRD until both Established
// and NamesAccepted conditions are True, or ctx (or the per-CRD
// timeout) elapses.
func waitForCRDEstablished(ctx context.Context, c client.Client, name string) error {
	waitCtx, cancel := context.WithTimeout(ctx, crdEstablishedTimeout)
	defer cancel()
	return wait.PollUntilContextCancel(waitCtx, 250*time.Millisecond, true,
		func(ctx context.Context) (bool, error) {
			crd := &apiextensionsv1.CustomResourceDefinition{}
			if err := c.Get(ctx, client.ObjectKey{Name: name}, crd); err != nil {
				if apierrors.IsNotFound(err) {
					return false, nil
				}
				return false, err
			}
			return crdReady(crd), nil
		})
}

// crdReady reports Established=True AND NamesAccepted=True.
func crdReady(crd *apiextensionsv1.CustomResourceDefinition) bool {
	var establishedOK, namesOK bool
	for _, cond := range crd.Status.Conditions {
		switch cond.Type {
		case apiextensionsv1.Established:
			establishedOK = cond.Status == apiextensionsv1.ConditionTrue
		case apiextensionsv1.NamesAccepted:
			namesOK = cond.Status == apiextensionsv1.ConditionTrue
		}
	}
	return establishedOK && namesOK
}

// crdScheme returns the minimum scheme needed for client.New to talk
// to the apiextensions group.
func crdScheme() (*runtime.Scheme, error) {
	s := runtime.NewScheme()
	if err := apiextensionsv1.AddToScheme(s); err != nil {
		return nil, err
	}
	return s, nil
}

// yamlEncode marshals v to YAML via sigs.k8s.io/yaml (JSON-as-YAML
// over the apimachinery JSON codec — same round-trip envtest uses).
// Indirection so callers don't import yet another yaml package.
func yamlEncode(w io.Writer, v interface{}) error {
	b, err := sigsyaml.Marshal(v)
	if err != nil {
		return err
	}
	_, err = w.Write(b)
	return err
}
