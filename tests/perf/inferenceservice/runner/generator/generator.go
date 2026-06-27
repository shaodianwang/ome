// Package generator turns a config.Config into live cluster objects: a
// shared catalog of ClusterServingRuntimes + ClusterBaseModels, then the
// per-class InferenceServices, created with the configured stagger.
//
// It reuses OME's own testing builders (pkg/utils/testing/v1beta1) so the
// generated objects are shaped exactly like the integration suites' — no
// second, drifting definition of "a valid ISVC". Every generated object
// carries the harness label so cleanup and the executor can target them.
package generator

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	v1beta1testing "github.com/ome-projects/ome/pkg/utils/testing/v1beta1"
	"github.com/ome-projects/ome/tests/perf/inferenceservice/runner/config"
	perflabels "github.com/ome-projects/ome/tests/perf/inferenceservice/runner/labels"
)

// CreatedSink receives the creation time of each InferenceService so the
// recorder can later compute time-to-Ready. The generator calls it
// immediately after a successful Create. Implemented by *recorder.Recorder.
type CreatedSink interface {
	RecordCreated(key types.NamespacedName, class string, at time.Time)
}

// Generator holds the wiring a run needs.
type Generator struct {
	client    client.Client
	cfg       *config.Config
	namespace string
	sink      CreatedSink
}

// New constructs a Generator. namespace is where all InferenceServices
// (and the SeedDefaultConfigMaps-style config) live; the runner creates
// it before calling Generate.
func New(c client.Client, cfg *config.Config, namespace string, sink CreatedSink) *Generator {
	return &Generator{client: c, cfg: cfg, namespace: namespace, sink: sink}
}

// Generate seeds the catalog, then creates every class's
// InferenceServices with the configured stagger. It blocks until all
// objects are created (or ctx is cancelled / a create fails). Per-class
// creation runs concurrently so a slow-stagger class doesn't hold up a
// fast one; within a class, creation is sequential to honor the interval.
func (g *Generator) Generate(ctx context.Context) error {
	log := ctrl.LoggerFrom(ctx).WithName("generator")

	if err := g.seedCatalog(ctx); err != nil {
		return fmt.Errorf("seeding catalog: %w", err)
	}
	log.Info("catalog seeded",
		"servingRuntimes", g.cfg.Catalog.ServingRuntimes,
		"baseModels", g.cfg.Catalog.BaseModels)

	var wg sync.WaitGroup
	errCh := make(chan error, len(g.cfg.Classes))
	for ci := range g.cfg.Classes {
		cls := g.cfg.Classes[ci]
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := g.generateClass(ctx, cls, ci); err != nil {
				errCh <- fmt.Errorf("class %q: %w", cls.ClassName, err)
			}
		}()
	}
	wg.Wait()
	close(errCh)

	var errs []error
	for err := range errCh {
		errs = append(errs, err)
	}
	if err := errors.Join(errs...); err != nil {
		return err
	}
	log.Info("generation complete", "total", g.cfg.Total())
	return nil
}

// seedCatalog creates the shared ClusterServingRuntimes and
// ClusterBaseModels every InferenceService references. Names are
// index-suffixed so the generator can round-robin InferenceServices
// across the pool.
func (g *Generator) seedCatalog(ctx context.Context) error {
	for i := 0; i < g.cfg.Catalog.ServingRuntimes; i++ {
		csr := v1beta1testing.MakeClusterServingRuntime(g.runtimeName(i)).
			SupportsModelFormat(g.cfg.Catalog.ModelFormat, true, 10).
			WithLastFormatArchitecture(g.cfg.Catalog.ModelArchitecture).
			Obj()
		if csr.Labels == nil {
			csr.Labels = map[string]string{}
		}
		csr.Labels[perflabels.HarnessLabel] = perflabels.HarnessLabelValue
		if err := g.client.Create(ctx, csr); err != nil {
			return fmt.Errorf("create %s: %w", csr.Name, err)
		}
	}
	for i := 0; i < g.cfg.Catalog.BaseModels; i++ {
		cbm := v1beta1testing.MakeClusterBaseModel(g.modelName(i)).
			StorageURI(g.cfg.Catalog.StorageURI).
			ModelFormat(g.cfg.Catalog.ModelFormat).
			ModelArchitecture(g.cfg.Catalog.ModelArchitecture).
			ModelFramework(g.cfg.Catalog.ModelFramework).
			Label(perflabels.HarnessLabel, perflabels.HarnessLabelValue).
			Obj()
		if err := g.client.Create(ctx, cbm); err != nil {
			return fmt.Errorf("create %s: %w", cbm.Name, err)
		}
	}
	return nil
}

// generateClass creates one class's InferenceServices, sleeping
// CreationIntervalMs between each. classIdx scopes object names so two
// classes never collide.
func (g *Generator) generateClass(ctx context.Context, cls config.SizeClass, classIdx int) error {
	interval := time.Duration(cls.CreationIntervalMs) * time.Millisecond
	for i := 0; i < cls.Count; i++ {
		if interval > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(interval):
			}
		} else if ctx.Err() != nil {
			return ctx.Err()
		}

		isvc := g.buildISVC(cls, classIdx, i)
		if err := g.client.Create(ctx, isvc); err != nil {
			return fmt.Errorf("create %s: %w", isvc.Name, err)
		}
		if g.sink != nil {
			g.sink.RecordCreated(
				types.NamespacedName{Namespace: isvc.Namespace, Name: isvc.Name},
				cls.ClassName, time.Now())
		}
	}
	return nil
}

// buildISVC produces one RawDeployment InferenceService for the class,
// referencing a round-robined runtime + model from the catalog and
// carrying the harness labels the executor and observer read.
func (g *Generator) buildISVC(cls config.SizeClass, classIdx, i int) *v1beta1.InferenceService {
	name := fmt.Sprintf("perf-%s-%d-%d", cls.ClassName, classIdx, i)
	runtime := g.runtimeName(i % g.cfg.Catalog.ServingRuntimes)
	model := g.modelName(i % g.cfg.Catalog.BaseModels)

	isvc := v1beta1testing.MakeInferenceService(name, g.namespace).
		Model(model).
		Runtime(runtime).
		DeploymentMode(constants.RawDeployment).
		Label(perflabels.HarnessLabel, perflabels.HarnessLabelValue).
		Label(perflabels.ClassLabel, cls.ClassName).
		Label(perflabels.ReadyAfterMsLabel, fmt.Sprintf("%d", cls.ReadyAfterMs)).
		Engine(&v1beta1.EngineSpec{
			ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
				MinReplicas: ptr.To(int(cls.Replicas)),
				MaxReplicas: int(cls.Replicas),
			},
			PodSpec: v1beta1.PodSpec{
				Containers: []corev1.Container{{
					Name:  "ome-container",
					Image: cls.Image,
				}},
			},
		}).
		Obj()
	return isvc
}

func (g *Generator) runtimeName(i int) string { return fmt.Sprintf("perf-runtime-%d", i) }
func (g *Generator) modelName(i int) string   { return fmt.Sprintf("perf-model-%d", i) }

// Cleanup best-effort deletes every harness-owned cluster-scoped catalog
// object. Namespaced InferenceServices go away with their namespace, so
// they are not listed here. Errors are returned joined; the runner logs
// them but does not fail a completed run on cleanup trouble.
func Cleanup(ctx context.Context, c client.Client) error {
	var errs []error
	sel := client.MatchingLabels{perflabels.HarnessLabel: perflabels.HarnessLabelValue}
	if err := c.DeleteAllOf(ctx, &v1beta1.ClusterServingRuntime{}, sel); err != nil {
		errs = append(errs, fmt.Errorf("delete ClusterServingRuntimes: %w", err))
	}
	if err := c.DeleteAllOf(ctx, &v1beta1.ClusterBaseModel{}, sel); err != nil {
		errs = append(errs, fmt.Errorf("delete ClusterBaseModels: %w", err))
	}
	return errors.Join(errs...)
}
