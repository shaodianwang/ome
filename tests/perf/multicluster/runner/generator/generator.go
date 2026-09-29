// Package generator creates source services and their member runtime catalog
// for the placement benchmark. Harness and class labels identify each source
// throughout fan-out and observation.
package generator

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/tests/perf/multicluster/runner/config"
	perflabels "github.com/ome-projects/ome/tests/perf/multicluster/runner/labels"
)

// CreatedSink receives the creation time of each source ISVC so the recorder
// can later compute placement latency. The generator calls it immediately
// after a successful Create. Implemented by *recorder.Recorder.
type CreatedSink interface {
	RecordCreated(key types.NamespacedName, class string, at time.Time)
}

// Generator holds the wiring a generation pass needs.
type Generator struct {
	client    client.Client
	cfg       *config.Config
	namespace string
	sink      CreatedSink
	// nameSuffix identifies the comparison pass in source names and diagnostics.
	nameSuffix string
}

// New constructs a Generator. namespace is where all source ISVCs live on the
// control cluster; nameSuffix disambiguates the two passes' object names.
func New(c client.Client, cfg *config.Config, namespace, nameSuffix string, sink CreatedSink) *Generator {
	return &Generator{client: c, cfg: cfg, namespace: namespace, sink: sink, nameSuffix: nameSuffix}
}

// Generate creates every class's source ISVCs with the configured stagger. It
// blocks until all are created (or ctx is cancelled / a create fails).
// Per-class creation runs concurrently so a slow-stagger class doesn't hold up
// a fast one; within a class, creation is sequential to honor the interval.
func (g *Generator) Generate(ctx context.Context) error {
	log := ctrl.LoggerFrom(ctx).WithName("generator")

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
	log.Info("generation complete", "total", g.cfg.Total(), "suffix", g.nameSuffix)
	return nil
}

// generateClass creates one class's source ISVCs, sleeping CreationIntervalMs
// between each. classIdx scopes object names so two classes never collide.
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

		isvc := g.buildSource(cls, classIdx, i)
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

// buildSource produces one control-plane source InferenceService for the class:
// a single Engine component with explicit placement affinity. The
// harness + class labels ride through DeriveISVC onto the derived copies so the
// member-side admitter and the observer can scope to this run's objects.
func (g *Generator) buildSource(cls config.SizeClass, classIdx, i int) *v1beta1.InferenceService {
	name := fmt.Sprintf("src-%s-%s-%d-%d", g.nameSuffix, cls.ClassName, classIdx, i)
	return &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: g.namespace,
			Labels: map[string]string{
				perflabels.HarnessLabel: perflabels.HarnessLabelValue,
				perflabels.ClassLabel:   cls.ClassName,
			},
		},
		Spec: v1beta1.InferenceServiceSpec{
			DeploymentMode: ptr.To(constants.OMENative),
			Runtime:        &v1beta1.ServingRuntimeRef{Name: cls.RuntimeName, Kind: ptr.To("ServingRuntime")},
			Placement:      &v1beta1.PlacementSpec{Policy: v1beta1.PlacementPolicyClusterAffinity, Mode: v1beta1.PlacementModeSingle, ClusterAffinity: []v1beta1.ClusterAffinityTerm{{MatchExpressions: []v1beta1.ClusterSelectorRequirement{{Key: g.cfg.Placement.AcceleratorLabel, Operator: corev1.NodeSelectorOpIn, Values: []string{g.cfg.Placement.AcceleratorValue}}}}}},
			Engine: &v1beta1.EngineSpec{
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: ptr.To(cls.MinReplicas)},
				Runner: &v1beta1.RunnerSpec{
					Container: corev1.Container{Name: constants.MainContainerName, Image: cls.Image},
				},
			},
		},
	}
}

// SeedRuntimes gives backend preflight a real catalog entry on each member.
func SeedRuntimes(ctx context.Context, c client.Client, cfg *config.Config, namespace string) error {
	seen := map[string]bool{}
	for _, cls := range cfg.Classes {
		if seen[cls.RuntimeName] {
			continue
		}
		seen[cls.RuntimeName] = true
		rt := &v1beta1.ServingRuntime{ObjectMeta: metav1.ObjectMeta{Name: cls.RuntimeName, Namespace: namespace},
			Spec: v1beta1.ServingRuntimeSpec{}}
		if err := c.Create(ctx, rt); err != nil {
			return fmt.Errorf("seed runtime %q: %w", cls.RuntimeName, err)
		}
	}
	return nil
}
