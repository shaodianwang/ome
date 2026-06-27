// Package generator turns a config.Config into the control-plane source
// InferenceServices the placement harness measures. Each source declares the
// accelerator requirement (so it fans out onto the matching member clusters)
// and carries the harness + class labels the observer and member-side admitter
// read. The WorkloadClusters and their kubeconfig Secrets are stood up by the
// runner before generation (they depend on the member rest.Configs); the
// generator owns only the source ISVCs.
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
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/placement"
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
	// nameSuffix scopes the source-ISVC names to one pass so the funnel pass and
	// the poll pass never collide on the control cluster (both run in the same
	// process against the same apiserver).
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
// a single Engine component plus the accelerator-requirements annotation that
// makes it eligible for fan-out onto the matching candidate members. The
// harness + class labels ride through DeriveISVC onto the derived copies so the
// member-side admitter and the observer can scope to this run's objects.
func (g *Generator) buildSource(cls config.SizeClass, classIdx, i int) *v1beta1.InferenceService {
	name := fmt.Sprintf("src-%s-%s-%d-%d", g.nameSuffix, cls.ClassName, classIdx, i)
	requirement := g.cfg.Placement.AcceleratorLabel + "=" + g.cfg.Placement.AcceleratorValue
	return &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: g.namespace,
			Labels: map[string]string{
				perflabels.HarnessLabel: perflabels.HarnessLabelValue,
				perflabels.ClassLabel:   cls.ClassName,
			},
			Annotations: map[string]string{
				placement.AcceleratorRequirementsAnnotation: requirement,
			},
		},
		Spec: v1beta1.InferenceServiceSpec{
			Engine: &v1beta1.EngineSpec{
				Runner: &v1beta1.RunnerSpec{
					Container: corev1.Container{Name: "ome-container", Image: cls.Image},
				},
			},
		},
	}
}
