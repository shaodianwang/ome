package gangpack

import (
	"context"

	"k8s.io/client-go/rest"
	schedv1alpha1 "sigs.k8s.io/scheduler-plugins/apis/scheduling/v1alpha1"
	schedclient "sigs.k8s.io/scheduler-plugins/pkg/generated/clientset/versioned"
	schedinformers "sigs.k8s.io/scheduler-plugins/pkg/generated/informers/externalversions"
)

// informerReader is the live podGroupReader: it resolves a pod's PodGroup facts
// from a scheduler-plugins PodGroup informer cache.
type informerReader struct {
	// getPG fetches a PodGroup by namespace/name (from the informer lister). A
	// func rather than the lister interface so the read path is unit-testable
	// without standing up an informer.
	getPG func(namespace, name string) (*schedv1alpha1.PodGroup, error)
}

var _ podGroupReader = &informerReader{}

// factsFromPodGroup extracts the placement facts the plugin needs from a
// PodGroup: the gang size (Spec.MinMember) and the domain topology key the
// workload declared (the topologyKeyAnnotation). A PodGroup without the
// annotation yields an empty topology key, which resolveGang treats as
// unresolvable (no domain to pin to).
func factsFromPodGroup(pg *schedv1alpha1.PodGroup) (minMember int, topologyKey string) {
	if pg == nil {
		return 0, ""
	}
	return int(pg.Spec.MinMember), pg.Annotations[topologyKeyAnnotation]
}

// get resolves a PodGroup into (minMember, topologyKey, found). Any lookup error
// or a missing PodGroup is reported as not-found — the pod is then left
// unconstrained, never rejected on a transient read.
func (r *informerReader) get(namespace, name string) (minMember int, topologyKey string, found bool) {
	pg, err := r.getPG(namespace, name)
	if err != nil || pg == nil {
		return 0, "", false
	}
	mm, tk := factsFromPodGroup(pg)
	return mm, tk, true
}

// newInformerReader builds a PodGroup informer from the scheduler's shared
// kubeconfig, starts it, waits for the initial cache sync, and returns a reader
// backed by its lister. ctx bounds the informer's lifetime (the scheduler's).
func newInformerReader(ctx context.Context, cfg *rest.Config) (*informerReader, error) {
	client, err := schedclient.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}
	factory := schedinformers.NewSharedInformerFactory(client, 0)
	lister := factory.Scheduling().V1alpha1().PodGroups().Lister()
	factory.Start(ctx.Done())
	factory.WaitForCacheSync(ctx.Done())
	return &informerReader{
		getPG: func(namespace, name string) (*schedv1alpha1.PodGroup, error) {
			return lister.PodGroups(namespace).Get(name)
		},
	}, nil
}
