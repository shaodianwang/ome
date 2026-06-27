// Package clusters stands up the multi-cluster placement topology IN ONE
// PROCESS: one control-plane envtest instance plus N member (workload) envtest
// instances, each via the shared tests/integration/framework. It is the
// scaffolding the placement perf harness measures against, adapted from the
// integration multicluster suite's createCluster + startControlPlaneManager.
//
// Two hazards drive the structure:
//
//   - Scheme data race: framework.Init mutates the process-global client-go
//     scheme. Init across the clusters is serialized by a mutex, and — the
//     critical ordering — the control-plane MANAGER (whose cache informers also
//     read the scheme) is started only AFTER every cluster's Init has
//     completed. Initialize all clusters first, then start managers.
//
//   - Per-cluster apiserver cost: each member is a full in-process
//     apiserver+etcd, so member count dominates boot time and memory.
package clusters

import (
	"context"
	"fmt"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/tests/integration/framework"
	"github.com/ome-projects/ome/tests/integration/util"
)

// Cluster bundles one envtest instance: its framework, rest.Config, a
// non-cached client, and a context to drive it.
type Cluster struct {
	fwk    *framework.Framework
	cfg    *rest.Config
	client client.Client
	ctx    context.Context
}

// Config returns the cluster's rest.Config (used to serialize a member's
// kubeconfig into a Secret on the control plane).
func (c *Cluster) Config() *rest.Config { return c.cfg }

// Client returns the cluster's non-cached client.
func (c *Cluster) Client() client.Client { return c.client }

// Context returns the cluster's driving context.
func (c *Cluster) Context() context.Context { return c.ctx }

// Topology is the stood-up control plane + members. The caller starts the
// member admitters and the per-pass control-plane manager via the wiring
// helpers in wiring.go, then tears everything down with Teardown.
type Topology struct {
	Control *Cluster
	Members []*Cluster
	// MemberNames are the WorkloadCluster names (config order), parallel to
	// Members, so callers can correlate a member apiserver with its CR.
	MemberNames []string

	// schemeMu serializes framework.Init across the concurrent bring-up
	// (framework.Init mutates the process-global scheme; the same race the
	// integration suite guards). It guards Init-vs-Init only; managers start
	// after all Init has completed.
	schemeMu sync.Mutex
}

// BringUp stands up the control plane and the named members concurrently
// (two-phase: every framework.Init completes before any manager is started),
// then creates each member's kubeconfig Secret + WorkloadCluster on the control
// plane carrying the accelerator label so every member is a placement
// candidate. It does NOT start any manager — the caller wires the member
// admitters and the per-pass control-plane placer afterward.
func BringUp(ctx context.Context, memberNames []string, acceleratorLabel, acceleratorValue string) (*Topology, error) {
	t := &Topology{MemberNames: memberNames}

	// Phase 1: concurrent Init + SetupClient for every cluster, mutex-guarded.
	var wg sync.WaitGroup
	errs := make([]error, len(memberNames)+1)
	clusters := make([]*Cluster, len(memberNames)+1)

	bring := func(idx int) {
		defer wg.Done()
		c, err := t.createCluster(ctx)
		if err != nil {
			errs[idx] = err
			return
		}
		clusters[idx] = c
	}

	wg.Add(len(memberNames) + 1)
	go bring(0) // control plane is index 0
	for i := range memberNames {
		go bring(i + 1)
	}
	wg.Wait()

	for _, err := range errs {
		if err != nil {
			// Best-effort teardown of whatever came up before failing.
			for _, c := range clusters {
				if c != nil {
					_ = c.fwk.Teardown()
				}
			}
			return nil, fmt.Errorf("cluster bring-up: %w", err)
		}
	}

	t.Control = clusters[0]
	t.Members = clusters[1:]

	// Phase 2 (caller-driven) starts managers. Here we only seed the control
	// plane with the member WorkloadClusters; that is plain apiserver I/O
	// (no manager cache), so it is safe before managers start.
	if err := t.registerMembers(ctx, acceleratorLabel, acceleratorValue); err != nil {
		t.Teardown()
		return nil, err
	}
	return t, nil
}

// createCluster stands up one envtest instance: framework.Init (apiserver +
// scheme registration) and SetupClient, mutex-guarded because Init mutates the
// process-global scheme. SkipWriteConflictCheck is set because the placer
// retries conflicts (expected under load); a perf run is not a correctness gate.
func (t *Topology) createCluster(_ context.Context) (*Cluster, error) {
	fwk := &framework.Framework{SkipWriteConflictCheck: true}

	t.schemeMu.Lock()
	cfg, err := fwk.Init()
	if err != nil {
		t.schemeMu.Unlock()
		return nil, fmt.Errorf("framework.Init: %w", err)
	}
	cctx, cl, err := fwk.SetupClient(cfg)
	t.schemeMu.Unlock()
	if err != nil {
		return nil, fmt.Errorf("framework.SetupClient: %w", err)
	}
	return &Cluster{fwk: fwk, cfg: cfg, client: cl, ctx: cctx}, nil
}

// registerMembers writes each member's kubeconfig Secret and a WorkloadCluster
// CR (labeled so it matches the sources' accelerator requirement) onto the
// control cluster. The WorkloadCluster reconciler (started later by the
// control-plane manager) connects to the member from this Secret and flips it
// Ready, at which point it is an eligible placement candidate.
func (t *Topology) registerMembers(ctx context.Context, acceleratorLabel, acceleratorValue string) error {
	for i, name := range t.MemberNames {
		secretName := name + "-kubeconfig"
		raw, err := util.RestConfigToKubeConfig(t.Members[i].cfg)
		if err != nil {
			return fmt.Errorf("member %q: serialize kubeconfig: %w", name, err)
		}
		secret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: secretName, Namespace: metav1.NamespaceDefault},
			Data:       map[string][]byte{"kubeconfig": raw},
		}
		if err := t.Control.client.Create(ctx, secret); err != nil && !apierrors.IsAlreadyExists(err) {
			return fmt.Errorf("member %q: create kubeconfig secret: %w", name, err)
		}
		wc := &v1beta1.WorkloadCluster{
			ObjectMeta: metav1.ObjectMeta{
				Name:   name,
				Labels: map[string]string{acceleratorLabel: acceleratorValue},
			},
			Spec: v1beta1.WorkloadClusterSpec{
				ClusterSource: v1beta1.ClusterConnectionSource{
					KubeConfig: &v1beta1.KubeConfigSource{
						SecretRef: corev1.SecretReference{Name: secretName, Namespace: metav1.NamespaceDefault},
						Key:       "kubeconfig",
					},
				},
			},
		}
		if err := t.Control.client.Create(ctx, wc); err != nil && !apierrors.IsAlreadyExists(err) {
			return fmt.Errorf("member %q: create WorkloadCluster: %w", name, err)
		}
	}
	return nil
}

// EnsureNamespaceEverywhere creates the workload namespace on the control plane
// and every member (the placer creates the derived ISVC in the source's
// namespace on each member, so the namespace must already exist there).
func (t *Topology) EnsureNamespaceEverywhere(ctx context.Context, name string) error {
	all := append([]*Cluster{t.Control}, t.Members...)
	for _, c := range all {
		ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}}
		if err := c.client.Create(ctx, ns); err != nil && !apierrors.IsAlreadyExists(err) {
			return fmt.Errorf("create namespace %q: %w", name, err)
		}
	}
	return nil
}

// WaitWorkloadClustersReady blocks until every member's WorkloadCluster reports
// Ready=True on the control plane (so it is an eligible candidate) or the
// timeout elapses. Polled, not watched — this runs before the measured passes,
// so a simple poll is clearer than wiring an informer.
func (t *Topology) WaitWorkloadClustersReady(ctx context.Context, timeout, poll time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		ready := 0
		for _, name := range t.MemberNames {
			wc := &v1beta1.WorkloadCluster{}
			if err := t.Control.client.Get(ctx, client.ObjectKey{Name: name}, wc); err != nil {
				break
			}
			if apimeta.IsStatusConditionTrue(wc.Status.Conditions, v1beta1.WorkloadClusterReady) {
				ready++
			}
		}
		if ready == len(t.MemberNames) {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("only %d/%d WorkloadClusters Ready after %s", ready, len(t.MemberNames), timeout)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(poll):
		}
	}
}

// Teardown stops every cluster's manager and envtest instance. Best-effort and
// idempotent; safe to call on a partially-constructed Topology.
func (t *Topology) Teardown() {
	stop := func(c *Cluster) {
		if c == nil || c.fwk == nil {
			return
		}
		_ = c.fwk.StopManager(context.Background())
		_ = c.fwk.Teardown()
	}
	for _, m := range t.Members {
		stop(m)
	}
	stop(t.Control)
}

// StartManager starts a controller-runtime manager on this cluster, running the
// supplied setup closure (which registers the reconcilers the pass needs). The
// caller builds the closure — clusters stays free of placement/admitter
// wiring details. Member admitters call this once; the control-plane placer
// calls it per pass (paired with StopManager).
func (c *Cluster) StartManager(ctx context.Context, setup framework.ManagerSetup) error {
	return c.fwk.StartManager(ctx, c.cfg, setup)
}

// StopManager stops this cluster's manager (used to restart the control-plane
// manager between the funnel-on and funnel-off passes).
func (c *Cluster) StopManager(ctx context.Context) error {
	return c.fwk.StopManager(ctx)
}
