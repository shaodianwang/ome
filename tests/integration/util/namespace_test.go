package util

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

// etcdTimeout is the answer the apiserver gives when etcd does not commit a
// write in time. The write may still have been applied.
var etcdTimeout = apierrors.NewInternalError(errors.New("etcdserver: request timed out"))

// fastRetry keeps the unit tests quick while still exercising the loop.
var fastRetry = cleanupRetry{window: 100 * time.Millisecond, interval: 10 * time.Millisecond}

// namespaceAPI is a fake apiserver holding one namespace. Every namespace
// Delete first asks answer for the attempt's verdict: a non-nil error is
// returned as the apiserver's answer, nil lets the fake perform the delete.
type namespaceAPI struct {
	client.Client
	deletes int
}

func newNamespaceAPI(t *testing.T, ns *corev1.Namespace, answer func(attempt int) error) *namespaceAPI {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	api := &namespaceAPI{}
	base := fake.NewClientBuilder().WithScheme(scheme).WithObjects(ns).Build()
	api.Client = interceptor.NewClient(base, interceptor.Funcs{
		Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			if _, isNamespace := obj.(*corev1.Namespace); !isNamespace {
				return c.Delete(ctx, obj, opts...)
			}
			api.deletes++
			if err := answer(api.deletes); err != nil {
				return err
			}
			return c.Delete(ctx, obj, opts...)
		},
	})
	return api
}

func liveNamespace(name string) *corev1.Namespace {
	return &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}}
}

func always(err error) func(int) error { return func(int) error { return err } }

func TestDeleteNamespaceRetriesATransientAnswer(t *testing.T) {
	api := newNamespaceAPI(t, liveNamespace("retry"), func(attempt int) error {
		if attempt <= 2 {
			return etcdTimeout
		}
		return nil
	})
	if err := deleteNamespace(t.Context(), api, "retry", fastRetry); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if api.deletes != 3 {
		t.Fatalf("deletes = %d, want 3: two timed out, the third went through", api.deletes)
	}
	err := api.Get(t.Context(), client.ObjectKey{Name: "retry"}, &corev1.Namespace{})
	if !apierrors.IsNotFound(err) {
		t.Fatalf("namespace still present after a successful delete: %v", err)
	}
}

func TestDeleteNamespaceCountsATerminatingNamespaceAsDone(t *testing.T) {
	// A namespace already on its way out keeps a finalizer while its content
	// drains, exactly as a real namespace does in Terminating phase.
	now := metav1.Now()
	ns := liveNamespace("terminating")
	ns.DeletionTimestamp = &now
	ns.Finalizers = []string{"kubernetes"}
	api := newNamespaceAPI(t, ns, always(etcdTimeout))
	if err := deleteNamespace(t.Context(), api, "terminating", fastRetry); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if api.deletes != 1 {
		t.Fatalf("deletes = %d, want 1: the re-read shows the delete already happened", api.deletes)
	}
}

func TestDeleteNamespaceCountsAMissingNamespaceAsDone(t *testing.T) {
	api := newNamespaceAPI(t, liveNamespace("other"), always(etcdTimeout))
	if err := deleteNamespace(t.Context(), api, "missing", fastRetry); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if api.deletes != 1 {
		t.Fatalf("deletes = %d, want 1", api.deletes)
	}
}

func TestDeleteNamespaceReportsATransientAnswerOnceTheWindowCloses(t *testing.T) {
	api := newNamespaceAPI(t, liveNamespace("stuck"), always(etcdTimeout))
	start := time.Now()
	err := deleteNamespace(t.Context(), api, "stuck", fastRetry)
	if !errors.Is(err, etcdTimeout) || !apierrors.IsInternalError(err) {
		t.Fatalf("err = %v, want the apiserver's last answer", err)
	}
	if api.deletes < 2 {
		t.Fatalf("deletes = %d, want at least one retry", api.deletes)
	}
	if elapsed := time.Since(start); elapsed < fastRetry.window {
		t.Fatalf("gave up after %s, before the %s window closed", elapsed, fastRetry.window)
	}
}

func TestDeleteNamespaceDoesNotRetryAnAnswerWaitingWillNotClear(t *testing.T) {
	forbidden := apierrors.NewForbidden(schema.GroupResource{Resource: "namespaces"}, "denied", errors.New("no"))
	api := newNamespaceAPI(t, liveNamespace("denied"), always(forbidden))
	err := deleteNamespace(t.Context(), api, "denied", fastRetry)
	if !apierrors.IsForbidden(err) {
		t.Fatalf("err = %v, want forbidden", err)
	}
	if api.deletes != 1 {
		t.Fatalf("deletes = %d, want 1", api.deletes)
	}
}

func TestDeleteNamespaceStopsRetryingWhenTheContextEnds(t *testing.T) {
	api := newNamespaceAPI(t, liveNamespace("cancelled"), always(etcdTimeout))
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	slow := cleanupRetry{window: time.Hour, interval: time.Hour}
	err := deleteNamespace(ctx, api, "cancelled", slow)
	if !errors.Is(err, etcdTimeout) {
		t.Fatalf("err = %v, want the apiserver's last answer", err)
	}
	if api.deletes != 1 {
		t.Fatalf("deletes = %d, want 1", api.deletes)
	}
}

func TestDeleteNamespaceRetriesATransientCleanupHook(t *testing.T) {
	const name = "hook-retry"
	calls := 0
	RegisterCleanup(func(ctx context.Context, c client.Client, namespace string) error {
		if namespace != name {
			return nil
		}
		calls++
		if calls == 1 {
			return etcdTimeout
		}
		return nil
	})
	api := newNamespaceAPI(t, liveNamespace(name), always(nil))
	if err := deleteNamespace(t.Context(), api, name, fastRetry); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if calls != 2 {
		t.Fatalf("hook calls = %d, want 2: one timed out, the second went through", calls)
	}
	if api.deletes != 1 {
		t.Fatalf("deletes = %d, want 1", api.deletes)
	}
}

func TestDeleteNamespaceReportsACleanupHookErrorAtOnce(t *testing.T) {
	const name = "hook-broken"
	broken := errors.New("hook broken")
	calls := 0
	RegisterCleanup(func(ctx context.Context, c client.Client, namespace string) error {
		if namespace != name {
			return nil
		}
		calls++
		return broken
	})
	api := newNamespaceAPI(t, liveNamespace(name), always(nil))
	err := deleteNamespace(t.Context(), api, name, fastRetry)
	if !errors.Is(err, broken) {
		t.Fatalf("err = %v, want the hook's error", err)
	}
	if calls != 1 || api.deletes != 0 {
		t.Fatalf("hook calls = %d, deletes = %d; want one call and no delete", calls, api.deletes)
	}
}

type dialTimeout struct{}

func (dialTimeout) Error() string   { return "dial tcp: i/o timeout" }
func (dialTimeout) Timeout() bool   { return true }
func (dialTimeout) Temporary() bool { return false }

func TestTransientAPIError(t *testing.T) {
	gr := schema.GroupResource{Resource: "namespaces"}
	for _, tt := range []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil", err: nil, want: false},
		{name: "etcd timeout", err: etcdTimeout, want: true},
		{name: "wrapped etcd timeout", err: fmt.Errorf("delete: %w", etcdTimeout), want: true},
		{name: "gateway timeout", err: apierrors.NewTimeoutError("timed out", 1), want: true},
		{name: "server timeout", err: apierrors.NewServerTimeout(gr, "delete", 1), want: true},
		{name: "too many requests", err: apierrors.NewTooManyRequests("slow down", 1), want: true},
		{name: "service unavailable", err: apierrors.NewServiceUnavailable("down"), want: true},
		{name: "bad gateway", err: apierrors.NewGenericServerResponse(http.StatusBadGateway, "delete", gr, "", "bad gateway", 0, true), want: true},
		{name: "connection timeout", err: dialTimeout{}, want: true},
		{name: "wrapped connection timeout", err: fmt.Errorf("get: %w", dialTimeout{}), want: true},
		{name: "not found", err: apierrors.NewNotFound(gr, "x"), want: false},
		{name: "forbidden", err: apierrors.NewForbidden(gr, "x", errors.New("no")), want: false},
		{name: "conflict", err: apierrors.NewConflict(gr, "x", errors.New("stale")), want: false},
		{name: "invalid", err: apierrors.NewBadRequest("bad"), want: false},
		{name: "plain error", err: errors.New("boom"), want: false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := transientAPIError(tt.err); got != tt.want {
				t.Fatalf("transientAPIError(%v) = %t, want %t", tt.err, got, tt.want)
			}
		})
	}
}
