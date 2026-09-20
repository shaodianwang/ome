package inferencereplica

// The force-delete sweep inside the migration source drain, driven
// through the adapter.
//
// The sweep runs after the surge has passed its rotation and
// availability gates, and the availability half reads the row's PodCount
// and AvailablePodCount — counters only the adapter's status publication
// writes. Driving the arrow therefore needs a walk that publishes them,
// which is what this full-loop reconcile does.

import (
	"context"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	clocktesting "k8s.io/utils/clock/testing"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	schedulingv1alpha1 "sigs.k8s.io/scheduler-plugins/apis/scheduling/v1alpha1"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/irstatus"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload"
	workloadgang "github.com/ome-projects/ome/pkg/controller/v1beta1/workload/gang"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
)

// TestReconcile_MigrationSourceWedged_ForceDeletesAndCompletes drives the
// migration whose source pods never leave: the kubelet behind them is
// gone, so the graceful delete the drain issues leaves a Terminating
// object nothing will ever clear. With lifecycle.forceDelete configured
// the drain force-deletes them at grace zero and the migration completes
// on the resulting absence.
//
// Arrows: T-migrate-force-delete
func TestReconcile_MigrationSourceWedged_ForceDeletesAndCompletes(t *testing.T) {
	base := time.Now()
	ir := baselineIR("llama-engine", "prod", 1)
	ir.Spec.Runners = []v1beta1.Runner{
		{Name: v1beta1.RunnerNameLeader, Size: 1, Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "ome-container", Image: "test:v1",
				Ports: []corev1.ContainerPort{{Name: "http", ContainerPort: 8080}}}},
		}}},
		{Name: v1beta1.RunnerNameWorker, Size: 1, Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "ome-container", Image: "test:v1",
				Ports: []corev1.ContainerPort{{Name: "http", ContainerPort: 8080}}}},
		}}},
	}

	// The kubelet behind the source Instance is gone: its pods keep the
	// deletion timestamp the graceful delete stamped and no finalizer,
	// and only a grace-zero delete takes them out. The fake client cannot
	// store a DeletionTimestamp without finalizers, so the wedge is held
	// here and presented on every List, the way the apiserver would.
	wedged := map[string]metav1.Time{}
	var forced []string
	c := fake.NewClientBuilder().
		WithScheme(testScheme(t)).
		WithObjects(ir).
		WithStatusSubresource(&v1beta1.InferenceReplica{}).
		WithIndex(&schedulingv1alpha1.PodGroup{}, workloadgang.PodGroupControllerUIDIndexField, workloadgang.PodGroupControllerUIDIndexExtractor).
		WithInterceptorFuncs(interceptor.Funcs{
			List: func(ctx context.Context, cl client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
				if err := cl.List(ctx, list, opts...); err != nil {
					return err
				}
				pl, ok := list.(*corev1.PodList)
				if !ok {
					return nil
				}
				for i := range pl.Items {
					if dt, stuck := wedged[pl.Items[i].Name]; stuck {
						pl.Items[i].DeletionTimestamp = &dt
					}
				}
				return nil
			},
			Delete: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
				pod, ok := obj.(*corev1.Pod)
				if !ok {
					return cl.Delete(ctx, obj, opts...)
				}
				do := &client.DeleteOptions{}
				for _, o := range opts {
					o.ApplyToDelete(do)
				}
				if do.GracePeriodSeconds != nil && *do.GracePeriodSeconds == 0 {
					forced = append(forced, pod.Name)
					return cl.Delete(ctx, obj, opts...)
				}
				if pod.Labels[query.LabelInstanceIdx] == "0" {
					// Graceful delete on a pod whose node is dead: the
					// object stays, overdue from the instant of the request.
					wedged[pod.Name] = metav1.NewTime(base.Add(-10 * time.Minute))
					return nil
				}
				return cl.Delete(ctx, obj, opts...)
			},
		}).
		Build()

	rec := record.NewFakeRecorder(256)
	r := &Reconciler{
		Client:                   c,
		APIReader:                c,
		Log:                      logf.Log.WithName("test"),
		InstanceStatusTarget:     irstatus.EncodingDenseV1,
		Expectations:             workload.NewExpectations(),
		ScaleDownRequeueInterval: testScaleDownRequeueInterval,
		Recorder:                 rec,
		Clock:                    clocktesting.NewFakeClock(base),
		GangSchedulingAvailable:  true,
	}
	withLifecycleConfig(r, `{"audit":{"maxInFlightMigrations":3,"maxMigrationsPerWindow":10,"window":"1h"},`+
		`"forceDelete":{"overdueSlack":"2m","nodeUnreachableThreshold":"5m"}}`)

	ctx := context.Background()
	request := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(ir)}
	var events []string
	step := func(tag string) {
		r.Expectations = workload.NewExpectations()
		if _, err := r.Reconcile(ctx, request); err != nil {
			t.Fatalf("%s reconcile: %v", tag, err)
		}
		events = append(events, drainEvents(rec)...)
		gangMigSimulate(t, c, ir.Namespace, ir.Spec.ParentRef.Name)
	}
	get := func() *v1beta1.InferenceReplica {
		fresh := &v1beta1.InferenceReplica{}
		if err := c.Get(ctx, client.ObjectKeyFromObject(ir), fresh); err != nil {
			t.Fatalf("get IR: %v", err)
		}
		return fresh
	}

	for i := 0; i < 10; i++ {
		step("startup")
	}
	if fresh := get(); len(fresh.Status.InstanceStatuses) != 1 ||
		fresh.Status.InstanceStatuses[0].Phase != v1beta1.OMENativeInstanceReady {
		t.Fatalf("gang did not reach Ready: %+v", fresh.Status.InstanceStatuses)
	}

	fresh := get()
	fresh.Status.Migrations = []v1beta1.MigrationStatus{{
		RequestUUID:    "mig-source-wedged",
		Trigger:        v1beta1.MigrationTriggerManual,
		Phase:          v1beta1.MigrationPhaseAccepted,
		SourceInstance: 0,
		FromNode:       gangMigSourceNode,
		Reason:         "test",
		StartedAt:      metav1.NewTime(base),
		Deadline:       metav1.NewTime(base.Add(30 * time.Minute)),
	}}
	if err := c.Status().Update(ctx, fresh); err != nil {
		t.Fatalf("seed migration record: %v", err)
	}

	completed := false
	for i := 0; i < 30 && !completed; i++ {
		step("migration")
		got := get()
		if len(got.Status.Migrations) != 1 {
			t.Fatalf("migration record count: %+v", got.Status.Migrations)
		}
		switch got.Status.Migrations[0].Phase {
		case v1beta1.MigrationPhaseCompleted:
			completed = true
		case v1beta1.MigrationPhaseFailed:
			t.Fatalf("migration failed: %+v", got.Status.Migrations[0])
		}
	}
	if !completed {
		got := get()
		t.Fatalf("migration never completed: record=%+v instances=%+v wedged=%v forced=%v",
			got.Status.Migrations[0], got.Status.InstanceStatuses, wedged, forced)
	}

	if len(wedged) == 0 {
		t.Fatal("the source pods were never wedged, so the sweep was not the thing that cleared them")
	}
	for name := range wedged {
		if !containsString(forced, name) {
			t.Errorf("wedged source pod %s was not force-deleted at grace zero; forced=%v", name, forced)
		}
	}
	if len(eventsContaining(events, string(workload.EventReasonPodForceDeleted))) == 0 {
		t.Errorf("the sweep must warn PodForceDeleted; events=%v", events)
	}
	if got := get(); len(got.Status.InstanceStatuses) != 1 || got.Status.InstanceStatuses[0].Index == 0 {
		t.Errorf("only the promoted surge survives, got %+v", got.Status.InstanceStatuses)
	}
}

func containsString(values []string, want string) bool {
	for _, v := range values {
		if strings.EqualFold(v, want) {
			return true
		}
	}
	return false
}
