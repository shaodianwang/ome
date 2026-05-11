package components

import (
	"context"
	"fmt"

	"k8s.io/client-go/kubernetes"
	ctrl "sigs.k8s.io/controller-runtime"

	v1beta1benchmarkjobcontroller "github.com/ome-projects/ome/pkg/controller/v1beta1/benchmark"
)

// SetupBenchmarkJob registers the BenchmarkJobReconciler. The
// reconciler reads benchmarkjob-config from the OME namespace via
// Clientset; SeedDefaultConfigMaps already provides this seed.
//
// Suites that exercise the InferenceService-endpoint path also need
// SetupBaseModel + SetupInferenceService since the reconciler
// resolves the referenced ISVC via the manager's client. Suites
// that test only the URL-endpoint path (no InferenceService field
// in Endpoint) can omit those.
func SetupBenchmarkJob(ctx context.Context, mgr ctrl.Manager) error {
	clientset, err := kubernetes.NewForConfig(mgr.GetConfig())
	if err != nil {
		return fmt.Errorf("SetupBenchmarkJob: build clientset: %w", err)
	}
	r := &v1beta1benchmarkjobcontroller.BenchmarkJobReconciler{
		Client:    mgr.GetClient(),
		Clientset: clientset,
		Log:       ctrl.Log.WithName("BenchmarkJob"),
		Scheme:    mgr.GetScheme(),
		Recorder:  mgr.GetEventRecorderFor("BenchmarkJob"),
	}
	if err := r.SetupWithManager(mgr); err != nil {
		return fmt.Errorf("SetupBenchmarkJob: SetupWithManager: %w", err)
	}
	return nil
}
