package components

import (
	"context"
	"fmt"

	ctrl "sigs.k8s.io/controller-runtime"

	v1beta1acreconciler "github.com/sgl-project/ome/pkg/controller/v1beta1/acceleratorclass"
)

// SetupAcceleratorClass registers the AcceleratorClassReconciler.
// The reconciler reads no ConfigMaps; only inputs are AC spec + Node
// list. Suite must register a Recorder source if event assertions
// matter; mgr.GetEventRecorderFor is sufficient for tests.
func SetupAcceleratorClass(ctx context.Context, mgr ctrl.Manager) error {
	r := &v1beta1acreconciler.AcceleratorClassReconciler{
		Client:   mgr.GetClient(),
		Log:      ctrl.Log.WithName("AcceleratorClass"),
		Scheme:   mgr.GetScheme(),
		Recorder: mgr.GetEventRecorderFor("AcceleratorClass"),
	}
	if err := r.SetupWithManager(mgr); err != nil {
		return fmt.Errorf("SetupAcceleratorClass: SetupWithManager: %w", err)
	}
	return nil
}
