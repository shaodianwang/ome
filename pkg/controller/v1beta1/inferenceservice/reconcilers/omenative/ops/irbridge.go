package ops

import (
	"context"

	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/core"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/audit"
)

// irbridge.go exposes the migration-detection helpers to the IR-managed
// path. Both the direct OMENative dispatcher and the InferenceReplica
// controller drive migration through the same workload.Migrate op; the
// detection (which reads the user-facing ISVC's migration-request
// annotation + the ISVC-owned audit ledger + the per-Component
// InstanceStatuses that AggregateIRStatus mirrors back onto the ISVC) is
// inherently ISVC-shaped. These thin wrappers let the IR controller call
// it with the PARENT ISVC while its workload.Reconcile drives the IR's
// own pods — the IR sets input.LedgerOwner=parent so the ledger Migrate
// reads/writes is the same ISVC-owned ledger these helpers use.
//
// The IR path passes only the fields the detectors actually read
// (ISVC, Client, APIReader, Recorder, Component, ComponentExt); the rest
// of core.ReconcileParams is direct-path render state the detectors never
// touch.

// DetectMigrationRequestForISVC resolves a pending migration request for
// `component` from the ISVC's annotation (Source 1) or in-flight ledger
// (Source 2). Wraps detectMigrationRequest for the IR controller's
// DetectMigrationRequest callback. The returned *audit.MigrationRequest
// is the same type as workload.MigrationRequestSummary (alias), so it
// threads straight through the workload dispatcher.
func DetectMigrationRequestForISVC(
	ctx context.Context,
	isvc *v1beta1.InferenceService,
	reader client.Reader,
	c client.Client,
	recorder record.EventRecorder,
	component v1beta1.ComponentType,
) (string, *audit.MigrationRequest, int32, bool, error) {
	p := core.ReconcileParams{
		ISVC:      isvc,
		Client:    c,
		APIReader: reader,
		Recorder:  recorder,
		Component: component,
	}
	return detectMigrationRequest(ctx, p, component)
}
