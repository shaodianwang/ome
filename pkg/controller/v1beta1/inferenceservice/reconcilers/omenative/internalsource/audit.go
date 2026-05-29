package internalsource

import (
	"context"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/audit"
)

// AuditConfigMapNameForISVC returns the deterministic audit ConfigMap
// name for the given ISVC. Thin wrapper around the owner-agnostic
// audit.ConfigMapNameForOwner so callers holding a typed ISVC handle
// don't need to convert to metav1.Object themselves.
func AuditConfigMapNameForISVC(isvc *v1beta1.InferenceService) string {
	return audit.ConfigMapNameForOwner(isvc)
}

// LoadAuditLedgerForISVC is the *v1beta1.InferenceService-typed wrapper
// around audit.LoadLedgerForOwner. The workload-side audit helper is
// owner-agnostic (takes metav1.Object); this adapter binds the ISVC
// shape so the existing OMENative-on-ISVC call sites read naturally.
func LoadAuditLedgerForISVC(ctx context.Context, reads client.Reader, isvc *v1beta1.InferenceService) (*audit.Ledger, error) {
	return audit.LoadLedgerForOwner(ctx, reads, owner(isvc))
}

// PersistAuditLedgerForISVC is the *v1beta1.InferenceService-typed
// wrapper around audit.PersistLedgerForOwner. Passes the ISVC
// GroupVersionKind so the audit ConfigMap's controller OwnerReference
// points at the right Kind — workload-side audit code itself does not
// know about the v1beta1 scheme.
func PersistAuditLedgerForISVC(ctx context.Context, c client.Client, isvc *v1beta1.InferenceService, l *audit.Ledger) error {
	return audit.PersistLedgerForOwner(ctx, c, owner(isvc), isvcGVK, l)
}

// owner returns the metav1.Object form of the ISVC handle. Guarded
// nil-check so a nil ISVC returns a nil metav1.Object rather than
// panicking inside the audit helpers (which expect a non-nil owner).
func owner(isvc *v1beta1.InferenceService) metav1.Object {
	if isvc == nil {
		return nil
	}
	return isvc
}
