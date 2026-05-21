package omenative

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

const (
	// migrationRequestAnnotationPrefix is the canonical key prefix Alfred
	// uses to write migration requests onto the InferenceService. The
	// suffix is the request UUID and is what's recorded in the audit
	// ledger for dedup.
	migrationRequestAnnotationPrefix = "ome.io/migration-request-v1-"

	// migrationRequestSchemaV1 is the only schemaVersion currently
	// understood. Requests carrying any other value are rejected as
	// UnsupportedSchemaVersion.
	migrationRequestSchemaV1 = "v1"

	// auditConfigMapNameSuffix is appended to the ISVC name to form the
	// audit ConfigMap name. One CM per ISVC keeps blast radius small
	// and matches the parent's namespace + lifecycle automatically via
	// OwnerReference cascade.
	auditConfigMapNameSuffix = "-ome-migration-audit"

	// auditLedgerKey is the data key inside the audit ConfigMap.
	auditLedgerKey = "history.json"

	// auditPhaseStarted / auditPhaseCompleted / auditPhaseFailed are
	// the lifecycle phases recorded against each migration request.
	auditPhaseStarted   = "Started"
	auditPhaseCompleted = "Completed"
	auditPhaseFailed    = "Failed"
)

// errUnsupportedSchemaVersion is returned by parseMigrationRequest
// when the request's schemaVersion isn't one we know how to handle.
// Callers should record this and reject the request rather than
// retrying.
var errUnsupportedSchemaVersion = fmt.Errorf("UnsupportedSchemaVersion")

// MigrationRequest is the JSON shape Alfred writes into the
// ome.io/migration-request-v1-<uuid> annotation value. Fields match
// the canonical request schema; unknown additive fields are silently
// dropped by json.Unmarshal (the version-skew rule).
type MigrationRequest struct {
	SchemaVersion   string   `json:"schemaVersion"`
	Component       string   `json:"component"`
	Instance        int32    `json:"instance"`
	FromNode        string   `json:"from_node"`
	HintTargetNodes []string `json:"hint_target_nodes,omitempty"`
	Reason          string   `json:"reason,omitempty"`
	RequestedAt     string   `json:"requested_at,omitempty"`
	RequestedBy     string   `json:"requested_by,omitempty"`
}

// AuditEntry is one row of the per-ISVC migration ledger. Stored in
// the audit ConfigMap; persists across controller restarts so UUID
// dedup outlives the in-memory expectations cache.
type AuditEntry struct {
	RequestUUID    string `json:"requestUUID"`
	Component      string `json:"component"`
	SourceInstance int32  `json:"sourceInstance"`
	SurgeInstance  int32  `json:"surgeInstance,omitempty"`
	Phase          string `json:"phase"`
	Reason         string `json:"reason,omitempty"`
	FromNode       string `json:"fromNode,omitempty"`
	StartedAt      string `json:"startedAt"`
	CompletedAt    string `json:"completedAt,omitempty"`
	Outcome        string `json:"outcome,omitempty"`
}

// auditLedger is the in-memory shape of the audit ConfigMap's
// history.json payload.
type auditLedger struct {
	Entries []AuditEntry `json:"entries"`
}

// extractRequestUUID returns the UUID suffix of a migration-request
// annotation key, or "" if the key isn't a migration request
// annotation. Used to scan an ISVC's annotations for pending requests.
func extractRequestUUID(key string) string {
	if !strings.HasPrefix(key, migrationRequestAnnotationPrefix) {
		return ""
	}
	return strings.TrimPrefix(key, migrationRequestAnnotationPrefix)
}

// parseMigrationRequest unmarshals an annotation value into a
// MigrationRequest and validates its schemaVersion. Returns
// errUnsupportedSchemaVersion when the schema is unknown; the
// reconciler records that as a rejected request and stops driving
// the UUID.
func parseMigrationRequest(raw string) (*MigrationRequest, error) {
	var req MigrationRequest
	if err := json.Unmarshal([]byte(raw), &req); err != nil {
		return nil, fmt.Errorf("parse migration request: %w", err)
	}
	if req.SchemaVersion != migrationRequestSchemaV1 {
		return nil, fmt.Errorf("%w: %q", errUnsupportedSchemaVersion, req.SchemaVersion)
	}
	return &req, nil
}

// auditConfigMapName returns the deterministic audit ConfigMap name
// for the ISVC. One ConfigMap per ISVC.
func auditConfigMapName(isvc *v1beta1.InferenceService) string {
	return isvc.Name + auditConfigMapNameSuffix
}

// loadAuditLedger fetches the per-ISVC audit ConfigMap and parses its
// history.json payload. Returns an empty ledger (and no error) when
// the ConfigMap doesn't exist yet — first migration on this ISVC.
func loadAuditLedger(ctx context.Context, reads client.Reader, isvc *v1beta1.InferenceService) (*auditLedger, error) {
	cm := &corev1.ConfigMap{}
	key := client.ObjectKey{Namespace: isvc.Namespace, Name: auditConfigMapName(isvc)}
	if err := reads.Get(ctx, key, cm); err != nil {
		if apierrors.IsNotFound(err) {
			return &auditLedger{}, nil
		}
		return nil, fmt.Errorf("get audit configmap %s: %w", key.Name, err)
	}
	raw, ok := cm.Data[auditLedgerKey]
	if !ok || raw == "" {
		return &auditLedger{}, nil
	}
	var l auditLedger
	if err := json.Unmarshal([]byte(raw), &l); err != nil {
		return nil, fmt.Errorf("unmarshal audit ledger %s: %w", key.Name, err)
	}
	return &l, nil
}

// hasCompletedOrFailedRequest reports whether the ledger already
// terminally records this UUID. Both Completed and Failed are
// terminal — a re-delivered request with the same UUID is a no-op
// regardless of outcome.
func (l *auditLedger) hasCompletedOrFailedRequest(uuid string) bool {
	for _, e := range l.Entries {
		if e.RequestUUID != uuid {
			continue
		}
		if e.Phase == auditPhaseCompleted || e.Phase == auditPhaseFailed {
			return true
		}
	}
	return false
}

// inFlightEntry returns the in-progress entry for uuid, or nil when
// the ledger has no Started-but-not-yet-terminal entry for this UUID.
// Used by the migration state machine to resume across reconciles.
func (l *auditLedger) inFlightEntry(uuid string) *AuditEntry {
	for i := range l.Entries {
		if l.Entries[i].RequestUUID != uuid {
			continue
		}
		if l.Entries[i].Phase == auditPhaseStarted {
			return &l.Entries[i]
		}
	}
	return nil
}

// auditLedgerMaxTerminalEntries caps the number of terminal
// (Completed / Failed) entries the ledger retains. ConfigMaps are
// capped at 1 MiB; a terminal entry is ~280 bytes, so ~3700 entries
// fit and 200 is well clear of the cap while still preserving plenty
// of dedup history. In-flight Started entries are NEVER truncated —
// dropping them would lose the resume anchor for an active migration.
const auditLedgerMaxTerminalEntries = 200

// upsertEntry adds the entry if its UUID is new, or replaces the
// existing same-UUID entry in place. Last-writer-wins on Phase / Outcome
// for the lifetime of one migration. After mutation the ledger is
// ring-buffer-trimmed: every Started entry is kept, and only the
// most recent auditLedgerMaxTerminalEntries terminal entries survive.
func (l *auditLedger) upsertEntry(entry AuditEntry) {
	for i := range l.Entries {
		if l.Entries[i].RequestUUID == entry.RequestUUID {
			l.Entries[i] = entry
			l.trim()
			return
		}
	}
	l.Entries = append(l.Entries, entry)
	l.trim()
}

// trim drops the oldest terminal entries when the count exceeds the
// configured cap. Started entries are protected — losing one would
// strand an in-flight migration that crashed before its destructive
// step recorded a terminal Completed/Failed row.
func (l *auditLedger) trim() {
	var terminal []int
	for i, e := range l.Entries {
		if e.Phase == auditPhaseCompleted || e.Phase == auditPhaseFailed {
			terminal = append(terminal, i)
		}
	}
	overflow := len(terminal) - auditLedgerMaxTerminalEntries
	if overflow <= 0 {
		return
	}
	drop := make(map[int]struct{}, overflow)
	for _, i := range terminal[:overflow] {
		drop[i] = struct{}{}
	}
	kept := l.Entries[:0]
	for i, e := range l.Entries {
		if _, gone := drop[i]; gone {
			continue
		}
		kept = append(kept, e)
	}
	l.Entries = kept
}

// persistAuditLedger writes the ledger back to the per-ISVC audit
// ConfigMap. Creates the CM with an OwnerReference to the ISVC when
// it doesn't exist yet, so cascading delete cleans it up. Idempotent.
func persistAuditLedger(ctx context.Context, c client.Client, isvc *v1beta1.InferenceService, l *auditLedger) error {
	raw, err := json.Marshal(l)
	if err != nil {
		return fmt.Errorf("marshal audit ledger: %w", err)
	}
	cm := &corev1.ConfigMap{}
	key := client.ObjectKey{Namespace: isvc.Namespace, Name: auditConfigMapName(isvc)}
	if err := c.Get(ctx, key, cm); err != nil {
		if !apierrors.IsNotFound(err) {
			return fmt.Errorf("get audit configmap %s: %w", key.Name, err)
		}
		cm = &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      key.Name,
				Namespace: key.Namespace,
				Labels: map[string]string{
					labelManagedBy: managedByOMENative,
				},
				OwnerReferences: []metav1.OwnerReference{
					*metav1.NewControllerRef(isvc, isvcGroupVersionKind),
				},
			},
			Data: map[string]string{auditLedgerKey: string(raw)},
		}
		if err := c.Create(ctx, cm); err != nil {
			if !apierrors.IsAlreadyExists(err) {
				return fmt.Errorf("create audit configmap %s: %w", key.Name, err)
			}
			// Lost a race; fall through to read-modify-write below.
			if err := c.Get(ctx, key, cm); err != nil {
				return fmt.Errorf("re-get audit configmap after race: %w", err)
			}
		} else {
			return nil
		}
	}
	if cm.Data == nil {
		cm.Data = map[string]string{}
	}
	cm.Data[auditLedgerKey] = string(raw)
	// Claim ownership when we land on a pre-existing CM without our
	// controller ref. The race path (Create -> AlreadyExists -> Get)
	// silently took whatever was there; without this we'd happily
	// write to a CM the cluster GC won't sweep when the ISVC is deleted.
	if !hasControllerRefForISVC(cm, isvc) {
		cm.OwnerReferences = append(cm.OwnerReferences, *metav1.NewControllerRef(isvc, isvcGroupVersionKind))
	}
	if err := c.Update(ctx, cm); err != nil {
		return fmt.Errorf("update audit configmap %s: %w", key.Name, err)
	}
	return nil
}

// hasControllerRefForISVC reports whether obj already has a controller
// OwnerReference pointing at the given ISVC.
func hasControllerRefForISVC(obj metav1.Object, isvc *v1beta1.InferenceService) bool {
	for _, ref := range obj.GetOwnerReferences() {
		if ref.Controller != nil && *ref.Controller && ref.UID == isvc.UID {
			return true
		}
	}
	return false
}

// auditStartedEntry constructs a freshly-Started ledger row from a
// validated request. StartedAt prefers req.RequestedAt so duration
// graphs (CompletedAt - StartedAt) reflect Alfred's accept-time
// rather than the controller's processing-time; falls back to now
// when the request omits the field.
func auditStartedEntry(req *MigrationRequest, uuid string, surgeIdx int32) AuditEntry {
	startedAt := req.RequestedAt
	if startedAt == "" {
		startedAt = metav1.Now().UTC().Format(time.RFC3339)
	}
	return AuditEntry{
		RequestUUID:    uuid,
		Component:      req.Component,
		SourceInstance: req.Instance,
		SurgeInstance:  surgeIdx,
		Phase:          auditPhaseStarted,
		Reason:         req.Reason,
		FromNode:       req.FromNode,
		StartedAt:      startedAt,
	}
}

// auditTerminalEntry rewrites a Started entry with the terminal phase
// and outcome. Returns a copy ready for upsertEntry; does not write.
func auditTerminalEntry(prev AuditEntry, phase, outcome string) AuditEntry {
	out := prev
	out.Phase = phase
	out.Outcome = outcome
	out.CompletedAt = metav1.Now().UTC().Format(time.RFC3339)
	return out
}
