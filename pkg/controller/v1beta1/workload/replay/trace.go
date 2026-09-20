package replay

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	types "github.com/ome-projects/ome/pkg/controller/v1beta1/workload/types"
)

// rfc3339 matches the timestamps the engine embeds in event messages, so
// they can be rewritten to the scenario-relative form the rest of the
// trace uses.
var rfc3339 = regexp.MustCompile(`\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})`)

// normalizer replaces every identifier that is stable within one run but
// arbitrary across builds with a token derived from first-appearance
// order. Two runs of one scenario therefore render byte-identically while
// a changed write still shows up as a diff.
type normalizer struct {
	start time.Time
	ops   map[string]string
	uids  map[string]string
	revs  map[string]string
}

func newNormalizer(start time.Time) *normalizer {
	return &normalizer{
		start: start,
		ops:   map[string]string{},
		uids:  map[string]string{},
		revs:  map[string]string{},
	}
}

func (n *normalizer) op(id string) string {
	if id == "" {
		return ""
	}
	if token, ok := n.ops[id]; ok {
		return token
	}
	token := fmt.Sprintf("op#%d", len(n.ops)+1)
	n.ops[id] = token
	return token
}

func (n *normalizer) uid(uid string) string {
	if uid == "" {
		return ""
	}
	if token, ok := n.uids[uid]; ok {
		return token
	}
	token := fmt.Sprintf("uid#%d", len(n.uids)+1)
	n.uids[uid] = token
	return token
}

// revision registers one revision under both its object name and its
// hash, so a trace line naming either renders the same token.
func (n *normalizer) revision(name, hash string) string {
	if token, ok := n.revs[name]; ok {
		return token
	}
	token := fmt.Sprintf("rev#%d", n.revisionCount()+1)
	n.revs[name] = token
	if hash != "" {
		n.revs[hash] = token
	}
	return token
}

func (n *normalizer) revisionCount() int {
	seen := map[string]struct{}{}
	for _, token := range n.revs {
		seen[token] = struct{}{}
	}
	return len(seen)
}

func (n *normalizer) rev(nameOrHash string) string {
	if nameOrHash == "" {
		return ""
	}
	if token, ok := n.revs[nameOrHash]; ok {
		return token
	}
	return nameOrHash
}

// at renders an absolute instant relative to the scenario start.
func (n *normalizer) at(t time.Time) string {
	if t.IsZero() {
		return "nil"
	}
	d := t.Sub(n.start)
	if d < 0 {
		return "t-" + d.Abs().String()
	}
	return "t+" + d.String()
}

func (n *normalizer) atMeta(t *metav1.Time) string {
	if t == nil || t.IsZero() {
		return "nil"
	}
	return n.at(t.Time)
}

// text rewrites every registered identifier inside a free-form string —
// the event messages the engine composes from ids and revision names.
func (n *normalizer) text(s string) string {
	if s == "" {
		return s
	}
	for _, table := range []map[string]string{n.ops, n.uids, n.revs} {
		for _, from := range sortedByLengthDesc(table) {
			s = strings.ReplaceAll(s, from, table[from])
		}
	}
	return rfc3339.ReplaceAllStringFunc(s, func(match string) string {
		parsed, err := time.Parse(time.RFC3339, match)
		if err != nil {
			return match
		}
		return n.at(parsed)
	})
}

// sortedByLengthDesc orders the replacement keys longest-first so a hash
// that is a substring of a revision name cannot shadow it. The order is
// total, because map iteration order is not.
func sortedByLengthDesc(table map[string]string) []string {
	keys := make([]string, 0, len(table))
	for k := range table {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if len(keys[i]) != len(keys[j]) {
			return len(keys[i]) > len(keys[j])
		}
		return keys[i] < keys[j]
	})
	return keys
}

// Trace accumulates the recorded effects of one run, already normalized.
type Trace struct {
	norm  *normalizer
	lines []string
	tick  int
	now   func() time.Time
}

func newTrace(norm *normalizer, now func() time.Time) *Trace {
	return &Trace{norm: norm, now: now}
}

// String renders the trace as the golden file stores it: one effect per
// line, newline-terminated.
func (t *Trace) String() string {
	if len(t.lines) == 0 {
		return ""
	}
	return strings.Join(t.lines, "\n") + "\n"
}

func (t *Trace) emit(format string, args ...any) {
	t.lines = append(t.lines, fmt.Sprintf("tick=%d ", t.tick)+fmt.Sprintf(format, args...))
}

func (t *Trace) beginTick(tick int, note string) {
	t.tick = tick
	if note != "" {
		t.emit("note %s", note)
	}
}

func (t *Trace) applied(ev TimelineEvent, detail string) {
	line := "apply " + ev.Key()
	if detail != "" {
		line += " " + detail
	}
	t.emit("%s now=%s", line, t.norm.at(t.now()))
}

// RowCommitted renders one resolved row write, field by field. A mutation
// that did not commit has no diff by definition, so the line carries the
// attempt and the verdict and nothing else — the attempt itself is engine
// behavior worth locking.
func (t *Trace) RowCommitted(commit RowCommit) {
	verb := "write"
	if commit.Remove {
		verb = "remove"
	}
	if commit.Outcome != PreconditionOK {
		t.emit("row=%d %s no-op precondition=%s", commit.Index, verb, commit.Outcome)
		return
	}
	diff := t.rowDiff(commit.Before, commit.After)
	if diff == "" {
		diff = "-"
	}
	t.emit("row=%d %s %s precondition=%s", commit.Index, verb, diff, commit.Outcome)
}

// RetryBlockCommitted renders one resolved retry-block write.
func (t *Trace) RetryBlockCommitted(commit RetryBlockCommit) {
	t.emit("retryblock target=%s %s %s", t.norm.rev(commit.TargetRevision), commit.Disposition,
		t.retryBlockDiff(commit.Before, commit.After))
}

func (t *Trace) event(eventType, reason, message string) {
	t.emit("event %s %s %s", eventType, reason, t.norm.text(message))
}

func (t *Trace) object(kind, verb, name, detail string) {
	line := fmt.Sprintf("%s %s name=%s", kind, verb, name)
	if detail != "" {
		line += " " + detail
	}
	t.emit("%s", line)
}

func (t *Trace) callback(format string, args ...any) {
	t.emit("%s", fmt.Sprintf(format, args...))
}

func (t *Trace) rowDiff(before, after *types.InstanceStatus) string {
	var parts []string
	add := func(field, from, to string) {
		if from == to {
			return
		}
		parts = append(parts, fmt.Sprintf("%s=%s→%s", field, from, to))
	}
	zero := types.InstanceStatus{}
	from, to := zero, zero
	switch {
	case before == nil && after == nil:
		return ""
	case before == nil:
		to = *after
		parts = append(parts, "created")
	case after == nil:
		from = *before
		parts = append(parts, "removed")
	default:
		from, to = *before, *after
	}
	if after == nil {
		return strings.Join(append(parts, t.rowFields(from)...), " ")
	}
	if before == nil {
		return strings.Join(append(parts, t.rowFields(to)...), " ")
	}
	add("incarnation", fmt.Sprint(from.Incarnation), fmt.Sprint(to.Incarnation))
	add("phase", phaseOrNil(from.Phase), phaseOrNil(to.Phase))
	add("runningRevision", t.norm.rev(orNil(from.RunningRevision)), t.norm.rev(orNil(to.RunningRevision)))
	add("targetRevision", t.norm.rev(orNil(from.TargetRevision)), t.norm.rev(orNil(to.TargetRevision)))
	add("podCount", fmt.Sprint(from.PodCount), fmt.Sprint(to.PodCount))
	add("servingPodCount", fmt.Sprint(from.ServingPodCount), fmt.Sprint(to.ServingPodCount))
	add("availablePodCount", fmt.Sprint(from.AvailablePodCount), fmt.Sprint(to.AvailablePodCount))
	add("admitted", fmt.Sprint(from.Admitted), fmt.Sprint(to.Admitted))
	add("activeOrdinal", fmt.Sprint(from.ActiveOrdinal), fmt.Sprint(to.ActiveOrdinal))
	// ReadyPodCount, ScheduledPodCount and NodesOccupied are materialized
	// only on the status publication path, which the driver does not run,
	// and the workload package forbids reading them anywhere else. They are
	// therefore outside what a row diff can honestly report.
	add("readySince", t.norm.atMeta(from.ReadySince), t.norm.atMeta(to.ReadySince))
	add("conditions", t.conditions(from.Conditions), t.conditions(to.Conditions))
	add("lastFailure", t.failure(from.LastFailure), t.failure(to.LastFailure))
	parts = append(parts, t.operationDiff(from.Operation, to.Operation)...)
	return strings.Join(parts, " ")
}

// rowFields renders a whole row, for a create or a removal where there is
// no counterpart to diff against.
func (t *Trace) rowFields(row types.InstanceStatus) []string {
	parts := []string{
		"incarnation=" + fmt.Sprint(row.Incarnation),
		"phase=" + phaseOrNil(row.Phase),
	}
	if row.RunningRevision != "" {
		parts = append(parts, "runningRevision="+t.norm.rev(row.RunningRevision))
	}
	if row.TargetRevision != "" {
		parts = append(parts, "targetRevision="+t.norm.rev(row.TargetRevision))
	}
	if row.ActiveOrdinal != 0 {
		parts = append(parts, "activeOrdinal="+fmt.Sprint(row.ActiveOrdinal))
	}
	if row.ReadySince != nil {
		parts = append(parts, "readySince="+t.norm.atMeta(row.ReadySince))
	}
	if row.Operation != nil {
		parts = append(parts, "op="+t.operation(row.Operation))
	}
	if row.LastFailure != nil {
		parts = append(parts, "lastFailure="+t.failure(row.LastFailure))
	}
	return parts
}

func (t *Trace) operationDiff(before, after *types.InstanceOperation) []string {
	switch {
	case before == nil && after == nil:
		return nil
	case before == nil:
		return []string{"op=nil→" + t.operation(after)}
	case after == nil:
		return []string{"op=" + t.norm.op(before.ID) + "→nil"}
	case before.ID != after.ID:
		return []string{"op=" + t.norm.op(before.ID) + "→" + t.operation(after)}
	}
	token := t.norm.op(before.ID)
	var parts []string
	add := func(field, from, to string) {
		if from != to {
			parts = append(parts, fmt.Sprintf("%s.%s=%s→%s", token, field, from, to))
		}
	}
	add("type", string(before.Type), string(after.Type))
	add("step", orNil(before.Step), orNil(after.Step))
	add("target", t.norm.rev(orNil(before.TargetRevision)), t.norm.rev(orNil(after.TargetRevision)))
	add("reason", t.norm.text(orNil(before.Reason)), t.norm.text(orNil(after.Reason)))
	add("waiting", orNil(before.Waiting), orNil(after.Waiting))
	add("strategy", orNil(string(before.Strategy)), orNil(string(after.Strategy)))
	add("retryCount", fmt.Sprint(before.RetryCount), fmt.Sprint(after.RetryCount))
	add("surgeIndex", indexOrNil(before.SurgeIndex), indexOrNil(after.SurgeIndex))
	add("deadline", t.norm.at(before.Deadline.Time), t.norm.at(after.Deadline.Time))
	add("lastProgressAt", t.norm.at(before.LastProgressAt.Time), t.norm.at(after.LastProgressAt.Time))
	return parts
}

func (t *Trace) operation(op *types.InstanceOperation) string {
	if op == nil {
		return "nil"
	}
	parts := []string{
		"type=" + string(op.Type),
		"step=" + orNil(op.Step),
	}
	if op.TargetRevision != "" {
		parts = append(parts, "target="+t.norm.rev(op.TargetRevision))
	}
	if op.Reason != "" {
		parts = append(parts, "reason="+t.norm.text(op.Reason))
	}
	if op.Waiting != "" {
		parts = append(parts, "waiting="+op.Waiting)
	}
	if op.Strategy != "" {
		parts = append(parts, "strategy="+string(op.Strategy))
	}
	if op.SurgeIndex != nil {
		parts = append(parts, "surgeIndex="+indexOrNil(op.SurgeIndex))
	}
	if op.RetryCount != 0 {
		parts = append(parts, "retryCount="+fmt.Sprint(op.RetryCount))
	}
	parts = append(parts, "started="+t.norm.at(op.StartedAt.Time), "deadline="+t.norm.at(op.Deadline.Time))
	return t.norm.op(op.ID) + "{" + strings.Join(parts, " ") + "}"
}

func (t *Trace) retryBlockDiff(before, after *types.RetryBlock) string {
	render := func(b *types.RetryBlock) string {
		if b == nil {
			return "nil"
		}
		parts := []string{
			"state=" + string(b.State),
			"attempts=" + fmt.Sprint(b.AttemptsStarted),
		}
		if b.NextRetryAt != nil {
			parts = append(parts, "nextRetryAt="+t.norm.atMeta(b.NextRetryAt))
		}
		if b.Reason != "" {
			parts = append(parts, "reason="+t.norm.text(b.Reason))
		}
		return "{" + strings.Join(parts, " ") + "}"
	}
	return render(before) + "→" + render(after)
}

func (t *Trace) conditions(conds []metav1.Condition) string {
	if len(conds) == 0 {
		return "nil"
	}
	parts := make([]string, 0, len(conds))
	for _, c := range conds {
		parts = append(parts, fmt.Sprintf("%s=%s(%s)", c.Type, c.Status, c.Reason))
	}
	sort.Strings(parts)
	return "[" + strings.Join(parts, ",") + "]"
}

func (t *Trace) failure(f *types.InstanceTermination) string {
	if f == nil {
		return "nil"
	}
	parts := []string{"pod=" + f.PodName, "reason=" + f.Reason}
	if f.ContainerName != "" {
		parts = append(parts, "container="+f.ContainerName)
	}
	if f.ExitCode != nil {
		parts = append(parts, fmt.Sprintf("exitCode=%d", *f.ExitCode))
	}
	if f.Message != "" {
		parts = append(parts, "message="+t.norm.text(f.Message))
	}
	return "{" + strings.Join(parts, " ") + "}"
}

func phaseOrNil(p types.InstancePhase) string {
	if p == "" {
		return "nil"
	}
	return string(p)
}

func orNil(s string) string {
	if s == "" {
		return "nil"
	}
	return s
}

func indexOrNil(i *int32) string {
	if i == nil {
		return "nil"
	}
	return fmt.Sprint(*i)
}
