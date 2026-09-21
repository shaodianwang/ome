package status

import (
	"context"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8stypes "k8s.io/apimachinery/pkg/types"
	clocktesting "k8s.io/utils/clock/testing"

	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/types"
)

type terminalMutationStore struct {
	ownerUID   k8stypes.UID
	statuses   map[int32]types.InstanceStatus
	retryBlock map[string]types.RetryBlock
	readErr    error
	applyErr   error
	applyCall  int
	writes     int
	log        *[]string
}

func (s *terminalMutationStore) apply(
	_ context.Context,
	mutations []types.InstanceMutation,
	targetRevision string,
	mutateRetryBlock func(*types.RetryBlock) types.RetryBlockDisposition,
) error {
	s.applyCall++
	if s.log != nil {
		*s.log = append(*s.log, "status")
	}
	if s.readErr != nil {
		return s.readErr
	}
	snapshot := types.InstanceMutationSnapshot{
		OwnerUID:  s.ownerUID,
		Instances: cloneTerminalStatuses(s.statuses),
	}
	for _, mutation := range mutations {
		if mutation.BatchPrecondition != nil && !mutation.BatchPrecondition(snapshot) {
			return types.ErrStatusMutationPrecondition
		}
	}

	next := cloneTerminalStatuses(s.statuses)
	type callback struct {
		fn      func(*types.InstanceStatus, *types.InstanceStatus)
		before  *types.InstanceStatus
		current *types.InstanceStatus
	}
	callbacks := make([]callback, 0, len(mutations))
	changed := false
	for _, mutation := range mutations {
		current, found := next[mutation.Index]
		if mutation.Remove {
			if !found {
				continue
			}
			before := cloneTerminalStatus(current)
			if mutation.Precondition != nil && !mutation.Precondition(&before) {
				continue
			}
			delete(next, mutation.Index)
			changed = true
			if mutation.OnCommit != nil {
				callbacks = append(callbacks, callback{fn: mutation.OnCommit, before: &before})
			}
			continue
		}
		if !found {
			current = types.InstanceStatus{Index: mutation.Index}
		}
		before := cloneTerminalStatus(current)
		if mutation.Precondition != nil && !mutation.Precondition(&before) {
			continue
		}
		if !mutation.Mutate(&current) {
			continue
		}
		next[mutation.Index] = current
		changed = true
		if mutation.OnCommit != nil {
			committed := cloneTerminalStatus(current)
			callbacks = append(callbacks, callback{fn: mutation.OnCommit, before: &before, current: &committed})
		}
	}
	nextRetryBlocks := cloneTerminalRetryBlocks(s.retryBlock)
	retryBlockChanged := false
	if mutateRetryBlock != nil {
		block, found := nextRetryBlocks[targetRevision]
		if !found {
			block = types.RetryBlock{TargetRevision: targetRevision}
		}
		switch mutateRetryBlock(&block) {
		case types.RetryBlockPersist:
			block.TargetRevision = targetRevision
			nextRetryBlocks[targetRevision] = block
			retryBlockChanged = true
		case types.RetryBlockRemove:
			if found {
				delete(nextRetryBlocks, targetRevision)
				retryBlockChanged = true
			}
		}
	}
	if !changed && !retryBlockChanged {
		return nil
	}
	if s.applyErr != nil {
		return s.applyErr
	}
	s.statuses = next
	s.retryBlock = nextRetryBlocks
	s.writes++
	for _, callback := range callbacks {
		callback.fn(callback.before, callback.current)
	}
	return nil
}

func cloneTerminalRetryBlocks(in map[string]types.RetryBlock) map[string]types.RetryBlock {
	out := make(map[string]types.RetryBlock, len(in))
	for revision, block := range in {
		copy := block
		if block.NextRetryAt != nil {
			next := *block.NextRetryAt
			copy.NextRetryAt = &next
		}
		if block.FirstFailureAt != nil {
			first := *block.FirstFailureAt
			copy.FirstFailureAt = &first
		}
		if block.LastFailureAt != nil {
			last := *block.LastFailureAt
			copy.LastFailureAt = &last
		}
		out[revision] = copy
	}
	return out
}

func cloneTerminalStatuses(in map[int32]types.InstanceStatus) map[int32]types.InstanceStatus {
	out := make(map[int32]types.InstanceStatus, len(in))
	for index, row := range in {
		out[index] = cloneTerminalStatus(row)
	}
	return out
}

func cloneTerminalStatus(row types.InstanceStatus) types.InstanceStatus {
	out := row
	if row.Operation != nil {
		operation := *row.Operation
		operation.HintTargetNodes = append([]string(nil), row.Operation.HintTargetNodes...)
		if row.Operation.SurgeIndex != nil {
			surge := *row.Operation.SurgeIndex
			operation.SurgeIndex = &surge
		}
		out.Operation = &operation
	}
	out.NodesOccupied = append([]string(nil), row.NodesOccupied...)
	out.Conditions = append([]metav1.Condition(nil), row.Conditions...)
	out.Announced = append([]string(nil), row.Announced...)
	return out
}
func terminalStatusFixture(index int32) types.InstanceStatus {
	surge := int32(9)
	started := metav1.NewTime(time.Date(2026, time.August, 15, 10, 0, 0, 987654321, time.UTC))
	return types.InstanceStatus{
		Index:           index,
		Incarnation:     7,
		Phase:           types.InstancePhaseUpdating,
		RunningRevision: "rev-old",
		TargetRevision:  "rev-new",
		ActiveOrdinal:   1,
		PodCount:        8,
		ReadyPodCount:   7,
		Operation: &types.InstanceOperation{
			ID:              "gang-update-7",
			Type:            types.InstanceOperationUpdate,
			Step:            types.UpdateStepSurge,
			RequestUUID:     "request-a",
			TargetRevision:  "rev-new",
			RetryCount:      2,
			Reason:          "rollout",
			FromNode:        "node-a",
			HintTargetNodes: []string{"node-b", "node-c"},
			SurgeIndex:      &surge,
			StartedAt:       started,
			LastProgressAt:  started,
			Deadline:        metav1.NewTime(started.Add(time.Hour)),
		},
	}
}

// rowWriter is a one-row status store over ReconcileInput. It records how
// many writes actually landed, which is how the evidence stamps' no-op
// contracts are checked.
type rowWriter struct {
	rows   map[int32]types.InstanceStatus
	writes int
}

func newRowWriter(rows ...types.InstanceStatus) *rowWriter {
	byIndex := make(map[int32]types.InstanceStatus, len(rows))
	for _, row := range rows {
		byIndex[row.Index] = row
	}
	return &rowWriter{rows: byIndex}
}

func (w *rowWriter) input(now time.Time) types.ReconcileInput {
	observed := make([]types.InstanceStatus, 0, len(w.rows))
	for _, row := range w.rows {
		observed = append(observed, row)
	}
	return types.ReconcileInput{
		Clock:         clocktesting.NewFakeClock(now),
		ObservedState: types.WorkloadObservedState{InstanceStatuses: observed},
		MutateInstance: func(_ context.Context, idx int32, mutate func(*types.InstanceStatus) bool) error {
			row, found := w.rows[idx]
			if !found {
				row = types.InstanceStatus{Index: idx}
			}
			if mutate(&row) {
				w.rows[idx] = row
				w.writes++
			}
			return nil
		},
	}
}

func openRow(idx int32, phase types.InstancePhase) types.InstanceStatus {
	return types.InstanceStatus{
		Index: idx, Phase: phase,
		Operation: &types.InstanceOperation{ID: "op-1", Type: types.InstanceOperationUpdate},
	}
}
