package irstatus

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

const (
	fixtureRevision      = "example-engine-2f32f6fe"
	fixtureOtherRevision = "example-engine-9b1c0d2e"
	fixtureContainer     = "ome-container"
)

var fixtureTime = metav1.NewTime(time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC))

func fixtureTimePlus(d time.Duration) metav1.Time {
	return metav1.NewTime(fixtureTime.Add(d))
}

// uniformRows is the steady-state fixture: every row Ready on one revision at
// incarnation 1 with single-pod counts and admission.
func uniformRows(n int) []v1beta1.OMENativeInstanceStatus {
	rows := make([]v1beta1.OMENativeInstanceStatus, n)
	for i := range rows {
		rows[i] = v1beta1.OMENativeInstanceStatus{
			Index:             int32(i),
			Incarnation:       1,
			Phase:             v1beta1.OMENativeInstanceReady,
			RunningRevision:   fixtureRevision,
			PodCount:          1,
			ServingPodCount:   1,
			AvailablePodCount: 1,
			Admitted:          true,
		}
	}
	return rows
}

// The mass-failure fixture is a 4,000-row single-pod fleet after a
// widespread failure: 2,812 Ready, 814 Restarting with an in-flight Restart,
// 374 Failed with a preserved Restart, every row carrying a lastFailure with
// a message of about 140 bytes, and one row on a second revision. Phases are
// spread over the index space by a fixed permutation so the index sets
// fragment as they would in a real fleet.
const (
	massFailureRowCount        = 4000
	massFailureReadyCount      = 2812
	massFailureRestartingCount = 814
	massFailureFailedCount     = 374
	massFailureStride          = 1103
	massFailureMessage         = "container ome-container exited with code 137: OOMKilled while loading model weights; " +
		"the kubelet restarted it three times before the recreate"
)

func massFailureRows() []v1beta1.OMENativeInstanceStatus {
	rows := make([]v1beta1.OMENativeInstanceStatus, massFailureRowCount)
	for i := range rows {
		exitCode := int32(137)
		row := v1beta1.OMENativeInstanceStatus{
			Index:           int32(i),
			Incarnation:     1,
			RunningRevision: fixtureRevision,
			LastFailure: &v1beta1.InstanceTermination{
				PodName:       fmt.Sprintf("example-engine-%d-0", i),
				ContainerName: fixtureContainer,
				Reason:        "OOMKilled",
				ExitCode:      &exitCode,
				Message:       massFailureMessage,
				Time:          fixtureTime,
			},
		}
		operation := &v1beta1.InstanceOperation{
			ID:             fmt.Sprintf("2f6b6c8e-0000-4000-8000-%012d", i),
			Type:           v1beta1.InstanceOperationRestart,
			StartedAt:      fixtureTimePlus(time.Minute),
			LastProgressAt: fixtureTimePlus(2 * time.Minute),
			Deadline:       fixtureTimePlus(30 * time.Minute),
			TargetRevision: fixtureRevision,
			Reason:         "PodFailed",
		}
		switch rank := (i * massFailureStride) % massFailureRowCount; {
		case rank < massFailureReadyCount:
			row.Phase = v1beta1.OMENativeInstanceReady
			row.Incarnation = 2
			row.PodCount, row.ServingPodCount, row.AvailablePodCount = 1, 1, 1
			row.Admitted = true
		case rank < massFailureReadyCount+massFailureRestartingCount:
			row.Phase = v1beta1.OMENativeInstanceRestarting
			operation.Step = "WaitReady"
			row.Operation = operation
		default:
			row.Phase = v1beta1.OMENativeInstanceFailed
			operation.Step = "Recreate"
			operation.RetryCount = 3
			operation.Reason = "RestartBudgetExhausted"
			row.Operation = operation
		}
		rows[i] = row
	}
	rows[massFailureRowCount-1].RunningRevision = fixtureOtherRevision
	return rows
}

// representativeRows mixes two revisions, three incarnations, a rollout in
// flight, sparse indices, partial admission, alternating ordinals, and
// thirteen sparse failures and operations.
func representativeRows(n int) []v1beta1.OMENativeInstanceStatus {
	rows := make([]v1beta1.OMENativeInstanceStatus, 0, n)
	exitCode := int32(1)
	index := int32(0)
	for len(rows) < n {
		i := len(rows)
		if i%97 == 96 {
			index++ // leave a gap so the members set is sparse
		}
		row := v1beta1.OMENativeInstanceStatus{
			Index:             index,
			Incarnation:       int64(1 + (i*7)%3),
			Phase:             v1beta1.OMENativeInstanceReady,
			RunningRevision:   fixtureRevision,
			PodCount:          1,
			ServingPodCount:   1,
			AvailablePodCount: 1,
			Admitted:          i%53 != 0,
			ActiveOrdinal:     int32(i % 4 / 3),
		}
		if i%10 == 3 {
			row.RunningRevision = fixtureOtherRevision
		}
		if i%20 == 7 {
			row.Phase = v1beta1.OMENativeInstanceUpdating
			row.TargetRevision = fixtureOtherRevision
			row.ServingPodCount, row.AvailablePodCount = 0, 0
		}
		if i%(n/13+1) == 5 && i < 13*(n/13+1) {
			row.LastFailure = &v1beta1.InstanceTermination{
				PodName: fmt.Sprintf("example-engine-%d-0", index), ContainerName: fixtureContainer,
				Reason: "Error", ExitCode: &exitCode, Time: fixtureTime,
			}
			row.Operation = &v1beta1.InstanceOperation{
				ID: fmt.Sprintf("2f6b6c8e-0000-4000-8000-%012d", index), Type: v1beta1.InstanceOperationRestart,
				Step: "DeletePods", StartedAt: fixtureTime, LastProgressAt: fixtureTime, Deadline: fixtureTimePlus(time.Hour),
				TargetRevision: fixtureRevision, Reason: "PodFailed",
			}
			row.Phase = v1beta1.OMENativeInstanceRestarting
		}
		rows = append(rows, row)
		index++
	}
	return rows
}

// fullFeatureRows touches every retained field: all phases, every operation
// type, conditions, timestamps, pointers, a negative incarnation, both
// ordinals, sparse indices, and a non-ascending row order.
func fullFeatureRows() []v1beta1.OMENativeInstanceStatus {
	exitCode := int32(143)
	surgeIndex := int32(21)
	readySince := fixtureTimePlus(-time.Hour)
	condition := func(kind string, status metav1.ConditionStatus, reason string) metav1.Condition {
		return metav1.Condition{Type: kind, Status: status, Reason: reason, Message: reason + " observed", LastTransitionTime: fixtureTime, ObservedGeneration: 3}
	}
	operation := func(id string, kind v1beta1.InstanceOperationType, step string) *v1beta1.InstanceOperation {
		return &v1beta1.InstanceOperation{
			ID: id, Type: kind, Step: step, StartedAt: fixtureTime, LastProgressAt: fixtureTimePlus(time.Minute),
			Deadline: fixtureTimePlus(time.Hour), RetryCount: 1, TargetRevision: fixtureOtherRevision, Reason: "rollout",
		}
	}
	migrate := operation("op-migrate", v1beta1.InstanceOperationMigrate, "SurgeReady")
	migrate.SurgeIndex = &surgeIndex
	migrate.FromNode = "node-a"
	migrate.HintTargetNodes = []string{"node-c", "node-b"}
	migrate.RequestUUID = "9c0d1e2f-0000-4000-8000-000000000001"
	return []v1beta1.OMENativeInstanceStatus{
		{Index: 5, Incarnation: 3, Phase: v1beta1.OMENativeInstanceReady, RunningRevision: fixtureRevision, PodCount: 8, ServingPodCount: 8, AvailablePodCount: 8, Admitted: true, ActiveOrdinal: 1, ReadySince: &readySince,
			Conditions: []metav1.Condition{condition("AllPodsReady", metav1.ConditionTrue, "Ready"), condition("AllPodsAvailable", metav1.ConditionTrue, "Available")}},
		{Index: 0, Incarnation: 1, Phase: v1beta1.OMENativeInstancePending, Admitted: false},
		{Index: 1, Phase: v1beta1.OMENativeInstanceCreating, TargetRevision: fixtureRevision, Operation: operation("op-create", v1beta1.InstanceOperationCreate, "WaitReady")},
		{Index: 2, Incarnation: -1, Phase: v1beta1.OMENativeInstanceUpdating, RunningRevision: fixtureRevision, TargetRevision: fixtureOtherRevision, PodCount: 8, ServingPodCount: 4, Admitted: true, Operation: operation("op-update", v1beta1.InstanceOperationUpdate, "Drain")},
		{Index: 3, Incarnation: 2, Phase: v1beta1.OMENativeInstanceRestarting, RunningRevision: fixtureRevision, PodCount: 8, Admitted: true, Operation: operation("op-restart", v1beta1.InstanceOperationRestart, "DeletePods"),
			LastFailure: &v1beta1.InstanceTermination{PodName: "example-engine-3-2", ContainerName: fixtureContainer, Reason: "Error", ExitCode: &exitCode, Message: "exit status 143", Time: fixtureTime}},
		{Index: 20, Incarnation: 1, Phase: v1beta1.OMENativeInstanceMigrating, RunningRevision: fixtureRevision, PodCount: 8, ServingPodCount: 8, AvailablePodCount: 8, Admitted: true, Operation: migrate},
		{Index: 7, Incarnation: 4, Phase: v1beta1.OMENativeInstanceFailed, RunningRevision: fixtureOtherRevision, Admitted: true, ActiveOrdinal: 1,
			LastFailure: &v1beta1.InstanceTermination{PodName: "example-engine-7-0", Reason: "PodFailed", Time: fixtureTime},
			Conditions:  []metav1.Condition{condition("Failed", metav1.ConditionTrue, "PodFailed")}},
		{Index: 2147483647, Incarnation: 9223372036854775807, Phase: v1beta1.OMENativeInstanceDeleting, RunningRevision: fixtureOtherRevision, PodCount: 2147483647, ServingPodCount: 1, AvailablePodCount: 2147483647, Operation: operation("op-delete", v1beta1.InstanceOperationDelete, "Drain")},
	}
}

// oepExampleRows reproduces the OEP wire-shape example: 500 Ready rows on
// one revision, two reincarnated rows, three rows on ordinal one, and one
// preserved failure.
func oepExampleRows() []v1beta1.OMENativeInstanceStatus {
	rows := uniformRows(500)
	rows[9].Incarnation = 2
	rows[115].Incarnation = 2
	for _, index := range []int{6, 43, 175} {
		rows[index].ActiveOrdinal = 1
	}
	exitCode := int32(143)
	rows[9].LastFailure = &v1beta1.InstanceTermination{PodName: "example-engine-9", ContainerName: fixtureContainer, Reason: "Error", ExitCode: &exitCode, Time: fixtureTime}
	return rows
}

func logicalStatus(rows []v1beta1.OMENativeInstanceStatus) *v1beta1.InferenceReplicaStatus {
	return &v1beta1.InferenceReplicaStatus{
		Replicas:         int32(len(rows)),
		CurrentRevision:  fixtureRevision,
		LabelSelector:    "ome.io/inferenceservice=llama-70b,ome.io/component=engine",
		InstanceStatuses: rows,
	}
}

func columnarStatus(columns *v1beta1.InstanceStatusColumns) *v1beta1.InferenceReplicaStatus {
	encoding := v1beta1.InstanceStatusEncodingColumnarV2
	return &v1beta1.InferenceReplicaStatus{InstanceStatusEncoding: &encoding, InstanceStatusColumns: columns}
}

func mustEncode(t testing.TB, rows []v1beta1.OMENativeInstanceStatus, limit uint64) *v1beta1.InstanceStatusColumns {
	t.Helper()
	columns, err := EncodeColumns(rows, limit)
	if err != nil {
		t.Fatalf("EncodeColumns() error = %v", err)
	}
	return columns
}

func mustDecode(t testing.TB, columns *v1beta1.InstanceStatusColumns, limit uint64) []v1beta1.OMENativeInstanceStatus {
	t.Helper()
	rows, err := DecodeColumns(columns, limit)
	if err != nil {
		t.Fatalf("DecodeColumns() error = %v", err)
	}
	return rows
}

func mustJSON(t testing.TB, value any) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return data
}

// assertRowsEqual compares rows with API semantics: nil and empty slices are
// the same, times compare by instant, and pointers compare by value.
func assertRowsEqual(t testing.TB, want, got []v1beta1.OMENativeInstanceStatus) {
	t.Helper()
	if len(want) != len(got) {
		t.Fatalf("row count = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if !equality.Semantic.DeepEqual(want[i], got[i]) {
			t.Fatalf("row %d differs:\n want %s\n got  %s", i, mustJSON(t, want[i]), mustJSON(t, got[i]))
		}
		if wantJSON, gotJSON := mustJSON(t, want[i]), mustJSON(t, got[i]); string(wantJSON) != string(gotJSON) {
			t.Fatalf("row %d wire differs:\n want %s\n got  %s", i, wantJSON, gotJSON)
		}
	}
}

func assertColumnsEqual(t testing.TB, want, got *v1beta1.InstanceStatusColumns) {
	t.Helper()
	if wantJSON, gotJSON := mustJSON(t, want), mustJSON(t, got); string(wantJSON) != string(gotJSON) {
		t.Fatalf("columns differ:\n want %s\n got  %s", wantJSON, gotJSON)
	}
}
