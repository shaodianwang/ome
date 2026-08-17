package inferencereplica

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
	"github.com/ome-projects/ome/pkg/constants"
	"github.com/ome-projects/ome/pkg/controller/v1beta1/workload/query"
)

const (
	measurementCurrentRevision = "example-engine-6d8f7b9c"
	measurementUpdateRevision  = "example-engine-a3e129d4"
	measurementDerivedMarker   = "DerivedV1"
	measurementLargeMessageLen = 1024
)

var measurementInstanceCounts = []int32{0, 1, 100, 1000, 2000}

var (
	measurementEncoderOnce sync.Once
	measurementEncoder     runtime.Encoder
	measurementEncoderErr  error
)

type statusSizeShape string

const (
	shapeSteady           statusSizeShape = "steady"
	shapeCreating         statusSizeShape = "creating"
	shapeUpdatingInPlace  statusSizeShape = "updating-in-place"
	shapeUpdatingSurge    statusSizeShape = "updating-surge"
	shapeDeleting         statusSizeShape = "deleting"
	shapeKueueGated       statusSizeShape = "kueue-gated"
	shapeFailureRealistic statusSizeShape = "failure-realistic"
	shapeFailureLarge     statusSizeShape = "failure-large"
	shapeMigrationHeavy   statusSizeShape = "migration-heavy"
)

type statusSizeFixture struct {
	name            string
	shape           statusSizeShape
	podsPerInstance int32
}

var statusSizeFixtures = []statusSizeFixture{
	{name: "steady-singleton", shape: shapeSteady, podsPerInstance: 1},
	{name: "steady-gang-8", shape: shapeSteady, podsPerInstance: 8},
	{name: "creating-operation", shape: shapeCreating, podsPerInstance: 8},
	{name: "updating-in-place", shape: shapeUpdatingInPlace, podsPerInstance: 8},
	{name: "updating-surge", shape: shapeUpdatingSurge, podsPerInstance: 1},
	{name: "deleting-wave", shape: shapeDeleting, podsPerInstance: 8},
	{name: "kueue-gated", shape: shapeKueueGated, podsPerInstance: 8},
	{name: "failure-realistic", shape: shapeFailureRealistic, podsPerInstance: 8},
	{name: "failure-large", shape: shapeFailureLarge, podsPerInstance: 8},
	{name: "migration-heavy", shape: shapeMigrationHeavy, podsPerInstance: 8},
}

type statusSizeMeasurement struct {
	fixture                   string
	instances                 int32
	normalizedInlineBytes     int
	normalizedDerivedBytes    int
	inlineStatusBytes         int
	derivedStatusBytes        int
	instanceRowsBytes         int
	readyPodCountBytes        int
	scheduledPodCountBytes    int
	nodesOccupiedBytes        int
	liveObservationBytes      int
	representationMarkerBytes int
	operationBytes            int
	lastFailureBytes          int
	migrationsBytes           int
	reductionBytes            int
}

func TestInferenceReplicaStatusSizeProjectionDropsExactlyThreeFields(t *testing.T) {
	inline := newStatusSizeIR(statusSizeFixtures[1], 1)
	inline.Status.InstanceStatuses[0] = fullyPopulatedInstanceStatus(0)
	original := inline.DeepCopy()

	derived := projectDerivedV1(inline)
	want := inline.DeepCopy()
	clearDerivedV1Fields(want)

	if diff := cmp.Diff(want, derived); diff != "" {
		t.Fatalf("DerivedV1 projection changed fields outside the three live observations (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(original, inline); diff != "" {
		t.Fatalf("projection mutated the InlineV1 source (-want +got):\n%s", diff)
	}
}

func TestInferenceReplicaStatusSizeNormalizedFixtures(t *testing.T) {
	measurements := collectStatusSizeMeasurements(t)
	actual := renderStatusSizeCSV(t, measurements)
	expected, err := os.ReadFile("testdata/status_size_v1.csv")
	if err != nil {
		t.Fatalf("read status-size golden: %v\nactual report:\n%s", err, actual)
	}
	if diff := cmp.Diff(string(expected), actual); diff != "" {
		t.Fatalf("status-size report changed; review the normalized payload delta before updating the golden (-want +got):\n%s", diff)
	}
}

func TestInferenceReplicaStatusSizeNormalizedAcceptance(t *testing.T) {
	const (
		instances                 = int32(2000)
		maxNormalizedDerivedBytes = 330_000
	)
	tests := []struct {
		fixture             statusSizeFixture
		minReductionBytes   int
		minReductionPercent float64
	}{
		{fixture: statusSizeFixtures[0], minReductionBytes: 140_000, minReductionPercent: 30},
		{fixture: statusSizeFixtures[1], minReductionBytes: 350_000, minReductionPercent: 50},
	}
	for _, tt := range tests {
		t.Run(tt.fixture.name, func(t *testing.T) {
			measurement := measureStatusSize(t, tt.fixture, instances)
			if measurement.normalizedDerivedBytes > maxNormalizedDerivedBytes {
				t.Fatalf("normalized DerivedV1 projection = %d bytes, want at most %d", measurement.normalizedDerivedBytes, maxNormalizedDerivedBytes)
			}
			if measurement.reductionBytes < tt.minReductionBytes {
				t.Fatalf("request reduction = %d bytes, want at least %d", measurement.reductionBytes, tt.minReductionBytes)
			}
			percent := float64(measurement.reductionBytes) * 100 / float64(measurement.normalizedInlineBytes)
			if percent < tt.minReductionPercent {
				t.Fatalf("request reduction = %.2f%%, want at least %.2f%%", percent, tt.minReductionPercent)
			}
		})
	}
}

func BenchmarkInferenceReplicaStatusNormalizedJSON(b *testing.B) {
	fixtures := []statusSizeFixture{
		statusSizeFixtures[0],
		statusSizeFixtures[1],
		statusSizeFixtures[7],
		statusSizeFixtures[9],
	}
	for _, fixture := range fixtures {
		inline := newStatusSizeIR(fixture, 2000)
		derived := projectDerivedV1(inline)
		for _, representation := range []struct {
			name   string
			object *v1beta1.InferenceReplica
		}{
			{name: "InlineV1", object: inline},
			{name: "DerivedV1Projection", object: derived},
		} {
			body, err := marshalStatusUpdateBody(representation.object, false)
			if err != nil {
				b.Fatalf("marshal benchmark fixture: %v", err)
			}
			b.Run(fixture.name+"/"+representation.name+"/marshal", func(b *testing.B) {
				b.ReportAllocs()
				b.SetBytes(int64(len(body)))
				for range b.N {
					if _, err := marshalStatusUpdateBody(representation.object, false); err != nil {
						b.Fatal(err)
					}
				}
			})
			b.Run(fixture.name+"/"+representation.name+"/unmarshal", func(b *testing.B) {
				b.ReportAllocs()
				b.SetBytes(int64(len(body)))
				for range b.N {
					decoded := &v1beta1.InferenceReplica{}
					if err := json.Unmarshal(body, decoded); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

func collectStatusSizeMeasurements(t *testing.T) []statusSizeMeasurement {
	t.Helper()
	measurements := make([]statusSizeMeasurement, 0, len(statusSizeFixtures)*len(measurementInstanceCounts))
	for _, fixture := range statusSizeFixtures {
		for _, instances := range measurementInstanceCounts {
			measurements = append(measurements, measureStatusSize(t, fixture, instances))
		}
	}
	return measurements
}

func measureStatusSize(t *testing.T, fixture statusSizeFixture, instances int32) statusSizeMeasurement {
	t.Helper()
	inline := newStatusSizeIR(fixture, instances)
	derived := projectDerivedV1(inline)
	if len(inline.ManagedFields) != 0 || len(derived.ManagedFields) != 0 {
		t.Fatal("normalized fixtures must omit API-server-generated managedFields")
	}

	inlineRequest := mustMarshalStatusUpdateBody(t, inline, false)
	derivedRequest := mustMarshalStatusUpdateBody(t, derived, true)
	inlineStatus := mustMarshalStatus(t, &inline.Status, false)
	derivedStatus := mustMarshalStatus(t, &derived.Status, true)
	rows, err := json.Marshal(inline.Status.InstanceStatuses)
	if err != nil {
		t.Fatalf("marshal InstanceStatuses: %v", err)
	}
	withoutMarker := mustMarshalStatusUpdateBody(t, derived, false)
	readyBytes, scheduledBytes, nodesBytes, observationBytes := liveObservationRequestBytes(t, inline)

	measurement := statusSizeMeasurement{
		fixture:                   fixture.name,
		instances:                 instances,
		normalizedInlineBytes:     len(inlineRequest),
		normalizedDerivedBytes:    len(derivedRequest),
		inlineStatusBytes:         len(inlineStatus),
		derivedStatusBytes:        len(derivedStatus),
		instanceRowsBytes:         len(rows),
		readyPodCountBytes:        readyBytes,
		scheduledPodCountBytes:    scheduledBytes,
		nodesOccupiedBytes:        nodesBytes,
		liveObservationBytes:      observationBytes,
		representationMarkerBytes: len(derivedRequest) - len(withoutMarker),
		operationBytes:            marginalRequestBytes(t, inline, clearOperations),
		lastFailureBytes:          marginalRequestBytes(t, inline, clearLastFailures),
		migrationsBytes:           marginalRequestBytes(t, inline, clearMigrations),
		reductionBytes:            len(inlineRequest) - len(derivedRequest),
	}
	return measurement
}

func renderStatusSizeCSV(t *testing.T, measurements []statusSizeMeasurement) string {
	t.Helper()
	var out bytes.Buffer
	w := csv.NewWriter(&out)
	header := []string{
		"fixture",
		"instances",
		"normalized_inline_request_bytes",
		"normalized_derived_request_bytes",
		"inline_status_json_bytes",
		"derived_status_json_bytes",
		"instance_rows_total_bytes",
		"ready_pod_count_marginal_bytes",
		"scheduled_pod_count_marginal_bytes",
		"nodes_occupied_marginal_bytes",
		"live_observation_marginal_bytes",
		"representation_marker_bytes",
		"operation_marginal_bytes",
		"last_failure_marginal_bytes",
		"top_level_migrations_marginal_bytes",
		"normalized_reduction_bytes",
		"normalized_reduction_percent",
	}
	if err := w.Write(header); err != nil {
		t.Fatalf("write CSV header: %v", err)
	}
	for _, m := range measurements {
		percent := float64(0)
		if m.normalizedInlineBytes > 0 {
			percent = float64(m.reductionBytes) * 100 / float64(m.normalizedInlineBytes)
		}
		record := []string{
			m.fixture,
			strconv.FormatInt(int64(m.instances), 10),
			strconv.Itoa(m.normalizedInlineBytes),
			strconv.Itoa(m.normalizedDerivedBytes),
			strconv.Itoa(m.inlineStatusBytes),
			strconv.Itoa(m.derivedStatusBytes),
			strconv.Itoa(m.instanceRowsBytes),
			strconv.Itoa(m.readyPodCountBytes),
			strconv.Itoa(m.scheduledPodCountBytes),
			strconv.Itoa(m.nodesOccupiedBytes),
			strconv.Itoa(m.liveObservationBytes),
			strconv.Itoa(m.representationMarkerBytes),
			strconv.Itoa(m.operationBytes),
			strconv.Itoa(m.lastFailureBytes),
			strconv.Itoa(m.migrationsBytes),
			strconv.Itoa(m.reductionBytes),
			fmt.Sprintf("%.2f", percent),
		}
		if err := w.Write(record); err != nil {
			t.Fatalf("write CSV record: %v", err)
		}
	}
	w.Flush()
	if err := w.Error(); err != nil {
		t.Fatalf("flush CSV: %v", err)
	}
	return out.String()
}

func newStatusSizeIR(fixture statusSizeFixture, instances int32) *v1beta1.InferenceReplica {
	created := measurementTime()
	maxUnavailable := intstr.FromString("10%")
	replicas := instances
	ir := &v1beta1.InferenceReplica{
		TypeMeta: metav1.TypeMeta{
			APIVersion: v1beta1.SchemeGroupVersion.String(),
			Kind:       "InferenceReplica",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:              "example-engine",
			Namespace:         "default",
			UID:               types.UID("example-engine-uid"),
			ResourceVersion:   "123456",
			Generation:        7,
			CreationTimestamp: created,
			Labels: map[string]string{
				constants.InferenceServicePodLabelKey: "example",
				constants.OMEComponentLabel:           string(v1beta1.EngineComponent),
				query.LabelManagedBy:                  query.ManagedByOMENative,
			},
			Annotations: map[string]string{
				constants.InferenceReplicaControllerWriteAnnotationKey: constants.InferenceReplicaControllerWriteAnnotationVal,
			},
			Finalizers: []string{TeardownFinalizer},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: v1beta1.SchemeGroupVersion.String(),
				Kind:       "InferenceService",
				Name:       "example",
				UID:        types.UID("example-isvc-uid"),
				Controller: ptr.To(true),
			}},
		},
		Spec: v1beta1.InferenceReplicaSpec{
			ParentRef: v1beta1.ParentReference{Name: "example"},
			Component: v1beta1.EngineComponent,
			Replicas:  &replicas,
			Runners:   measurementRunners(fixture.podsPerInstance),
			Pacing: &v1beta1.InferenceReplicaPacing{
				Partition:      ptr.To(int32(0)),
				MaxUnavailable: &maxUnavailable,
			},
			MinReadySeconds: 10,
		},
	}
	if fixture.podsPerInstance > 1 {
		topologyKey := corev1.LabelTopologyZone
		ir.Spec.TopologyKey = &topologyKey
	}

	ir.Status = measurementStatus(fixture, instances)
	return ir
}

func measurementRunners(podsPerInstance int32) []v1beta1.Runner {
	template := func(role v1beta1.RunnerName) corev1.PodTemplateSpec {
		return corev1.PodTemplateSpec{
			ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
				constants.OMEComponentLabel: string(v1beta1.EngineComponent),
				"ome.io/runner":             string(role),
			}},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{{
					Name:  constants.MainContainerName,
					Image: "example.com/ome/model-server:v1",
					Ports: []corev1.ContainerPort{{Name: "http", ContainerPort: 8000}},
					Env:   []corev1.EnvVar{{Name: "MODEL_ID", Value: "example/model"}},
				}},
			},
		}
	}
	if podsPerInstance == 1 {
		return []v1beta1.Runner{{
			Name:     v1beta1.RunnerNameDefault,
			Size:     1,
			Template: template(v1beta1.RunnerNameDefault),
		}}
	}
	return []v1beta1.Runner{
		{Name: v1beta1.RunnerNameLeader, Size: 1, Template: template(v1beta1.RunnerNameLeader)},
		{Name: v1beta1.RunnerNameWorker, Size: podsPerInstance - 1, Template: template(v1beta1.RunnerNameWorker)},
	}
}

func measurementStatus(fixture statusSizeFixture, instances int32) v1beta1.InferenceReplicaStatus {
	updateRevision := measurementCurrentRevision
	if fixture.shape == shapeCreating || fixture.shape == shapeUpdatingInPlace || fixture.shape == shapeUpdatingSurge {
		updateRevision = measurementUpdateRevision
	}
	status := v1beta1.InferenceReplicaStatus{
		ObservedGeneration: 7,
		CurrentRevision:    measurementCurrentRevision,
		UpdateRevision:     updateRevision,
		CollisionCount:     ptr.To(int32(1)),
		LabelSelector: fmt.Sprintf("%s=example,%s=%s,%s=%s",
			constants.InferenceServicePodLabelKey,
			constants.OMEComponentLabel, v1beta1.EngineComponent,
			query.LabelManagedBy, query.ManagedByOMENative),
		InstanceStatuses: make([]v1beta1.OMENativeInstanceStatus, 0, instances),
	}
	for index := int32(0); index < instances; index++ {
		status.InstanceStatuses = append(status.InstanceStatuses, measurementInstanceStatus(fixture, index, instances))
	}
	if fixture.shape == shapeFailureRealistic || fixture.shape == shapeFailureLarge {
		firstFailure := measurementTime()
		lastFailure := metav1.NewTime(firstFailure.Add(5 * time.Minute))
		nextRetry := metav1.NewTime(lastFailure.Add(10 * time.Minute))
		status.RetryBlocks = []v1beta1.RetryBlock{{
			TargetRevision:  measurementCurrentRevision,
			State:           v1beta1.RetryBlockBackoff,
			AttemptsStarted: 2,
			NextRetryAt:     &nextRetry,
			FirstFailureAt:  &firstFailure,
			LastFailureAt:   &lastFailure,
			Reason:          "container-restart",
		}}
	}
	if fixture.shape == shapeMigrationHeavy {
		status.Migrations = measurementMigrations(instances)
	}
	measurementAggregateStatus(&status, fixture.podsPerInstance)
	return status
}

func measurementInstanceStatus(fixture statusSizeFixture, index, instances int32) v1beta1.OMENativeInstanceStatus {
	status := v1beta1.OMENativeInstanceStatus{
		Index:           index,
		Incarnation:     1,
		RunningRevision: measurementCurrentRevision,
	}
	switch fixture.shape {
	case shapeSteady:
		setMeasurementPodObservation(&status, fixture.podsPerInstance, fixture.podsPerInstance, fixture.podsPerInstance, fixture.podsPerInstance, fixture.podsPerInstance, true)
		status.Phase = v1beta1.OMENativeInstanceReady
	case shapeCreating:
		status.Phase = v1beta1.OMENativeInstanceCreating
		status.RunningRevision = ""
		status.TargetRevision = measurementUpdateRevision
		status.Operation = measurementOperation(index, v1beta1.InstanceOperationCreate, "CreatePods", measurementUpdateRevision, false)
	case shapeUpdatingInPlace:
		status.Phase = v1beta1.OMENativeInstanceUpdating
		status.TargetRevision = measurementUpdateRevision
		setMeasurementPodObservation(&status, fixture.podsPerInstance, fixture.podsPerInstance, fixture.podsPerInstance-1, fixture.podsPerInstance-1, fixture.podsPerInstance, true)
		status.Operation = measurementOperation(index, v1beta1.InstanceOperationUpdate, "WaitReady", measurementUpdateRevision, false)
	case shapeUpdatingSurge:
		status.Phase = v1beta1.OMENativeInstanceUpdating
		status.TargetRevision = measurementUpdateRevision
		setMeasurementPodObservation(&status, 2, 2, 1, 1, 2, true)
		status.ActiveOrdinal = index % 2
		status.Operation = measurementOperation(index, v1beta1.InstanceOperationUpdate, "DrainOld", measurementUpdateRevision, false)
	case shapeDeleting:
		status.Phase = v1beta1.OMENativeInstanceDeleting
		setMeasurementPodObservation(&status, fixture.podsPerInstance, fixture.podsPerInstance, 0, 0, fixture.podsPerInstance, true)
		status.Operation = measurementOperation(index, v1beta1.InstanceOperationDelete, "Drain", measurementCurrentRevision, false)
	case shapeKueueGated:
		status.Phase = v1beta1.OMENativeInstanceCreating
		status.PodCount = fixture.podsPerInstance
		status.TargetRevision = measurementCurrentRevision
		status.Operation = measurementOperation(index, v1beta1.InstanceOperationCreate, "WaitReady", measurementCurrentRevision, true)
	case shapeFailureRealistic, shapeFailureLarge:
		status.Phase = v1beta1.OMENativeInstanceRestarting
		setMeasurementPodObservation(&status, fixture.podsPerInstance-1, fixture.podsPerInstance-2, fixture.podsPerInstance-2, fixture.podsPerInstance-2, fixture.podsPerInstance-1, true)
		status.Operation = measurementOperation(index, v1beta1.InstanceOperationRestart, "DeletePods", measurementCurrentRevision, false)
		status.LastFailure = measurementFailure(index, fixture.shape == shapeFailureLarge)
	case shapeMigrationHeavy:
		status.Phase = v1beta1.OMENativeInstanceMigrating
		setMeasurementPodObservation(&status, fixture.podsPerInstance, fixture.podsPerInstance, fixture.podsPerInstance, fixture.podsPerInstance, fixture.podsPerInstance, true)
		operation := measurementOperation(index, v1beta1.InstanceOperationMigrate, "WaitReady", measurementCurrentRevision, false)
		operation.SurgeIndex = ptr.To(instances + index)
		operation.FromNode = measurementNodes(index, 1)[0]
		operation.HintTargetNodes = []string{"target-node-a", "target-node-b"}
		operation.RequestUUID = fmt.Sprintf("migration-%04d", index)
		status.Operation = operation
	default:
		panic(fmt.Sprintf("unsupported measurement shape %q", fixture.shape))
	}
	return status
}

func setMeasurementPodObservation(status *v1beta1.OMENativeInstanceStatus, podCount, ready, serving, available, scheduled int32, admitted bool) {
	status.PodCount = podCount
	status.ReadyPodCount = ready
	status.ServingPodCount = serving
	status.AvailablePodCount = available
	status.ScheduledPodCount = scheduled
	status.Admitted = admitted
	status.NodesOccupied = measurementNodes(status.Index, scheduled)
}

func measurementOperation(index int32, operationType v1beta1.InstanceOperationType, step, targetRevision string, parked bool) *v1beta1.InstanceOperation {
	started := measurementTime()
	progress := metav1.NewTime(started.Add(2 * time.Minute))
	deadline := metav1.NewTime(started.Add(30 * time.Minute))
	if parked {
		deadline = metav1.Time{}
	}
	return &v1beta1.InstanceOperation{
		ID:             fmt.Sprintf("%s-%04d", strings.ToLower(string(operationType)), index),
		Type:           operationType,
		Step:           step,
		StartedAt:      started,
		LastProgressAt: progress,
		Deadline:       deadline,
		TargetRevision: targetRevision,
		Reason:         "measurement-fixture",
	}
}

func measurementFailure(index int32, large bool) *v1beta1.InstanceTermination {
	exitCode := int32(137)
	message := "container exited after exceeding its memory limit"
	if large {
		message = strings.Repeat("x", measurementLargeMessageLen)
	}
	return &v1beta1.InstanceTermination{
		PodName:       fmt.Sprintf("example-engine-%04d-worker-6", index),
		ContainerName: constants.MainContainerName,
		Reason:        "OOMKilled",
		ExitCode:      &exitCode,
		Message:       message,
		Time:          measurementTime(),
	}
}

func measurementMigrations(instances int32) []v1beta1.MigrationStatus {
	started := measurementTime()
	deadline := metav1.NewTime(started.Add(time.Hour))
	phases := []v1beta1.MigrationPhase{
		v1beta1.MigrationPhaseAccepted,
		v1beta1.MigrationPhaseSurgePending,
		v1beta1.MigrationPhaseSurgeReady,
		v1beta1.MigrationPhaseDraining,
	}
	migrations := make([]v1beta1.MigrationStatus, 0, instances)
	for index := int32(0); index < instances; index++ {
		phase := phases[int(index)%len(phases)]
		migration := v1beta1.MigrationStatus{
			RequestUUID:     fmt.Sprintf("migration-%04d", index),
			Trigger:         v1beta1.MigrationTriggerManual,
			SourceInstance:  index,
			FromNode:        measurementNodes(index, 1)[0],
			HintTargetNodes: []string{"target-node-a", "target-node-b"},
			Phase:           phase,
			Attempt:         1,
			Reason:          "node-maintenance",
			Message:         "replacement is progressing",
			StartedAt:       started,
			Deadline:        deadline,
		}
		if phase != v1beta1.MigrationPhaseAccepted {
			migration.SurgeInstance = ptr.To(instances + index)
			allocated := metav1.NewTime(started.Add(time.Minute))
			migration.AllocatedAt = &allocated
		}
		migrations = append(migrations, migration)
	}
	return migrations
}

func measurementAggregateStatus(status *v1beta1.InferenceReplicaStatus, expectedPods int32) {
	status.Replicas = int32(len(status.InstanceStatuses))
	for _, instance := range status.InstanceStatuses {
		ready := instance.PodCount >= expectedPods && instance.ReadyPodCount >= expectedPods
		serving := instance.PodCount >= expectedPods && instance.ServingPodCount >= expectedPods
		available := instance.PodCount >= expectedPods && instance.AvailablePodCount >= expectedPods
		updated := instance.RunningRevision == status.UpdateRevision
		if ready {
			status.ReadyReplicas++
		}
		if serving {
			status.ServingReplicas++
		}
		if available {
			status.AvailableReplicas++
		}
		if updated {
			status.UpdatedReplicas++
			if ready {
				status.UpdatedReadyReplicas++
			}
		}
	}
	readyCondition := metav1.ConditionFalse
	reason := "InstancesNotReady"
	if status.ReadyReplicas == status.Replicas {
		readyCondition = metav1.ConditionTrue
		reason = "AllInstancesReady"
	}
	status.Conditions = []metav1.Condition{{
		Type:               "Ready",
		Status:             readyCondition,
		ObservedGeneration: status.ObservedGeneration,
		LastTransitionTime: measurementTime(),
		Reason:             reason,
		Message:            "measurement fixture aggregate",
	}}
}

func measurementNodes(index, count int32) []string {
	if count <= 0 {
		return nil
	}
	nodes := make([]string, 0, count)
	for ordinal := int32(0); ordinal < count; ordinal++ {
		nodes = append(nodes, fmt.Sprintf("worker-%04d-%02d.example", index, ordinal))
	}
	return nodes
}

func measurementTime() metav1.Time {
	return metav1.NewTime(time.Date(2026, time.August, 17, 12, 0, 0, 0, time.UTC))
}

func projectDerivedV1(inline *v1beta1.InferenceReplica) *v1beta1.InferenceReplica {
	derived := inline.DeepCopy()
	clearDerivedV1Fields(derived)
	return derived
}

func clearDerivedV1Fields(ir *v1beta1.InferenceReplica) {
	clearReadyPodCount(ir)
	clearScheduledPodCount(ir)
	clearNodesOccupied(ir)
}

func clearReadyPodCount(ir *v1beta1.InferenceReplica) {
	for index := range ir.Status.InstanceStatuses {
		ir.Status.InstanceStatuses[index].ReadyPodCount = 0
	}
}

func clearScheduledPodCount(ir *v1beta1.InferenceReplica) {
	for index := range ir.Status.InstanceStatuses {
		ir.Status.InstanceStatuses[index].ScheduledPodCount = 0
	}
}

func clearNodesOccupied(ir *v1beta1.InferenceReplica) {
	for index := range ir.Status.InstanceStatuses {
		ir.Status.InstanceStatuses[index].NodesOccupied = nil
	}
}

func clearOperations(ir *v1beta1.InferenceReplica) {
	for index := range ir.Status.InstanceStatuses {
		ir.Status.InstanceStatuses[index].Operation = nil
	}
}

func clearLastFailures(ir *v1beta1.InferenceReplica) {
	for index := range ir.Status.InstanceStatuses {
		ir.Status.InstanceStatuses[index].LastFailure = nil
	}
}

func clearMigrations(ir *v1beta1.InferenceReplica) {
	ir.Status.Migrations = nil
}

func marginalRequestBytes(t *testing.T, ir *v1beta1.InferenceReplica, clear func(*v1beta1.InferenceReplica)) int {
	t.Helper()
	before := len(mustMarshalStatusUpdateBody(t, ir, false))
	without := ir.DeepCopy()
	clear(without)
	after := len(mustMarshalStatusUpdateBody(t, without, false))
	if after > before {
		t.Fatalf("clearing a status field grew the request from %d to %d bytes", before, after)
	}
	return before - after
}

func liveObservationRequestBytes(t *testing.T, ir *v1beta1.InferenceReplica) (ready, scheduled, nodes, combined int) {
	t.Helper()
	projected := ir.DeepCopy()
	before := len(mustMarshalStatusUpdateBody(t, projected, false))
	initial := before

	clearReadyPodCount(projected)
	after := len(mustMarshalStatusUpdateBody(t, projected, false))
	ready = before - after
	before = after

	clearScheduledPodCount(projected)
	after = len(mustMarshalStatusUpdateBody(t, projected, false))
	scheduled = before - after
	before = after

	clearNodesOccupied(projected)
	after = len(mustMarshalStatusUpdateBody(t, projected, false))
	nodes = before - after
	combined = initial - after
	return ready, scheduled, nodes, combined
}

func mustMarshalStatusUpdateBody(t *testing.T, ir *v1beta1.InferenceReplica, derivedMarker bool) []byte {
	t.Helper()
	body, err := marshalStatusUpdateBody(ir, derivedMarker)
	if err != nil {
		t.Fatalf("marshal status update body: %v", err)
	}
	return body
}

func marshalStatusUpdateBody(ir *v1beta1.InferenceReplica, derivedMarker bool) ([]byte, error) {
	encoder, err := statusUpdateEncoder()
	if err != nil {
		return nil, err
	}
	body, err := runtime.Encode(encoder, ir)
	if err != nil || !derivedMarker {
		return body, err
	}
	newline := len(body) > 0 && body[len(body)-1] == '\n'
	var object map[string]json.RawMessage
	if err := json.Unmarshal(body, &object); err != nil {
		return nil, fmt.Errorf("decode request object: %w", err)
	}
	status, err := addDerivedMarker(object["status"])
	if err != nil {
		return nil, err
	}
	object["status"] = status
	body, err = json.Marshal(object)
	if err == nil && newline {
		body = append(body, '\n')
	}
	return body, err
}

func statusUpdateEncoder() (runtime.Encoder, error) {
	measurementEncoderOnce.Do(func() {
		scheme := runtime.NewScheme()
		if err := v1beta1.AddToScheme(scheme); err != nil {
			measurementEncoderErr = fmt.Errorf("register OME API: %w", err)
			return
		}
		codecs := serializer.NewCodecFactory(scheme)
		negotiator := runtime.NewClientNegotiator(
			serializer.WithoutConversionCodecFactory{CodecFactory: codecs},
			v1beta1.SchemeGroupVersion,
		)
		measurementEncoder, measurementEncoderErr = negotiator.Encoder(runtime.ContentTypeJSON, nil)
	})
	return measurementEncoder, measurementEncoderErr
}

func mustMarshalStatus(t *testing.T, status *v1beta1.InferenceReplicaStatus, derivedMarker bool) []byte {
	t.Helper()
	body, err := json.Marshal(status)
	if err != nil {
		t.Fatalf("marshal status: %v", err)
	}
	if !derivedMarker {
		return body
	}
	body, err = addDerivedMarker(body)
	if err != nil {
		t.Fatalf("add DerivedV1 marker: %v", err)
	}
	return body
}

func addDerivedMarker(statusBody []byte) ([]byte, error) {
	status := make(map[string]json.RawMessage)
	if len(statusBody) > 0 {
		if err := json.Unmarshal(statusBody, &status); err != nil {
			return nil, fmt.Errorf("decode status: %w", err)
		}
	}
	marker, err := json.Marshal(measurementDerivedMarker)
	if err != nil {
		return nil, fmt.Errorf("encode representation marker: %w", err)
	}
	status["instanceObservationStorage"] = marker
	return json.Marshal(status)
}
