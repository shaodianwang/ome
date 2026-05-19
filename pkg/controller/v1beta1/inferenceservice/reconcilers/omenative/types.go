package omenative

// ComponentPlan is the desired Component → Instance → Runner → Pod shape
// computed each reconcile from the InferenceService spec, ServingRuntime,
// and BaseModel. Not persisted; recomputed every iteration.
type ComponentPlan struct{}

// InstancePlan is the desired state for one Instance — the atomic unit of
// gang scheduling, restart, and migration.
type InstancePlan struct{}

// RunnerPlan is the desired state for one Runner within an Instance
// (leader / worker / default).
type RunnerPlan struct{}

// OperationStep is the fine-grained resume point stored in
// InstanceStatus.Operation.Step (Drain, DeletePods, WaitZero, Recreate,
// WaitReady, ...). The value space is per-operation and filled in by the
// operation that introduces each step.
type OperationStep string
