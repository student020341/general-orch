package events

import (
	"fmt"
	"time"

	"orch/internal/model"
)

const (
	StreamName     = "ORCH"
	StreamSubjects = "orch.>"

	SubjectIntentPrefix        = "orch.intent.requested"
	SubjectPlanPrefix          = "orch.plan.created"
	SubjectStepCompletedPrefix = "orch.execution.step_completed"
	SubjectFinishedPrefix      = "orch.execution.finished"
	SubjectCatalogReloaded     = "orch.catalog.reloaded"
)

// IntentSubject formats the NATS subject for an incoming entity intent patch.
func IntentSubject(kind, vendor, entityID string) string {
	return fmt.Sprintf("%s.%s.%s.%s", SubjectIntentPrefix, kind, vendor, entityID)
}

// PlanCreatedSubject formats the NATS subject for a newly persisted execution plan.
func PlanCreatedSubject(entityID string) string {
	return fmt.Sprintf("%s.%s", SubjectPlanPrefix, entityID)
}

// StepCompletedSubject formats the NATS subject for a completed/failed DAG step.
func StepCompletedSubject(entityID string) string {
	return fmt.Sprintf("%s.%s", SubjectStepCompletedPrefix, entityID)
}

// ExecutionFinishedSubject formats the NATS subject when a plan run finishes.
func ExecutionFinishedSubject(entityID string) string {
	return fmt.Sprintf("%s.%s", SubjectFinishedPrefix, entityID)
}

// IntentRequestedEvent notifies the state manager of a desired-state patch.
type IntentRequestedEvent struct {
	EntityID    string      `json:"entity_id"`
	Kind        string      `json:"kind"`
	Vendor      string      `json:"vendor"`
	TaskQueue   string      `json:"task_queue,omitempty"`
	Patch       model.State `json:"patch"`
	Replace     bool        `json:"replace,omitempty"`
	RequestedAt time.Time   `json:"requested_at"`
}

// PlanCreatedEvent notifies the Temporal workflow runner that a plan is ready to execute.
type PlanCreatedEvent struct {
	PlanID    string    `json:"plan_id"`
	EntityID  string    `json:"entity_id"`
	Version   int64     `json:"version"`
	CreatedAt time.Time `json:"created_at"`
}

// StepCompletedEvent reports step execution status and any observed device state changes.
type StepCompletedEvent struct {
	PlanID        string      `json:"plan_id"`
	EntityID      string      `json:"entity_id"`
	Version       int64       `json:"version"`
	StepID        string      `json:"step_id"`
	Capability    string      `json:"capability"`
	ObservedDelta model.State `json:"observed_delta,omitempty"`
	FullState     model.State `json:"full_state,omitempty"`
	Error         string      `json:"error,omitempty"`
	CompletedAt   time.Time   `json:"completed_at"`
}

// ExecutionOutcome summarizes why a plan execution terminated.
type ExecutionOutcome string

const (
	OutcomeCompleted   ExecutionOutcome = "completed"
	OutcomeFailed      ExecutionOutcome = "failed"
	OutcomeDriftReplan ExecutionOutcome = "drift_replan"
	OutcomeSuperseded  ExecutionOutcome = "superseded"
)

// ExecutionFinishedEvent notifies the state manager that a workflow run ended.
type ExecutionFinishedEvent struct {
	PlanID     string           `json:"plan_id"`
	EntityID   string           `json:"entity_id"`
	Version    int64            `json:"version"`
	Outcome    ExecutionOutcome `json:"outcome"`
	Error      string           `json:"error,omitempty"`
	FinishedAt time.Time        `json:"finished_at"`
}

// CatalogReloadedEvent broadcasts that device profiles changed in the store.
type CatalogReloadedEvent struct {
	Source     string    `json:"source"`
	ReloadedAt time.Time `json:"reloaded_at"`
}
