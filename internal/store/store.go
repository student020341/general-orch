package store

import (
	"context"
	"errors"

	"orch/internal/model"
)

var (
	ErrNotFound        = errors.New("not found")
	ErrVersionConflict = errors.New("version conflict")
)

// IntentPatchInput holds the validated fields needed to upsert an entity's target intent.
type IntentPatchInput struct {
	EntityID  string
	Kind      string
	Vendor    string
	TaskQueue string
	Patch     model.State
}

// EntityStore persists and updates entity current/target configuration state.
type EntityStore interface {
	GetEntity(ctx context.Context, id string) (*model.Entity, error)
	ListEntities(ctx context.Context) ([]*model.Entity, error)
	ApplyIntentPatch(ctx context.Context, in IntentPatchInput) (*model.Entity, error)
	UpdateEntity(ctx context.Context, id string, fn func(*model.Entity) error) (*model.Entity, error)
}

// PlanStore persists execution DAG plans and per-step progress.
type PlanStore interface {
	CreatePlan(ctx context.Context, plan *model.Plan) error
	GetPlan(ctx context.Context, id string) (*model.Plan, error)
	ListPlansByEntity(ctx context.Context, entityID string) ([]*model.Plan, error)
	UpdatePlanStatus(ctx context.Context, planID string, status model.PlanStatus) error
	UpdatePlanStep(ctx context.Context, planID, stepID string, status model.StepStatus, errMsg string) error
}

// Store combines entity and plan persistence.
type Store interface {
	EntityStore
	PlanStore
	Close()
}
