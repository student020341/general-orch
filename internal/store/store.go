package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"

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
	Replace   bool
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

// CatalogStore persists device profiles for dynamic catalog updates and reloads.
type CatalogStore interface {
	ListProfiles(ctx context.Context) ([]model.DeviceProfile, error)
	GetProfile(ctx context.Context, kind string) (*model.DeviceProfile, error)
	UpsertProfile(ctx context.Context, profile model.DeviceProfile) (*model.DeviceProfile, error)
	DeleteProfile(ctx context.Context, kind string) error
}

// Store combines entity, plan, and profile catalog persistence.
type Store interface {
	EntityStore
	PlanStore
	CatalogStore
	Close()
}

// LoadProfilesFile parses a JSON array of DeviceProfile entries from disk.
func LoadProfilesFile(path string) ([]model.DeviceProfile, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read profiles file %s: %w", path, err)
	}
	var profiles []model.DeviceProfile
	if err := json.Unmarshal(raw, &profiles); err != nil {
		return nil, fmt.Errorf("parse profiles file %s: %w", path, err)
	}
	return profiles, nil
}

// SyncProfilesFromFile loads profiles from path and upserts them into cs.
func SyncProfilesFromFile(ctx context.Context, cs CatalogStore, path string) (int, error) {
	profiles, err := LoadProfilesFile(path)
	if err != nil {
		return 0, err
	}
	for _, p := range profiles {
		if p.Kind == "" {
			return 0, fmt.Errorf("profile in %s missing kind", path)
		}
		if _, err := cs.UpsertProfile(ctx, p); err != nil {
			return 0, fmt.Errorf("upsert profile %q: %w", p.Kind, err)
		}
	}
	return len(profiles), nil
}
