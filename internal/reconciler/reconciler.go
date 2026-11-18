package reconciler

import (
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"

	"orch/internal/model"
)

// AspectResult holds the steps and state-sensitive keys emitted by one aspect differ.
type AspectResult struct {
	Steps     []model.PlanStep
	DriftKeys []string
}

// AspectPlanner diffs a single capability slice between current and target state.
type AspectPlanner interface {
	Name() string
	Plan(entity *model.Entity, taskQueue string) (AspectResult, error)
}

// Reconciler diffs an entity's current and target state and emits a DAG Plan
// using registered AspectPlanners and a hot-reloadable ProfileCatalog.
type Reconciler struct {
	catalog *ProfileCatalog
	mu      sync.RWMutex
	aspects map[string]AspectPlanner
}

// New creates a generic Reconciler with no built-in device-specific aspects.
// Register aspect planners via RegisterAspect or aspects.RegisterAll.
func New(catalog *ProfileCatalog) *Reconciler {
	if catalog == nil {
		catalog = DefaultCatalog()
	}
	return &Reconciler{
		catalog: catalog,
		aspects: make(map[string]AspectPlanner),
	}
}

// Catalog returns the ProfileCatalog used by this Reconciler.
func (r *Reconciler) Catalog() *ProfileCatalog {
	return r.catalog
}

// RegisterAspect registers or overrides an AspectPlanner by name.
func (r *Reconciler) RegisterAspect(ap AspectPlanner) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.aspects[ap.Name()] = ap
}

func (r *Reconciler) getAspect(name string) (AspectPlanner, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	ap, ok := r.aspects[name]
	return ap, ok
}

// Reconcile produces a DAG Plan to converge entity.Current toward entity.Target.
// Returns (nil, nil) when no changes are required.
func (r *Reconciler) Reconcile(entity *model.Entity) (*model.Plan, error) {
	profile, err := r.catalog.Lookup(entity.Kind)
	if err != nil {
		return nil, err
	}

	var actionSteps []model.PlanStep
	var driftKeys []string

	for _, aspectName := range profile.Aspects {
		ap, ok := r.getAspect(aspectName)
		if !ok {
			return nil, fmt.Errorf("missing aspect planner %q for kind %q", aspectName, entity.Kind)
		}
		queue := resolveTaskQueue(entity, profile, aspectName)
		res, err := ap.Plan(entity, queue)
		if err != nil {
			return nil, fmt.Errorf("plan aspect %q: %w", aspectName, err)
		}
		actionSteps = append(actionSteps, res.Steps...)
		driftKeys = append(driftKeys, res.DriftKeys...)
	}

	if len(actionSteps) == 0 {
		return nil, nil
	}

	var allSteps []model.PlanStep
	if profile.SupportsLiveState {
		fetchQueue := entity.TaskQueue
		if fetchQueue == "" {
			fetchQueue = profile.FetchStateQueue
		}
		fetchStep := model.PlanStep{
			ID:         "fetch-state",
			TaskQueue:  fetchQueue,
			Activity:   "FetchState",
			Capability: "fetch_state",
			Vendor:     entity.Vendor,
			Payload: model.State{
				"entity_id":        entity.ID,
				"expected_current": model.CloneState(entity.Current),
				"drift_keys":       driftKeys,
			},
			Status: model.StepPending,
		}
		allSteps = append(allSteps, fetchStep)
		for i := range actionSteps {
			actionSteps[i].DependsOn = append(actionSteps[i].DependsOn, fetchStep.ID)
		}
	}

	// Hardware constraint pass: serialize mutation steps if the device profile requires it.
	if profile.SerialExecution {
		for i := 1; i < len(actionSteps); i++ {
			actionSteps[i].DependsOn = append(actionSteps[i].DependsOn, actionSteps[i-1].ID)
		}
	}

	allSteps = append(allSteps, actionSteps...)
	now := time.Now().UTC()
	return &model.Plan{
		ID:        uuid.NewString(),
		EntityID:  entity.ID,
		Version:   entity.Version,
		Status:    model.PlanPending,
		Steps:     allSteps,
		CreatedAt: now,
		UpdatedAt: now,
	}, nil
}
