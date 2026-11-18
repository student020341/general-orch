package reconciler

import (
	"fmt"
	"slices"
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

// AspectPlanner diffs a single named resource slot (e.g. "light1", "eth1", "rotation")
// between current and target state.
type AspectPlanner interface {
	Name() string
	Schema() model.AspectSchema
	Plan(entity *model.Entity, resource string, taskQueue string) (AspectResult, error)
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

// AspectSchemas returns sorted contract schemas for all registered aspect types.
func (r *Reconciler) AspectSchemas() []model.AspectSchema {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]model.AspectSchema, 0, len(r.aspects))
	for _, ap := range r.aspects {
		s := ap.Schema()
		if s.Type == "" {
			s.Type = ap.Name()
		}
		out = append(out, s)
	}
	slices.SortFunc(out, func(a, b model.AspectSchema) int {
		if a.Type < b.Type {
			return -1
		}
		if a.Type > b.Type {
			return 1
		}
		return 0
	})
	return out
}

func (r *Reconciler) getAspect(name string) (AspectPlanner, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	ap, ok := r.aspects[name]
	return ap, ok
}

// Reconcile validates entity.Target against the profile's declared resources,
// diffs each slot, and emits a DAG Plan. Returns (nil, nil) when already converged.
func (r *Reconciler) Reconcile(entity *model.Entity) (*model.Plan, error) {
	profile, err := r.catalog.Lookup(entity.Kind)
	if err != nil {
		return nil, err
	}

	// Validate that every key in target corresponds to a declared resource slot on this kind.
	var unknownKeys []string
	for key := range entity.Target {
		if _, ok := profile.Resources[key]; !ok {
			unknownKeys = append(unknownKeys, key)
		}
	}
	if len(unknownKeys) > 0 {
		slices.Sort(unknownKeys)
		return nil, fmt.Errorf("unknown resource(s) %v for kind %q", unknownKeys, entity.Kind)
	}

	resourceNames := make([]string, 0, len(profile.Resources))
	for name := range profile.Resources {
		resourceNames = append(resourceNames, name)
	}
	slices.Sort(resourceNames)

	var actionSteps []model.PlanStep
	var driftKeys []string

	for _, resName := range resourceNames {
		aspectType := profile.Resources[resName]
		ap, ok := r.getAspect(aspectType)
		if !ok {
			return nil, fmt.Errorf("missing aspect planner %q for resource %q (kind %q)", aspectType, resName, entity.Kind)
		}
		queue := resolveTaskQueue(entity, profile, resName, aspectType)
		res, err := ap.Plan(entity, resName, queue)
		if err != nil {
			return nil, fmt.Errorf("plan resource %q (%s): %w", resName, aspectType, err)
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
