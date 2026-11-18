package reconciler

import (
	"fmt"
	"math"
	"reflect"
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

// Reconciler diffs an entity's current and target state and emits a DAG Plan.
type Reconciler struct {
	catalog *ProfileCatalog
	aspects map[string]AspectPlanner
}

// New creates a Reconciler with the given device profile catalog and default aspects.
func New(catalog *ProfileCatalog) *Reconciler {
	if catalog == nil {
		catalog = DefaultCatalog()
	}
	r := &Reconciler{
		catalog: catalog,
		aspects: make(map[string]AspectPlanner),
	}
	r.RegisterAspect(lightsAspect{})
	r.RegisterAspect(rotationAspect{})
	return r
}

// RegisterAspect registers or overrides an AspectPlanner by name.
func (r *Reconciler) RegisterAspect(ap AspectPlanner) {
	r.aspects[ap.Name()] = ap
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
		ap, ok := r.aspects[aspectName]
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

type lightsAspect struct{}

func (lightsAspect) Name() string { return "lights" }

func (lightsAspect) Plan(entity *model.Entity, taskQueue string) (AspectResult, error) {
	var steps []model.PlanStep
	for _, key := range []string{"light1", "light2"} {
		rawTarget, hasTarget := entity.Target[key]
		if !hasTarget {
			continue
		}
		targetColor, err := parseColor(rawTarget)
		if err != nil {
			return AspectResult{}, fmt.Errorf("%s: %w", key, err)
		}
		if rawCurr, hasCurr := entity.Current[key]; hasCurr {
			if currColor, err := parseColor(rawCurr); err == nil && reflect.DeepEqual(currColor, targetColor) {
				continue
			}
		}
		steps = append(steps, model.PlanStep{
			ID:         "set-" + key,
			TaskQueue:  taskQueue,
			Activity:   "ExecuteCapability",
			Capability: "set_light",
			Vendor:     entity.Vendor,
			Payload: model.State{
				"entity_id": entity.ID,
				"light":     key,
				"color":     targetColor,
			},
			Status: model.StepPending,
		})
	}
	return AspectResult{Steps: steps}, nil
}

type rotationAspect struct{}

func (rotationAspect) Name() string { return "rotation" }

func (rotationAspect) Plan(entity *model.Entity, taskQueue string) (AspectResult, error) {
	rawTarget, hasTarget := entity.Target["rotation"]
	if !hasTarget {
		return AspectResult{}, nil
	}
	targetAngle, err := parseAngle(rawTarget)
	if err != nil {
		return AspectResult{}, fmt.Errorf("rotation: %w", err)
	}

	var currentAngle float64
	if rawCurr, hasCurr := entity.Current["rotation"]; hasCurr {
		if parsed, err := parseAngle(rawCurr); err == nil {
			currentAngle = parsed
		}
	}

	delta := ShortestTurnDelta(currentAngle, targetAngle)
	if math.Abs(delta) < 1e-6 {
		return AspectResult{}, nil
	}

	step := model.PlanStep{
		ID:         "turn",
		TaskQueue:  taskQueue,
		Activity:   "ExecuteCapability",
		Capability: "turn",
		Vendor:     entity.Vendor,
		Payload: model.State{
			"entity_id":     entity.ID,
			"delta":         delta,
			"expected_from": currentAngle,
			"expected_to":   targetAngle,
		},
		Status: model.StepPending,
	}
	return AspectResult{
		Steps:     []model.PlanStep{step},
		DriftKeys: []string{"rotation"},
	}, nil
}

// ShortestTurnDelta returns the signed rotation in (-180, 180] needed to turn
// from currentDeg to targetDeg.
func ShortestTurnDelta(currentDeg, targetDeg float64) float64 {
	curr := NormalizeAngle(currentDeg)
	tgt := NormalizeAngle(targetDeg)
	delta := math.Mod(tgt-curr, 360)
	if delta > 180 {
		delta -= 360
	} else if delta <= -180 {
		delta += 360
	}
	if math.Abs(delta) < 1e-9 {
		return 0
	}
	return delta
}

// NormalizeAngle normalizes any degree value into [0, 360).
func NormalizeAngle(deg float64) float64 {
	m := math.Mod(deg, 360)
	if m < 0 {
		m += 360
	}
	if m == 360 {
		return 0
	}
	return m
}

func parseColor(v any) (model.State, error) {
	m, ok := v.(map[string]any)
	if !ok {
		if sm, ok2 := v.(model.State); ok2 {
			m = map[string]any(sm)
		} else {
			return nil, fmt.Errorf("expected RGB object, got %T", v)
		}
	}
	out := model.State{}
	for _, ch := range []string{"r", "g", "b"} {
		val, exists := m[ch]
		if !exists {
			return nil, fmt.Errorf("missing channel %q", ch)
		}
		num, err := toFloat(val)
		if err != nil {
			return nil, fmt.Errorf("channel %q: %w", ch, err)
		}
		if num < 0 || num > 255 || math.Trunc(num) != num {
			return nil, fmt.Errorf("channel %q must be integer 0..255, got %v", ch, num)
		}
		out[ch] = float64(int(num))
	}
	return out, nil
}

func parseAngle(v any) (float64, error) {
	num, err := toFloat(v)
	if err != nil {
		return 0, err
	}
	if math.IsNaN(num) || math.IsInf(num, 0) {
		return 0, fmt.Errorf("invalid angle %v", num)
	}
	return NormalizeAngle(num), nil
}

func toFloat(v any) (float64, error) {
	switch n := v.(type) {
	case int:
		return float64(n), nil
	case int64:
		return float64(n), nil
	case float64:
		return n, nil
	case float32:
		return float64(n), nil
	default:
		return 0, fmt.Errorf("expected number, got %T", v)
	}
}
