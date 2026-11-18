package light

import (
	"fmt"
	"math"
	"reflect"

	"orch/internal/model"
	"orch/internal/reconciler"
)

// Aspect diffs one named RGB light resource slot between current and target state.
type Aspect struct{}

// New returns a single-resource light AspectPlanner.
func New() Aspect { return Aspect{} }

func (Aspect) Name() string { return "light" }

func (Aspect) Schema() model.AspectSchema {
	return model.AspectSchema{
		Type:        "light",
		Description: "RGB light color (0-255 per channel)",
		Example:     model.State{"r": 255, "g": 120, "b": 0},
	}
}

func (Aspect) Plan(entity *model.Entity, resource string, taskQueue string) (reconciler.AspectResult, error) {
	rawTarget, hasTarget := entity.Target[resource]
	if !hasTarget || rawTarget == nil {
		return reconciler.AspectResult{}, nil
	}
	targetColor, err := parseColor(rawTarget)
	if err != nil {
		return reconciler.AspectResult{}, fmt.Errorf("%s: %w", resource, err)
	}
	if rawCurr, hasCurr := entity.Current[resource]; hasCurr {
		if currColor, err := parseColor(rawCurr); err == nil && reflect.DeepEqual(currColor, targetColor) {
			return reconciler.AspectResult{}, nil
		}
	}

	step := model.PlanStep{
		ID:         "set-" + resource,
		TaskQueue:  taskQueue,
		Activity:   "ExecuteCapability",
		Capability: "set_light",
		Vendor:     entity.Vendor,
		Payload: model.State{
			"entity_id": entity.ID,
			"resource":  resource,
			"color":     targetColor,
		},
		Status: model.StepPending,
	}
	return reconciler.AspectResult{Steps: []model.PlanStep{step}}, nil
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
