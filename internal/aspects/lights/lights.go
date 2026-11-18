package lights

import (
	"fmt"
	"math"
	"reflect"
	"slices"
	"strings"

	"orch/internal/model"
	"orch/internal/reconciler"
)

// Aspect diffs any RGB light keys (e.g. "light", "light1", "light2") present in target state.
type Aspect struct{}

// New returns a lights AspectPlanner.
func New() Aspect { return Aspect{} }

func (Aspect) Name() string { return "lights" }

func (Aspect) Plan(entity *model.Entity, taskQueue string) (reconciler.AspectResult, error) {
	keys := lightKeys(entity.Target)
	var steps []model.PlanStep
	for _, key := range keys {
		rawTarget := entity.Target[key]
		targetColor, err := parseColor(rawTarget)
		if err != nil {
			return reconciler.AspectResult{}, fmt.Errorf("%s: %w", key, err)
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
	return reconciler.AspectResult{Steps: steps}, nil
}

func lightKeys(target model.State) []string {
	var keys []string
	for k, v := range target {
		if v == nil {
			continue
		}
		if k == "light" || (strings.HasPrefix(k, "light") && k != "lights") {
			keys = append(keys, k)
		}
	}
	slices.Sort(keys)
	return keys
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
