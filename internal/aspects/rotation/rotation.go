package rotation

import (
	"fmt"
	"math"

	"orch/internal/model"
	"orch/internal/reconciler"
)

// Aspect diffs one named rotation resource slot (degrees) and emits relative turn(delta) steps.
type Aspect struct{}

// New returns a rotation AspectPlanner.
func New() Aspect { return Aspect{} }

func (Aspect) Name() string { return "rotation" }

func (Aspect) Schema() model.AspectSchema {
	return model.AspectSchema{
		Type:        "rotation",
		Description: "Target angle in degrees (0-359), executed via shortest relative turn",
		Example:     90,
	}
}

func (Aspect) Plan(entity *model.Entity, resource string, taskQueue string) (reconciler.AspectResult, error) {
	rawTarget, hasTarget := entity.Target[resource]
	if !hasTarget || rawTarget == nil {
		return reconciler.AspectResult{}, nil
	}
	targetAngle, err := parseAngle(rawTarget)
	if err != nil {
		return reconciler.AspectResult{}, fmt.Errorf("%s: %w", resource, err)
	}

	var currentAngle float64
	if rawCurr, hasCurr := entity.Current[resource]; hasCurr {
		if parsed, err := parseAngle(rawCurr); err == nil {
			currentAngle = parsed
		}
	}

	delta := ShortestTurnDelta(currentAngle, targetAngle)
	if math.Abs(delta) < 1e-6 {
		return reconciler.AspectResult{}, nil
	}

	stepID := "turn"
	if resource != "rotation" {
		stepID = "turn-" + resource
	}

	step := model.PlanStep{
		ID:         stepID,
		TaskQueue:  taskQueue,
		Activity:   "ExecuteCapability",
		Capability: "turn",
		Vendor:     entity.Vendor,
		Payload: model.State{
			"entity_id":     entity.ID,
			"resource":      resource,
			"delta":         delta,
			"expected_from": currentAngle,
			"expected_to":   targetAngle,
		},
		Status: model.StepPending,
	}
	return reconciler.AspectResult{
		Steps:     []model.PlanStep{step},
		DriftKeys: []string{resource},
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
