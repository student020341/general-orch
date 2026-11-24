package lightspinner

import (
	"encoding/json"
	"fmt"

	"orch/sdk"
)

const (
	ActionSetLight       = "set_light"
	ActionRotatePlatform = "rotate_platform"
)

// SetLightParams holds the resolved target state for a single light step.
type SetLightParams struct {
	Light string     `json:"light"`
	State LightState `json:"state"`
}

// RotatePlatformParams holds the target angle for a platform rotation step.
type RotatePlatformParams struct {
	Angle int `json:"angle"`
}

func buildPlan(current State, target TargetConfig) (sdk.Plan, error) {
	var groups []sdk.StepGroup

	var lightSteps []sdk.Step
	for _, spec := range []struct {
		name string
		cur  LightState
		tgt  *LightPatch
	}{
		{name: "light1", cur: current.Light1, tgt: target.Light1},
		{name: "light2", cur: current.Light2, tgt: target.Light2},
	} {
		if spec.tgt == nil {
			continue
		}
		next, changed := mergeLight(spec.cur, spec.tgt)
		if !changed {
			continue
		}
		params, err := json.Marshal(SetLightParams{Light: spec.name, State: next})
		if err != nil {
			return sdk.Plan{}, err
		}
		lightSteps = append(lightSteps, sdk.Step{
			ID:          "set-" + spec.name,
			Action:      ActionSetLight,
			Description: fmt.Sprintf("set %s to on=%v rgb=%v brightness=%d", spec.name, next.On, next.RGB, next.Brightness),
			Params:      params,
		})
	}

	if len(lightSteps) > 0 {
		// Lights can be configured together in parallel, before rotating the platform.
		groups = append(groups, sdk.StepGroup{
			Name:     "lights",
			Parallel: true,
			Steps:    lightSteps,
		})
	}

	if target.Platform != nil && target.Platform.Angle != nil && *target.Platform.Angle != current.Platform.Angle {
		angle := *target.Platform.Angle
		params, err := json.Marshal(RotatePlatformParams{Angle: angle})
		if err != nil {
			return sdk.Plan{}, err
		}
		groups = append(groups, sdk.StepGroup{
			Name:     "platform",
			Parallel: false,
			Steps: []sdk.Step{
				{
					ID:          "rotate-platform",
					Action:      ActionRotatePlatform,
					Description: fmt.Sprintf("rotate platform from %d to %d degrees", current.Platform.Angle, angle),
					Params:      params,
				},
			},
		})
	}

	return sdk.Plan{
		Driver: driverInfo,
		Groups: groups,
	}, nil
}

func mergeLight(cur LightState, patch *LightPatch) (LightState, bool) {
	next := cur
	if patch.On != nil {
		next.On = *patch.On
	}
	if patch.RGB != nil {
		next.RGB = *patch.RGB
	}
	if patch.Brightness != nil {
		next.Brightness = *patch.Brightness
	}
	return next, next != cur
}
