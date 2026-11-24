package lightspinner

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
)

// Resource identifies a specific light-spinner device.
type Resource struct {
	DeviceID string `json:"device_id"`
}

// LightState represents the full concrete state of a single RGB light.
type LightState struct {
	On         bool     `json:"on"`
	RGB        [3]uint8 `json:"rgb"`
	Brightness uint8    `json:"brightness"`
}

// PlatformState represents the full concrete state of the rotating platform.
type PlatformState struct {
	Angle int `json:"angle"`
}

// State represents the complete hardware state of a light-spinner device.
type State struct {
	Light1   LightState    `json:"light1"`
	Light2   LightState    `json:"light2"`
	Platform PlatformState `json:"platform"`
}

// LightPatch specifies optional fields to update on a single RGB light.
// Pointer fields distinguish omitted fields from intentional zero values (e.g. on=false).
type LightPatch struct {
	On         *bool     `json:"on,omitempty"`
	RGB        *[3]uint8 `json:"rgb,omitempty"`
	Brightness *uint8    `json:"brightness,omitempty"`
}

// PlatformPatch specifies optional fields to update on the rotating platform.
type PlatformPatch struct {
	Angle *int `json:"angle,omitempty"`
}

// TargetConfig is a partial configuration describing the target state to converge toward.
type TargetConfig struct {
	Light1   *LightPatch    `json:"light1,omitempty"`
	Light2   *LightPatch    `json:"light2,omitempty"`
	Platform *PlatformPatch `json:"platform,omitempty"`
}

const schemaJSON = `{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "title": "diy/light-spinner",
  "type": "object",
  "additionalProperties": false,
  "properties": {
    "light1": { "$ref": "#/$defs/light" },
    "light2": { "$ref": "#/$defs/light" },
    "platform": {
      "type": "object",
      "additionalProperties": false,
      "properties": {
        "angle": { "type": "integer", "minimum": 0, "maximum": 359 }
      }
    }
  },
  "$defs": {
    "light": {
      "type": "object",
      "additionalProperties": false,
      "properties": {
        "on": { "type": "boolean" },
        "rgb": {
          "type": "array",
          "items": { "type": "integer", "minimum": 0, "maximum": 255 },
          "minItems": 3,
          "maxItems": 3
        },
        "brightness": { "type": "integer", "minimum": 0, "maximum": 100 }
      }
    }
  }
}`

func decodeStrict[T any](raw json.RawMessage) (T, error) {
	var out T
	if len(bytes.TrimSpace(raw)) == 0 {
		return out, errors.New("empty JSON payload")
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&out); err != nil {
		return out, err
	}
	return out, nil
}

func validateResourcePayload(raw json.RawMessage) (Resource, error) {
	r, err := decodeStrict[Resource](raw)
	if err != nil {
		return Resource{}, err
	}
	if r.DeviceID == "" {
		return Resource{}, errors.New("resource.device_id is required")
	}
	return r, nil
}

func validateTargetPayload(raw json.RawMessage) (TargetConfig, error) {
	cfg, err := decodeStrict[TargetConfig](raw)
	if err != nil {
		return TargetConfig{}, err
	}
	for name, light := range map[string]*LightPatch{"light1": cfg.Light1, "light2": cfg.Light2} {
		if light != nil && light.Brightness != nil && *light.Brightness > 100 {
			return TargetConfig{}, fmt.Errorf("%s.brightness must be between 0 and 100 (got %d)", name, *light.Brightness)
		}
	}
	if cfg.Platform != nil && cfg.Platform.Angle != nil {
		angle := *cfg.Platform.Angle
		if angle < 0 || angle > 359 {
			return TargetConfig{}, fmt.Errorf("platform.angle must be between 0 and 359 (got %d)", angle)
		}
	}
	return cfg, nil
}
