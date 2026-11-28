package wired

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"orch/sdk"
)

var splitterDriverInfo = sdk.DriverInfo{Provider: "wired", Kind: "splitter"}

// SplitterResource scopes configuration to one slot on an intermediate splitter.
type SplitterResource struct {
	SplitterID string `json:"splitter_id"`
	Slot       int    `json:"slot"`
}

// SplitterTarget describes desired slot configuration.
type SplitterTarget struct {
	Active      *bool   `json:"active,omitempty"`
	SpeedMbps   *int    `json:"speed_mbps,omitempty"`
	ServiceVLAN *int    `json:"service_vlan,omitempty"`
	ConfigTier  *string `json:"config_tier,omitempty"`
}

// SplitterDriver configures slots on multi-unit splitters ("wired/splitter").
type SplitterDriver struct {
	hw *MockHardware
}

func NewSplitterDriver(hw *MockHardware) *SplitterDriver { return &SplitterDriver{hw: hw} }

func (d *SplitterDriver) Info() sdk.DriverInfo { return splitterDriverInfo }
func (d *SplitterDriver) Schema() []byte {
	return []byte(`{"type":"object","properties":{"active":{"type":"boolean"},"speed_mbps":{"type":"integer"},"service_vlan":{"type":"integer"},"config_tier":{"type":"string"}}}`)
}

func (d *SplitterDriver) ValidateResource(raw json.RawMessage) error {
	_, err := parseSplitterResource(raw)
	return err
}

func (d *SplitterDriver) ValidateTarget(raw json.RawMessage) error {
	_, err := decodeStrict[SplitterTarget](raw)
	return err
}

func (d *SplitterDriver) Read(_ context.Context, rawRes json.RawMessage) (json.RawMessage, error) {
	res, err := parseSplitterResource(rawRes)
	if err != nil {
		return nil, err
	}
	return json.Marshal(d.hw.GetSplitterSlot(res.SplitterID, res.Slot))
}

func (d *SplitterDriver) Plan(_ context.Context, rawCurrent, rawTarget json.RawMessage) (sdk.Plan, error) {
	var current SplitterSlotState
	if err := json.Unmarshal(rawCurrent, &current); err != nil {
		return sdk.Plan{}, err
	}
	target, err := decodeStrict[SplitterTarget](rawTarget)
	if err != nil {
		return sdk.Plan{}, err
	}

	next := current
	if target.Active != nil && !*target.Active {
		next = SplitterSlotState{Active: false}
	} else {
		if target.Active != nil {
			next.Active = *target.Active
		}
		if target.SpeedMbps != nil {
			next.SpeedMbps = *target.SpeedMbps
		}
		if target.ServiceVLAN != nil {
			next.ServiceVLAN = *target.ServiceVLAN
		}
		if target.ConfigTier != nil {
			next.ConfigTier = *target.ConfigTier
		}
	}

	if next == current {
		return sdk.Plan{Driver: splitterDriverInfo}, nil
	}

	params, err := json.Marshal(next)
	if err != nil {
		return sdk.Plan{}, err
	}

	action := "configure_splitter_slot"
	if !next.Active {
		action = "clear_splitter_slot"
	}
	return sdk.Plan{
		Driver: splitterDriverInfo,
		Groups: []sdk.StepGroup{
			{
				Name: "splitter-slot",
				Steps: []sdk.Step{
					{ID: action, Action: action, Params: params},
				},
			},
		},
	}, nil
}

func (d *SplitterDriver) ApplyStep(_ context.Context, rawRes json.RawMessage, step sdk.Step) error {
	res, err := parseSplitterResource(rawRes)
	if err != nil {
		return err
	}
	switch step.Action {
	case "configure_splitter_slot", "clear_splitter_slot":
		var st SplitterSlotState
		if err := json.Unmarshal(step.Params, &st); err != nil {
			return sdk.Terminal(err)
		}
		d.hw.mu.Lock()
		d.hw.slots[splitterSlotKey(res.SplitterID, res.Slot)] = st
		d.hw.mu.Unlock()
		return nil
	default:
		return sdk.Terminal(fmt.Errorf("unsupported splitter action %q", step.Action))
	}
}

func parseSplitterResource(raw json.RawMessage) (SplitterResource, error) {
	res, err := decodeStrict[SplitterResource](raw)
	if err != nil {
		return SplitterResource{}, err
	}
	if res.SplitterID == "" || res.Slot <= 0 {
		return SplitterResource{}, errors.New("splitter_id and positive slot are required")
	}
	return res, nil
}
