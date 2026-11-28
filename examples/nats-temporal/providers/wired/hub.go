package wired

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"orch/sdk"
)

var hubDriverInfo = sdk.DriverInfo{Provider: "wired", Kind: "hub"}

// HubResource scopes hub configuration to a single port on a shared hub.
type HubResource struct {
	HubID string `json:"hub_id"`
	Port  int    `json:"port"`
}

// HubTarget describes desired port configuration (supports partial updates or teardown).
type HubTarget struct {
	Active      *bool `json:"active,omitempty"`
	SpeedMbps   *int  `json:"speed_mbps,omitempty"`
	ServiceVLAN *int  `json:"service_vlan,omitempty"`
	UtilityVLAN *int  `json:"utility_vlan,omitempty"`
}

// HubDriver configures ports on shared hubs ("wired/hub").
type HubDriver struct {
	hw *MockHardware
}

func NewHubDriver(hw *MockHardware) *HubDriver { return &HubDriver{hw: hw} }

func (d *HubDriver) Info() sdk.DriverInfo { return hubDriverInfo }
func (d *HubDriver) Schema() []byte {
	return []byte(`{"type":"object","properties":{"active":{"type":"boolean"},"speed_mbps":{"type":"integer"},"service_vlan":{"type":"integer"},"utility_vlan":{"type":"integer"}}}`)
}

func (d *HubDriver) ValidateResource(raw json.RawMessage) error {
	_, err := parseHubResource(raw)
	return err
}

func (d *HubDriver) ValidateTarget(raw json.RawMessage) error {
	_, err := parseHubTarget(raw)
	return err
}

func (d *HubDriver) Read(_ context.Context, rawRes json.RawMessage) (json.RawMessage, error) {
	res, err := parseHubResource(rawRes)
	if err != nil {
		return nil, err
	}
	return json.Marshal(d.hw.GetHubPort(res.HubID, res.Port))
}

func (d *HubDriver) Plan(_ context.Context, rawCurrent, rawTarget json.RawMessage) (sdk.Plan, error) {
	var current HubPortState
	if err := json.Unmarshal(rawCurrent, &current); err != nil {
		return sdk.Plan{}, err
	}
	target, err := parseHubTarget(rawTarget)
	if err != nil {
		return sdk.Plan{}, err
	}

	next := current
	if target.Active != nil && !*target.Active {
		next = HubPortState{Active: false}
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
		if target.UtilityVLAN != nil {
			next.UtilityVLAN = *target.UtilityVLAN
		}
	}

	if next == current {
		return sdk.Plan{Driver: hubDriverInfo}, nil
	}

	params, err := json.Marshal(next)
	if err != nil {
		return sdk.Plan{}, err
	}

	action := "configure_hub_port"
	desc := fmt.Sprintf("configure hub port (active=%v speed=%d service_vlan=%d utility_vlan=%d)", next.Active, next.SpeedMbps, next.ServiceVLAN, next.UtilityVLAN)
	if !next.Active {
		action = "clear_hub_port"
		desc = "clear and disable hub port"
	}

	return sdk.Plan{
		Driver: hubDriverInfo,
		Groups: []sdk.StepGroup{
			{
				Name: "hub-port",
				Steps: []sdk.Step{
					{ID: action, Action: action, Description: desc, Params: params},
				},
			},
		},
	}, nil
}

func (d *HubDriver) ApplyStep(_ context.Context, rawRes json.RawMessage, step sdk.Step) error {
	res, err := parseHubResource(rawRes)
	if err != nil {
		return err
	}
	switch step.Action {
	case "configure_hub_port", "clear_hub_port":
		var st HubPortState
		if err := json.Unmarshal(step.Params, &st); err != nil {
			return sdk.Terminal(err)
		}
		d.hw.mu.Lock()
		d.hw.hubPorts[hubPortKey(res.HubID, res.Port)] = st
		d.hw.mu.Unlock()
		return nil
	default:
		return sdk.Terminal(fmt.Errorf("unsupported hub action %q", step.Action))
	}
}

func parseHubResource(raw json.RawMessage) (HubResource, error) {
	res, err := decodeStrict[HubResource](raw)
	if err != nil {
		return HubResource{}, err
	}
	if res.HubID == "" || res.Port <= 0 {
		return HubResource{}, errors.New("hub_id and positive port are required")
	}
	return res, nil
}

func parseHubTarget(raw json.RawMessage) (HubTarget, error) {
	tgt, err := decodeStrict[HubTarget](raw)
	if err != nil {
		return HubTarget{}, err
	}
	if tgt.SpeedMbps != nil && *tgt.SpeedMbps < 0 {
		return HubTarget{}, errors.New("speed_mbps cannot be negative")
	}
	return tgt, nil
}

var hubUplinkDriverInfo = sdk.DriverInfo{Provider: "wired", Kind: "hub-uplink"}

// HubUplinkResource identifies a hub's aggregate uplink interface.
type HubUplinkResource struct {
	HubID string `json:"hub_id"`
}

// HubUplinkTarget describes the desired aggregate bandwidth for a hub's uplink.
type HubUplinkTarget struct {
	Active    *bool `json:"active,omitempty"`
	SpeedMbps *int  `json:"speed_mbps,omitempty"`
}

// HubUplinkDriver converges total hub uplink capacity ("wired/hub-uplink").
type HubUplinkDriver struct {
	hw *MockHardware
}

func NewHubUplinkDriver(hw *MockHardware) *HubUplinkDriver { return &HubUplinkDriver{hw: hw} }

func (d *HubUplinkDriver) Info() sdk.DriverInfo { return hubUplinkDriverInfo }
func (d *HubUplinkDriver) Schema() []byte {
	return []byte(`{"type":"object","properties":{"active":{"type":"boolean"},"speed_mbps":{"type":"integer"}}}`)
}

func (d *HubUplinkDriver) ValidateResource(raw json.RawMessage) error {
	_, err := parseHubUplinkResource(raw)
	return err
}

func (d *HubUplinkDriver) ValidateTarget(raw json.RawMessage) error {
	tgt, err := decodeStrict[HubUplinkTarget](raw)
	if err != nil {
		return err
	}
	if tgt.SpeedMbps != nil && *tgt.SpeedMbps < 0 {
		return errors.New("speed_mbps cannot be negative")
	}
	return nil
}

func (d *HubUplinkDriver) Read(_ context.Context, rawRes json.RawMessage) (json.RawMessage, error) {
	res, err := parseHubUplinkResource(rawRes)
	if err != nil {
		return nil, err
	}
	return json.Marshal(d.hw.GetHubUplink(res.HubID))
}

func (d *HubUplinkDriver) Plan(_ context.Context, rawCurrent, rawTarget json.RawMessage) (sdk.Plan, error) {
	var current HubUplinkState
	if err := json.Unmarshal(rawCurrent, &current); err != nil {
		return sdk.Plan{}, err
	}
	target, err := decodeStrict[HubUplinkTarget](rawTarget)
	if err != nil {
		return sdk.Plan{}, err
	}

	next := current
	if target.Active != nil && !*target.Active {
		next = HubUplinkState{Active: false}
	} else {
		if target.Active != nil {
			next.Active = *target.Active
		}
		if target.SpeedMbps != nil {
			next.SpeedMbps = *target.SpeedMbps
		}
	}
	if next == current {
		return sdk.Plan{Driver: hubUplinkDriverInfo}, nil
	}

	params, err := json.Marshal(next)
	if err != nil {
		return sdk.Plan{}, err
	}
	action := "configure_hub_uplink"
	if !next.Active {
		action = "clear_hub_uplink"
	}
	return sdk.Plan{
		Driver: hubUplinkDriverInfo,
		Groups: []sdk.StepGroup{
			{
				Name:  "hub-uplink",
				Steps: []sdk.Step{{ID: action, Action: action, Params: params}},
			},
		},
	}, nil
}

func (d *HubUplinkDriver) ApplyStep(_ context.Context, rawRes json.RawMessage, step sdk.Step) error {
	res, err := parseHubUplinkResource(rawRes)
	if err != nil {
		return err
	}
	switch step.Action {
	case "configure_hub_uplink", "clear_hub_uplink":
		var st HubUplinkState
		if err := json.Unmarshal(step.Params, &st); err != nil {
			return sdk.Terminal(err)
		}
		d.hw.mu.Lock()
		d.hw.hubUplinks[res.HubID] = st
		d.hw.mu.Unlock()
		return nil
	default:
		return sdk.Terminal(fmt.Errorf("unsupported hub-uplink action %q", step.Action))
	}
}

func parseHubUplinkResource(raw json.RawMessage) (HubUplinkResource, error) {
	res, err := decodeStrict[HubUplinkResource](raw)
	if err != nil {
		return HubUplinkResource{}, err
	}
	if res.HubID == "" {
		return HubUplinkResource{}, errors.New("hub_id is required")
	}
	return res, nil
}
