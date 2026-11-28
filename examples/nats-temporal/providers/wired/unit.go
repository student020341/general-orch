package wired

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"orch/sdk"
)

var unitDriverInfo = sdk.DriverInfo{Provider: "wired", Kind: "unit"}

// UnitResource identifies a home unit by serial.
type UnitResource struct {
	Serial string `json:"serial"`
}

// UnitTarget describes desired unit configuration.
type UnitTarget struct {
	Active     *bool   `json:"active,omitempty"`
	SpeedMbps  *int    `json:"speed_mbps,omitempty"`
	ConfigTier *string `json:"config_tier,omitempty"`
	IPMode     *string `json:"ip_mode,omitempty"`
}

// UnitDriver converges home units ("wired/unit"), supporting both SSH-configured and auto no-op models.
type UnitDriver struct {
	hw *MockHardware
}

func NewUnitDriver(hw *MockHardware) *UnitDriver { return &UnitDriver{hw: hw} }

func (d *UnitDriver) Info() sdk.DriverInfo { return unitDriverInfo }
func (d *UnitDriver) Schema() []byte {
	return []byte(`{"type":"object","properties":{"active":{"type":"boolean"},"speed_mbps":{"type":"integer"},"config_tier":{"type":"string"},"ip_mode":{"type":"string"}}}`)
}

func (d *UnitDriver) ValidateResource(raw json.RawMessage) error {
	_, err := parseUnitResource(raw)
	return err
}

func (d *UnitDriver) ValidateTarget(raw json.RawMessage) error {
	_, err := decodeStrict[UnitTarget](raw)
	return err
}

func (d *UnitDriver) Read(_ context.Context, rawRes json.RawMessage) (json.RawMessage, error) {
	res, err := parseUnitResource(rawRes)
	if err != nil {
		return nil, err
	}
	u, ok := d.hw.GetUnit(res.Serial)
	if !ok {
		return nil, fmt.Errorf("unit %q not present in hardware pool", res.Serial)
	}
	if !u.Online {
		return nil, fmt.Errorf("unit %q is unreachable", res.Serial)
	}
	return json.Marshal(u)
}

func (d *UnitDriver) Plan(_ context.Context, rawCurrent, rawTarget json.RawMessage) (sdk.Plan, error) {
	var current UnitDeviceState
	if err := json.Unmarshal(rawCurrent, &current); err != nil {
		return sdk.Plan{}, err
	}
	target, err := decodeStrict[UnitTarget](rawTarget)
	if err != nil {
		return sdk.Plan{}, err
	}

	// Auto-configured units require no driver-side configuration steps.
	if current.Model == "auto" {
		return sdk.Plan{Driver: unitDriverInfo}, nil
	}

	next := current
	if target.Active != nil && !*target.Active {
		next.Active = false
		next.SpeedMbps = 0
		next.ConfigTier = ""
		next.IPMode = ""
	} else {
		if target.Active != nil {
			next.Active = *target.Active
		}
		if target.SpeedMbps != nil {
			next.SpeedMbps = *target.SpeedMbps
		}
		if target.ConfigTier != nil {
			next.ConfigTier = *target.ConfigTier
		}
		if target.IPMode != nil {
			next.IPMode = *target.IPMode
		}
	}

	if next == current {
		return sdk.Plan{Driver: unitDriverInfo}, nil
	}

	params, err := json.Marshal(next)
	if err != nil {
		return sdk.Plan{}, err
	}

	groups := []sdk.StepGroup{
		{
			Name: "ssh-config",
			Steps: []sdk.Step{
				{
					ID:          "ssh-apply-unit",
					Action:      "ssh_configure_unit",
					Description: fmt.Sprintf("apply unit config over SSH (active=%v speed=%d tier=%s)", next.Active, next.SpeedMbps, next.ConfigTier),
					Params:      params,
				},
			},
		},
	}

	// Active configurations trigger a post-config DHCP/lease reset in a follow-up group.
	if next.Active {
		groups = append(groups, sdk.StepGroup{
			Name: "post-config-reset",
			Steps: []sdk.Step{
				{
					ID:          "ssh-dhcp-reset",
					Action:      "ssh_dhcp_reset",
					Description: "reset unit DHCP lease after config convergence",
				},
			},
		})
	}

	return sdk.Plan{Driver: unitDriverInfo, Groups: groups}, nil
}

func (d *UnitDriver) ApplyStep(_ context.Context, rawRes json.RawMessage, step sdk.Step) error {
	res, err := parseUnitResource(rawRes)
	if err != nil {
		return err
	}

	d.hw.mu.Lock()
	defer d.hw.mu.Unlock()

	u, ok := d.hw.units[res.Serial]
	if !ok {
		return fmt.Errorf("unit %q not present in hardware pool", res.Serial)
	}
	if !u.Online {
		return fmt.Errorf("unit %q is unreachable", res.Serial)
	}

	switch step.Action {
	case "ssh_configure_unit":
		var desired UnitDeviceState
		if err := json.Unmarshal(step.Params, &desired); err != nil {
			return sdk.Terminal(err)
		}
		u.Active = desired.Active
		u.SpeedMbps = desired.SpeedMbps
		u.ConfigTier = desired.ConfigTier
		u.IPMode = desired.IPMode
	case "ssh_dhcp_reset":
		u.LeaseEpoch++
	default:
		return sdk.Terminal(fmt.Errorf("unsupported unit action %q", step.Action))
	}

	d.hw.units[res.Serial] = u
	return nil
}

func parseUnitResource(raw json.RawMessage) (UnitResource, error) {
	res, err := decodeStrict[UnitResource](raw)
	if err != nil {
		return UnitResource{}, err
	}
	if res.Serial == "" {
		return UnitResource{}, errors.New("serial is required")
	}
	return res, nil
}
