package wired

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"orch/sdk"
)

// HubPortState represents the configured state of a single hub port.
type HubPortState struct {
	Active      bool `json:"active"`
	SpeedMbps   int  `json:"speed_mbps,omitempty"`
	ServiceVLAN int  `json:"service_vlan,omitempty"`
	UtilityVLAN int  `json:"utility_vlan,omitempty"`
}

// SplitterSlotState represents the configured state of one splitter slot.
type SplitterSlotState struct {
	Active      bool   `json:"active"`
	SpeedMbps   int    `json:"speed_mbps,omitempty"`
	ServiceVLAN int    `json:"service_vlan,omitempty"`
	ConfigTier  string `json:"config_tier,omitempty"`
}

// UnitDeviceState represents the hardware state of a home unit.
type UnitDeviceState struct {
	Serial     string `json:"serial"`
	Model      string `json:"model"` // "ssh" or "auto"
	Online     bool   `json:"online"`
	Active     bool   `json:"active"`
	SpeedMbps  int    `json:"speed_mbps,omitempty"`
	ConfigTier string `json:"config_tier,omitempty"`
	IPMode     string `json:"ip_mode,omitempty"`
	LeaseEpoch int    `json:"lease_epoch"`
}

// HubUplinkState represents the aggregate uplink bandwidth state for an entire hub.
type HubUplinkState struct {
	Active    bool `json:"active"`
	SpeedMbps int  `json:"speed_mbps,omitempty"`
}

// MockHardware simulates the underlying wired devices (hubs, splitters, units).
type MockHardware struct {
	mu         sync.RWMutex
	hubPorts   map[string]HubPortState
	hubUplinks map[string]HubUplinkState
	slots      map[string]SplitterSlotState
	units      map[string]UnitDeviceState
}

// NewMockHardware creates an empty simulated hardware pool.
func NewMockHardware() *MockHardware {
	return &MockHardware{
		hubPorts:   make(map[string]HubPortState),
		hubUplinks: make(map[string]HubUplinkState),
		slots:      make(map[string]SplitterSlotState),
		units:      make(map[string]UnitDeviceState),
	}
}

func hubPortKey(hubID string, port int) string {
	return fmt.Sprintf("%s:%d", hubID, port)
}

func splitterSlotKey(splitterID string, slot int) string {
	return fmt.Sprintf("%s:%d", splitterID, slot)
}

// SeedUnit registers or updates a simulated home unit.
func (m *MockHardware) SeedUnit(u UnitDeviceState) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.units[u.Serial] = u
}

// SetUnitOnline toggles reachability for a simulated unit (for retry/partial failure tests).
func (m *MockHardware) SetUnitOnline(serial string, online bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if u, ok := m.units[serial]; ok {
		u.Online = online
		m.units[serial] = u
	}
}

func (m *MockHardware) GetUnit(serial string) (UnitDeviceState, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	u, ok := m.units[serial]
	return u, ok
}

func (m *MockHardware) GetHubPort(hubID string, port int) HubPortState {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.hubPorts[hubPortKey(hubID, port)]
}

func (m *MockHardware) GetHubUplink(hubID string) HubUplinkState {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.hubUplinks[hubID]
}

func (m *MockHardware) GetSplitterSlot(splitterID string, slot int) SplitterSlotState {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.slots[splitterSlotKey(splitterID, slot)]
}

// Provider groups the wired infrastructure drivers ("wired/hub", "wired/hub-uplink", "wired/splitter", "wired/unit").
type Provider struct {
	Hardware *MockHardware
}

// NewProvider creates a wired provider backed by hardware (or a fresh MockHardware if nil).
func NewProvider(hardware *MockHardware) *Provider {
	if hardware == nil {
		hardware = NewMockHardware()
	}
	return &Provider{Hardware: hardware}
}

func (p *Provider) Name() string { return "wired" }

func (p *Provider) Drivers() []sdk.Driver {
	return []sdk.Driver{
		NewHubDriver(p.Hardware),
		NewHubUplinkDriver(p.Hardware),
		NewSplitterDriver(p.Hardware),
		NewUnitDriver(p.Hardware),
	}
}

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
