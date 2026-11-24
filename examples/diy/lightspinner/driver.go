package lightspinner

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"orch/sdk"
)

var driverInfo = sdk.DriverInfo{
	Provider: "diy",
	Kind:     "light-spinner",
}

// MockStore holds in-memory device states for local testing and demonstrations.
type MockStore struct {
	mu      sync.RWMutex
	devices map[string]State
}

// NewMockStore creates an empty in-memory device store.
func NewMockStore() *MockStore {
	return &MockStore{
		devices: make(map[string]State),
	}
}

// Set seeds or replaces the simulated hardware state for deviceID.
func (s *MockStore) Set(deviceID string, state State) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.devices[deviceID] = state
}

// Get retrieves the simulated hardware state for deviceID.
func (s *MockStore) Get(deviceID string) (State, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	st, ok := s.devices[deviceID]
	return st, ok
}

// Driver implements sdk.Driver for the mock "diy/light-spinner" device.
type Driver struct {
	store *MockStore
}

// NewDriver constructs a light-spinner driver backed by store.
func NewDriver(store *MockStore) *Driver {
	if store == nil {
		store = NewMockStore()
	}
	return &Driver{store: store}
}

func (d *Driver) Info() sdk.DriverInfo { return driverInfo }
func (d *Driver) Schema() []byte       { return []byte(schemaJSON) }

func (d *Driver) ValidateResource(resource json.RawMessage) error {
	_, err := validateResourcePayload(resource)
	return err
}

func (d *Driver) ValidateTarget(target json.RawMessage) error {
	_, err := validateTargetPayload(target)
	return err
}

func (d *Driver) Read(_ context.Context, rawResource json.RawMessage) (json.RawMessage, error) {
	resource, err := validateResourcePayload(rawResource)
	if err != nil {
		return nil, err
	}
	state, ok := d.store.Get(resource.DeviceID)
	if !ok {
		return nil, fmt.Errorf("device %q not found", resource.DeviceID)
	}
	return json.Marshal(state)
}

func (d *Driver) Plan(_ context.Context, rawCurrent, rawTarget json.RawMessage) (sdk.Plan, error) {
	var current State
	if err := json.Unmarshal(rawCurrent, &current); err != nil {
		return sdk.Plan{}, fmt.Errorf("decode current state: %w", err)
	}
	target, err := validateTargetPayload(rawTarget)
	if err != nil {
		return sdk.Plan{}, err
	}
	return buildPlan(current, target)
}

func (d *Driver) ApplyStep(_ context.Context, rawResource json.RawMessage, step sdk.Step) error {
	resource, err := validateResourcePayload(rawResource)
	if err != nil {
		return err
	}

	d.store.mu.Lock()
	defer d.store.mu.Unlock()

	state, ok := d.store.devices[resource.DeviceID]
	if !ok {
		return fmt.Errorf("device %q not found", resource.DeviceID)
	}

	switch step.Action {
	case ActionSetLight:
		var p SetLightParams
		if err := json.Unmarshal(step.Params, &p); err != nil {
			return fmt.Errorf("decode %s params: %w", step.Action, err)
		}
		switch p.Light {
		case "light1":
			state.Light1 = p.State
		case "light2":
			state.Light2 = p.State
		default:
			return fmt.Errorf("unknown light %q", p.Light)
		}
	case ActionRotatePlatform:
		var p RotatePlatformParams
		if err := json.Unmarshal(step.Params, &p); err != nil {
			return fmt.Errorf("decode %s params: %w", step.Action, err)
		}
		state.Platform.Angle = p.Angle
	default:
		return fmt.Errorf("unsupported action %q", step.Action)
	}

	d.store.devices[resource.DeviceID] = state
	return nil
}
