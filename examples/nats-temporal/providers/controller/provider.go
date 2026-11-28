package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"orch/sdk"
)

var entryDriverInfo = sdk.DriverInfo{Provider: "controller", Kind: "entry"}

// EntryState represents an immutable entry stored in the external controller.
type EntryState struct {
	Exists      bool   `json:"exists"`
	UnitSerial  string `json:"unit_serial,omitempty"`
	ServiceVLAN int    `json:"service_vlan,omitempty"`
	SpeedMbps   int    `json:"speed_mbps,omitempty"`
	ConfigTier  string `json:"config_tier,omitempty"`
}

// EntryResource identifies the controller entry by subscription ID.
type EntryResource struct {
	SubscriptionID string `json:"subscription_id"`
}

// EntryTarget describes the desired state of a controller entry.
type EntryTarget struct {
	Active      bool   `json:"active"`
	UnitSerial  string `json:"unit_serial,omitempty"`
	ServiceVLAN int    `json:"service_vlan,omitempty"`
	SpeedMbps   int    `json:"speed_mbps,omitempty"`
	ConfigTier  string `json:"config_tier,omitempty"`
}

// MockStore simulates the central controller API (immutable entries: delete then create).
type MockStore struct {
	mu      sync.RWMutex
	entries map[string]EntryState
}

func NewMockStore() *MockStore {
	return &MockStore{entries: make(map[string]EntryState)}
}

func (s *MockStore) Get(subscriptionID string) EntryState {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.entries[subscriptionID]
}

// Provider exposes the "controller/entry" driver.
type Provider struct {
	Store *MockStore
}

func NewProvider(store *MockStore) *Provider {
	if store == nil {
		store = NewMockStore()
	}
	return &Provider{Store: store}
}

func (p *Provider) Name() string          { return "controller" }
func (p *Provider) Drivers() []sdk.Driver { return []sdk.Driver{NewEntryDriver(p.Store)} }

// EntryDriver implements sdk.Driver for immutable controller entries.
type EntryDriver struct {
	store *MockStore
}

func NewEntryDriver(store *MockStore) *EntryDriver { return &EntryDriver{store: store} }

func (d *EntryDriver) Info() sdk.DriverInfo { return entryDriverInfo }
func (d *EntryDriver) Schema() []byte {
	return []byte(`{"type":"object","required":["active"],"properties":{"active":{"type":"boolean"},"unit_serial":{"type":"string"},"service_vlan":{"type":"integer"},"speed_mbps":{"type":"integer"},"config_tier":{"type":"string"}}}`)
}

func (d *EntryDriver) ValidateResource(raw json.RawMessage) error {
	_, err := parseResource(raw)
	return err
}

func (d *EntryDriver) ValidateTarget(raw json.RawMessage) error {
	_, err := parseTarget(raw)
	return err
}

func (d *EntryDriver) Read(_ context.Context, rawRes json.RawMessage) (json.RawMessage, error) {
	res, err := parseResource(rawRes)
	if err != nil {
		return nil, err
	}
	return json.Marshal(d.store.Get(res.SubscriptionID))
}

func (d *EntryDriver) Plan(_ context.Context, rawCurrent, rawTarget json.RawMessage) (sdk.Plan, error) {
	var current EntryState
	if err := json.Unmarshal(rawCurrent, &current); err != nil {
		return sdk.Plan{}, err
	}
	target, err := parseTarget(rawTarget)
	if err != nil {
		return sdk.Plan{}, err
	}

	if !target.Active {
		if !current.Exists {
			return sdk.Plan{Driver: entryDriverInfo}, nil
		}
		return sdk.Plan{
			Driver: entryDriverInfo,
			Groups: []sdk.StepGroup{
				{
					Name:  "delete-existing",
					Steps: []sdk.Step{{ID: "delete-entry", Action: "delete_entry", Description: "delete existing controller entry"}},
				},
			},
		}, nil
	}

	desired := EntryState{
		Exists:      true,
		UnitSerial:  target.UnitSerial,
		ServiceVLAN: target.ServiceVLAN,
		SpeedMbps:   target.SpeedMbps,
		ConfigTier:  target.ConfigTier,
	}
	if current == desired {
		return sdk.Plan{Driver: entryDriverInfo}, nil
	}

	var groups []sdk.StepGroup
	if current.Exists {
		// Entries are immutable in the controller API, so updates must delete first.
		groups = append(groups, sdk.StepGroup{
			Name:  "delete-existing",
			Steps: []sdk.Step{{ID: "delete-entry", Action: "delete_entry", Description: "delete stale controller entry"}},
		})
	}

	params, err := json.Marshal(desired)
	if err != nil {
		return sdk.Plan{}, err
	}
	groups = append(groups, sdk.StepGroup{
		Name:  "create-target",
		Steps: []sdk.Step{{ID: "create-entry", Action: "create_entry", Description: "create target controller entry", Params: params}},
	})

	return sdk.Plan{Driver: entryDriverInfo, Groups: groups}, nil
}

func (d *EntryDriver) ApplyStep(_ context.Context, rawRes json.RawMessage, step sdk.Step) error {
	res, err := parseResource(rawRes)
	if err != nil {
		return err
	}

	switch step.Action {
	case "delete_entry":
		d.store.mu.Lock()
		delete(d.store.entries, res.SubscriptionID)
		d.store.mu.Unlock()
		return nil
	case "create_entry":
		var entry EntryState
		if err := json.Unmarshal(step.Params, &entry); err != nil {
			return sdk.Terminal(err)
		}
		entry.Exists = true
		d.store.mu.Lock()
		d.store.entries[res.SubscriptionID] = entry
		d.store.mu.Unlock()
		return nil
	default:
		return sdk.Terminal(fmt.Errorf("unsupported controller action %q", step.Action))
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

func parseResource(raw json.RawMessage) (EntryResource, error) {
	res, err := decodeStrict[EntryResource](raw)
	if err != nil {
		return EntryResource{}, err
	}
	if res.SubscriptionID == "" {
		return EntryResource{}, errors.New("subscription_id is required")
	}
	return res, nil
}

func parseTarget(raw json.RawMessage) (EntryTarget, error) {
	tgt, err := decodeStrict[EntryTarget](raw)
	if err != nil {
		return EntryTarget{}, err
	}
	if tgt.Active && (tgt.UnitSerial == "" || tgt.SpeedMbps <= 0) {
		return EntryTarget{}, errors.New("unit_serial and positive speed_mbps are required when active")
	}
	return tgt, nil
}
