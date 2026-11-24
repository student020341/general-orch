package engine

import (
	"errors"
	"fmt"
	"sort"
	"sync"

	"orch/sdk"
)

var (
	// ErrDriverNotFound is returned when no driver matches the requested provider and kind.
	ErrDriverNotFound = errors.New("driver not found")

	// ErrDriverExists is returned when registering a driver with an already-registered ID.
	ErrDriverExists = errors.New("driver already registered")
)

// Registry stores registered drivers keyed by "provider/kind".
type Registry struct {
	mu      sync.RWMutex
	drivers map[string]sdk.Driver
}

// NewRegistry creates an empty driver registry.
func NewRegistry() *Registry {
	return &Registry{
		drivers: make(map[string]sdk.Driver),
	}
}

// RegisterDriver registers a single standalone driver.
func (r *Registry) RegisterDriver(d sdk.Driver) error {
	if d == nil {
		return errors.New("cannot register nil driver")
	}
	info := d.Info()
	if err := validateDriverInfo(info); err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	id := info.ID()
	if _, exists := r.drivers[id]; exists {
		return fmt.Errorf("%w: %s", ErrDriverExists, id)
	}
	r.drivers[id] = d
	return nil
}

// RegisterProvider atomically registers all drivers belonging to a provider.
func (r *Registry) RegisterProvider(p sdk.Provider) error {
	if p == nil {
		return errors.New("cannot register nil provider")
	}
	name := p.Name()
	if name == "" {
		return errors.New("provider name cannot be empty")
	}

	drivers := p.Drivers()
	seen := make(map[string]struct{}, len(drivers))
	for _, d := range drivers {
		if d == nil {
			return fmt.Errorf("provider %q returned nil driver", name)
		}
		info := d.Info()
		if err := validateDriverInfo(info); err != nil {
			return err
		}
		if info.Provider != name {
			return fmt.Errorf("driver %q provider mismatch: got %q, want %q", info.Kind, info.Provider, name)
		}
		id := info.ID()
		if _, dup := seen[id]; dup {
			return fmt.Errorf("%w: %s", ErrDriverExists, id)
		}
		seen[id] = struct{}{}
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	for _, d := range drivers {
		id := d.Info().ID()
		if _, exists := r.drivers[id]; exists {
			return fmt.Errorf("%w: %s", ErrDriverExists, id)
		}
	}
	for _, d := range drivers {
		r.drivers[d.Info().ID()] = d
	}
	return nil
}

// Resolve looks up a driver by provider and kind.
func (r *Registry) Resolve(provider, kind string) (sdk.Driver, error) {
	id := sdk.DriverInfo{Provider: provider, Kind: kind}.ID()

	r.mu.RLock()
	d, ok := r.drivers[id]
	r.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrDriverNotFound, id)
	}
	return d, nil
}

// List returns metadata for all registered drivers sorted by ID.
func (r *Registry) List() []sdk.DriverInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()

	out := make([]sdk.DriverInfo, 0, len(r.drivers))
	for _, d := range r.drivers {
		out = append(out, d.Info())
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].ID() < out[j].ID()
	})
	return out
}

func validateDriverInfo(info sdk.DriverInfo) error {
	if info.Provider == "" || info.Kind == "" {
		return fmt.Errorf("driver info requires both provider and kind (got %+v)", info)
	}
	return nil
}
