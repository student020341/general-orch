package driver

import (
	"context"
	"fmt"
	"sync"

	"orch/internal/model"
)

// Driver executes a device capability for a specific vendor.
type Driver interface {
	Vendor() string
	Execute(ctx context.Context, capability string, payload model.State) (model.State, error)
}

// StateFetcher is an optional capability implemented by drivers that can read
// live state directly from hardware to detect drift before mutating.
type StateFetcher interface {
	FetchState(ctx context.Context, entityID string) (model.State, error)
}

// Registry looks up vendor drivers by name.
type Registry struct {
	mu      sync.RWMutex
	drivers map[string]Driver
}

// NewRegistry returns an empty driver Registry.
func NewRegistry() *Registry {
	return &Registry{drivers: make(map[string]Driver)}
}

// Register adds a Driver under its Vendor() key.
func (r *Registry) Register(d Driver) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.drivers[d.Vendor()] = d
}

// Get retrieves a registered Driver by vendor name.
func (r *Registry) Get(vendor string) (Driver, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	d, ok := r.drivers[vendor]
	if !ok {
		return nil, fmt.Errorf("no driver registered for vendor %q", vendor)
	}
	return d, nil
}
