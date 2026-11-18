package reconciler

import (
	"context"
	"fmt"
	"slices"
	"sync"

	"orch/internal/model"
	"orch/internal/store"
)

// ProfileCatalog is a thread-safe, hot-reloadable registry of DeviceProfiles keyed by Kind.
type ProfileCatalog struct {
	mu       sync.RWMutex
	profiles map[string]model.DeviceProfile
}

// DefaultProfiles returns built-in profiles used as a fallback seed when no file/store entries exist.
func DefaultProfiles() []model.DeviceProfile {
	return []model.DeviceProfile{
		{
			Kind: "lamp",
			Resources: map[string]string{
				"main": "light",
			},
			DefaultQueues: map[string]string{
				"light": "lighting-workers",
			},
			SupportsLiveState: false,
			SerialExecution:   false,
		},
		{
			Kind: "beacon_bot",
			Resources: map[string]string{
				"light1":   "light",
				"light2":   "light",
				"rotation": "rotation",
			},
			DefaultQueues: map[string]string{
				"light":    "lighting-workers",
				"rotation": "motion-workers",
			},
			FetchStateQueue:   "motion-workers",
			SupportsLiveState: true,
			SerialExecution:   false,
		},
		{
			Kind: "beacon_bot_serial",
			Resources: map[string]string{
				"light1":   "light",
				"light2":   "light",
				"rotation": "rotation",
			},
			DefaultQueues: map[string]string{
				"light":    "pi-workers",
				"rotation": "pi-workers",
			},
			FetchStateQueue:   "pi-workers",
			SupportsLiveState: true,
			SerialExecution:   true,
		},
	}
}

// DefaultCatalog builds a ProfileCatalog populated with DefaultProfiles.
func DefaultCatalog() *ProfileCatalog {
	return NewCatalog(DefaultProfiles())
}

// NewCatalog builds a ProfileCatalog from a slice of DeviceProfile definitions.
func NewCatalog(defs []model.DeviceProfile) *ProfileCatalog {
	c := &ProfileCatalog{profiles: make(map[string]model.DeviceProfile, len(defs))}
	c.Replace(defs)
	return c
}

// Replace atomically swaps the catalog's profiles.
func (c *ProfileCatalog) Replace(defs []model.DeviceProfile) {
	next := make(map[string]model.DeviceProfile, len(defs))
	for _, d := range defs {
		next[d.Kind] = d
	}
	c.mu.Lock()
	c.profiles = next
	c.mu.Unlock()
}

// LoadFromStore refreshes the catalog from a CatalogStore.
func (c *ProfileCatalog) LoadFromStore(ctx context.Context, cs store.CatalogStore) error {
	defs, err := cs.ListProfiles(ctx)
	if err != nil {
		return err
	}
	c.Replace(defs)
	return nil
}

// List returns a sorted snapshot of all loaded profiles.
func (c *ProfileCatalog) List() []model.DeviceProfile {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]model.DeviceProfile, 0, len(c.profiles))
	for _, p := range c.profiles {
		out = append(out, p)
	}
	slices.SortFunc(out, func(a, b model.DeviceProfile) int {
		if a.Kind < b.Kind {
			return -1
		}
		if a.Kind > b.Kind {
			return 1
		}
		return 0
	})
	return out
}

// Lookup returns the DeviceProfile for a given entity kind.
func (c *ProfileCatalog) Lookup(kind string) (model.DeviceProfile, error) {
	c.mu.RLock()
	p, ok := c.profiles[kind]
	c.mu.RUnlock()
	if !ok {
		return model.DeviceProfile{}, fmt.Errorf("unsupported entity kind %q", kind)
	}
	return p, nil
}

func resolveTaskQueue(entity *model.Entity, profile model.DeviceProfile, resource, aspectType string) string {
	if entity.TaskQueue != "" {
		return entity.TaskQueue
	}
	if q, ok := profile.DefaultQueues[resource]; ok && q != "" {
		return q
	}
	if q, ok := profile.DefaultQueues[aspectType]; ok && q != "" {
		return q
	}
	if profile.FetchStateQueue != "" {
		return profile.FetchStateQueue
	}
	return "default-workers"
}
