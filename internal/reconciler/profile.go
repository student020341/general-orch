package reconciler

import (
	"fmt"

	"orch/internal/model"
)

// DeviceProfile describes the capabilities, default worker queues, and hardware
// constraints for a supported entity kind.
type DeviceProfile struct {
	Kind              string
	Aspects           []string
	DefaultQueues     map[string]string // aspect -> Temporal task queue
	FetchStateQueue   string
	SupportsLiveState bool
	SerialExecution   bool // true chains mutation steps sequentially (e.g. power-limited board)
}

// ProfileCatalog holds known device profiles keyed by entity.Kind.
type ProfileCatalog struct {
	profiles map[string]DeviceProfile
}

// DefaultCatalog returns the built-in device profiles.
func DefaultCatalog() *ProfileCatalog {
	return NewCatalog([]DeviceProfile{
		{
			Kind:    "beacon_bot",
			Aspects: []string{"lights", "rotation"},
			DefaultQueues: map[string]string{
				"lights":   "lighting-workers",
				"rotation": "motion-workers",
			},
			FetchStateQueue:   "motion-workers",
			SupportsLiveState: true,
			SerialExecution:   false,
		},
		{
			Kind:    "beacon_bot_serial",
			Aspects: []string{"lights", "rotation"},
			DefaultQueues: map[string]string{
				"lights":   "pi-workers",
				"rotation": "pi-workers",
			},
			FetchStateQueue:   "pi-workers",
			SupportsLiveState: true,
			SerialExecution:   true,
		},
	})
}

// NewCatalog builds a ProfileCatalog from a slice of DeviceProfile definitions.
func NewCatalog(defs []DeviceProfile) *ProfileCatalog {
	m := make(map[string]DeviceProfile, len(defs))
	for _, d := range defs {
		m[d.Kind] = d
	}
	return &ProfileCatalog{profiles: m}
}

// Lookup returns the DeviceProfile for a given entity kind.
func (c *ProfileCatalog) Lookup(kind string) (DeviceProfile, error) {
	p, ok := c.profiles[kind]
	if !ok {
		return DeviceProfile{}, fmt.Errorf("unsupported entity kind %q", kind)
	}
	return p, nil
}

func resolveTaskQueue(entity *model.Entity, profile DeviceProfile, aspect string) string {
	if entity.TaskQueue != "" {
		return entity.TaskQueue
	}
	if q, ok := profile.DefaultQueues[aspect]; ok && q != "" {
		return q
	}
	if profile.FetchStateQueue != "" {
		return profile.FetchStateQueue
	}
	return "default-workers"
}
