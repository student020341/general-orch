package mock

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"orch/internal/model"
)

// Driver simulates dual-light + relative-turn devices in memory.
type Driver struct {
	mu         sync.Mutex
	vendorName string
	latency    time.Duration
	devices    map[string]model.State
	failOnce   map[string]bool // key: entityID + ":" + capability
}

// New creates a mock driver with optional simulated hardware latency.
func New(vendorName string, latency time.Duration) *Driver {
	if vendorName == "" {
		vendorName = "mock"
	}
	return &Driver{
		vendorName: vendorName,
		latency:    latency,
		devices:    make(map[string]model.State),
		failOnce:   make(map[string]bool),
	}
}

func (d *Driver) Vendor() string { return d.vendorName }

// FetchState returns the current simulated hardware state for entityID.
func (d *Driver) FetchState(ctx context.Context, entityID string) (model.State, error) {
	if err := sleepCtx(ctx, d.latency/2); err != nil {
		return nil, err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	st := d.ensureDeviceLocked(entityID)
	return model.CloneState(st), nil
}

// Execute applies set_light or turn against the simulated device.
func (d *Driver) Execute(ctx context.Context, capability string, payload model.State) (model.State, error) {
	entityID, _ := payload["entity_id"].(string)
	if entityID == "" {
		return nil, fmt.Errorf("missing entity_id in payload")
	}

	if err := sleepCtx(ctx, d.latency); err != nil {
		return nil, err
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	failKey := entityID + ":" + capability
	if d.failOnce[failKey] {
		delete(d.failOnce, failKey)
		return nil, fmt.Errorf("simulated transient failure on %s/%s", entityID, capability)
	}

	dev := d.ensureDeviceLocked(entityID)

	switch capability {
	case "set_light":
		light, _ := payload["light"].(string)
		color, ok := payload["color"]
		if light == "" || !ok {
			return nil, fmt.Errorf("set_light requires light and color")
		}
		dev[light] = model.CloneState(toState(color))
		return model.State{light: model.CloneState(toState(color))}, nil

	case "turn":
		delta, err := toFloat(payload["delta"])
		if err != nil {
			return nil, fmt.Errorf("turn requires numeric delta: %w", err)
		}
		curr, _ := toFloat(dev["rotation"])
		next := normalizeAngle(curr + delta)
		dev["rotation"] = next
		return model.State{"rotation": next}, nil

	default:
		return nil, fmt.Errorf("unsupported capability %q", capability)
	}
}

// SetDeviceState overrides simulated hardware state (useful for simulating physical drift).
func (d *Driver) SetDeviceState(entityID string, patch model.State) {
	d.mu.Lock()
	defer d.mu.Unlock()
	dev := d.ensureDeviceLocked(entityID)
	d.devices[entityID] = model.MergeState(dev, patch)
}

// FailNext arms a one-shot simulated error for entityID + capability.
func (d *Driver) FailNext(entityID, capability string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.failOnce[entityID+":"+capability] = true
}

// Snapshot returns a copy of the simulated device state (for inspection/testing).
func (d *Driver) Snapshot(entityID string) model.State {
	d.mu.Lock()
	defer d.mu.Unlock()
	return model.CloneState(d.ensureDeviceLocked(entityID))
}

func (d *Driver) ensureDeviceLocked(entityID string) model.State {
	st, ok := d.devices[entityID]
	if !ok {
		st = model.State{"rotation": float64(0)}
		d.devices[entityID] = st
	}
	return st
}

func sleepCtx(ctx context.Context, dur time.Duration) error {
	if dur <= 0 {
		return nil
	}
	t := time.NewTimer(dur)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

func normalizeAngle(deg float64) float64 {
	m := math.Mod(deg, 360)
	if m < 0 {
		m += 360
	}
	if m == 360 {
		return 0
	}
	return m
}

func toState(v any) model.State {
	switch m := v.(type) {
	case model.State:
		return m
	case map[string]any:
		return model.State(m)
	default:
		return model.State{}
	}
}

func toFloat(v any) (float64, error) {
	switch n := v.(type) {
	case int:
		return float64(n), nil
	case int64:
		return float64(n), nil
	case float64:
		return n, nil
	case float32:
		return float64(n), nil
	default:
		return 0, fmt.Errorf("expected number, got %T", v)
	}
}
