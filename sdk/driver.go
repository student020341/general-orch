package sdk

import (
	"context"
	"encoding/json"
)

// Driver defines the contract for inspecting, planning, and configuring one resource kind.
type Driver interface {
	// Info returns the provider and kind identifiers for this driver.
	Info() DriverInfo

	// Schema returns a JSON Schema describing the configurable target state.
	Schema() []byte

	// ValidateResource validates the resource addressing/connection payload.
	ValidateResource(resource json.RawMessage) error

	// ValidateTarget validates the (potentially partial) target config payload.
	ValidateTarget(target json.RawMessage) error

	// Read retrieves the current state of the resource as JSON.
	Read(ctx context.Context, resource json.RawMessage) (json.RawMessage, error)

	// Plan computes the grouped steps needed to converge from current state to target state.
	Plan(ctx context.Context, current, target json.RawMessage) (Plan, error)

	// ApplyStep executes a single planned step against the resource.
	ApplyStep(ctx context.Context, resource json.RawMessage, step Step) error
}

// Provider groups one or more drivers under a shared vendor or platform namespace.
type Provider interface {
	// Name returns the provider identifier (e.g., "mikrotik", "diy").
	Name() string

	// Drivers returns the drivers supplied by this provider.
	Drivers() []Driver
}
