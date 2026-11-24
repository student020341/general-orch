package sdk

import "encoding/json"

// DriverInfo identifies a driver by its vendor/platform provider and specific kind.
type DriverInfo struct {
	Provider string `json:"provider"`
	Kind     string `json:"kind"`
}

// ID returns the canonical "provider/kind" identifier.
func (d DriverInfo) ID() string {
	return d.Provider + "/" + d.Kind
}

// Request describes a planning or reconciliation request for a single resource.
type Request struct {
	Provider string          `json:"provider"`
	Kind     string          `json:"kind"`
	Resource json.RawMessage `json:"resource"`
	Target   json.RawMessage `json:"target"`
}

// Step represents a single atomic operation within a Plan.
type Step struct {
	ID          string          `json:"id"`
	Action      string          `json:"action"`
	Description string          `json:"description,omitempty"`
	Params      json.RawMessage `json:"params,omitempty"`
}

// StepGroup is an ordered batch of steps that complete before the next group starts.
type StepGroup struct {
	Name     string `json:"name"`
	Parallel bool   `json:"parallel,omitempty"`
	Steps    []Step `json:"steps"`
}

// Plan describes the grouped steps required to converge a resource from current state to its target config.
type Plan struct {
	Driver DriverInfo  `json:"driver"`
	Groups []StepGroup `json:"groups"`
}

// IsEmpty reports whether the plan contains any executable steps.
func (p Plan) IsEmpty() bool {
	for _, g := range p.Groups {
		if len(g.Steps) > 0 {
			return false
		}
	}
	return true
}

// StepResult captures the execution outcome of a single step.
type StepResult struct {
	StepID string `json:"step_id"`
	Action string `json:"action"`
	Error  string `json:"error,omitempty"`
}

// GroupResult captures the execution outcomes of all steps in a group.
type GroupResult struct {
	Name  string       `json:"name"`
	Steps []StepResult `json:"steps"`
}

// ApplyResult captures the execution outcomes across all groups of a Plan.
type ApplyResult struct {
	Driver DriverInfo    `json:"driver"`
	Groups []GroupResult `json:"groups"`
}
