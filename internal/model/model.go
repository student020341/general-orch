package model

import (
	"encoding/json"
	"maps"
	"time"
)

// State holds arbitrary JSON-compatible configuration or observed device fields.
type State map[string]any

// EntityStatus represents the convergence lifecycle state of an entity.
type EntityStatus string

const (
	StatusPending   EntityStatus = "pending"
	StatusPlanning  EntityStatus = "planning"
	StatusExecuting EntityStatus = "executing"
	StatusConverged EntityStatus = "converged"
	StatusFailed    EntityStatus = "failed"
)

// Entity tracks desired vs. current configuration state for a managed target.
type Entity struct {
	ID        string       `json:"id"`
	Kind      string       `json:"kind"`
	Vendor    string       `json:"vendor"`
	TaskQueue string       `json:"task_queue,omitempty"`
	Current   State        `json:"current"`
	Target    State        `json:"target"`
	Status    EntityStatus `json:"status"`
	Version   int64        `json:"version"`
	LastError string       `json:"last_error,omitempty"`
	UpdatedAt time.Time    `json:"updated_at"`
}

// PlanStatus represents the lifecycle state of an execution plan.
type PlanStatus string

const (
	PlanPending    PlanStatus = "pending"
	PlanRunning    PlanStatus = "running"
	PlanCompleted  PlanStatus = "completed"
	PlanSuperseded PlanStatus = "superseded"
	PlanFailed     PlanStatus = "failed"
)

// StepStatus represents the execution state of a single DAG step.
type StepStatus string

const (
	StepPending   StepStatus = "pending"
	StepRunning   StepStatus = "running"
	StepCompleted StepStatus = "completed"
	StepFailed    StepStatus = "failed"
	StepSkipped   StepStatus = "skipped"
)

// PlanStep is one node in an execution DAG targeting a Temporal task queue and capability.
type PlanStep struct {
	ID         string     `json:"id"`
	TaskQueue  string     `json:"task_queue"`
	Activity   string     `json:"activity"`
	Capability string     `json:"capability"`
	Vendor     string     `json:"vendor"`
	Payload    State      `json:"payload"`
	DependsOn  []string   `json:"depends_on,omitempty"`
	Status     StepStatus `json:"status"`
	Error      string     `json:"error,omitempty"`
}

// Plan is a persisted DAG of steps generated to converge an entity at a given version.
type Plan struct {
	ID        string     `json:"id"`
	EntityID  string     `json:"entity_id"`
	Version   int64      `json:"version"`
	Status    PlanStatus `json:"status"`
	Steps     []PlanStep `json:"steps"`
	CreatedAt time.Time  `json:"created_at"`
	UpdatedAt time.Time  `json:"updated_at"`
}

// DeviceProfile describes the capabilities, default worker queues, and hardware
// constraints for a supported entity kind.
type DeviceProfile struct {
	Kind              string            `json:"kind"`
	Aspects           []string          `json:"aspects"`
	DefaultQueues     map[string]string `json:"default_queues"`
	FetchStateQueue   string            `json:"fetch_state_queue"`
	SupportsLiveState bool              `json:"supports_live_state"`
	SerialExecution   bool              `json:"serial_execution"`
	UpdatedAt         time.Time         `json:"updated_at,omitempty"`
}

// CloneState returns a deep copy of a State map via JSON round-trip normalization.
func CloneState(src State) State {
	if len(src) == 0 {
		return State{}
	}
	raw, err := json.Marshal(src)
	if err != nil {
		out := make(State, len(src))
		maps.Copy(out, src)
		return out
	}
	var out State
	if err := json.Unmarshal(raw, &out); err != nil || out == nil {
		return State{}
	}
	return out
}

// MergeState recursively merges patch into a copy of base (JSON Merge Patch semantics:
// nested maps merge recursively, nil deletes a key, other values overwrite).
func MergeState(base, patch State) State {
	out := CloneState(base)
	for k, v := range patch {
		if v == nil {
			delete(out, k)
			continue
		}
		patchMap, patchIsMap := toStateMap(v)
		baseMap, baseIsMap := toStateMap(out[k])
		if patchIsMap && baseIsMap {
			out[k] = MergeState(baseMap, patchMap)
			continue
		}
		if patchIsMap {
			out[k] = CloneState(patchMap)
			continue
		}
		out[k] = normalizeValue(v)
	}
	return out
}

func toStateMap(v any) (State, bool) {
	switch m := v.(type) {
	case State:
		return m, true
	case map[string]any:
		return State(m), true
	default:
		return nil, false
	}
}

func normalizeValue(v any) any {
	raw, err := json.Marshal(v)
	if err != nil {
		return v
	}
	var out any
	if err := json.Unmarshal(raw, &out); err != nil {
		return v
	}
	return out
}
