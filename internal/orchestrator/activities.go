package orchestrator

import (
	"context"
	"fmt"

	"orch/internal/bus"
	"orch/internal/driver"
	"orch/internal/events"
	"orch/internal/model"
	"orch/internal/store"
)

const OrchestrationTaskQueue = "orchestration-workflow"

// FetchStateInput is passed to a domain worker's FetchState activity.
type FetchStateInput struct {
	EntityID string `json:"entity_id"`
	Vendor   string `json:"vendor"`
}

// FetchStateOutput carries the live state pulled from hardware.
type FetchStateOutput struct {
	Supported bool        `json:"supported"`
	State     model.State `json:"state,omitempty"`
}

// ExecuteCapabilityInput is passed to a domain worker's ExecuteCapability activity.
type ExecuteCapabilityInput struct {
	Vendor     string      `json:"vendor"`
	Capability string      `json:"capability"`
	Payload    model.State `json:"payload"`
}

// ExecuteCapabilityOutput returns the state delta observed after applying a capability.
type ExecuteCapabilityOutput struct {
	ObservedDelta model.State `json:"observed_delta,omitempty"`
}

// OrchestratorActivities run on the orchestration-workflow queue to load plans and emit NATS events.
type OrchestratorActivities struct {
	Store store.PlanStore
	Pub   bus.Publisher
}

// LoadPlan retrieves a persisted Plan and marks it as running.
func (a *OrchestratorActivities) LoadPlan(ctx context.Context, planID string) (*model.Plan, error) {
	plan, err := a.Store.GetPlan(ctx, planID)
	if err != nil {
		return nil, fmt.Errorf("get plan %s: %w", planID, err)
	}
	if plan.Status == model.PlanSuperseded {
		return plan, nil
	}
	if err := a.Store.UpdatePlanStatus(ctx, planID, model.PlanRunning); err != nil {
		return nil, fmt.Errorf("mark plan running %s: %w", planID, err)
	}
	return plan, nil
}

// PublishStepCompleted emits a step completion/failure event to NATS.
func (a *OrchestratorActivities) PublishStepCompleted(ctx context.Context, ev events.StepCompletedEvent) error {
	return a.Pub.PublishJSON(ctx, events.StepCompletedSubject(ev.EntityID), ev)
}

// PublishExecutionFinished emits a workflow completion/outcome event to NATS.
func (a *OrchestratorActivities) PublishExecutionFinished(ctx context.Context, ev events.ExecutionFinishedEvent) error {
	return a.Pub.PublishJSON(ctx, events.ExecutionFinishedSubject(ev.EntityID), ev)
}

// DomainActivities run on domain/device worker queues to talk to hardware drivers.
type DomainActivities struct {
	Drivers *driver.Registry
}

// FetchState reads live hardware state when the driver supports StateFetcher.
func (a *DomainActivities) FetchState(ctx context.Context, in FetchStateInput) (FetchStateOutput, error) {
	drv, err := a.Drivers.Get(in.Vendor)
	if err != nil {
		return FetchStateOutput{}, err
	}
	fetcher, ok := drv.(driver.StateFetcher)
	if !ok {
		return FetchStateOutput{Supported: false}, nil
	}
	st, err := fetcher.FetchState(ctx, in.EntityID)
	if err != nil {
		return FetchStateOutput{}, err
	}
	return FetchStateOutput{Supported: true, State: st}, nil
}

// ExecuteCapability executes one capability step via the registered vendor driver.
func (a *DomainActivities) ExecuteCapability(ctx context.Context, in ExecuteCapabilityInput) (ExecuteCapabilityOutput, error) {
	drv, err := a.Drivers.Get(in.Vendor)
	if err != nil {
		return ExecuteCapabilityOutput{}, err
	}
	delta, err := drv.Execute(ctx, in.Capability, in.Payload)
	if err != nil {
		return ExecuteCapabilityOutput{}, err
	}
	return ExecuteCapabilityOutput{ObservedDelta: delta}, nil
}
