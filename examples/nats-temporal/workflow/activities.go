package workflow

import (
	"context"
	"encoding/json"

	"go.temporal.io/sdk/temporal"

	"orch/engine"
	"orch/examples/nats-temporal/domain"
	"orch/examples/nats-temporal/inventory"
	"orch/examples/nats-temporal/resolver"
	"orch/sdk"
)

// Event represents an outbound lifecycle or per-device convergence event.
type Event struct {
	Type          string                      `json:"type"`
	AccountID     string                      `json:"account_id"`
	CorrelationID string                      `json:"correlation_id,omitempty"`
	DeviceID      string                      `json:"device_id,omitempty"`
	Status        domain.JobStatus            `json:"status,omitempty"`
	Devices       map[string]domain.JobStatus `json:"devices,omitempty"`
	Detail        string                      `json:"detail,omitempty"`
}

// EventPublisher emits workflow and per-device events to an external bus (e.g. NATS).
type EventPublisher interface {
	PublishEvent(ctx context.Context, evt Event) error
}

// ApplyStepInput carries the parameters required by ApplyStepActivity.
type ApplyStepInput struct {
	Provider string          `json:"provider"`
	Kind     string          `json:"kind"`
	Resource json.RawMessage `json:"resource"`
	Step     sdk.Step        `json:"step"`
}

// JobStatusInput records per-device convergence status in DeploymentStore.
type JobStatusInput struct {
	AccountID string           `json:"account_id"`
	JobID     string           `json:"job_id"`
	Status    domain.JobStatus `json:"status"`
}

// Activities exposes Stage 1 resolution and Stage 2 orch steps to Temporal workers.
type Activities struct {
	Resolver   *resolver.Resolver
	Reconciler *engine.Reconciler
	Store      *inventory.DeploymentStore
	Publisher  EventPublisher
}

func (a *Activities) ResolveActivity(_ context.Context, spec domain.ProvisionSpec) (domain.ExecutionPlan, error) {
	plan, err := a.Resolver.Resolve(spec)
	if err != nil {
		return domain.ExecutionPlan{}, classifyErr(err)
	}
	return plan, nil
}

func (a *Activities) PlanActivity(ctx context.Context, req sdk.Request) (sdk.Plan, error) {
	plan, err := a.Reconciler.Plan(ctx, req)
	if err != nil {
		return sdk.Plan{}, classifyErr(err)
	}
	return plan, nil
}

func (a *Activities) ApplyStepActivity(ctx context.Context, in ApplyStepInput) error {
	if err := a.Reconciler.ApplyStep(ctx, in.Provider, in.Kind, in.Resource, in.Step); err != nil {
		return classifyErr(err)
	}
	return nil
}

func (a *Activities) MarkUtilityReadyActivity(_ context.Context, accountID string) error {
	if rec, ok := a.Store.Get(accountID); ok {
		rec.UtilityReady = true
		a.Store.Save(rec)
	}
	return nil
}

func (a *Activities) MarkPartnerAckedActivity(_ context.Context, accountID string) error {
	if rec, ok := a.Store.Get(accountID); ok {
		rec.PartnerAcked = true
		a.Store.Save(rec)
	}
	return nil
}

func (a *Activities) RecordJobStatusActivity(_ context.Context, in JobStatusInput) error {
	if rec, ok := a.Store.Get(in.AccountID); ok {
		if rec.JobStates == nil {
			rec.JobStates = make(map[string]domain.JobStatus)
		}
		rec.JobStates[in.JobID] = in.Status
		a.Store.Save(rec)
	}
	return nil
}

func (a *Activities) PublishEventActivity(ctx context.Context, evt Event) error {
	if a.Publisher == nil {
		return nil
	}
	return a.Publisher.PublishEvent(ctx, evt)
}

func classifyErr(err error) error {
	if sdk.IsTerminal(err) {
		return temporal.NewNonRetryableApplicationError(err.Error(), "TerminalError", err)
	}
	return err
}
