package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	enums "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	"orch/internal/bus"
	"orch/internal/driver"
	"orch/internal/events"
	"orch/internal/model"
	"orch/internal/store"
)

// NewOrchestrationWorker creates a Temporal worker for OrchestrationWorkflow and its helper activities.
func NewOrchestrationWorker(tc client.Client, st store.PlanStore, pub bus.Publisher) worker.Worker {
	w := worker.New(tc, OrchestrationTaskQueue, worker.Options{})
	w.RegisterWorkflow(OrchestrationWorkflow)

	acts := &OrchestratorActivities{Store: st, Pub: pub}
	w.RegisterActivityWithOptions(acts.LoadPlan, activity.RegisterOptions{Name: "LoadPlan"})
	w.RegisterActivityWithOptions(acts.PublishStepCompleted, activity.RegisterOptions{Name: "PublishStepCompleted"})
	w.RegisterActivityWithOptions(acts.PublishExecutionFinished, activity.RegisterOptions{Name: "PublishExecutionFinished"})
	return w
}

// NewDomainWorker creates a Temporal worker polling taskQueue for FetchState and ExecuteCapability.
func NewDomainWorker(tc client.Client, taskQueue string, drivers *driver.Registry, maxConcurrent int) worker.Worker {
	opts := worker.Options{}
	if maxConcurrent > 0 {
		opts.MaxConcurrentActivityExecutionSize = maxConcurrent
	}
	w := worker.New(tc, taskQueue, opts)
	acts := &DomainActivities{Drivers: drivers}
	w.RegisterActivityWithOptions(acts.FetchState, activity.RegisterOptions{Name: "FetchState"})
	w.RegisterActivityWithOptions(acts.ExecuteCapability, activity.RegisterOptions{Name: "ExecuteCapability"})
	return w
}

// PlanStarter subscribes to PlanCreated events on JetStream and starts Temporal workflows.
type PlanStarter struct {
	tc     client.Client
	store  store.Store
	logger *slog.Logger
}

// NewPlanStarter constructs a PlanStarter.
func NewPlanStarter(tc client.Client, st store.Store, logger *slog.Logger) *PlanStarter {
	if logger == nil {
		logger = slog.Default()
	}
	return &PlanStarter{tc: tc, store: st, logger: logger}
}

// Run consumes orch.plan.created.> messages until ctx is cancelled.
func (s *PlanStarter) Run(ctx context.Context, b *bus.Bus) error {
	return b.Consume(ctx, "workflow-runner-plans", events.SubjectPlanPrefix+".>", s.handlePlanCreatedMsg)
}

func (s *PlanStarter) handlePlanCreatedMsg(ctx context.Context, data []byte) error {
	var ev events.PlanCreatedEvent
	if err := json.Unmarshal(data, &ev); err != nil {
		return fmt.Errorf("unmarshal plan created: %w", err)
	}
	return s.StartPlan(ctx, ev)
}

// StartPlan starts (or supersedes and restarts) the per-entity OrchestrationWorkflow.
func (s *PlanStarter) StartPlan(ctx context.Context, ev events.PlanCreatedEvent) error {
	entity, err := s.store.GetEntity(ctx, ev.EntityID)
	if err != nil {
		return fmt.Errorf("get entity %s: %w", ev.EntityID, err)
	}
	plan, err := s.store.GetPlan(ctx, ev.PlanID)
	if err != nil {
		return fmt.Errorf("get plan %s: %w", ev.PlanID, err)
	}

	if ev.Version < entity.Version || plan.Status == model.PlanSuperseded {
		s.logger.Info("skipping stale plan",
			"entity_id", ev.EntityID,
			"plan_id", ev.PlanID,
			"plan_version", ev.Version,
			"entity_version", entity.Version,
		)
		return nil
	}

	wfID := "reconcile-" + ev.EntityID
	if err := s.cancelIfRunning(ctx, wfID); err != nil {
		return err
	}

	run, err := s.tc.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:                    wfID,
		TaskQueue:             OrchestrationTaskQueue,
		WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
	}, OrchestrationWorkflow, WorkflowInput{
		PlanID:   ev.PlanID,
		EntityID: ev.EntityID,
		Version:  ev.Version,
	})
	if err != nil {
		return fmt.Errorf("execute workflow %s: %w", wfID, err)
	}

	s.logger.Info("started orchestration workflow",
		"workflow_id", wfID,
		"run_id", run.GetRunID(),
		"plan_id", ev.PlanID,
		"version", ev.Version,
	)
	return nil
}

func (s *PlanStarter) cancelIfRunning(ctx context.Context, wfID string) error {
	desc, err := s.tc.DescribeWorkflowExecution(ctx, wfID, "")
	if err != nil {
		var notFound *serviceerror.NotFound
		if errors.As(err, &notFound) {
			return nil
		}
		return fmt.Errorf("describe workflow %s: %w", wfID, err)
	}
	if desc.WorkflowExecutionInfo.Status != enums.WORKFLOW_EXECUTION_STATUS_RUNNING {
		return nil
	}

	runID := desc.WorkflowExecutionInfo.Execution.RunId
	s.logger.Info("cancelling in-flight workflow for newer plan", "workflow_id", wfID, "run_id", runID)
	if err := s.tc.CancelWorkflow(ctx, wfID, runID); err != nil {
		var notFound *serviceerror.NotFound
		if errors.As(err, &notFound) {
			return nil
		}
		return fmt.Errorf("cancel workflow %s: %w", wfID, err)
	}

	waitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	_ = s.tc.GetWorkflow(waitCtx, wfID, runID).Get(waitCtx, nil)
	return nil
}
