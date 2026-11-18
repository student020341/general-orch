package orchestrator

import (
	"context"
	"testing"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"

	"orch/internal/aspects"
	"orch/internal/driver"
	"orch/internal/driver/mock"
	"orch/internal/events"
	"orch/internal/model"
	"orch/internal/reconciler"
	"orch/internal/store/memory"
)

type wfFakePub struct {
	steps    []events.StepCompletedEvent
	finished []events.ExecutionFinishedEvent
}

func (p *wfFakePub) PublishJSON(_ context.Context, _ string, payload any) error {
	switch v := payload.(type) {
	case events.StepCompletedEvent:
		p.steps = append(p.steps, v)
	case events.ExecutionFinishedEvent:
		p.finished = append(p.finished, v)
	}
	return nil
}

func TestOrchestrationWorkflowEndToEndAndDrift(t *testing.T) {
	t.Run("converges dual lights and rotation", func(t *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()

		st := memory.New()
		pub := &wfFakePub{}
		drv := mock.New("mock", 0)
		reg := driver.NewRegistry()
		reg.Register(drv)

		entity := &model.Entity{
			ID:      "bot-wf-1",
			Kind:    "beacon_bot",
			Vendor:  "mock",
			Version: 1,
			Current: model.State{},
			Target: model.State{
				"light1":   model.State{"r": 255, "g": 0, "b": 0},
				"light2":   model.State{"r": 0, "g": 255, "b": 0},
				"rotation": 270,
			},
		}
		rec := reconciler.New(reconciler.DefaultCatalog())
		aspects.RegisterAll(rec)
		plan, err := rec.Reconcile(entity)
		if err != nil {
			t.Fatalf("reconcile: %v", err)
		}
		if err := st.CreatePlan(context.Background(), plan); err != nil {
			t.Fatalf("create plan: %v", err)
		}

		orchActs := &OrchestratorActivities{Store: st, Pub: pub}
		domActs := &DomainActivities{Drivers: reg}

		env.RegisterWorkflow(OrchestrationWorkflow)
		env.RegisterActivityWithOptions(orchActs.LoadPlan, activity.RegisterOptions{Name: "LoadPlan"})
		env.RegisterActivityWithOptions(orchActs.PublishStepCompleted, activity.RegisterOptions{Name: "PublishStepCompleted"})
		env.RegisterActivityWithOptions(orchActs.PublishExecutionFinished, activity.RegisterOptions{Name: "PublishExecutionFinished"})
		env.RegisterActivityWithOptions(domActs.FetchState, activity.RegisterOptions{Name: "FetchState"})
		env.RegisterActivityWithOptions(domActs.ExecuteCapability, activity.RegisterOptions{Name: "ExecuteCapability"})

		env.ExecuteWorkflow(OrchestrationWorkflow, WorkflowInput{
			PlanID:   plan.ID,
			EntityID: entity.ID,
			Version:  entity.Version,
		})

		if !env.IsWorkflowCompleted() {
			t.Fatalf("expected workflow to complete")
		}
		if err := env.GetWorkflowError(); err != nil {
			t.Fatalf("workflow error: %v", err)
		}
		if len(pub.finished) != 1 || pub.finished[0].Outcome != events.OutcomeCompleted {
			t.Fatalf("expected completed outcome, got %#v", pub.finished)
		}
		snap := drv.Snapshot("bot-wf-1")
		if snap["rotation"] != float64(270) {
			t.Fatalf("expected mock rotation 270, got %v", snap["rotation"])
		}
	})

	t.Run("detects hardware drift and emits drift_replan without mutating", func(t *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()

		st := memory.New()
		pub := &wfFakePub{}
		drv := mock.New("mock", 0)
		// Simulate physical bump to 45 deg before orchestration runs.
		drv.SetDeviceState("bot-drift-1", model.State{"rotation": float64(45)})
		reg := driver.NewRegistry()
		reg.Register(drv)

		entity := &model.Entity{
			ID:      "bot-drift-1",
			Kind:    "beacon_bot",
			Vendor:  "mock",
			Version: 1,
			Current: model.State{"rotation": float64(0)},
			Target:  model.State{"rotation": 90},
		}
		rec := reconciler.New(reconciler.DefaultCatalog())
		aspects.RegisterAll(rec)
		plan, err := rec.Reconcile(entity)
		if err != nil {
			t.Fatalf("reconcile: %v", err)
		}
		if err := st.CreatePlan(context.Background(), plan); err != nil {
			t.Fatalf("create plan: %v", err)
		}

		orchActs := &OrchestratorActivities{Store: st, Pub: pub}
		domActs := &DomainActivities{Drivers: reg}

		env.RegisterWorkflow(OrchestrationWorkflow)
		env.RegisterActivityWithOptions(orchActs.LoadPlan, activity.RegisterOptions{Name: "LoadPlan"})
		env.RegisterActivityWithOptions(orchActs.PublishStepCompleted, activity.RegisterOptions{Name: "PublishStepCompleted"})
		env.RegisterActivityWithOptions(orchActs.PublishExecutionFinished, activity.RegisterOptions{Name: "PublishExecutionFinished"})
		env.RegisterActivityWithOptions(domActs.FetchState, activity.RegisterOptions{Name: "FetchState"})
		env.RegisterActivityWithOptions(domActs.ExecuteCapability, activity.RegisterOptions{Name: "ExecuteCapability"})

		env.ExecuteWorkflow(OrchestrationWorkflow, WorkflowInput{
			PlanID:   plan.ID,
			EntityID: entity.ID,
			Version:  entity.Version,
		})

		if err := env.GetWorkflowError(); err != nil {
			t.Fatalf("workflow error: %v", err)
		}
		if len(pub.finished) != 1 || pub.finished[0].Outcome != events.OutcomeDriftReplan {
			t.Fatalf("expected drift_replan outcome, got %#v", pub.finished)
		}
		// Hardware must remain at 45 (turn(+90) was NOT executed blindly).
		snap := drv.Snapshot("bot-drift-1")
		if snap["rotation"] != float64(45) {
			t.Fatalf("expected hardware untouched at 45, got %v", snap["rotation"])
		}
	})
}
