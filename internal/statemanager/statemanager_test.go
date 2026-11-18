package statemanager

import (
	"context"
	"testing"
	"time"

	"orch/internal/events"
	"orch/internal/model"
	"orch/internal/reconciler"
	"orch/internal/store/memory"
)

type fakePub struct {
	subjects []string
	payloads []any
}

func (f *fakePub) PublishJSON(_ context.Context, subject string, payload any) error {
	f.subjects = append(f.subjects, subject)
	f.payloads = append(f.payloads, payload)
	return nil
}

func TestStateManagerPartialUpdateAndDriftReplan(t *testing.T) {
	ctx := context.Background()
	st := memory.New()
	pub := &fakePub{}
	mgr := New(st, reconciler.New(reconciler.DefaultCatalog()), pub, nil)

	// 1. Submit partial patch 1 (light1)
	if err := mgr.HandleIntentRequested(ctx, events.IntentRequestedEvent{
		EntityID: "bot-1",
		Kind:     "beacon_bot",
		Vendor:   "mock",
		Patch:    model.State{"light1": model.State{"r": 0, "g": 0, "b": 255}},
	}); err != nil {
		t.Fatalf("intent 1: %v", err)
	}

	// 2. Submit partial patch 2 (rotation) -> merged into single target
	if err := mgr.HandleIntentRequested(ctx, events.IntentRequestedEvent{
		EntityID: "bot-1",
		Patch:    model.State{"rotation": 90},
	}); err != nil {
		t.Fatalf("intent 2: %v", err)
	}

	entity, err := st.GetEntity(ctx, "bot-1")
	if err != nil {
		t.Fatalf("get entity: %v", err)
	}
	if entity.Version != 2 {
		t.Fatalf("expected version 2, got %d", entity.Version)
	}
	if len(entity.Target) != 2 {
		t.Fatalf("expected merged target with light1 and rotation, got %#v", entity.Target)
	}

	if len(pub.payloads) != 2 {
		t.Fatalf("expected 2 PlanCreatedEvent publishes, got %d", len(pub.payloads))
	}
	planEv := pub.payloads[1].(events.PlanCreatedEvent)

	// 3. Simulate partial step completion (light1 succeeded) followed by failure on turn.
	if err := mgr.HandleStepCompleted(ctx, events.StepCompletedEvent{
		PlanID:        planEv.PlanID,
		EntityID:      "bot-1",
		Version:       2,
		StepID:        "set-light1",
		Capability:    "set_light",
		ObservedDelta: model.State{"light1": model.State{"r": float64(0), "g": float64(0), "b": float64(255)}},
		CompletedAt:   time.Now().UTC(),
	}); err != nil {
		t.Fatalf("step completed: %v", err)
	}

	if err := mgr.HandleExecutionFinished(ctx, events.ExecutionFinishedEvent{
		PlanID:     planEv.PlanID,
		EntityID:   "bot-1",
		Version:    2,
		Outcome:    events.OutcomeFailed,
		Error:      "motor timeout",
		FinishedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("execution finished: %v", err)
	}

	entity, _ = st.GetEntity(ctx, "bot-1")
	if entity.Status != model.StatusFailed {
		t.Fatalf("expected failed status, got %s", entity.Status)
	}
	if entity.Current["light1"] == nil {
		t.Fatalf("expected partial current update for light1, got %#v", entity.Current)
	}

	// 4. Simulate live drift detected during a subsequent plan: rotation is actually 45.
	if err := mgr.HandleStepCompleted(ctx, events.StepCompletedEvent{
		PlanID:      planEv.PlanID,
		EntityID:    "bot-1",
		Version:     2,
		StepID:      "fetch-state",
		Capability:  "fetch_state",
		FullState:   model.State{"light1": model.State{"r": float64(0), "g": float64(0), "b": float64(255)}, "rotation": float64(45)},
		CompletedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("fetch-state step: %v", err)
	}

	if err := mgr.HandleExecutionFinished(ctx, events.ExecutionFinishedEvent{
		PlanID:     planEv.PlanID,
		EntityID:   "bot-1",
		Version:    2,
		Outcome:    events.OutcomeDriftReplan,
		FinishedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("drift replan: %v", err)
	}

	if len(pub.payloads) != 3 {
		t.Fatalf("expected drift replan to publish 3rd plan, got %d", len(pub.payloads))
	}
	replanEv := pub.payloads[2].(events.PlanCreatedEvent)
	replanned, err := st.GetPlan(ctx, replanEv.PlanID)
	if err != nil {
		t.Fatalf("get replan: %v", err)
	}
	// Light1 already converged, so only fetch-state + turn(+45) should be in the new plan.
	if len(replanned.Steps) != 2 {
		t.Fatalf("expected 2 steps in replanned DAG, got %d: %#v", len(replanned.Steps), replanned.Steps)
	}
	if replanned.Steps[1].Payload["delta"] != float64(45) {
		t.Fatalf("expected delta 45 from drifted state 45->90, got %v", replanned.Steps[1].Payload["delta"])
	}
}
