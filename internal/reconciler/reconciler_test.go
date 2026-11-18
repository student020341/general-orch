package reconciler

import (
	"slices"
	"testing"

	"orch/internal/model"
)

type stubAspect struct {
	name      string
	stepID    string
	driftKeys []string
}

func (s stubAspect) Name() string { return s.name }

func (s stubAspect) Plan(entity *model.Entity, taskQueue string) (AspectResult, error) {
	if _, ok := entity.Target[s.name]; !ok {
		return AspectResult{}, nil
	}
	return AspectResult{
		Steps: []model.PlanStep{{
			ID:         s.stepID,
			TaskQueue:  taskQueue,
			Activity:   "ExecuteCapability",
			Capability: s.name,
			Vendor:     entity.Vendor,
			Payload:    model.State{"entity_id": entity.ID},
			Status:     model.StepPending,
		}},
		DriftKeys: s.driftKeys,
	}, nil
}

func TestReconcilerGenericDAGAndReload(t *testing.T) {
	cat := DefaultCatalog()
	rec := New(cat)
	rec.RegisterAspect(stubAspect{name: "lights", stepID: "step-lights"})
	rec.RegisterAspect(stubAspect{name: "rotation", stepID: "step-rotation", driftKeys: []string{"rotation"}})

	e := &model.Entity{
		ID:      "dev-1",
		Kind:    "beacon_bot_serial",
		Vendor:  "mock",
		Version: 1,
		Target:  model.State{"lights": true, "rotation": 90},
	}

	plan, err := rec.Reconcile(e)
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}
	if len(plan.Steps) != 3 {
		t.Fatalf("expected 3 steps, got %d", len(plan.Steps))
	}
	if !slices.Equal(plan.Steps[2].DependsOn, []string{"fetch-state", "step-lights"}) {
		t.Fatalf("expected serial chain, got %v", plan.Steps[2].DependsOn)
	}

	// Hot-swap catalog to add a new custom kind dynamically.
	cat.Replace([]model.DeviceProfile{{
		Kind:              "lights_only",
		Aspects:           []string{"lights"},
		DefaultQueues:     map[string]string{"lights": "lighting-workers"},
		SupportsLiveState: false,
	}})

	if _, err := rec.Reconcile(e); err == nil {
		t.Fatalf("expected old kind to fail after catalog replace")
	}

	lightEntity := &model.Entity{
		ID:      "lamp-1",
		Kind:    "lights_only",
		Vendor:  "mock",
		Version: 1,
		Target:  model.State{"lights": true},
	}
	plan2, err := rec.Reconcile(lightEntity)
	if err != nil {
		t.Fatalf("Reconcile new kind failed: %v", err)
	}
	if len(plan2.Steps) != 1 || plan2.Steps[0].ID != "step-lights" {
		t.Fatalf("unexpected steps for lights_only: %#v", plan2.Steps)
	}
}
