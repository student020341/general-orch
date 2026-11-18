package reconciler

import (
	"slices"
	"testing"

	"orch/internal/model"
)

type stubAspect struct {
	name      string
	driftKeys []string
}

func (s stubAspect) Name() string { return s.name }

func (s stubAspect) Schema() model.AspectSchema {
	return model.AspectSchema{Type: s.name, Description: "stub aspect"}
}

func (s stubAspect) Plan(entity *model.Entity, resource string, taskQueue string) (AspectResult, error) {
	if _, ok := entity.Target[resource]; !ok {
		return AspectResult{}, nil
	}
	var drift []string
	if len(s.driftKeys) > 0 {
		drift = []string{resource}
	}
	return AspectResult{
		Steps: []model.PlanStep{{
			ID:         "step-" + resource,
			TaskQueue:  taskQueue,
			Activity:   "ExecuteCapability",
			Capability: s.name,
			Vendor:     entity.Vendor,
			Payload:    model.State{"entity_id": entity.ID, "resource": resource},
			Status:     model.StepPending,
		}},
		DriftKeys: drift,
	}, nil
}

func TestReconcilerGenericDAGAndReload(t *testing.T) {
	cat := DefaultCatalog()
	rec := New(cat)
	rec.RegisterAspect(stubAspect{name: "light"})
	rec.RegisterAspect(stubAspect{name: "rotation", driftKeys: []string{"rotation"}})

	e := &model.Entity{
		ID:      "dev-1",
		Kind:    "beacon_bot_serial",
		Vendor:  "mock",
		Version: 1,
		Target:  model.State{"light1": true, "rotation": 90},
	}

	plan, err := rec.Reconcile(e)
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}
	if len(plan.Steps) != 3 {
		t.Fatalf("expected 3 steps, got %d", len(plan.Steps))
	}
	if !slices.Equal(plan.Steps[2].DependsOn, []string{"fetch-state", "step-light1"}) {
		t.Fatalf("expected serial chain, got %v", plan.Steps[2].DependsOn)
	}

	// Reject unknown resource keys.
	bad := &model.Entity{
		ID:     "dev-bad",
		Kind:   "lamp",
		Vendor: "mock",
		Target: model.State{"light2": true},
	}
	if _, err := rec.Reconcile(bad); err == nil {
		t.Fatalf("expected error for unknown resource on lamp")
	}

	// Hot-swap catalog to add a new custom kind dynamically.
	cat.Replace([]model.DeviceProfile{{
		Kind:              "lights_only",
		Resources:         map[string]string{"status_led": "light"},
		DefaultQueues:     map[string]string{"light": "lighting-workers"},
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
		Target:  model.State{"status_led": true},
	}
	plan2, err := rec.Reconcile(lightEntity)
	if err != nil {
		t.Fatalf("Reconcile new kind failed: %v", err)
	}
	if len(plan2.Steps) != 1 || plan2.Steps[0].ID != "step-status_led" {
		t.Fatalf("unexpected steps for lights_only: %#v", plan2.Steps)
	}
}
