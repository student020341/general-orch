package light

import (
	"testing"

	"orch/internal/model"
)

func TestLightAspectSingleResource(t *testing.T) {
	ap := New()

	e := &model.Entity{
		ID:     "router-1",
		Vendor: "mock",
		Current: model.State{
			"status_led": model.State{"r": 10, "g": 20, "b": 30},
		},
		Target: model.State{
			"status_led": model.State{"r": 255, "g": 0, "b": 128},
		},
	}

	res, err := ap.Plan(e, "status_led", "lighting-workers")
	if err != nil {
		t.Fatalf("Plan failed: %v", err)
	}
	if len(res.Steps) != 1 {
		t.Fatalf("expected 1 step, got %d", len(res.Steps))
	}
	if res.Steps[0].ID != "set-status_led" || res.Steps[0].Payload["resource"] != "status_led" {
		t.Fatalf("unexpected step: %#v", res.Steps[0])
	}

	// Converged slot produces no step.
	e.Current["status_led"] = model.State{"r": 255, "g": 0, "b": 128}
	res2, err := ap.Plan(e, "status_led", "lighting-workers")
	if err != nil || len(res2.Steps) != 0 {
		t.Fatalf("expected 0 steps when converged, got %#v (err=%v)", res2.Steps, err)
	}
}
