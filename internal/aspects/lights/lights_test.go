package lights

import (
	"testing"

	"orch/internal/model"
)

func TestLightsAspectDynamicKeys(t *testing.T) {
	ap := New()

	e := &model.Entity{
		ID:     "lamp-1",
		Vendor: "mock",
		Current: model.State{
			"light1": model.State{"r": 10, "g": 20, "b": 30},
		},
		Target: model.State{
			"light":  model.State{"r": 255, "g": 255, "b": 0},
			"light1": model.State{"r": 10, "g": 20, "b": 30},
			"light3": model.State{"r": 0, "g": 128, "b": 255},
		},
	}

	res, err := ap.Plan(e, "lighting-workers")
	if err != nil {
		t.Fatalf("Plan failed: %v", err)
	}
	if len(res.Steps) != 2 {
		t.Fatalf("expected 2 steps (light and light3), got %d: %#v", len(res.Steps), res.Steps)
	}
	if res.Steps[0].ID != "set-light" || res.Steps[1].ID != "set-light3" {
		t.Fatalf("unexpected step IDs: %s, %s", res.Steps[0].ID, res.Steps[1].ID)
	}
}
