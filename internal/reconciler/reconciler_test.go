package reconciler

import (
	"slices"
	"testing"

	"orch/internal/model"
)

func TestShortestTurnDelta(t *testing.T) {
	cases := []struct {
		from float64
		to   float64
		want float64
	}{
		{350, 10, 20},
		{10, 350, -20},
		{0, 180, 180},
		{90, 90, 0},
		{270, 45, 135},
		{45, 270, -135},
	}
	for _, tc := range cases {
		got := ShortestTurnDelta(tc.from, tc.to)
		if got != tc.want {
			t.Errorf("ShortestTurnDelta(%v, %v) = %v, want %v", tc.from, tc.to, got, tc.want)
		}
	}
}

func TestReconcileBeaconBotParallelAndSerial(t *testing.T) {
	rec := New(DefaultCatalog())

	t.Run("parallel beacon_bot", func(t *testing.T) {
		e := &model.Entity{
			ID:      "bot-1",
			Kind:    "beacon_bot",
			Vendor:  "mock",
			Version: 1,
			Current: model.State{"rotation": float64(350)},
			Target: model.State{
				"light1":   model.State{"r": 255, "g": 0, "b": 0},
				"light2":   model.State{"r": 0, "g": 0, "b": 255},
				"rotation": 10,
			},
		}
		plan, err := rec.Reconcile(e)
		if err != nil {
			t.Fatalf("Reconcile failed: %v", err)
		}
		if len(plan.Steps) != 4 {
			t.Fatalf("expected 4 steps (fetch + 3 actions), got %d", len(plan.Steps))
		}
		if plan.Steps[0].ID != "fetch-state" || plan.Steps[0].Activity != "FetchState" {
			t.Fatalf("unexpected first step: %#v", plan.Steps[0])
		}
		for _, st := range plan.Steps[1:] {
			if !slices.Equal(st.DependsOn, []string{"fetch-state"}) {
				t.Fatalf("expected step %s to depend only on fetch-state, got %v", st.ID, st.DependsOn)
			}
		}
		turnStep := plan.Steps[3]
		if turnStep.Payload["delta"] != float64(20) {
			t.Fatalf("expected turn delta 20, got %v", turnStep.Payload["delta"])
		}
	})

	t.Run("serial beacon_bot_serial chains steps", func(t *testing.T) {
		e := &model.Entity{
			ID:      "pi-bot-1",
			Kind:    "beacon_bot_serial",
			Vendor:  "mock",
			Version: 1,
			Current: model.State{},
			Target: model.State{
				"light1":   model.State{"r": 10, "g": 20, "b": 30},
				"rotation": 90,
			},
		}
		plan, err := rec.Reconcile(e)
		if err != nil {
			t.Fatalf("Reconcile failed: %v", err)
		}
		if len(plan.Steps) != 3 {
			t.Fatalf("expected 3 steps, got %d", len(plan.Steps))
		}
		if !slices.Equal(plan.Steps[1].DependsOn, []string{"fetch-state"}) {
			t.Fatalf("step 1 deps = %v", plan.Steps[1].DependsOn)
		}
		if !slices.Equal(plan.Steps[2].DependsOn, []string{"fetch-state", "set-light1"}) {
			t.Fatalf("step 2 deps = %v", plan.Steps[2].DependsOn)
		}
		for _, st := range plan.Steps {
			if st.TaskQueue != "pi-workers" {
				t.Fatalf("expected pi-workers queue, got %s", st.TaskQueue)
			}
		}
	})

	t.Run("converged entity returns nil plan", func(t *testing.T) {
		e := &model.Entity{
			ID:      "bot-2",
			Kind:    "beacon_bot",
			Vendor:  "mock",
			Version: 2,
			Current: model.State{
				"light1":   model.State{"r": float64(1), "g": float64(2), "b": float64(3)},
				"rotation": float64(45),
			},
			Target: model.State{
				"light1":   model.State{"r": float64(1), "g": float64(2), "b": float64(3)},
				"rotation": float64(45),
			},
		}
		plan, err := rec.Reconcile(e)
		if err != nil {
			t.Fatalf("Reconcile failed: %v", err)
		}
		if plan != nil {
			t.Fatalf("expected nil plan when converged, got %#v", plan)
		}
	})
}
