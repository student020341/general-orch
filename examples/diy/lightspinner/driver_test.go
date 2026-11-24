package lightspinner_test

import (
	"context"
	"encoding/json"
	"testing"

	"orch/engine"
	"orch/examples/diy"
	"orch/examples/diy/lightspinner"
	"orch/sdk"
)

func setupReconciler(t *testing.T, initial lightspinner.State) (*engine.Reconciler, *lightspinner.MockStore) {
	t.Helper()

	provider := diy.NewProvider()
	provider.LightSpinner.Set("bench-1", initial)

	reg := engine.NewRegistry()
	if err := reg.RegisterProvider(provider); err != nil {
		t.Fatalf("RegisterProvider failed: %v", err)
	}
	return engine.NewReconciler(reg), provider.LightSpinner
}

func TestPartialConfigAndZeroValues(t *testing.T) {
	initial := lightspinner.State{
		Light1:   lightspinner.LightState{On: true, RGB: [3]uint8{10, 20, 30}, Brightness: 80},
		Light2:   lightspinner.LightState{On: true, RGB: [3]uint8{40, 50, 60}, Brightness: 50},
		Platform: lightspinner.PlatformState{Angle: 90},
	}
	rec, store := setupReconciler(t, initial)

	// Only touch light1.rgb (leaving on/brightness intact), turn light2 off, and set platform angle to 0.
	req := sdk.Request{
		Provider: "diy",
		Kind:     "light-spinner",
		Resource: json.RawMessage(`{"device_id":"bench-1"}`),
		Target: json.RawMessage(`{
			"light1": {"rgb": [255, 0, 128]},
			"light2": {"on": false},
			"platform": {"angle": 0}
		}`),
	}

	plan, res, err := rec.Reconcile(context.Background(), req)
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}
	if len(plan.Groups) != 2 {
		t.Fatalf("expected 2 groups (lights + platform), got %d", len(plan.Groups))
	}
	if plan.Groups[0].Name != "lights" || !plan.Groups[0].Parallel || len(plan.Groups[0].Steps) != 2 {
		t.Fatalf("unexpected lights group: %+v", plan.Groups[0])
	}
	if plan.Groups[1].Name != "platform" || plan.Groups[1].Parallel || len(plan.Groups[1].Steps) != 1 {
		t.Fatalf("unexpected platform group: %+v", plan.Groups[1])
	}
	if len(res.Groups) != 2 {
		t.Fatalf("expected 2 result groups, got %d", len(res.Groups))
	}

	got, ok := store.Get("bench-1")
	if !ok {
		t.Fatal("device missing from mock store")
	}
	want := lightspinner.State{
		Light1:   lightspinner.LightState{On: true, RGB: [3]uint8{255, 0, 128}, Brightness: 80},
		Light2:   lightspinner.LightState{On: false, RGB: [3]uint8{40, 50, 60}, Brightness: 50},
		Platform: lightspinner.PlatformState{Angle: 0},
	}
	if got != want {
		t.Fatalf("state mismatch:\ngot:  %+v\nwant: %+v", got, want)
	}

	// Re-applying the same partial config should yield an empty plan.
	plan2, err := rec.Plan(context.Background(), req)
	if err != nil {
		t.Fatalf("second Plan failed: %v", err)
	}
	if !plan2.IsEmpty() {
		t.Fatalf("expected idempotent empty plan, got %+v", plan2)
	}
}

func TestValidationRejectsInvalidFields(t *testing.T) {
	rec, _ := setupReconciler(t, lightspinner.State{})

	cases := []struct {
		name     string
		resource string
		target   string
	}{
		{
			name:     "unknown target field",
			resource: `{"device_id":"bench-1"}`,
			target:   `{"light3":{"on":true}}`,
		},
		{
			name:     "out of range angle",
			resource: `{"device_id":"bench-1"}`,
			target:   `{"platform":{"angle":360}}`,
		},
		{
			name:     "missing device_id",
			resource: `{}`,
			target:   `{"platform":{"angle":45}}`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := rec.Plan(context.Background(), sdk.Request{
				Provider: "diy",
				Kind:     "light-spinner",
				Resource: json.RawMessage(tc.resource),
				Target:   json.RawMessage(tc.target),
			})
			if err == nil {
				t.Fatal("expected validation error, got nil")
			}
		})
	}
}
