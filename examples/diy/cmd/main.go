package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"orch/engine"
	"orch/examples/diy"
	"orch/examples/diy/lightspinner"
	"orch/sdk"
)

func main() {
	ctx := context.Background()

	// 1. Wire up provider(s) and seed mock hardware state.
	provider := diy.NewProvider()
	provider.LightSpinner.Set("bench-1", lightspinner.State{
		Light1:   lightspinner.LightState{On: false, RGB: [3]uint8{0, 0, 0}, Brightness: 0},
		Light2:   lightspinner.LightState{On: true, RGB: [3]uint8{0, 255, 0}, Brightness: 40},
		Platform: lightspinner.PlatformState{Angle: 0},
	})

	registry := engine.NewRegistry()
	if err := registry.RegisterProvider(provider); err != nil {
		log.Fatal(err)
	}
	reconciler := engine.NewReconciler(registry)

	// 2. Submit a partial target config (leaves light2.rgb and light2.brightness untouched).
	req := sdk.Request{
		Provider: "diy",
		Kind:     "light-spinner",
		Resource: json.RawMessage(`{"device_id": "bench-1"}`),
		Target: json.RawMessage(`{
			"light1": {"on": true, "rgb": [255, 80, 0], "brightness": 90},
			"light2": {"on": false},
			"platform": {"angle": 135}
		}`),
	}

	plan, res, err := reconciler.Reconcile(ctx, req)
	if err != nil {
		log.Fatal(err)
	}

	printJSON("Generated Plan", plan)
	printJSON("Apply Result", res)

	state, _ := provider.LightSpinner.Get("bench-1")
	printJSON("Final Device State", state)

	// 3. Running again with the same partial config produces a no-op plan.
	plan2, err := reconciler.Plan(ctx, req)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Second run plan empty: %v\n", plan2.IsEmpty())
}

func printJSON(label string, v any) {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("=== %s ===\n%s\n\n", label, b)
}
