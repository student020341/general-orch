package engine_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"orch/engine"
	"orch/sdk"
)

type fakeDriver struct {
	info             sdk.DriverInfo
	validateResource func(json.RawMessage) error
	validateTarget   func(json.RawMessage) error
	readFn           func(context.Context, json.RawMessage) (json.RawMessage, error)
	planFn           func(context.Context, json.RawMessage, json.RawMessage) (sdk.Plan, error)
	applyFn          func(context.Context, json.RawMessage, sdk.Step) error
}

func (f *fakeDriver) Info() sdk.DriverInfo { return f.info }
func (f *fakeDriver) Schema() []byte       { return []byte(`{}`) }
func (f *fakeDriver) ValidateResource(r json.RawMessage) error {
	if f.validateResource != nil {
		return f.validateResource(r)
	}
	return nil
}
func (f *fakeDriver) ValidateTarget(t json.RawMessage) error {
	if f.validateTarget != nil {
		return f.validateTarget(t)
	}
	return nil
}
func (f *fakeDriver) Read(ctx context.Context, t json.RawMessage) (json.RawMessage, error) {
	if f.readFn != nil {
		return f.readFn(ctx, t)
	}
	return json.RawMessage(`{}`), nil
}
func (f *fakeDriver) Plan(ctx context.Context, cur, des json.RawMessage) (sdk.Plan, error) {
	if f.planFn != nil {
		return f.planFn(ctx, cur, des)
	}
	return sdk.Plan{}, nil
}
func (f *fakeDriver) ApplyStep(ctx context.Context, t json.RawMessage, s sdk.Step) error {
	if f.applyFn != nil {
		return f.applyFn(ctx, t, s)
	}
	return nil
}

type fakeProvider struct {
	name    string
	drivers []sdk.Driver
}

func (p *fakeProvider) Name() string          { return p.name }
func (p *fakeProvider) Drivers() []sdk.Driver { return p.drivers }

func TestRegistryProviderAndResolve(t *testing.T) {
	reg := engine.NewRegistry()

	p := &fakeProvider{
		name: "mikrotik",
		drivers: []sdk.Driver{
			&fakeDriver{info: sdk.DriverInfo{Provider: "mikrotik", Kind: "rb4011"}},
			&fakeDriver{info: sdk.DriverInfo{Provider: "mikrotik", Kind: "crs305"}},
		},
	}
	if err := reg.RegisterProvider(p); err != nil {
		t.Fatalf("RegisterProvider failed: %v", err)
	}

	// Duplicate should fail
	if err := reg.RegisterDriver(&fakeDriver{info: sdk.DriverInfo{Provider: "mikrotik", Kind: "crs305"}}); !errors.Is(err, engine.ErrDriverExists) {
		t.Fatalf("expected ErrDriverExists, got %v", err)
	}

	// Mismatched provider name should fail atomically
	bad := &fakeProvider{
		name: "google",
		drivers: []sdk.Driver{
			&fakeDriver{info: sdk.DriverInfo{Provider: "google", Kind: "account"}},
			&fakeDriver{info: sdk.DriverInfo{Provider: "other", Kind: "bad"}},
		},
	}
	if err := reg.RegisterProvider(bad); err == nil {
		t.Fatal("expected error for mismatched provider")
	}
	if _, err := reg.Resolve("google", "account"); !errors.Is(err, engine.ErrDriverNotFound) {
		t.Fatalf("expected atomic rollback on failed RegisterProvider, got %v", err)
	}

	// Sorted list check
	list := reg.List()
	if len(list) != 2 || list[0].ID() != "mikrotik/crs305" || list[1].ID() != "mikrotik/rb4011" {
		t.Fatalf("unexpected List output: %+v", list)
	}
}

func TestReconcileNoOpSkipsApply(t *testing.T) {
	reg := engine.NewRegistry()
	applyCalled := false
	_ = reg.RegisterDriver(&fakeDriver{
		info: sdk.DriverInfo{Provider: "diy", Kind: "light"},
		planFn: func(context.Context, json.RawMessage, json.RawMessage) (sdk.Plan, error) {
			return sdk.Plan{}, nil
		},
		applyFn: func(context.Context, json.RawMessage, sdk.Step) error {
			applyCalled = true
			return nil
		},
	})

	rec := engine.NewReconciler(reg)
	plan, res, err := rec.Reconcile(context.Background(), sdk.Request{
		Provider: "diy",
		Kind:     "light",
		Resource: json.RawMessage(`{"id":"1"}`),
		Target:   json.RawMessage(`{}`),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !plan.IsEmpty() || len(res.Groups) != 0 || applyCalled {
		t.Fatalf("expected no-op reconcile, got plan=%+v res=%+v applyCalled=%v", plan, res, applyCalled)
	}
}

func TestApplyPlanGroupOrderingAndParallelism(t *testing.T) {
	reg := engine.NewRegistry()

	var mu sync.Mutex
	var events []string
	var inGroup1 atomic.Int32
	bothStarted := make(chan struct{})

	_ = reg.RegisterDriver(&fakeDriver{
		info: sdk.DriverInfo{Provider: "diy", Kind: "spinner"},
		applyFn: func(ctx context.Context, _ json.RawMessage, s sdk.Step) error {
			if s.ID == "g1-a" || s.ID == "g1-b" {
				if inGroup1.Add(1) == 2 {
					close(bothStarted)
				}
				// Wait until both parallel steps have started to prove concurrency.
				select {
				case <-bothStarted:
				case <-time.After(2 * time.Second):
					return errors.New("parallel steps did not execute concurrently")
				}
			} else if inGroup1.Load() != 2 {
				return errors.New("group 2 started before group 1 finished")
			}

			mu.Lock()
			events = append(events, s.ID)
			mu.Unlock()
			return nil
		},
	})

	rec := engine.NewReconciler(reg)
	plan := sdk.Plan{
		Driver: sdk.DriverInfo{Provider: "diy", Kind: "spinner"},
		Groups: []sdk.StepGroup{
			{
				Name:     "lights",
				Parallel: true,
				Steps: []sdk.Step{
					{ID: "g1-a", Action: "set_light"},
					{ID: "g1-b", Action: "set_light"},
				},
			},
			{
				Name:     "platform",
				Parallel: false,
				Steps: []sdk.Step{
					{ID: "g2-a", Action: "rotate"},
				},
			},
		},
	}

	res, err := rec.ApplyPlan(context.Background(), json.RawMessage(`{}`), plan)
	if err != nil {
		t.Fatalf("ApplyPlan failed: %v", err)
	}
	if len(res.Groups) != 2 || len(events) != 3 || events[2] != "g2-a" {
		t.Fatalf("unexpected execution order: events=%v res=%+v", events, res)
	}
}

func TestApplyPlanStopsAfterFailedGroup(t *testing.T) {
	reg := engine.NewRegistry()
	var ran []string

	_ = reg.RegisterDriver(&fakeDriver{
		info: sdk.DriverInfo{Provider: "diy", Kind: "spinner"},
		applyFn: func(_ context.Context, _ json.RawMessage, s sdk.Step) error {
			ran = append(ran, s.ID)
			if s.ID == "step-1" {
				return errors.New("boom")
			}
			return nil
		},
	})

	rec := engine.NewReconciler(reg)
	plan := sdk.Plan{
		Driver: sdk.DriverInfo{Provider: "diy", Kind: "spinner"},
		Groups: []sdk.StepGroup{
			{
				Name: "first",
				Steps: []sdk.Step{
					{ID: "step-1", Action: "fail"},
					{ID: "step-2", Action: "skip"},
				},
			},
			{
				Name:  "second",
				Steps: []sdk.Step{{ID: "step-3", Action: "skip"}},
			},
		},
	}

	res, err := rec.ApplyPlan(context.Background(), json.RawMessage(`{}`), plan)
	if err == nil {
		t.Fatal("expected error from failed step")
	}
	if len(ran) != 1 || ran[0] != "step-1" {
		t.Fatalf("expected execution to stop at step-1, ran=%v", ran)
	}
	if len(res.Groups) != 1 || len(res.Groups[0].Steps) != 1 || res.Groups[0].Steps[0].Error == "" {
		t.Fatalf("unexpected partial ApplyResult: %+v", res)
	}
}
