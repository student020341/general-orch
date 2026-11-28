package natstemporal_test

import (
	"context"
	"fmt"
	"testing"

	"orch/engine"
	"orch/examples/nats-temporal/domain"
	"orch/examples/nats-temporal/inventory"
	"orch/examples/nats-temporal/providers/controller"
	"orch/examples/nats-temporal/providers/wired"
	"orch/examples/nats-temporal/resolver"
	"orch/sdk"
)

type testHarness struct {
	inv      *inventory.Registry
	store    *inventory.DeploymentStore
	hw       *wired.MockHardware
	ctrl     *controller.MockStore
	resolver *resolver.Resolver
	runner   *resolver.LocalRunner
}

func newHarness(t *testing.T) *testHarness {
	t.Helper()

	inv := inventory.NewRegistry()
	store := inventory.NewDeploymentStore()
	hw := wired.NewMockHardware()
	ctrl := controller.NewMockStore()

	reg := engine.NewRegistry()
	if err := reg.RegisterProvider(wired.NewProvider(hw)); err != nil {
		t.Fatal(err)
	}
	if err := reg.RegisterProvider(controller.NewProvider(ctrl)); err != nil {
		t.Fatal(err)
	}

	rec := engine.NewReconciler(reg)
	return &testHarness{
		inv:      inv,
		store:    store,
		hw:       hw,
		ctrl:     ctrl,
		resolver: resolver.New(inv, store),
		runner:   resolver.NewLocalRunner(rec, store),
	}
}

func TestWiredFullLifecycleUtilityMultiSplitterAndTeardown(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)

	h.inv.AddHub(inventory.HubRecord{ID: "hub-1", Cabinet: "cab-5"})
	h.inv.AddSplitter(inventory.SplitterRecord{ID: "split-1", HubID: "hub-1", HubPort: 13})
	h.inv.AddUnit(inventory.UnitRecord{Serial: "UNIT-SSH-1", Model: "ssh", UpstreamID: "split-1", PortOrSlot: 2})
	h.inv.AddChannelRule(inventory.ChannelRule{
		Cabinet:     "cab-5",
		HubID:       "hub-1",
		PartnerSlug: "acme",
		ServiceVLAN: 412,
		UtilityVLAN: 99,
	})
	h.hw.SeedUnit(wired.UnitDeviceState{Serial: "UNIT-SSH-1", Model: "ssh", Online: true})

	spec := domain.ProvisionSpec{
		AccountID:          "acct-100",
		SubscriptionID:     "sub-100",
		Active:             true,
		Platform:           domain.PlatformWired,
		ServiceType:        domain.ServiceUtility,
		DemarcType:         domain.DemarcMulti,
		IPMode:             "dhcp",
		ConfigTier:         "exclusive",
		PartnerSlug:        "acme",
		RequiresPartnerAck: true,
		UnitSerial:         "UNIT-SSH-1",
		TargetSpeedMbps:    500,
		CorrelationID:      "wf-1",
	}

	plan, err := h.resolver.Resolve(spec)
	if err != nil {
		t.Fatalf("Resolve subscribe failed: %v", err)
	}
	if len(plan.PreJobs) != 1 || !plan.RequiresPartnerAck || len(plan.Jobs) != 4 {
		t.Fatalf("unexpected execution plan: %+v", plan)
	}

	ackCalls := 0
	err = h.runner.Run(ctx, plan, func(_ context.Context, p domain.ExecutionPlan) error {
		ackCalls++
		// Utility pre-job must have already run before partner ack fires.
		port := h.hw.GetHubPort("hub-1", 13)
		if !port.Active || port.UtilityVLAN != 99 || port.SpeedMbps != 100 {
			t.Fatalf("expected utility pre-config before partner ack, got %+v", port)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Run subscribe failed: %v", err)
	}
	if ackCalls != 1 {
		t.Fatalf("expected 1 partner ack, got %d", ackCalls)
	}

	port := h.hw.GetHubPort("hub-1", 13)
	if !port.Active || port.SpeedMbps != 500 || port.ServiceVLAN != 412 || port.UtilityVLAN != 99 {
		t.Fatalf("expected hub port at intended demand 500 Mbps, got %+v", port)
	}
	if uplink := h.hw.GetHubUplink("hub-1"); !uplink.Active || uplink.SpeedMbps != 500 {
		t.Fatalf("expected hub uplink at 500 Mbps, got %+v", uplink)
	}
	slot := h.hw.GetSplitterSlot("split-1", 2)
	if !slot.Active || slot.SpeedMbps != 500 || slot.ServiceVLAN != 412 || slot.ConfigTier != "exclusive" {
		t.Fatalf("unexpected splitter slot state: %+v", slot)
	}
	unit, _ := h.hw.GetUnit("UNIT-SSH-1")
	if !unit.Active || unit.SpeedMbps != 500 || unit.LeaseEpoch != 1 {
		t.Fatalf("unexpected unit state: %+v", unit)
	}

	// Speed change should skip utility pre-job and partner ack since both already completed.
	spec.TargetSpeedMbps = 1000
	plan2, err := h.resolver.Resolve(spec)
	if err != nil {
		t.Fatalf("Resolve speed change failed: %v", err)
	}
	if len(plan2.PreJobs) != 0 || plan2.RequiresPartnerAck {
		t.Fatalf("expected no pre-jobs or partner ack on speed change: %+v", plan2)
	}
	if err := h.runner.Run(ctx, plan2, nil); err != nil {
		t.Fatalf("Run speed change failed: %v", err)
	}
	slot = h.hw.GetSplitterSlot("split-1", 2)
	if slot.SpeedMbps != 1000 {
		t.Fatalf("expected splitter slot speed=1000, got %+v", slot)
	}
	if port := h.hw.GetHubPort("hub-1", 13); port.SpeedMbps != 1000 {
		t.Fatalf("expected hub port to scale to 1000 Mbps, got %+v", port)
	}
	unit, _ = h.hw.GetUnit("UNIT-SSH-1")
	if unit.SpeedMbps != 1000 || unit.LeaseEpoch != 2 {
		t.Fatalf("expected unit speed=1000 and LeaseEpoch=2, got %+v", unit)
	}

	// Teardown (Active: false) clears unit, splitter slot, and hub port/uplink when last customer leaves.
	spec.Active = false
	plan3, err := h.resolver.Resolve(spec)
	if err != nil {
		t.Fatalf("Resolve teardown failed: %v", err)
	}
	if err := h.runner.Run(ctx, plan3, nil); err != nil {
		t.Fatalf("Run teardown failed: %v", err)
	}
	if h.hw.GetSplitterSlot("split-1", 2).Active || h.hw.GetHubPort("hub-1", 13).Active || h.hw.GetHubUplink("hub-1").Active {
		t.Fatal("expected splitter slot, hub port, and hub uplink to be cleared when demand drops to 0")
	}
	unit, _ = h.hw.GetUnit("UNIT-SSH-1")
	if unit.Active {
		t.Fatalf("expected unit inactive after teardown, got %+v", unit)
	}
}

func TestWiredAutoUnitAndPartialFailureForwardConvergence(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)

	h.inv.AddHub(inventory.HubRecord{ID: "hub-2", Cabinet: "cab-8"})
	h.inv.AddChannelRule(inventory.ChannelRule{
		Cabinet:     "cab-8",
		HubID:       "hub-2",
		PartnerSlug: "beta",
		ServiceVLAN: 305,
	})
	h.inv.AddUnit(inventory.UnitRecord{Serial: "UNIT-AUTO", Model: "auto", UpstreamID: "hub-2", PortOrSlot: 4})
	h.hw.SeedUnit(wired.UnitDeviceState{Serial: "UNIT-AUTO", Model: "auto", Online: true})

	// Auto unit configures hub port and succeeds with no-op on unit.
	autoPlan, err := h.resolver.Resolve(domain.ProvisionSpec{
		AccountID:       "acct-auto",
		SubscriptionID:  "sub-auto",
		Active:          true,
		Platform:        domain.PlatformWired,
		ServiceType:     domain.ServiceData,
		DemarcType:      domain.DemarcDirect,
		PartnerSlug:     "beta",
		UnitSerial:      "UNIT-AUTO",
		TargetSpeedMbps: 250,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := h.runner.Run(ctx, autoPlan, nil); err != nil {
		t.Fatalf("auto unit run failed: %v", err)
	}
	if port := h.hw.GetHubPort("hub-2", 4); !port.Active || port.SpeedMbps != 250 || port.ServiceVLAN != 305 {
		t.Fatalf("expected direct hub-2 port 4 at 250 Mbps, got %+v", port)
	}
	// Direct teardown clears the dedicated hub port.
	directTeardown, err := h.resolver.Resolve(domain.ProvisionSpec{
		AccountID:      "acct-auto",
		SubscriptionID: "sub-auto",
		Active:         false,
		Platform:       domain.PlatformWired,
		UnitSerial:     "UNIT-AUTO",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := h.runner.Run(ctx, directTeardown, nil); err != nil {
		t.Fatalf("direct teardown failed: %v", err)
	}
	if h.hw.GetHubPort("hub-2", 4).Active {
		t.Fatal("expected direct hub port to be cleared on teardown")
	}

	// Offline SSH unit leaves hub converged and unit failed (retryable).
	h.inv.AddUnit(inventory.UnitRecord{Serial: "UNIT-OFFLINE", Model: "ssh", UpstreamID: "hub-2", PortOrSlot: 5})
	h.hw.SeedUnit(wired.UnitDeviceState{Serial: "UNIT-OFFLINE", Model: "ssh", Online: false})

	spec := domain.ProvisionSpec{
		AccountID:       "acct-offline",
		SubscriptionID:  "sub-offline",
		Active:          true,
		Platform:        domain.PlatformWired,
		ServiceType:     domain.ServiceData,
		DemarcType:      domain.DemarcDirect,
		IPMode:          "dhcp",
		ConfigTier:      "standard",
		PartnerSlug:     "beta",
		UnitSerial:      "UNIT-OFFLINE",
		TargetSpeedMbps: 400,
	}
	p1, err := h.resolver.Resolve(spec)
	if err != nil {
		t.Fatal(err)
	}
	err = h.runner.Run(ctx, p1, nil)
	if err == nil {
		t.Fatal("expected unit unreachable error")
	}
	if sdk.IsTerminal(err) {
		t.Fatalf("expected offline unit error to be retryable, got terminal: %v", err)
	}

	rec, _ := h.store.Get("acct-offline")
	if rec.JobStates["hub"] != domain.JobConverged || rec.JobStates["unit"] != domain.JobFailed {
		t.Fatalf("expected partial convergence (hub=converged, unit=failed), got %+v", rec.JobStates)
	}

	// Bring unit online and converge forward without compensating rollbacks.
	h.hw.SetUnitOnline("UNIT-OFFLINE", true)
	p2, err := h.resolver.Resolve(spec)
	if err != nil {
		t.Fatal(err)
	}
	if err := h.runner.Run(ctx, p2, nil); err != nil {
		t.Fatalf("forward convergence retry failed: %v", err)
	}
	rec, _ = h.store.Get("acct-offline")
	if rec.JobStates["hub"] != domain.JobConverged || rec.JobStates["unit"] != domain.JobConverged {
		t.Fatalf("expected all jobs converged, got %+v", rec.JobStates)
	}
}

func TestWiredMissingResourceTeardownIsCleanNoOp(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)

	plan, err := h.resolver.Resolve(domain.ProvisionSpec{
		AccountID:      "acct-missing",
		SubscriptionID: "sub-missing",
		Active:         false,
		Platform:       domain.PlatformWired,
		UnitSerial:     "DOES-NOT-EXIST",
	})
	if err != nil {
		t.Fatalf("unexpected error on missing teardown: %v", err)
	}
	if !plan.IsNoOp() {
		t.Fatalf("expected no-op plan, got %+v", plan)
	}
	if err := h.runner.Run(ctx, plan, nil); err != nil {
		t.Fatalf("no-op run failed: %v", err)
	}
	if _, exists := h.store.Get("acct-missing"); exists {
		t.Fatal("expected no deployment record to be autovivified")
	}
}

func TestControllerImmutableDeleteThenCreateAndTeardown(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)

	spec := domain.ProvisionSpec{
		AccountID:       "acct-ctrl",
		SubscriptionID:  "sub-ctrl-1",
		Active:          true,
		Platform:        domain.PlatformController,
		ConfigTier:      "standard",
		UnitSerial:      "CTRL-UNIT-1",
		TargetSpeedMbps: 300,
	}

	p1, err := h.resolver.Resolve(spec)
	if err != nil {
		t.Fatal(err)
	}
	if err := h.runner.Run(ctx, p1, nil); err != nil {
		t.Fatal(err)
	}
	entry := h.ctrl.Get("sub-ctrl-1")
	if !entry.Exists || entry.ServiceVLAN != 200 || entry.SpeedMbps != 300 {
		t.Fatalf("unexpected controller entry after create: %+v", entry)
	}

	// Changing speed/tier triggers delete-then-create on immutable controller.
	spec.ConfigTier = "exclusive"
	spec.TargetSpeedMbps = 600
	p2, err := h.resolver.Resolve(spec)
	if err != nil {
		t.Fatal(err)
	}
	if err := h.runner.Run(ctx, p2, nil); err != nil {
		t.Fatal(err)
	}
	entry = h.ctrl.Get("sub-ctrl-1")
	if !entry.Exists || entry.ServiceVLAN != 250 || entry.SpeedMbps != 600 {
		t.Fatalf("unexpected controller entry after replace: %+v", entry)
	}

	// Teardown deletes entry.
	spec.Active = false
	p3, err := h.resolver.Resolve(spec)
	if err != nil {
		t.Fatal(err)
	}
	if err := h.runner.Run(ctx, p3, nil); err != nil {
		t.Fatal(err)
	}
	if entry = h.ctrl.Get("sub-ctrl-1"); entry.Exists {
		t.Fatalf("expected controller entry deleted, got %+v", entry)
	}
}

func TestHubAggregatesDirectAndMultiSplitterIntents(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)

	h.inv.AddHub(inventory.HubRecord{ID: "hub-agg", Cabinet: "cab-1"})
	h.inv.AddChannelRule(inventory.ChannelRule{
		Cabinet:     "cab-1",
		HubID:       "hub-agg",
		PartnerSlug: "acme",
		ServiceVLAN: 500,
	})

	// 2 direct customers on ports 1 & 2 (500 + 1000 = 1500 Mbps)
	for port, speed := range map[int]int{1: 500, 2: 1000} {
		serial := fmt.Sprintf("DIRECT-%d", port)
		h.inv.AddUnit(inventory.UnitRecord{Serial: serial, Model: "auto", UpstreamID: "hub-agg", PortOrSlot: port})
		h.hw.SeedUnit(wired.UnitDeviceState{Serial: serial, Model: "auto", Online: true})
		plan, err := h.resolver.Resolve(domain.ProvisionSpec{
			AccountID:       fmt.Sprintf("acct-d%d", port),
			SubscriptionID:  fmt.Sprintf("sub-d%d", port),
			Active:          true,
			Platform:        domain.PlatformWired,
			ServiceType:     domain.ServiceData,
			DemarcType:      domain.DemarcDirect,
			PartnerSlug:     "acme",
			UnitSerial:      serial,
			TargetSpeedMbps: speed,
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := h.runner.Run(ctx, plan, nil); err != nil {
			t.Fatal(err)
		}
	}

	// 3 splitters on ports 11, 12, 13, each feeding 4 customers at 500 Mbps (3 * 2000 = 6000 Mbps)
	for sIdx := 1; sIdx <= 3; sIdx++ {
		splitID := fmt.Sprintf("split-%d", sIdx)
		hubPort := 10 + sIdx
		h.inv.AddSplitter(inventory.SplitterRecord{ID: splitID, HubID: "hub-agg", HubPort: hubPort})
		for slot := 1; slot <= 4; slot++ {
			serial := fmt.Sprintf("S%d-U%d", sIdx, slot)
			h.inv.AddUnit(inventory.UnitRecord{Serial: serial, Model: "auto", UpstreamID: splitID, PortOrSlot: slot})
			h.hw.SeedUnit(wired.UnitDeviceState{Serial: serial, Model: "auto", Online: true})
			plan, err := h.resolver.Resolve(domain.ProvisionSpec{
				AccountID:       fmt.Sprintf("acct-s%d-%d", sIdx, slot),
				SubscriptionID:  fmt.Sprintf("sub-s%d-%d", sIdx, slot),
				Active:          true,
				Platform:        domain.PlatformWired,
				ServiceType:     domain.ServiceData,
				DemarcType:      domain.DemarcMulti,
				ConfigTier:      "standard",
				PartnerSlug:     "acme",
				UnitSerial:      serial,
				TargetSpeedMbps: 500,
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := h.runner.Run(ctx, plan, nil); err != nil {
				t.Fatal(err)
			}
		}
	}

	// Check direct ports, splitter-backed ports, and total hub uplink (1500 + 6000 = 7500 Mbps).
	if p := h.hw.GetHubPort("hub-agg", 1); p.SpeedMbps != 500 {
		t.Fatalf("expected direct port 1 = 500 Mbps, got %+v", p)
	}
	if p := h.hw.GetHubPort("hub-agg", 11); p.SpeedMbps != 2000 {
		t.Fatalf("expected splitter-1 port 11 = 2000 Mbps, got %+v", p)
	}
	if u := h.hw.GetHubUplink("hub-agg"); !u.Active || u.SpeedMbps != 7500 {
		t.Fatalf("expected total hub uplink = 7500 Mbps, got %+v", u)
	}

	// Removing 1 customer on split-1 scales port 11 down to 1500 Mbps and hub uplink to 7000 Mbps,
	// without shutting down port 11.
	teardownOne, err := h.resolver.Resolve(domain.ProvisionSpec{
		AccountID:      "acct-s1-1",
		SubscriptionID: "sub-s1-1",
		Active:         false,
		Platform:       domain.PlatformWired,
		UnitSerial:     "S1-U1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := h.runner.Run(ctx, teardownOne, nil); err != nil {
		t.Fatal(err)
	}
	if p := h.hw.GetHubPort("hub-agg", 11); !p.Active || p.SpeedMbps != 1500 {
		t.Fatalf("expected port 11 active at 1500 Mbps, got %+v", p)
	}
	if u := h.hw.GetHubUplink("hub-agg"); !u.Active || u.SpeedMbps != 7000 {
		t.Fatalf("expected hub uplink scaled to 7000 Mbps, got %+v", u)
	}
}
