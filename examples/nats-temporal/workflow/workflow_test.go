package workflow_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"go.temporal.io/sdk/testsuite"

	"orch/engine"
	"orch/examples/nats-temporal/domain"
	"orch/examples/nats-temporal/inventory"
	"orch/examples/nats-temporal/providers/controller"
	"orch/examples/nats-temporal/providers/wired"
	"orch/examples/nats-temporal/resolver"
	"orch/examples/nats-temporal/workflow"
)

type recordingPublisher struct {
	mu     sync.Mutex
	events []workflow.Event
}

func (p *recordingPublisher) PublishEvent(_ context.Context, evt workflow.Event) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.events = append(p.events, evt)
	return nil
}

func (p *recordingPublisher) Types() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]string, len(p.events))
	for i, e := range p.events {
		out[i] = e.Type
	}
	return out
}

func newTestEnv(t *testing.T) (*testsuite.TestWorkflowEnvironment, *inventory.Registry, *inventory.DeploymentStore, *wired.MockHardware, *recordingPublisher) {
	t.Helper()

	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

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

	pub := &recordingPublisher{}
	acts := &workflow.Activities{
		Resolver:   resolver.New(inv, store),
		Reconciler: engine.NewReconciler(reg),
		Store:      store,
		Publisher:  pub,
	}
	env.RegisterActivity(acts)
	env.RegisterWorkflow(workflow.DeploymentWorkflow)

	return env, inv, store, hw, pub
}

func TestDeploymentWorkflowWithPartnerAckAndEvents(t *testing.T) {
	env, inv, _, hw, pub := newTestEnv(t)

	inv.AddHub(inventory.HubRecord{ID: "hub-1", Cabinet: "cab-5"})
	inv.AddSplitter(inventory.SplitterRecord{ID: "split-1", HubID: "hub-1", HubPort: 13})
	inv.AddUnit(inventory.UnitRecord{Serial: "UNIT-1", Model: "ssh", UpstreamID: "split-1", PortOrSlot: 2})
	inv.AddChannelRule(inventory.ChannelRule{
		Cabinet:     "cab-5",
		HubID:       "hub-1",
		PartnerSlug: "acme",
		ServiceVLAN: 412,
		UtilityVLAN: 99,
	})
	hw.SeedUnit(wired.UnitDeviceState{Serial: "UNIT-1", Model: "ssh", Online: true})

	// While paused waiting for partner ack, verify live Query status and then deliver Signal.
	env.RegisterDelayedCallback(func() {
		res, err := env.QueryWorkflow(workflow.QueryStatus)
		if err != nil {
			t.Fatalf("QueryWorkflow failed: %v", err)
		}
		var st workflow.DeploymentStatus
		if err := res.Get(&st); err != nil {
			t.Fatal(err)
		}
		if st.Phase != "awaiting_partner_ack" {
			t.Fatalf("expected awaiting_partner_ack phase, got %q", st.Phase)
		}
		env.SignalWorkflow(workflow.SignalPartnerAck, workflow.PartnerAckSignal{ApprovedBy: "partner-ops"})
	}, 10*time.Millisecond)

	env.ExecuteWorkflow(workflow.DeploymentWorkflow, domain.ProvisionSpec{
		AccountID:          "acct-1",
		SubscriptionID:     "sub-1",
		Active:             true,
		Platform:           domain.PlatformWired,
		ServiceType:        domain.ServiceUtility,
		DemarcType:         domain.DemarcMulti,
		IPMode:             "dhcp",
		ConfigTier:         "exclusive",
		PartnerSlug:        "acme",
		RequiresPartnerAck: true,
		UnitSerial:         "UNIT-1",
		TargetSpeedMbps:    500,
		CorrelationID:      "corr-1",
	})

	if !env.IsWorkflowCompleted() {
		t.Fatal("expected workflow to complete")
	}
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow failed: %v", err)
	}

	var status workflow.DeploymentStatus
	if err := env.GetWorkflowResult(&status); err != nil {
		t.Fatal(err)
	}
	if status.Phase != "converged" {
		t.Fatalf("expected converged phase, got %+v", status)
	}
	for _, id := range []string{"hub", "hub-uplink", "splitter", "unit"} {
		if status.Devices[id] != domain.JobConverged {
			t.Fatalf("expected device %q converged, got %+v", id, status.Devices)
		}
	}

	events := pub.Types()
	if len(events) == 0 || events[0] != "partner.ack_required" || events[len(events)-1] != "deployment.converged" {
		t.Fatalf("unexpected event sequence: %v", events)
	}
}

func TestDeploymentWorkflowPartialFailureIsolatesOfflineUnit(t *testing.T) {
	env, inv, store, hw, _ := newTestEnv(t)

	inv.AddHub(inventory.HubRecord{ID: "hub-2", Cabinet: "cab-8"})
	inv.AddChannelRule(inventory.ChannelRule{
		Cabinet:     "cab-8",
		HubID:       "hub-2",
		PartnerSlug: "beta",
		ServiceVLAN: 305,
	})
	inv.AddUnit(inventory.UnitRecord{Serial: "UNIT-OFFLINE", Model: "ssh", UpstreamID: "hub-2", PortOrSlot: 4})
	hw.SeedUnit(wired.UnitDeviceState{Serial: "UNIT-OFFLINE", Model: "ssh", Online: false})

	env.ExecuteWorkflow(workflow.DeploymentWorkflow, domain.ProvisionSpec{
		AccountID:       "acct-offline",
		SubscriptionID:  "sub-offline",
		Active:          true,
		Platform:        domain.PlatformWired,
		ServiceType:     domain.ServiceData,
		DemarcType:      domain.DemarcDirect,
		PartnerSlug:     "beta",
		UnitSerial:      "UNIT-OFFLINE",
		TargetSpeedMbps: 500,
	})

	if !env.IsWorkflowCompleted() || env.GetWorkflowError() == nil {
		t.Fatalf("expected workflow to fail on offline unit, err=%v", env.GetWorkflowError())
	}

	// Hub and hub-uplink must still be persisted as converged while unit is failed.
	rec, ok := store.Get("acct-offline")
	if !ok {
		t.Fatal("expected deployment record")
	}
	if rec.JobStates["hub"] != domain.JobConverged || rec.JobStates["hub-uplink"] != domain.JobConverged || rec.JobStates["unit"] != domain.JobFailed {
		t.Fatalf("unexpected partial job states: %+v", rec.JobStates)
	}
}
