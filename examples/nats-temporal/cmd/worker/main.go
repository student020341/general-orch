package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/nats-io/nats.go"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	"orch/engine"
	"orch/examples/nats-temporal/bus"
	"orch/examples/nats-temporal/inventory"
	"orch/examples/nats-temporal/providers/controller"
	"orch/examples/nats-temporal/providers/wired"
	"orch/examples/nats-temporal/resolver"
	"orch/examples/nats-temporal/workflow"
)

func main() {
	natsURL := envOr("NATS_URL", nats.DefaultURL)
	temporalAddr := envOr("TEMPORAL_ADDRESS", client.DefaultHostPort)

	nc, err := nats.Connect(natsURL)
	if err != nil {
		log.Fatalf("connect NATS (%s): %v", natsURL, err)
	}
	defer nc.Drain()

	tc, err := client.Dial(client.Options{HostPort: temporalAddr})
	if err != nil {
		log.Fatalf("connect Temporal (%s): %v", temporalAddr, err)
	}
	defer tc.Close()

	inv, store, hw, ctrl := seedSampleWorld()

	reg := engine.NewRegistry()
	if err := reg.RegisterProvider(wired.NewProvider(hw)); err != nil {
		log.Fatal(err)
	}
	if err := reg.RegisterProvider(controller.NewProvider(ctrl)); err != nil {
		log.Fatal(err)
	}

	acts := &workflow.Activities{
		Resolver:   resolver.New(inv, store),
		Reconciler: engine.NewReconciler(reg),
		Store:      store,
		Publisher:  bus.NewNATSPublisher(nc),
	}

	w := worker.New(tc, workflow.TaskQueue, worker.Options{})
	w.RegisterWorkflow(workflow.DeploymentWorkflow)
	w.RegisterActivity(acts)

	if err := w.Start(); err != nil {
		log.Fatalf("start Temporal worker: %v", err)
	}
	defer w.Stop()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	ingress := bus.NewIngress(nc, tc)
	if err := ingress.Start(ctx); err != nil {
		log.Fatalf("start NATS ingress: %v", err)
	}
	defer ingress.Close()

	log.Printf("worker ready (nats=%s, temporal=%s, queue=%s)", natsURL, temporalAddr, workflow.TaskQueue)
	<-ctx.Done()
	log.Println("shutting down worker")
}

func seedSampleWorld() (*inventory.Registry, *inventory.DeploymentStore, *wired.MockHardware, *controller.MockStore) {
	inv := inventory.NewRegistry()
	store := inventory.NewDeploymentStore()
	hw := wired.NewMockHardware()
	ctrl := controller.NewMockStore()

	inv.AddHub(inventory.HubRecord{ID: "hub-1", Cabinet: "cab-5"})
	inv.AddSplitter(inventory.SplitterRecord{ID: "split-1", HubID: "hub-1", HubPort: 13})
	inv.AddUnit(inventory.UnitRecord{Serial: "UNIT-SSH-1", Model: "ssh", UpstreamID: "split-1", PortOrSlot: 2})
	inv.AddUnit(inventory.UnitRecord{Serial: "UNIT-AUTO-1", Model: "auto", UpstreamID: "hub-1", PortOrSlot: 4})
	inv.AddChannelRule(inventory.ChannelRule{
		Cabinet:     "cab-5",
		HubID:       "hub-1",
		PartnerSlug: "acme",
		ServiceVLAN: 412,
		UtilityVLAN: 99,
	})

	hw.SeedUnit(wired.UnitDeviceState{Serial: "UNIT-SSH-1", Model: "ssh", Online: true})
	hw.SeedUnit(wired.UnitDeviceState{Serial: "UNIT-AUTO-1", Model: "auto", Online: true})
	return inv, store, hw, ctrl
}

func envOr(k, fallback string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return fallback
}
