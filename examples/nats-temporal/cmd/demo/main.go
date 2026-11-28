package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/nats-io/nats.go"

	"orch/examples/nats-temporal/bus"
	"orch/examples/nats-temporal/domain"
	"orch/examples/nats-temporal/workflow"
)

func main() {
	natsURL := nats.DefaultURL
	if v := os.Getenv("NATS_URL"); v != "" {
		natsURL = v
	}

	nc, err := nats.Connect(natsURL)
	if err != nil {
		log.Fatalf("connect NATS (%s): %v", natsURL, err)
	}
	defer nc.Drain()

	done := make(chan struct{})
	sub, err := nc.Subscribe(bus.SubjectEventsPrefix+"acct-demo", func(msg *nats.Msg) {
		var evt workflow.Event
		if err := json.Unmarshal(msg.Data, &evt); err != nil {
			log.Printf("decode event: %v", err)
			return
		}
		fmt.Printf("[event] type=%-24s device=%-10s status=%-10s detail=%s\n",
			evt.Type, evt.DeviceID, evt.Status, evt.Detail)

		if evt.Type == "partner.ack_required" {
			fmt.Println("-> simulating external partner confirmation via NATS...")
			ackPayload, _ := json.Marshal(workflow.PartnerAckSignal{ApprovedBy: "demo-partner"})
			if err := nc.Publish(bus.SubjectPartnerAckPrefix+evt.AccountID, ackPayload); err != nil {
				log.Printf("publish ack: %v", err)
			}
		}
		if evt.Type == "deployment.converged" || evt.Type == "deployment.partial_failure" {
			close(done)
		}
	})
	if err != nil {
		log.Fatal(err)
	}
	defer sub.Unsubscribe()

	spec := domain.ProvisionSpec{
		AccountID:          "acct-demo",
		SubscriptionID:     "sub-demo",
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
		CorrelationID:      "demo-corr-1",
	}

	payload, err := json.Marshal(spec)
	if err != nil {
		log.Fatal(err)
	}
	if err := nc.Publish(bus.SubjectProvisionRequest, payload); err != nil {
		log.Fatal(err)
	}
	if err := nc.Flush(); err != nil {
		log.Fatal(err)
	}
	fmt.Println("-> published ProvisionSpec for acct-demo (500 Mbps, utility, partner ack)")

	select {
	case <-done:
		fmt.Println("-> workflow completed")
	case <-time.After(30 * time.Second):
		log.Fatal("timed out waiting for deployment completion")
	}
}
