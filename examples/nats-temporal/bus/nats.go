package bus

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"

	"github.com/nats-io/nats.go"
	"go.temporal.io/sdk/client"

	"orch/examples/nats-temporal/domain"
	"orch/examples/nats-temporal/workflow"
)

const (
	SubjectProvisionRequest = "orch.provision.request"
	SubjectPartnerAckPrefix = "orch.partner.ack."
	SubjectEventsPrefix     = "orch.events."
)

// WorkflowIDForAccount returns the deterministic Temporal workflow ID used as a per-account lock.
func WorkflowIDForAccount(accountID string) string {
	return "deploy-" + accountID
}

// NATSPublisher implements workflow.EventPublisher over NATS.
type NATSPublisher struct {
	nc *nats.Conn
}

func NewNATSPublisher(nc *nats.Conn) *NATSPublisher {
	return &NATSPublisher{nc: nc}
}

func (p *NATSPublisher) PublishEvent(_ context.Context, evt workflow.Event) error {
	payload, err := json.Marshal(evt)
	if err != nil {
		return err
	}
	return p.nc.Publish(SubjectEventsPrefix+evt.AccountID, payload)
}

// Ingress bridges inbound NATS commands and partner acks into Temporal workflows.
type Ingress struct {
	nc       *nats.Conn
	temporal client.Client
	subs     []*nats.Subscription
}

func NewIngress(nc *nats.Conn, tc client.Client) *Ingress {
	return &Ingress{nc: nc, temporal: tc}
}

// Start subscribes to provision requests and partner ack messages.
func (in *Ingress) Start(ctx context.Context) error {
	reqSub, err := in.nc.Subscribe(SubjectProvisionRequest, func(msg *nats.Msg) {
		var spec domain.ProvisionSpec
		if err := json.Unmarshal(msg.Data, &spec); err != nil {
			log.Printf("invalid provision request: %v", err)
			return
		}
		if spec.AccountID == "" {
			log.Printf("provision request missing account_id")
			return
		}

		opts := client.StartWorkflowOptions{
			ID:        WorkflowIDForAccount(spec.AccountID),
			TaskQueue: workflow.TaskQueue,
		}
		run, err := in.temporal.ExecuteWorkflow(ctx, opts, workflow.DeploymentWorkflow, spec)
		if err != nil {
			log.Printf("failed to start workflow for %s: %v", spec.AccountID, err)
			return
		}
		log.Printf("started workflow %s (run=%s)", run.GetID(), run.GetRunID())
	})
	if err != nil {
		return fmt.Errorf("subscribe %s: %w", SubjectProvisionRequest, err)
	}
	in.subs = append(in.subs, reqSub)

	ackSub, err := in.nc.Subscribe(SubjectPartnerAckPrefix+"*", func(msg *nats.Msg) {
		accountID := strings.TrimPrefix(msg.Subject, SubjectPartnerAckPrefix)
		var sig workflow.PartnerAckSignal
		if len(msg.Data) > 0 {
			_ = json.Unmarshal(msg.Data, &sig)
		}
		wfID := WorkflowIDForAccount(accountID)
		if err := in.temporal.SignalWorkflow(ctx, wfID, "", workflow.SignalPartnerAck, sig); err != nil {
			log.Printf("failed to signal partner ack to %s: %v", wfID, err)
			return
		}
		log.Printf("signaled partner ack to %s", wfID)
	})
	if err != nil {
		_ = in.Close()
		return fmt.Errorf("subscribe %s*: %w", SubjectPartnerAckPrefix, err)
	}
	in.subs = append(in.subs, ackSub)
	return nil
}

// Close unsubscribes all NATS subscriptions.
func (in *Ingress) Close() error {
	for _, s := range in.subs {
		if err := s.Unsubscribe(); err != nil {
			return err
		}
	}
	return nil
}
