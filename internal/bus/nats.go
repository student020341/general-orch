package bus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"orch/internal/events"
)

// Publisher publishes typed JSON messages to NATS JetStream subjects.
type Publisher interface {
	PublishJSON(ctx context.Context, subject string, payload any) error
}

// Broadcaster publishes fan-out control notifications across all active subscribers.
type Broadcaster interface {
	PublishBroadcast(subject string, payload any) error
}

// Bus manages a NATS connection and JetStream stream/consumers.
type Bus struct {
	nc     *nats.Conn
	js     jetstream.JetStream
	logger *slog.Logger
}

// Connect establishes a NATS connection and ensures the ORCH JetStream stream exists.
func Connect(ctx context.Context, url string, logger *slog.Logger) (*Bus, error) {
	if logger == nil {
		logger = slog.Default()
	}
	nc, err := nats.Connect(url, nats.Name("orch"))
	if err != nil {
		return nil, fmt.Errorf("connect nats: %w", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("create jetstream context: %w", err)
	}
	b := &Bus{nc: nc, js: js, logger: logger}
	if err := b.EnsureStream(ctx); err != nil {
		nc.Close()
		return nil, err
	}
	return b, nil
}

// Close closes the underlying NATS connection.
func (b *Bus) Close() {
	b.nc.Close()
}

// EnsureStream creates or updates the ORCH JetStream stream.
func (b *Bus) EnsureStream(ctx context.Context) error {
	_, err := b.js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:      events.StreamName,
		Subjects:  []string{events.StreamSubjects},
		Storage:   jetstream.FileStorage,
		Retention: jetstream.LimitsPolicy,
		MaxAge:    24 * time.Hour,
	})
	if err != nil {
		return fmt.Errorf("ensure stream %s: %w", events.StreamName, err)
	}
	return nil
}

// PublishJSON marshals payload as JSON and publishes it to JetStream.
func (b *Bus) PublishJSON(ctx context.Context, subject string, payload any) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal %s: %w", subject, err)
	}
	if _, err := b.js.Publish(ctx, subject, data); err != nil {
		return fmt.Errorf("publish %s: %w", subject, err)
	}
	return nil
}

// Consume creates a durable pull consumer on the ORCH stream and runs handler
// for each message until ctx is cancelled.
func (b *Bus) Consume(
	ctx context.Context,
	consumerName string,
	filterSubject string,
	handler func(context.Context, []byte) error,
) error {
	cons, err := b.js.CreateOrUpdateConsumer(ctx, events.StreamName, jetstream.ConsumerConfig{
		Durable:       consumerName,
		FilterSubject: filterSubject,
		AckPolicy:     jetstream.AckExplicitPolicy,
		DeliverPolicy: jetstream.DeliverAllPolicy,
		AckWait:       30 * time.Second,
		MaxDeliver:    5,
	})
	if err != nil {
		return fmt.Errorf("create consumer %s (%s): %w", consumerName, filterSubject, err)
	}

	cc, err := cons.Consume(func(msg jetstream.Msg) {
		msgCtx, cancel := context.WithTimeout(ctx, 25*time.Second)
		defer cancel()

		if err := handler(msgCtx, msg.Data()); err != nil {
			b.logger.Error("message handler failed",
				"consumer", consumerName,
				"subject", msg.Subject(),
				"err", err,
			)
			_ = msg.NakWithDelay(2 * time.Second)
			return
		}
		if err := msg.Ack(); err != nil {
			b.logger.Error("ack failed", "consumer", consumerName, "err", err)
		}
	})
	if err != nil {
		return fmt.Errorf("consume %s: %w", consumerName, err)
	}
	defer cc.Stop()

	<-ctx.Done()
	if errors.Is(ctx.Err(), context.Canceled) {
		return nil
	}
	return ctx.Err()
}

// PublishBroadcast sends a core NATS pub/sub message so every active subscriber instance receives it.
func (b *Bus) PublishBroadcast(subject string, payload any) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal broadcast %s: %w", subject, err)
	}
	if err := b.nc.Publish(subject, data); err != nil {
		return fmt.Errorf("publish broadcast %s: %w", subject, err)
	}
	return b.nc.Flush()
}

// SubscribeBroadcast listens on a core NATS subject (fan-out to all instances) until ctx is cancelled.
func (b *Bus) SubscribeBroadcast(ctx context.Context, subject string, handler func(context.Context, []byte) error) error {
	sub, err := b.nc.Subscribe(subject, func(msg *nats.Msg) {
		hCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		if err := handler(hCtx, msg.Data); err != nil {
			b.logger.Error("broadcast handler failed", "subject", subject, "err", err)
		}
	})
	if err != nil {
		return fmt.Errorf("subscribe broadcast %s: %w", subject, err)
	}
	defer func() { _ = sub.Unsubscribe() }()

	<-ctx.Done()
	if errors.Is(ctx.Err(), context.Canceled) {
		return nil
	}
	return ctx.Err()
}
