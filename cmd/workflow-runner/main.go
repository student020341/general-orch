package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"go.temporal.io/sdk/client"

	"orch/internal/bus"
	"orch/internal/config"
	"orch/internal/orchestrator"
	"orch/internal/store/postgres"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil)).With("component", "workflow-runner")
	cfg := config.Load()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	st, err := postgres.New(ctx, cfg.PostgresDSN)
	if err != nil {
		logger.Error("postgres init failed", "err", err)
		os.Exit(1)
	}
	defer st.Close()

	natsBus, err := bus.Connect(ctx, cfg.NatsURL, logger)
	if err != nil {
		logger.Error("nats init failed", "err", err)
		os.Exit(1)
	}
	defer natsBus.Close()

	tc, err := client.Dial(client.Options{HostPort: cfg.TemporalAddr})
	if err != nil {
		logger.Error("temporal dial failed", "err", err)
		os.Exit(1)
	}
	defer tc.Close()

	w := orchestrator.NewOrchestrationWorker(tc, st, natsBus)
	if err := w.Start(); err != nil {
		logger.Error("start orchestration worker failed", "err", err)
		os.Exit(1)
	}
	defer w.Stop()

	starter := orchestrator.NewPlanStarter(tc, st, logger)
	logger.Info("workflow-runner running", "task_queue", orchestrator.OrchestrationTaskQueue)
	if err := starter.Run(ctx, natsBus); err != nil {
		logger.Error("plan starter failed", "err", err)
		os.Exit(1)
	}
}
