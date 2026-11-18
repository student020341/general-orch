package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"orch/internal/bus"
	"orch/internal/config"
	"orch/internal/reconciler"
	"orch/internal/statemanager"
	"orch/internal/store/postgres"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil)).With("component", "state-manager")
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

	rec := reconciler.New(reconciler.DefaultCatalog())
	mgr := statemanager.New(st, rec, natsBus, logger)

	logger.Info("state-manager running")
	if err := mgr.Run(ctx, natsBus); err != nil {
		logger.Error("state-manager failed", "err", err)
		os.Exit(1)
	}
}
