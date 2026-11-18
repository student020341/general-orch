package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"orch/internal/bus"
	"orch/internal/config"
	"orch/internal/gateway"
	"orch/internal/store/postgres"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil)).With("component", "gateway")
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

	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           gateway.New(st, natsBus),
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	logger.Info("gateway listening", "addr", cfg.HTTPAddr)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Error("gateway failed", "err", err)
		os.Exit(1)
	}
}
