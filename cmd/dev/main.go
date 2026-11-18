package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.temporal.io/sdk/client"

	"orch/internal/bus"
	"orch/internal/config"
	"orch/internal/driver"
	"orch/internal/driver/mock"
	"orch/internal/gateway"
	"orch/internal/model"
	"orch/internal/orchestrator"
	"orch/internal/reconciler"
	"orch/internal/statemanager"
	"orch/internal/store/postgres"
)

func main() {
	baseLogger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	cfg := config.Load()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	st, err := postgres.New(ctx, cfg.PostgresDSN)
	if err != nil {
		baseLogger.Error("postgres init failed", "err", err)
		os.Exit(1)
	}
	defer st.Close()

	// Separate NATS connections per service boundary so dev mirrors production wiring.
	gwBus, err := bus.Connect(ctx, cfg.NatsURL, baseLogger.With("component", "gateway"))
	if err != nil {
		baseLogger.Error("gateway nats failed", "err", err)
		os.Exit(1)
	}
	defer gwBus.Close()

	smBus, err := bus.Connect(ctx, cfg.NatsURL, baseLogger.With("component", "state-manager"))
	if err != nil {
		baseLogger.Error("state-manager nats failed", "err", err)
		os.Exit(1)
	}
	defer smBus.Close()

	wfBus, err := bus.Connect(ctx, cfg.NatsURL, baseLogger.With("component", "workflow-runner"))
	if err != nil {
		baseLogger.Error("workflow-runner nats failed", "err", err)
		os.Exit(1)
	}
	defer wfBus.Close()

	tc, err := client.Dial(client.Options{HostPort: cfg.TemporalAddr})
	if err != nil {
		baseLogger.Error("temporal dial failed", "err", err)
		os.Exit(1)
	}
	defer tc.Close()

	mockDrv := mock.New("mock", 150*time.Millisecond)
	reg := driver.NewRegistry()
	reg.Register(mockDrv)

	// Start Temporal workers.
	orchWorker := orchestrator.NewOrchestrationWorker(tc, st, wfBus)
	if err := orchWorker.Start(); err != nil {
		baseLogger.Error("orchestration worker failed", "err", err)
		os.Exit(1)
	}
	defer orchWorker.Stop()

	lightingWorker := orchestrator.NewDomainWorker(tc, "lighting-workers", reg, 0)
	motionWorker := orchestrator.NewDomainWorker(tc, "motion-workers", reg, 0)
	piWorker := orchestrator.NewDomainWorker(tc, "pi-workers", reg, 1)
	for name, w := range map[string]interface{ Start() error }{
		"lighting-workers": lightingWorker,
		"motion-workers":   motionWorker,
		"pi-workers":       piWorker,
	} {
		if err := w.Start(); err != nil {
			baseLogger.Error("domain worker failed", "queue", name, "err", err)
			os.Exit(1)
		}
	}
	defer lightingWorker.Stop()
	defer motionWorker.Stop()
	defer piWorker.Stop()

	// Start NATS-backed StateManager and PlanStarter.
	rec := reconciler.New(reconciler.DefaultCatalog())
	mgr := statemanager.New(st, rec, smBus, baseLogger.With("component", "state-manager"))
	starter := orchestrator.NewPlanStarter(tc, st, baseLogger.With("component", "workflow-runner"))

	errCh := make(chan error, 3)
	go func() { errCh <- mgr.Run(ctx, smBus) }()
	go func() { errCh <- starter.Run(ctx, wfBus) }()

	// Mount Gateway plus dev-only mock device inspection/drift endpoints.
	gw := gateway.New(st, gwBus)
	rootMux := http.NewServeMux()
	rootMux.HandleFunc("GET /_dev/mock/{id}", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(mockDrv.Snapshot(r.PathValue("id")))
	})
	rootMux.HandleFunc("POST /_dev/mock/{id}", func(w http.ResponseWriter, r *http.Request) {
		var patch model.State
		if err := json.NewDecoder(r.Body).Decode(&patch); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		mockDrv.SetDeviceState(r.PathValue("id"), patch)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(mockDrv.Snapshot(r.PathValue("id")))
	})
	rootMux.Handle("/", gw)

	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           rootMux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	baseLogger.Info("dev runner started",
		"http_addr", cfg.HTTPAddr,
		"nats_url", cfg.NatsURL,
		"temporal_addr", cfg.TemporalAddr,
	)

	select {
	case <-ctx.Done():
	case err := <-errCh:
		if err != nil {
			baseLogger.Error("service exited with error", "err", err)
		}
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
}
