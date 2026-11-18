package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	"orch/internal/config"
	"orch/internal/driver"
	"orch/internal/driver/mock"
	"orch/internal/orchestrator"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil)).With("component", "domain-worker")
	cfg := config.Load()

	queuesFlag := flag.String("queues", strings.Join(cfg.TaskQueues, ","), "comma-separated Temporal task queues to poll")
	maxConcFlag := flag.Int("max-concurrent", config.EnvInt("ORCH_MAX_CONCURRENT", 0), "max concurrent activities per queue (0 = default)")
	latencyFlag := flag.Duration("mock-latency", 100*time.Millisecond, "simulated device latency for mock driver")
	flag.Parse()

	queues := splitNonEmpty(*queuesFlag)
	if len(queues) == 0 {
		logger.Error("at least one task queue is required")
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	tc, err := client.Dial(client.Options{HostPort: cfg.TemporalAddr})
	if err != nil {
		logger.Error("temporal dial failed", "err", err)
		os.Exit(1)
	}
	defer tc.Close()

	reg := driver.NewRegistry()
	reg.Register(mock.New("mock", *latencyFlag))

	workers := make([]worker.Worker, 0, len(queues))
	for _, q := range queues {
		w := orchestrator.NewDomainWorker(tc, q, reg, *maxConcFlag)
		if err := w.Start(); err != nil {
			logger.Error("start domain worker failed", "task_queue", q, "err", err)
			os.Exit(1)
		}
		workers = append(workers, w)
		logger.Info("domain worker polling", "task_queue", q, "max_concurrent", *maxConcFlag)
	}

	<-ctx.Done()
	for _, w := range workers {
		w.Stop()
	}
}

func splitNonEmpty(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if trimmed := strings.TrimSpace(p); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}
