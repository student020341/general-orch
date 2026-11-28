# `examples/nats-temporal`

A standalone Go submodule demonstrating how a consuming service wraps `orch` (`sdk/` + `engine/`) with **NATS** and **Temporal**, modeled around an abstract `unit -> splitter -> hub` wired topology and an immutable central `controller` platform.

## Components

* **`cmd/worker`** — Long-running backend service. Connects to both NATS and Temporal, seeds sample inventory/mock hardware (`UNIT-SSH-1`, `UNIT-AUTO-1`, `split-1`, `hub-1`), registers `wired` and `controller` drivers in `orch`, runs the Temporal worker (`DeploymentWorkflow` + activities), and bridges inbound NATS messages into Temporal workflows/signals.
* **`cmd/demo`** — Short-lived test client simulating an upstream platform and partner. Connects only to NATS, subscribes to `orch.events.acct-demo` to print progress, publishes a sample `ProvisionSpec` (`acct-demo`, `UNIT-SSH-1`, `500` Mbps, `utility`, `requires_partner_ack: true`), automatically sends the `orch.partner.ack.acct-demo` confirmation when `partner.ack_required` arrives, and exits on completion.

### NATS subjects
* `orch.provision.request` — Inbound `domain.ProvisionSpec` JSON (starts `deploy-<account_id>` workflow).
* `orch.partner.ack.<account_id>` — External partner confirmation (delivers the `partner_ack` Temporal Signal).
* `orch.events.<account_id>` — Outbound lifecycle and per-device status events (`partner.ack_required`, `device.converged`, `device.failed`, `deployment.converged`, `deployment.partial_failure`).

## Run tests offline (no Docker required)

```bash
go test -C examples/nats-temporal -v -race ./...
```

This runs both the in-process `e2e_test.go` suite and Temporal's `testsuite.WorkflowTestSuite` tests in `workflow/workflow_test.go`.

## Run live against NATS + Temporal

1. Start NATS JetStream (`:4222`) and the Temporal dev server (`:7233`, UI at `http://localhost:8233`):
   ```bash
   docker compose -f examples/nats-temporal/docker-compose.yml up -d
   ```
2. Start the worker:
   ```bash
   go run -C examples/nats-temporal ./cmd/worker
   ```
3. In another terminal, send a sample `ProvisionSpec` over NATS:
   ```bash
   go run -C examples/nats-temporal ./cmd/demo
   ```
