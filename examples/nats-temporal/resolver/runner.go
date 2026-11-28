package resolver

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"orch/engine"
	"orch/examples/nats-temporal/domain"
	"orch/examples/nats-temporal/inventory"
)

// PartnerAckFunc simulates pausing execution until an external partner confirms.
type PartnerAckFunc func(ctx context.Context, plan domain.ExecutionPlan) error

// LocalRunner executes an ExecutionPlan directly in-process using orch/engine.
// In Pass 2, a Temporal workflow will drive these same stages durably.
type LocalRunner struct {
	reconciler *engine.Reconciler
	store      *inventory.DeploymentStore
}

func NewLocalRunner(reconciler *engine.Reconciler, store *inventory.DeploymentStore) *LocalRunner {
	return &LocalRunner{reconciler: reconciler, store: store}
}

// Run executes pre-jobs, waits for optional partner ack, and converges all main jobs respecting DependsOn.
func (l *LocalRunner) Run(ctx context.Context, plan domain.ExecutionPlan, ackFn PartnerAckFunc) error {
	if plan.IsNoOp() {
		return nil
	}

	if len(plan.PreJobs) > 0 {
		if err := l.runJobGraph(ctx, plan.AccountID, plan.PreJobs, false); err != nil {
			return fmt.Errorf("pre-jobs: %w", err)
		}
		l.updateRecord(plan.AccountID, func(rec *domain.DeploymentRecord) {
			rec.UtilityReady = true
		})
	}

	if plan.RequiresPartnerAck {
		if ackFn == nil {
			return errors.New("partner acknowledgement required but no handler provided")
		}
		if err := ackFn(ctx, plan); err != nil {
			return fmt.Errorf("partner ack: %w", err)
		}
		l.updateRecord(plan.AccountID, func(rec *domain.DeploymentRecord) {
			rec.PartnerAcked = true
		})
	}

	return l.runJobGraph(ctx, plan.AccountID, plan.Jobs, true)
}

func (l *LocalRunner) runJobGraph(ctx context.Context, accountID string, jobs []domain.ResourceJob, trackStatus bool) error {
	done := make(map[string]bool, len(jobs))
	remaining := append([]domain.ResourceJob(nil), jobs...)
	var errs []error

	for len(remaining) > 0 {
		var ready, deferred []domain.ResourceJob
		for _, job := range remaining {
			if depsSatisfied(job.DependsOn, done) {
				ready = append(ready, job)
			} else {
				deferred = append(deferred, job)
			}
		}

		if len(ready) == 0 {
			return errors.Join(append(errs, errors.New("unresolvable job dependencies remaining"))...)
		}

		results := make([]error, len(ready))
		var wg sync.WaitGroup
		for i, job := range ready {
			wg.Add(1)
			go func(idx int, j domain.ResourceJob) {
				defer wg.Done()
				_, _, results[idx] = l.reconciler.Reconcile(ctx, j.Request)
			}(i, job)
		}
		wg.Wait()

		for i, job := range ready {
			status := domain.JobConverged
			if results[i] != nil {
				status = domain.JobFailed
				errs = append(errs, fmt.Errorf("job %q: %w", job.ID, results[i]))
			} else {
				done[job.ID] = true
			}
			if trackStatus {
				l.updateRecord(accountID, func(rec *domain.DeploymentRecord) {
					if rec.JobStates == nil {
						rec.JobStates = make(map[string]domain.JobStatus)
					}
					rec.JobStates[job.ID] = status
				})
			}
		}

		remaining = deferred
		if len(errs) > 0 {
			// Drop blocked dependents when an upstream job failed, and run any still-ready jobs.
			var runnable []domain.ResourceJob
			for _, job := range remaining {
				if depsSatisfied(job.DependsOn, done) {
					runnable = append(runnable, job)
				} else if trackStatus {
					l.updateRecord(accountID, func(rec *domain.DeploymentRecord) {
						rec.JobStates[job.ID] = domain.JobPending
					})
				}
			}
			remaining = runnable
		}
	}

	return errors.Join(errs...)
}

func (l *LocalRunner) updateRecord(accountID string, fn func(*domain.DeploymentRecord)) {
	if rec, ok := l.store.Get(accountID); ok {
		fn(&rec)
		l.store.Save(rec)
	}
}

func depsSatisfied(deps []string, done map[string]bool) bool {
	for _, d := range deps {
		if !done[d] {
			return false
		}
	}
	return true
}
