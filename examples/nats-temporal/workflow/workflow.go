package workflow

import (
	"fmt"
	"maps"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"orch/examples/nats-temporal/domain"
	"orch/sdk"
)

const (
	TaskQueue        = "orch-provisioning"
	SignalPartnerAck = "partner_ack"
	QueryStatus      = "status"
)

// PartnerAckSignal is delivered to unblock a deployment waiting for external approval.
type PartnerAckSignal struct {
	ApprovedBy string `json:"approved_by,omitempty"`
}

// DeploymentStatus exposes live per-device convergence state via Temporal Queries.
type DeploymentStatus struct {
	AccountID     string                      `json:"account_id"`
	CorrelationID string                      `json:"correlation_id,omitempty"`
	Phase         string                      `json:"phase"`
	Devices       map[string]domain.JobStatus `json:"devices"`
}

// DeploymentWorkflow coordinates Stage 1 resolution, optional gates, and parallel orch convergence.
func DeploymentWorkflow(ctx workflow.Context, spec domain.ProvisionSpec) (DeploymentStatus, error) {
	status := DeploymentStatus{
		AccountID:     spec.AccountID,
		CorrelationID: spec.CorrelationID,
		Phase:         "resolving",
		Devices:       make(map[string]domain.JobStatus),
	}

	if err := workflow.SetQueryHandler(ctx, QueryStatus, func() (DeploymentStatus, error) {
		return DeploymentStatus{
			AccountID:     status.AccountID,
			CorrelationID: status.CorrelationID,
			Phase:         status.Phase,
			Devices:       maps.Clone(status.Devices),
		}, nil
	}); err != nil {
		return status, err
	}

	actOpts := workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2.0,
			MaximumInterval:    30 * time.Second,
			MaximumAttempts:    5,
		},
	}
	actCtx := workflow.WithActivityOptions(ctx, actOpts)

	var a *Activities
	var plan domain.ExecutionPlan
	if err := workflow.ExecuteActivity(actCtx, a.ResolveActivity, spec).Get(ctx, &plan); err != nil {
		status.Phase = "failed"
		return status, err
	}

	for _, job := range plan.Jobs {
		status.Devices[job.ID] = domain.JobPending
	}

	if plan.IsNoOp() {
		status.Phase = "converged"
		emitEvent(actCtx, Event{
			Type:          "deployment.converged",
			AccountID:     plan.AccountID,
			CorrelationID: plan.CorrelationID,
			Devices:       status.Devices,
		})
		return status, nil
	}

	if len(plan.PreJobs) > 0 {
		status.Phase = "pre_jobs"
		if err := runJobWave(actCtx, plan.AccountID, plan.CorrelationID, plan.PreJobs, false, &status); err != nil {
			status.Phase = "failed"
			return status, err
		}
		if err := workflow.ExecuteActivity(actCtx, a.MarkUtilityReadyActivity, plan.AccountID).Get(ctx, nil); err != nil {
			return status, err
		}
	}

	if plan.RequiresPartnerAck {
		status.Phase = "awaiting_partner_ack"
		emitEvent(actCtx, Event{
			Type:          "partner.ack_required",
			AccountID:     plan.AccountID,
			CorrelationID: plan.CorrelationID,
		})
		var ack PartnerAckSignal
		workflow.GetSignalChannel(ctx, SignalPartnerAck).Receive(ctx, &ack)
		if err := workflow.ExecuteActivity(actCtx, a.MarkPartnerAckedActivity, plan.AccountID).Get(ctx, nil); err != nil {
			return status, err
		}
	}

	status.Phase = "converging"
	if err := runJobGraph(actCtx, plan.AccountID, plan.CorrelationID, plan.Jobs, &status); err != nil {
		status.Phase = "partial_failure"
		emitEvent(actCtx, Event{
			Type:          "deployment.partial_failure",
			AccountID:     plan.AccountID,
			CorrelationID: plan.CorrelationID,
			Devices:       status.Devices,
			Detail:        err.Error(),
		})
		return status, err
	}

	status.Phase = "converged"
	emitEvent(actCtx, Event{
		Type:          "deployment.converged",
		AccountID:     plan.AccountID,
		CorrelationID: plan.CorrelationID,
		Devices:       status.Devices,
	})
	return status, nil
}

func runJobGraph(ctx workflow.Context, accountID, correlationID string, jobs []domain.ResourceJob, status *DeploymentStatus) error {
	done := make(map[string]bool, len(jobs))
	remaining := append([]domain.ResourceJob(nil), jobs...)
	var failed []string

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
			return fmt.Errorf("blocked jobs remaining after failures: %v", failed)
		}

		if err := runJobWave(ctx, accountID, correlationID, ready, true, status); err != nil {
			failed = append(failed, err.Error())
		}
		for _, job := range ready {
			if status.Devices[job.ID] == domain.JobConverged {
				done[job.ID] = true
			}
		}

		if len(failed) > 0 {
			var runnable []domain.ResourceJob
			for _, job := range deferred {
				if depsSatisfied(job.DependsOn, done) {
					runnable = append(runnable, job)
				}
			}
			remaining = runnable
		} else {
			remaining = deferred
		}
	}

	if len(failed) > 0 {
		return fmt.Errorf("%v", failed)
	}
	return nil
}

func runJobWave(ctx workflow.Context, accountID, correlationID string, jobs []domain.ResourceJob, track bool, status *DeploymentStatus) error {
	type outcome struct {
		id  string
		err error
	}
	outcomes := make([]outcome, len(jobs))
	wg := workflow.NewWaitGroup(ctx)
	wg.Add(len(jobs))

	for i, job := range jobs {
		idx := i
		j := job
		workflow.Go(ctx, func(gCtx workflow.Context) {
			defer wg.Done()
			outcomes[idx] = outcome{id: j.ID, err: convergeJob(gCtx, j.Request)}
		})
	}
	wg.Wait(ctx)

	var a *Activities
	var firstErr error
	for _, out := range outcomes {
		jobStatus := domain.JobConverged
		evtType := "device.converged"
		detail := ""
		if out.err != nil {
			jobStatus = domain.JobFailed
			evtType = "device.failed"
			detail = out.err.Error()
			if firstErr == nil {
				firstErr = fmt.Errorf("job %q: %w", out.id, out.err)
			}
		}
		if track {
			status.Devices[out.id] = jobStatus
			_ = workflow.ExecuteActivity(ctx, a.RecordJobStatusActivity, JobStatusInput{
				AccountID: accountID,
				JobID:     out.id,
				Status:    jobStatus,
			}).Get(ctx, nil)
			emitEvent(ctx, Event{
				Type:          evtType,
				AccountID:     accountID,
				CorrelationID: correlationID,
				DeviceID:      out.id,
				Status:        jobStatus,
				Detail:        detail,
			})
		}
	}
	return firstErr
}

func convergeJob(ctx workflow.Context, req sdk.Request) error {
	var a *Activities
	var plan sdk.Plan
	if err := workflow.ExecuteActivity(ctx, a.PlanActivity, req).Get(ctx, &plan); err != nil {
		return err
	}
	if plan.IsEmpty() {
		return nil
	}

	for _, group := range plan.Groups {
		if len(group.Steps) == 0 {
			continue
		}
		if !group.Parallel {
			for _, step := range group.Steps {
				err := workflow.ExecuteActivity(ctx, a.ApplyStepActivity, ApplyStepInput{
					Provider: plan.Driver.Provider,
					Kind:     plan.Driver.Kind,
					Resource: req.Resource,
					Step:     step,
				}).Get(ctx, nil)
				if err != nil {
					return err
				}
			}
			continue
		}

		errs := make([]error, len(group.Steps))
		wg := workflow.NewWaitGroup(ctx)
		wg.Add(len(group.Steps))
		for i, step := range group.Steps {
			idx := i
			s := step
			workflow.Go(ctx, func(gCtx workflow.Context) {
				defer wg.Done()
				errs[idx] = workflow.ExecuteActivity(gCtx, a.ApplyStepActivity, ApplyStepInput{
					Provider: plan.Driver.Provider,
					Kind:     plan.Driver.Kind,
					Resource: req.Resource,
					Step:     s,
				}).Get(gCtx, nil)
			})
		}
		wg.Wait(ctx)
		for _, err := range errs {
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func emitEvent(ctx workflow.Context, evt Event) {
	var a *Activities
	_ = workflow.ExecuteActivity(ctx, a.PublishEventActivity, evt).Get(ctx, nil)
}

func depsSatisfied(deps []string, done map[string]bool) bool {
	for _, d := range deps {
		if !done[d] {
			return false
		}
	}
	return true
}
