package orchestrator

import (
	"fmt"
	"math"
	"reflect"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"orch/internal/events"
	"orch/internal/model"
)

// WorkflowInput starts an OrchestrationWorkflow for a specific persisted plan.
type WorkflowInput struct {
	PlanID   string `json:"plan_id"`
	EntityID string `json:"entity_id"`
	Version  int64  `json:"version"`
}

// OrchestrationWorkflow loads a DAG plan from the store and executes ready steps
// across domain worker task queues until complete, failed, superseded, or drifted.
func OrchestrationWorkflow(ctx workflow.Context, in WorkflowInput) error {
	orchOpts := workflow.ActivityOptions{
		TaskQueue:           OrchestrationTaskQueue,
		StartToCloseTimeout: 15 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    500 * time.Millisecond,
			BackoffCoefficient: 2.0,
			MaximumAttempts:    5,
		},
	}
	orchCtx := workflow.WithActivityOptions(ctx, orchOpts)

	var plan model.Plan
	if err := workflow.ExecuteActivity(orchCtx, "LoadPlan", in.PlanID).Get(ctx, &plan); err != nil {
		return err
	}
	if plan.Status == model.PlanSuperseded {
		return nil
	}
	if len(plan.Steps) == 0 {
		return emitFinished(orchCtx, &plan, events.OutcomeCompleted, "")
	}

	completed := make(map[string]bool, len(plan.Steps))
	inFlight := make(map[string]bool, len(plan.Steps))

	sel := workflow.NewSelector(ctx)
	var canceled bool
	sel.AddReceive(ctx.Done(), func(c workflow.ReceiveChannel, more bool) {
		canceled = true
	})

	for len(completed) < len(plan.Steps) {
		var launchedAny bool

		for i := range plan.Steps {
			step := plan.Steps[i]
			if completed[step.ID] || inFlight[step.ID] || !depsSatisfied(step.DependsOn, completed) {
				continue
			}

			inFlight[step.ID] = true
			launchedAny = true

			stepCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
				TaskQueue:           step.TaskQueue,
				StartToCloseTimeout: 30 * time.Second,
				RetryPolicy: &temporal.RetryPolicy{
					InitialInterval:    500 * time.Millisecond,
					BackoffCoefficient: 2.0,
					MaximumAttempts:    3,
				},
			})

			switch step.Activity {
			case "FetchState":
				fut := workflow.ExecuteActivity(stepCtx, "FetchState", FetchStateInput{
					EntityID: plan.EntityID,
					Vendor:   step.Vendor,
				})
				sel.AddFuture(fut, func(f workflow.Future) {
					delete(inFlight, step.ID)
					var out FetchStateOutput
					err := f.Get(ctx, &out)
					if err != nil {
						handleStepError(ctx, orchCtx, &plan, step, err)
						completed["__abort__"] = true
						return
					}
					if err := emitStepCompleted(orchCtx, &plan, step, nil, out.State, ""); err != nil {
						completed["__abort__"] = true
						return
					}
					if out.Supported && hasDrift(step.Payload, out.State) {
						_ = emitFinished(orchCtx, &plan, events.OutcomeDriftReplan, "")
						completed["__drift__"] = true
						return
					}
					completed[step.ID] = true
				})

			default:
				fut := workflow.ExecuteActivity(stepCtx, "ExecuteCapability", ExecuteCapabilityInput{
					Vendor:     step.Vendor,
					Capability: step.Capability,
					Payload:    step.Payload,
				})
				sel.AddFuture(fut, func(f workflow.Future) {
					delete(inFlight, step.ID)
					var out ExecuteCapabilityOutput
					err := f.Get(ctx, &out)
					if err != nil {
						handleStepError(ctx, orchCtx, &plan, step, err)
						completed["__abort__"] = true
						return
					}
					if err := emitStepCompleted(orchCtx, &plan, step, out.ObservedDelta, nil, ""); err != nil {
						completed["__abort__"] = true
						return
					}
					completed[step.ID] = true
				})
			}
		}

		if !launchedAny && len(inFlight) == 0 {
			err := fmt.Errorf("plan %s has unresolvable DAG dependencies", plan.ID)
			_ = emitFinished(orchCtx, &plan, events.OutcomeFailed, err.Error())
			return err
		}

		sel.Select(ctx)

		if canceled || ctx.Err() != nil {
			discCtx, _ := workflow.NewDisconnectedContext(ctx)
			discOrchCtx := workflow.WithActivityOptions(discCtx, orchOpts)
			_ = emitFinished(discOrchCtx, &plan, events.OutcomeSuperseded, "workflow cancelled")
			return ctx.Err()
		}
		if completed["__drift__"] {
			return nil
		}
		if completed["__abort__"] {
			return fmt.Errorf("plan %s failed", plan.ID)
		}
	}

	return emitFinished(orchCtx, &plan, events.OutcomeCompleted, "")
}

func handleStepError(ctx, orchCtx workflow.Context, plan *model.Plan, step model.PlanStep, err error) {
	if ctx.Err() != nil {
		return
	}
	_ = emitStepCompleted(orchCtx, plan, step, nil, nil, err.Error())
	_ = emitFinished(orchCtx, plan, events.OutcomeFailed, fmt.Sprintf("step %s failed: %v", step.ID, err))
}

func emitStepCompleted(
	orchCtx workflow.Context,
	plan *model.Plan,
	step model.PlanStep,
	delta model.State,
	fullState model.State,
	errMsg string,
) error {
	return workflow.ExecuteActivity(orchCtx, "PublishStepCompleted", events.StepCompletedEvent{
		PlanID:        plan.ID,
		EntityID:      plan.EntityID,
		Version:       plan.Version,
		StepID:        step.ID,
		Capability:    step.Capability,
		ObservedDelta: delta,
		FullState:     fullState,
		Error:         errMsg,
		CompletedAt:   workflow.Now(orchCtx),
	}).Get(orchCtx, nil)
}

func emitFinished(
	orchCtx workflow.Context,
	plan *model.Plan,
	outcome events.ExecutionOutcome,
	errMsg string,
) error {
	return workflow.ExecuteActivity(orchCtx, "PublishExecutionFinished", events.ExecutionFinishedEvent{
		PlanID:     plan.ID,
		EntityID:   plan.EntityID,
		Version:    plan.Version,
		Outcome:    outcome,
		Error:      errMsg,
		FinishedAt: workflow.Now(orchCtx),
	}).Get(orchCtx, nil)
}

func depsSatisfied(deps []string, completed map[string]bool) bool {
	for _, dep := range deps {
		if !completed[dep] {
			return false
		}
	}
	return true
}

func hasDrift(payload model.State, live model.State) bool {
	expected := toState(payload["expected_current"])
	rawKeys, ok := payload["drift_keys"]
	if !ok {
		return false
	}
	keys := toStringSlice(rawKeys)
	for _, k := range keys {
		if !valuesMatchForDrift(k, expected[k], live[k]) {
			return true
		}
	}
	return false
}

func valuesMatchForDrift(key string, expectedVal, actualVal any) bool {
	if key == "rotation" {
		expNum, expOK := toFloat(expectedVal)
		if expectedVal == nil {
			expNum, expOK = 0, true
		}
		actNum, actOK := toFloat(actualVal)
		if actualVal == nil {
			actNum, actOK = 0, true
		}
		if expOK && actOK {
			return math.Abs(expNum-actNum) < 1e-6
		}
	}
	return reflect.DeepEqual(expectedVal, actualVal)
}

func toStringSlice(v any) []string {
	switch s := v.(type) {
	case []string:
		return s
	case []any:
		out := make([]string, 0, len(s))
		for _, item := range s {
			if str, ok := item.(string); ok {
				out = append(out, str)
			}
		}
		return out
	default:
		return nil
	}
}

func toState(v any) model.State {
	switch m := v.(type) {
	case model.State:
		return m
	case map[string]any:
		return model.State(m)
	default:
		return model.State{}
	}
}

func toFloat(v any) (float64, bool) {
	switch n := v.(type) {
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	case float64:
		return n, true
	case float32:
		return float64(n), true
	default:
		return 0, false
	}
}
