package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"orch/sdk"
)

// Reconciler coordinates validation, state reading, planning, and step execution.
type Reconciler struct {
	registry *Registry
}

// NewReconciler creates a Reconciler backed by the given driver registry.
func NewReconciler(registry *Registry) *Reconciler {
	return &Reconciler{registry: registry}
}

// Plan validates the request, reads current resource state, and computes a grouped execution plan.
func (r *Reconciler) Plan(ctx context.Context, req sdk.Request) (sdk.Plan, error) {
	driver, err := r.registry.Resolve(req.Provider, req.Kind)
	if err != nil {
		return sdk.Plan{}, err
	}
	if err := driver.ValidateResource(req.Resource); err != nil {
		return sdk.Plan{}, fmt.Errorf("invalid resource for %s: %w", driver.Info().ID(), err)
	}
	if err := driver.ValidateTarget(req.Target); err != nil {
		return sdk.Plan{}, fmt.Errorf("invalid target config for %s: %w", driver.Info().ID(), err)
	}

	current, err := driver.Read(ctx, req.Resource)
	if err != nil {
		return sdk.Plan{}, fmt.Errorf("read current state for %s: %w", driver.Info().ID(), err)
	}

	plan, err := driver.Plan(ctx, current, req.Target)
	if err != nil {
		return sdk.Plan{}, fmt.Errorf("plan for %s: %w", driver.Info().ID(), err)
	}
	plan.Driver = driver.Info()
	return plan, nil
}

// ApplyPlan executes each step group in order against resource.
// Within a group, steps run concurrently when Parallel is true or sequentially otherwise.
func (r *Reconciler) ApplyPlan(ctx context.Context, resource json.RawMessage, plan sdk.Plan) (sdk.ApplyResult, error) {
	driver, err := r.registry.Resolve(plan.Driver.Provider, plan.Driver.Kind)
	if err != nil {
		return sdk.ApplyResult{}, err
	}
	if err := driver.ValidateResource(resource); err != nil {
		return sdk.ApplyResult{}, fmt.Errorf("invalid resource for %s: %w", driver.Info().ID(), err)
	}

	result := sdk.ApplyResult{Driver: driver.Info()}
	if plan.IsEmpty() {
		return result, nil
	}

	for _, group := range plan.Groups {
		if len(group.Steps) == 0 {
			continue
		}
		groupRes, err := runGroup(ctx, driver, resource, group)
		result.Groups = append(result.Groups, groupRes)
		if err != nil {
			return result, fmt.Errorf("group %q failed: %w", group.Name, err)
		}
	}
	return result, nil
}

// ApplyStep executes a single step through the resolved driver.
// Useful for external workflow runners (such as Temporal) driving Plan.Groups directly.
func (r *Reconciler) ApplyStep(ctx context.Context, provider, kind string, resource json.RawMessage, step sdk.Step) error {
	driver, err := r.registry.Resolve(provider, kind)
	if err != nil {
		return err
	}
	if err := driver.ValidateResource(resource); err != nil {
		return fmt.Errorf("invalid resource for %s: %w", driver.Info().ID(), err)
	}
	return driver.ApplyStep(ctx, resource, step)
}

// Reconcile plans and immediately applies any required changes for the request.
func (r *Reconciler) Reconcile(ctx context.Context, req sdk.Request) (sdk.Plan, sdk.ApplyResult, error) {
	plan, err := r.Plan(ctx, req)
	if err != nil {
		return sdk.Plan{}, sdk.ApplyResult{}, err
	}
	if plan.IsEmpty() {
		return plan, sdk.ApplyResult{Driver: plan.Driver}, nil
	}
	res, err := r.ApplyPlan(ctx, req.Resource, plan)
	return plan, res, err
}

func runGroup(ctx context.Context, driver sdk.Driver, resource json.RawMessage, group sdk.StepGroup) (sdk.GroupResult, error) {
	res := sdk.GroupResult{
		Name:  group.Name,
		Steps: make([]sdk.StepResult, len(group.Steps)),
	}

	if !group.Parallel {
		for i, step := range group.Steps {
			stepRes, err := runStep(ctx, driver, resource, step)
			res.Steps[i] = stepRes
			if err != nil {
				res.Steps = res.Steps[:i+1]
				return res, err
			}
		}
		return res, nil
	}

	groupCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	errs := make([]error, len(group.Steps))
	var wg sync.WaitGroup
	for i, step := range group.Steps {
		wg.Add(1)
		go func(idx int, s sdk.Step) {
			defer wg.Done()
			stepRes, err := runStep(groupCtx, driver, resource, s)
			res.Steps[idx] = stepRes
			if err != nil {
				errs[idx] = err
				cancel()
			}
		}(i, step)
	}
	wg.Wait()

	return res, errors.Join(errs...)
}

func runStep(ctx context.Context, driver sdk.Driver, resource json.RawMessage, step sdk.Step) (sdk.StepResult, error) {
	stepRes := sdk.StepResult{
		StepID: step.ID,
		Action: step.Action,
	}
	if err := driver.ApplyStep(ctx, resource, step); err != nil {
		stepRes.Error = err.Error()
		return stepRes, fmt.Errorf("step %q (%s): %w", step.ID, step.Action, err)
	}
	return stepRes, nil
}
