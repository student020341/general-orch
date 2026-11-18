package statemanager

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"orch/internal/bus"
	"orch/internal/events"
	"orch/internal/model"
	"orch/internal/reconciler"
	"orch/internal/store"
)

// Manager handles intent requests, generates DAG plans, and updates entity/plan state
// from NATS execution events.
type Manager struct {
	store      store.Store
	reconciler *reconciler.Reconciler
	pub        bus.Publisher
	logger     *slog.Logger
}

// New constructs a StateManager.
func New(st store.Store, rec *reconciler.Reconciler, pub bus.Publisher, logger *slog.Logger) *Manager {
	if logger == nil {
		logger = slog.Default()
	}
	return &Manager{
		store:      st,
		reconciler: rec,
		pub:        pub,
		logger:     logger,
	}
}

// SyncCatalog ensures device_profiles in the store are initialized (seeding from
// profilesPath if empty) and loads them into the reconciler's ProfileCatalog.
func (m *Manager) SyncCatalog(ctx context.Context, profilesPath string) error {
	existing, err := m.store.ListProfiles(ctx)
	if err != nil {
		return fmt.Errorf("list profiles: %w", err)
	}
	if len(existing) == 0 {
		if profilesPath != "" {
			if n, err := store.SyncProfilesFromFile(ctx, m.store, profilesPath); err == nil {
				m.logger.Info("seeded device catalog from file", "path", profilesPath, "count", n)
			} else {
				m.logger.Warn("seed from profiles file failed, using defaults", "path", profilesPath, "err", err)
				for _, p := range reconciler.DefaultProfiles() {
					if _, uerr := m.store.UpsertProfile(ctx, p); uerr != nil {
						return uerr
					}
				}
			}
		} else {
			for _, p := range reconciler.DefaultProfiles() {
				if _, uerr := m.store.UpsertProfile(ctx, p); uerr != nil {
					return uerr
				}
			}
		}
	}
	return m.reconciler.Catalog().LoadFromStore(ctx, m.store)
}

// Run starts durable NATS JetStream consumers for intents, step updates, completion events,
// and a broadcast subscriber for live catalog reloads.
func (m *Manager) Run(ctx context.Context, b *bus.Bus) error {
	errCh := make(chan error, 4)

	go func() {
		errCh <- b.Consume(ctx, "state-manager-intents", events.SubjectIntentPrefix+".>", m.handleIntentMsg)
	}()
	go func() {
		errCh <- b.Consume(ctx, "state-manager-steps", events.SubjectStepCompletedPrefix+".>", m.handleStepMsg)
	}()
	go func() {
		errCh <- b.Consume(ctx, "state-manager-finished", events.SubjectFinishedPrefix+".>", m.handleFinishedMsg)
	}()
	go func() {
		errCh <- b.SubscribeBroadcast(ctx, events.SubjectCatalogReloaded, m.handleCatalogReloadMsg)
	}()

	select {
	case <-ctx.Done():
		return nil
	case err := <-errCh:
		return err
	}
}

func (m *Manager) handleCatalogReloadMsg(ctx context.Context, _ []byte) error {
	if err := m.reconciler.Catalog().LoadFromStore(ctx, m.store); err != nil {
		return fmt.Errorf("reload catalog from store: %w", err)
	}
	m.logger.Info("reloaded device catalog", "profiles", len(m.reconciler.Catalog().List()))
	return nil
}

func (m *Manager) handleIntentMsg(ctx context.Context, data []byte) error {
	var ev events.IntentRequestedEvent
	if err := json.Unmarshal(data, &ev); err != nil {
		return fmt.Errorf("unmarshal intent event: %w", err)
	}
	return m.HandleIntentRequested(ctx, ev)
}

func (m *Manager) handleStepMsg(ctx context.Context, data []byte) error {
	var ev events.StepCompletedEvent
	if err := json.Unmarshal(data, &ev); err != nil {
		return fmt.Errorf("unmarshal step event: %w", err)
	}
	return m.HandleStepCompleted(ctx, ev)
}

func (m *Manager) handleFinishedMsg(ctx context.Context, data []byte) error {
	var ev events.ExecutionFinishedEvent
	if err := json.Unmarshal(data, &ev); err != nil {
		return fmt.Errorf("unmarshal finished event: %w", err)
	}
	return m.HandleExecutionFinished(ctx, ev)
}

// HandleIntentRequested merges a desired-state patch, builds a DAG plan, and emits PlanCreatedEvent.
func (m *Manager) HandleIntentRequested(ctx context.Context, ev events.IntentRequestedEvent) error {
	entity, err := m.store.ApplyIntentPatch(ctx, store.IntentPatchInput{
		EntityID:  ev.EntityID,
		Kind:      ev.Kind,
		Vendor:    ev.Vendor,
		TaskQueue: ev.TaskQueue,
		Patch:     ev.Patch,
		Replace:   ev.Replace,
	})
	if err != nil {
		return fmt.Errorf("apply intent patch: %w", err)
	}

	m.logger.Info("intent merged",
		"entity_id", entity.ID,
		"kind", entity.Kind,
		"version", entity.Version,
	)
	return m.planAndPublish(ctx, entity)
}

// HandleStepCompleted persists step status and merges observed physical changes into Entity.Current.
func (m *Manager) HandleStepCompleted(ctx context.Context, ev events.StepCompletedEvent) error {
	stepStatus := model.StepCompleted
	if ev.Error != "" {
		stepStatus = model.StepFailed
	}
	if err := m.store.UpdatePlanStep(ctx, ev.PlanID, ev.StepID, stepStatus, ev.Error); err != nil {
		return fmt.Errorf("update plan step %s/%s: %w", ev.PlanID, ev.StepID, err)
	}

	if ev.Error == "" && (len(ev.FullState) > 0 || len(ev.ObservedDelta) > 0) {
		_, err := m.store.UpdateEntity(ctx, ev.EntityID, func(e *model.Entity) error {
			if len(ev.FullState) > 0 {
				e.Current = model.CloneState(ev.FullState)
			}
			if len(ev.ObservedDelta) > 0 {
				e.Current = model.MergeState(e.Current, ev.ObservedDelta)
			}
			return nil
		})
		if err != nil {
			return fmt.Errorf("update entity current %s: %w", ev.EntityID, err)
		}
	}
	return nil
}

// HandleExecutionFinished updates plan/entity status and triggers re-planning if live drift was detected.
func (m *Manager) HandleExecutionFinished(ctx context.Context, ev events.ExecutionFinishedEvent) error {
	var planStatus model.PlanStatus
	switch ev.Outcome {
	case events.OutcomeCompleted:
		planStatus = model.PlanCompleted
	case events.OutcomeFailed:
		planStatus = model.PlanFailed
	case events.OutcomeDriftReplan, events.OutcomeSuperseded:
		planStatus = model.PlanSuperseded
	default:
		planStatus = model.PlanFailed
	}

	if err := m.store.UpdatePlanStatus(ctx, ev.PlanID, planStatus); err != nil {
		return fmt.Errorf("update plan status %s: %w", ev.PlanID, err)
	}

	entity, err := m.store.GetEntity(ctx, ev.EntityID)
	if err != nil {
		return fmt.Errorf("get entity %s: %w", ev.EntityID, err)
	}

	// Ignore stale completion outcomes if a newer target version has already been submitted.
	if entity.Version != ev.Version {
		m.logger.Info("ignoring stale execution outcome",
			"entity_id", entity.ID,
			"plan_version", ev.Version,
			"entity_version", entity.Version,
			"outcome", ev.Outcome,
		)
		return nil
	}

	switch ev.Outcome {
	case events.OutcomeCompleted:
		_, err = m.store.UpdateEntity(ctx, entity.ID, func(e *model.Entity) error {
			e.Status = model.StatusConverged
			e.LastError = ""
			return nil
		})
		return err

	case events.OutcomeFailed:
		_, err = m.store.UpdateEntity(ctx, entity.ID, func(e *model.Entity) error {
			e.Status = model.StatusFailed
			e.LastError = ev.Error
			return nil
		})
		return err

	case events.OutcomeDriftReplan:
		m.logger.Info("drift detected, replanning",
			"entity_id", entity.ID,
			"version", entity.Version,
		)
		return m.planAndPublish(ctx, entity)

	case events.OutcomeSuperseded:
		return nil
	}
	return nil
}

func (m *Manager) planAndPublish(ctx context.Context, entity *model.Entity) error {
	plan, err := m.reconciler.Reconcile(entity)
	if err != nil {
		_, _ = m.store.UpdateEntity(ctx, entity.ID, func(e *model.Entity) error {
			e.Status = model.StatusFailed
			e.LastError = err.Error()
			return nil
		})
		return fmt.Errorf("reconcile %s: %w", entity.ID, err)
	}

	if plan == nil {
		_, err := m.store.UpdateEntity(ctx, entity.ID, func(e *model.Entity) error {
			e.Status = model.StatusConverged
			e.LastError = ""
			return nil
		})
		m.logger.Info("entity already converged", "entity_id", entity.ID, "version", entity.Version)
		return err
	}

	if err := m.store.CreatePlan(ctx, plan); err != nil {
		return fmt.Errorf("create plan: %w", err)
	}

	_, err = m.store.UpdateEntity(ctx, entity.ID, func(e *model.Entity) error {
		e.Status = model.StatusExecuting
		e.LastError = ""
		return nil
	})
	if err != nil {
		return fmt.Errorf("mark entity executing: %w", err)
	}

	if err := m.pub.PublishJSON(ctx, events.PlanCreatedSubject(entity.ID), events.PlanCreatedEvent{
		PlanID:    plan.ID,
		EntityID:  entity.ID,
		Version:   entity.Version,
		CreatedAt: time.Now().UTC(),
	}); err != nil {
		return fmt.Errorf("publish plan created: %w", err)
	}

	m.logger.Info("plan created and published",
		"entity_id", entity.ID,
		"plan_id", plan.ID,
		"version", plan.Version,
		"steps", len(plan.Steps),
	)
	return nil
}
