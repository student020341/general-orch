package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"orch/internal/model"
	"orch/internal/store"
)

const schemaSQL = `
CREATE TABLE IF NOT EXISTS entities (
	id TEXT PRIMARY KEY,
	kind TEXT NOT NULL,
	vendor TEXT NOT NULL,
	task_queue TEXT NOT NULL DEFAULT '',
	current_state JSONB NOT NULL DEFAULT '{}'::jsonb,
	target_state JSONB NOT NULL DEFAULT '{}'::jsonb,
	status TEXT NOT NULL,
	version BIGINT NOT NULL DEFAULT 0,
	last_error TEXT NOT NULL DEFAULT '',
	updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS plans (
	id TEXT PRIMARY KEY,
	entity_id TEXT NOT NULL REFERENCES entities(id) ON DELETE CASCADE,
	version BIGINT NOT NULL,
	status TEXT NOT NULL,
	created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
	updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_plans_entity_version ON plans(entity_id, version DESC);

CREATE TABLE IF NOT EXISTS plan_steps (
	plan_id TEXT NOT NULL REFERENCES plans(id) ON DELETE CASCADE,
	step_id TEXT NOT NULL,
	step_order INT NOT NULL,
	task_queue TEXT NOT NULL,
	activity TEXT NOT NULL,
	capability TEXT NOT NULL,
	vendor TEXT NOT NULL,
	payload JSONB NOT NULL DEFAULT '{}'::jsonb,
	depends_on JSONB NOT NULL DEFAULT '[]'::jsonb,
	status TEXT NOT NULL,
	error TEXT NOT NULL DEFAULT '',
	PRIMARY KEY (plan_id, step_id)
);

CREATE TABLE IF NOT EXISTS device_profiles (
	kind TEXT PRIMARY KEY,
	resources JSONB NOT NULL DEFAULT '{}'::jsonb,
	default_queues JSONB NOT NULL DEFAULT '{}'::jsonb,
	fetch_state_queue TEXT NOT NULL DEFAULT '',
	supports_live_state BOOLEAN NOT NULL DEFAULT false,
	serial_execution BOOLEAN NOT NULL DEFAULT false,
	updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

ALTER TABLE device_profiles ADD COLUMN IF NOT EXISTS resources JSONB NOT NULL DEFAULT '{}'::jsonb;
ALTER TABLE device_profiles DROP COLUMN IF EXISTS aspects;
`

// Store implements store.Store backed by PostgreSQL.
type Store struct {
	pool *pgxpool.Pool
}

// New connects to Postgres and ensures the schema exists.
func New(ctx context.Context, dsn string) (*Store, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("connect postgres: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	if _, err := pool.Exec(ctx, schemaSQL); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ensure schema: %w", err)
	}
	return &Store{pool: pool}, nil
}

func (s *Store) Close() {
	s.pool.Close()
}

func (s *Store) GetEntity(ctx context.Context, id string) (*model.Entity, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT id, kind, vendor, task_queue, current_state, target_state, status, version, last_error, updated_at
		FROM entities
		WHERE id = $1
	`, id)
	return scanEntity(row)
}

func (s *Store) ListEntities(ctx context.Context) ([]*model.Entity, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, kind, vendor, task_queue, current_state, target_state, status, version, last_error, updated_at
		FROM entities
		ORDER BY id ASC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []*model.Entity
	for rows.Next() {
		e, err := scanEntity(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

func (s *Store) ApplyIntentPatch(ctx context.Context, in store.IntentPatchInput) (*model.Entity, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	now := time.Now().UTC()
	var e *model.Entity
	row := tx.QueryRow(ctx, `
		SELECT id, kind, vendor, task_queue, current_state, target_state, status, version, last_error, updated_at
		FROM entities
		WHERE id = $1
		FOR UPDATE
	`, in.EntityID)

	existing, err := scanEntity(row)
	switch {
	case errors.Is(err, store.ErrNotFound):
		mergedTarget := model.MergeState(model.State{}, in.Patch)
		e = &model.Entity{
			ID:        in.EntityID,
			Kind:      in.Kind,
			Vendor:    in.Vendor,
			TaskQueue: in.TaskQueue,
			Current:   model.State{},
			Target:    mergedTarget,
			Status:    model.StatusPlanning,
			Version:   1,
			UpdatedAt: now,
		}
		targetJSON, err := json.Marshal(e.Target)
		if err != nil {
			return nil, err
		}
		_, err = tx.Exec(ctx, `
			INSERT INTO entities (id, kind, vendor, task_queue, current_state, target_state, status, version, last_error, updated_at)
			VALUES ($1, $2, $3, $4, '{}'::jsonb, $5, $6, $7, '', $8)
		`, e.ID, e.Kind, e.Vendor, e.TaskQueue, targetJSON, string(e.Status), e.Version, e.UpdatedAt)
		if err != nil {
			return nil, err
		}
	case err != nil:
		return nil, err
	default:
		e = existing
		if in.Kind != "" {
			e.Kind = in.Kind
		}
		if in.Vendor != "" {
			e.Vendor = in.Vendor
		}
		if in.TaskQueue != "" {
			e.TaskQueue = in.TaskQueue
		}
		if in.Replace {
			e.Target = model.MergeState(model.State{}, in.Patch)
		} else {
			e.Target = model.MergeState(e.Target, in.Patch)
		}
		e.Version++
		e.Status = model.StatusPlanning
		e.LastError = ""
		e.UpdatedAt = now

		targetJSON, err := json.Marshal(e.Target)
		if err != nil {
			return nil, err
		}
		_, err = tx.Exec(ctx, `
			UPDATE entities
			SET kind = $2, vendor = $3, task_queue = $4, target_state = $5, status = $6, version = $7, last_error = '', updated_at = $8
			WHERE id = $1
		`, e.ID, e.Kind, e.Vendor, e.TaskQueue, targetJSON, string(e.Status), e.Version, e.UpdatedAt)
		if err != nil {
			return nil, err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return e, nil
}

func (s *Store) UpdateEntity(ctx context.Context, id string, fn func(*model.Entity) error) (*model.Entity, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	row := tx.QueryRow(ctx, `
		SELECT id, kind, vendor, task_queue, current_state, target_state, status, version, last_error, updated_at
		FROM entities
		WHERE id = $1
		FOR UPDATE
	`, id)
	e, err := scanEntity(row)
	if err != nil {
		return nil, err
	}

	if err := fn(e); err != nil {
		return nil, err
	}
	e.UpdatedAt = time.Now().UTC()

	currentJSON, err := json.Marshal(e.Current)
	if err != nil {
		return nil, err
	}
	targetJSON, err := json.Marshal(e.Target)
	if err != nil {
		return nil, err
	}
	_, err = tx.Exec(ctx, `
		UPDATE entities
		SET kind = $2, vendor = $3, task_queue = $4, current_state = $5, target_state = $6,
		    status = $7, version = $8, last_error = $9, updated_at = $10
		WHERE id = $1
	`, e.ID, e.Kind, e.Vendor, e.TaskQueue, currentJSON, targetJSON, string(e.Status), e.Version, e.LastError, e.UpdatedAt)
	if err != nil {
		return nil, err
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return e, nil
}

func (s *Store) CreatePlan(ctx context.Context, plan *model.Plan) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	now := time.Now().UTC()
	if plan.CreatedAt.IsZero() {
		plan.CreatedAt = now
	}
	plan.UpdatedAt = now

	if _, err := tx.Exec(ctx, `
		UPDATE plans
		SET status = $2, updated_at = $3
		WHERE entity_id = $1 AND status IN ($4, $5)
	`, plan.EntityID, string(model.PlanSuperseded), now, string(model.PlanPending), string(model.PlanRunning)); err != nil {
		return err
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO plans (id, entity_id, version, status, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6)
	`, plan.ID, plan.EntityID, plan.Version, string(plan.Status), plan.CreatedAt, plan.UpdatedAt); err != nil {
		return err
	}

	for idx, step := range plan.Steps {
		payloadJSON, err := json.Marshal(step.Payload)
		if err != nil {
			return err
		}
		deps := step.DependsOn
		if deps == nil {
			deps = []string{}
		}
		depsJSON, err := json.Marshal(deps)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO plan_steps (plan_id, step_id, step_order, task_queue, activity, capability, vendor, payload, depends_on, status, error)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
		`, plan.ID, step.ID, idx, step.TaskQueue, step.Activity, step.Capability, step.Vendor, payloadJSON, depsJSON, string(step.Status), step.Error); err != nil {
			return err
		}
	}

	return tx.Commit(ctx)
}

func (s *Store) GetPlan(ctx context.Context, id string) (*model.Plan, error) {
	var p model.Plan
	var statusStr string
	err := s.pool.QueryRow(ctx, `
		SELECT id, entity_id, version, status, created_at, updated_at
		FROM plans
		WHERE id = $1
	`, id).Scan(&p.ID, &p.EntityID, &p.Version, &statusStr, &p.CreatedAt, &p.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, store.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	p.Status = model.PlanStatus(statusStr)

	steps, err := s.loadPlanSteps(ctx, p.ID)
	if err != nil {
		return nil, err
	}
	p.Steps = steps
	return &p, nil
}

func (s *Store) ListPlansByEntity(ctx context.Context, entityID string) ([]*model.Plan, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, entity_id, version, status, created_at, updated_at
		FROM plans
		WHERE entity_id = $1
		ORDER BY created_at DESC
	`, entityID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var plans []*model.Plan
	for rows.Next() {
		var p model.Plan
		var statusStr string
		if err := rows.Scan(&p.ID, &p.EntityID, &p.Version, &statusStr, &p.CreatedAt, &p.UpdatedAt); err != nil {
			return nil, err
		}
		p.Status = model.PlanStatus(statusStr)
		plans = append(plans, &p)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for _, p := range plans {
		steps, err := s.loadPlanSteps(ctx, p.ID)
		if err != nil {
			return nil, err
		}
		p.Steps = steps
	}
	return plans, nil
}

func (s *Store) UpdatePlanStatus(ctx context.Context, planID string, status model.PlanStatus) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE plans SET status = $2, updated_at = $3 WHERE id = $1
	`, planID, string(status), time.Now().UTC())
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return store.ErrNotFound
	}
	return nil
}

func (s *Store) UpdatePlanStep(ctx context.Context, planID, stepID string, status model.StepStatus, errMsg string) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE plan_steps SET status = $3, error = $4
		WHERE plan_id = $1 AND step_id = $2
	`, planID, stepID, string(status), errMsg)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return store.ErrNotFound
	}
	return nil
}

func (s *Store) loadPlanSteps(ctx context.Context, planID string) ([]model.PlanStep, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT step_id, task_queue, activity, capability, vendor, payload, depends_on, status, error
		FROM plan_steps
		WHERE plan_id = $1
		ORDER BY step_order ASC
	`, planID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var steps []model.PlanStep
	for rows.Next() {
		var st model.PlanStep
		var payloadRaw, depsRaw []byte
		var statusStr string
		if err := rows.Scan(
			&st.ID, &st.TaskQueue, &st.Activity, &st.Capability, &st.Vendor,
			&payloadRaw, &depsRaw, &statusStr, &st.Error,
		); err != nil {
			return nil, err
		}
		st.Status = model.StepStatus(statusStr)
		if len(payloadRaw) > 0 {
			if err := json.Unmarshal(payloadRaw, &st.Payload); err != nil {
				return nil, err
			}
		}
		if st.Payload == nil {
			st.Payload = model.State{}
		}
		if len(depsRaw) > 0 {
			if err := json.Unmarshal(depsRaw, &st.DependsOn); err != nil {
				return nil, err
			}
		}
		steps = append(steps, st)
	}
	return steps, rows.Err()
}

func scanEntity(row pgx.Row) (*model.Entity, error) {
	var e model.Entity
	var currentRaw, targetRaw []byte
	var statusStr string
	err := row.Scan(
		&e.ID, &e.Kind, &e.Vendor, &e.TaskQueue,
		&currentRaw, &targetRaw, &statusStr, &e.Version, &e.LastError, &e.UpdatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, store.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	e.Status = model.EntityStatus(statusStr)
	if len(currentRaw) > 0 {
		if err := json.Unmarshal(currentRaw, &e.Current); err != nil {
			return nil, err
		}
	}
	if e.Current == nil {
		e.Current = model.State{}
	}
	if len(targetRaw) > 0 {
		if err := json.Unmarshal(targetRaw, &e.Target); err != nil {
			return nil, err
		}
	}
	if e.Target == nil {
		e.Target = model.State{}
	}
	return &e, nil
}

func (s *Store) ListProfiles(ctx context.Context) ([]model.DeviceProfile, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT kind, resources, default_queues, fetch_state_queue, supports_live_state, serial_execution, updated_at
		FROM device_profiles
		ORDER BY kind ASC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []model.DeviceProfile
	for rows.Next() {
		p, err := scanProfile(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *p)
	}
	return out, rows.Err()
}

func (s *Store) GetProfile(ctx context.Context, kind string) (*model.DeviceProfile, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT kind, resources, default_queues, fetch_state_queue, supports_live_state, serial_execution, updated_at
		FROM device_profiles
		WHERE kind = $1
	`, kind)
	return scanProfile(row)
}

func (s *Store) UpsertProfile(ctx context.Context, profile model.DeviceProfile) (*model.DeviceProfile, error) {
	if profile.Resources == nil {
		profile.Resources = map[string]string{}
	}
	if profile.DefaultQueues == nil {
		profile.DefaultQueues = map[string]string{}
	}
	resourcesJSON, err := json.Marshal(profile.Resources)
	if err != nil {
		return nil, err
	}
	queuesJSON, err := json.Marshal(profile.DefaultQueues)
	if err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	profile.UpdatedAt = now

	_, err = s.pool.Exec(ctx, `
		INSERT INTO device_profiles (kind, resources, default_queues, fetch_state_queue, supports_live_state, serial_execution, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (kind) DO UPDATE SET
			resources = EXCLUDED.resources,
			default_queues = EXCLUDED.default_queues,
			fetch_state_queue = EXCLUDED.fetch_state_queue,
			supports_live_state = EXCLUDED.supports_live_state,
			serial_execution = EXCLUDED.serial_execution,
			updated_at = EXCLUDED.updated_at
	`, profile.Kind, resourcesJSON, queuesJSON, profile.FetchStateQueue, profile.SupportsLiveState, profile.SerialExecution, now)
	if err != nil {
		return nil, err
	}
	return &profile, nil
}

func (s *Store) DeleteProfile(ctx context.Context, kind string) error {
	tag, err := s.pool.Exec(ctx, `DELETE FROM device_profiles WHERE kind = $1`, kind)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return store.ErrNotFound
	}
	return nil
}

func scanProfile(row pgx.Row) (*model.DeviceProfile, error) {
	var p model.DeviceProfile
	var resourcesRaw, queuesRaw []byte
	err := row.Scan(
		&p.Kind, &resourcesRaw, &queuesRaw,
		&p.FetchStateQueue, &p.SupportsLiveState, &p.SerialExecution, &p.UpdatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, store.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	if len(resourcesRaw) > 0 {
		if err := json.Unmarshal(resourcesRaw, &p.Resources); err != nil {
			return nil, err
		}
	}
	if p.Resources == nil {
		p.Resources = map[string]string{}
	}
	if len(queuesRaw) > 0 {
		if err := json.Unmarshal(queuesRaw, &p.DefaultQueues); err != nil {
			return nil, err
		}
	}
	if p.DefaultQueues == nil {
		p.DefaultQueues = map[string]string{}
	}
	return &p, nil
}
