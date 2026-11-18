package memory

import (
	"context"
	"maps"
	"slices"
	"sync"
	"time"

	"orch/internal/model"
	"orch/internal/store"
)

// Store is an in-memory implementation of store.Store for unit tests.
type Store struct {
	mu       sync.Mutex
	entities map[string]*model.Entity
	plans    map[string]*model.Plan
	profiles map[string]model.DeviceProfile
}

// New creates an empty in-memory Store.
func New() *Store {
	return &Store{
		entities: make(map[string]*model.Entity),
		plans:    make(map[string]*model.Plan),
		profiles: make(map[string]model.DeviceProfile),
	}
}

func (s *Store) Close() {}

func (s *Store) GetEntity(_ context.Context, id string) (*model.Entity, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.entities[id]
	if !ok {
		return nil, store.ErrNotFound
	}
	return cloneEntity(e), nil
}

func (s *Store) ListEntities(_ context.Context) ([]*model.Entity, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*model.Entity, 0, len(s.entities))
	for _, e := range s.entities {
		out = append(out, cloneEntity(e))
	}
	slices.SortFunc(out, func(a, b *model.Entity) int {
		if a.ID < b.ID {
			return -1
		}
		if a.ID > b.ID {
			return 1
		}
		return 0
	})
	return out, nil
}

func (s *Store) ApplyIntentPatch(_ context.Context, in store.IntentPatchInput) (*model.Entity, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().UTC()
	e, ok := s.entities[in.EntityID]
	if !ok {
		e = &model.Entity{
			ID:        in.EntityID,
			Kind:      in.Kind,
			Vendor:    in.Vendor,
			TaskQueue: in.TaskQueue,
			Current:   model.State{},
			Target:    model.State{},
			Status:    model.StatusPlanning,
			Version:   0,
		}
		s.entities[in.EntityID] = e
	}
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
	return cloneEntity(e), nil
}

func (s *Store) UpdateEntity(_ context.Context, id string, fn func(*model.Entity) error) (*model.Entity, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.entities[id]
	if !ok {
		return nil, store.ErrNotFound
	}
	cp := cloneEntity(e)
	if err := fn(cp); err != nil {
		return nil, err
	}
	cp.UpdatedAt = time.Now().UTC()
	s.entities[id] = cp
	return cloneEntity(cp), nil
}

func (s *Store) CreatePlan(_ context.Context, plan *model.Plan) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().UTC()
	for _, p := range s.plans {
		if p.EntityID == plan.EntityID && (p.Status == model.PlanPending || p.Status == model.PlanRunning) {
			p.Status = model.PlanSuperseded
			p.UpdatedAt = now
		}
	}
	cp := clonePlan(plan)
	if cp.CreatedAt.IsZero() {
		cp.CreatedAt = now
	}
	cp.UpdatedAt = now
	s.plans[cp.ID] = cp
	return nil
}

func (s *Store) GetPlan(_ context.Context, id string) (*model.Plan, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, ok := s.plans[id]
	if !ok {
		return nil, store.ErrNotFound
	}
	return clonePlan(p), nil
}

func (s *Store) ListPlansByEntity(_ context.Context, entityID string) ([]*model.Plan, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*model.Plan
	for _, p := range s.plans {
		if p.EntityID == entityID {
			out = append(out, clonePlan(p))
		}
	}
	slices.SortFunc(out, func(a, b *model.Plan) int {
		return b.CreatedAt.Compare(a.CreatedAt)
	})
	return out, nil
}

func (s *Store) UpdatePlanStatus(_ context.Context, planID string, status model.PlanStatus) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, ok := s.plans[planID]
	if !ok {
		return store.ErrNotFound
	}
	p.Status = status
	p.UpdatedAt = time.Now().UTC()
	return nil
}

func (s *Store) UpdatePlanStep(_ context.Context, planID, stepID string, status model.StepStatus, errMsg string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, ok := s.plans[planID]
	if !ok {
		return store.ErrNotFound
	}
	for i := range p.Steps {
		if p.Steps[i].ID == stepID {
			p.Steps[i].Status = status
			p.Steps[i].Error = errMsg
			p.UpdatedAt = time.Now().UTC()
			return nil
		}
	}
	return store.ErrNotFound
}

func (s *Store) ListProfiles(_ context.Context) ([]model.DeviceProfile, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]model.DeviceProfile, 0, len(s.profiles))
	for _, p := range s.profiles {
		out = append(out, cloneProfile(p))
	}
	slices.SortFunc(out, func(a, b model.DeviceProfile) int {
		if a.Kind < b.Kind {
			return -1
		}
		if a.Kind > b.Kind {
			return 1
		}
		return 0
	})
	return out, nil
}

func (s *Store) GetProfile(_ context.Context, kind string) (*model.DeviceProfile, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, ok := s.profiles[kind]
	if !ok {
		return nil, store.ErrNotFound
	}
	cp := cloneProfile(p)
	return &cp, nil
}

func (s *Store) UpsertProfile(_ context.Context, profile model.DeviceProfile) (*model.DeviceProfile, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := cloneProfile(profile)
	cp.UpdatedAt = time.Now().UTC()
	s.profiles[cp.Kind] = cp
	ret := cloneProfile(cp)
	return &ret, nil
}

func (s *Store) DeleteProfile(_ context.Context, kind string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.profiles[kind]; !ok {
		return store.ErrNotFound
	}
	delete(s.profiles, kind)
	return nil
}

func cloneProfile(p model.DeviceProfile) model.DeviceProfile {
	cp := p
	cp.Resources = maps.Clone(p.Resources)
	if cp.Resources == nil {
		cp.Resources = map[string]string{}
	}
	cp.DefaultQueues = maps.Clone(p.DefaultQueues)
	if cp.DefaultQueues == nil {
		cp.DefaultQueues = map[string]string{}
	}
	return cp
}

func cloneEntity(e *model.Entity) *model.Entity {
	cp := *e
	cp.Current = model.CloneState(e.Current)
	cp.Target = model.CloneState(e.Target)
	return &cp
}

func clonePlan(p *model.Plan) *model.Plan {
	cp := *p
	cp.Steps = make([]model.PlanStep, len(p.Steps))
	for i, step := range p.Steps {
		cp.Steps[i] = step
		cp.Steps[i].Payload = model.CloneState(step.Payload)
		cp.Steps[i].DependsOn = slices.Clone(step.DependsOn)
	}
	return &cp
}
