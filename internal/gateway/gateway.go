package gateway

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"orch/internal/bus"
	"orch/internal/events"
	"orch/internal/model"
	"orch/internal/store"
)

// Server exposes the HTTP API for submitting configuration intents and inspecting state/plans.
type Server struct {
	store store.Store
	pub   bus.Publisher
	mux   *http.ServeMux
}

// IntentRequest is the HTTP body for submitting a full or partial target config.
type IntentRequest struct {
	Kind      string      `json:"kind"`
	Vendor    string      `json:"vendor"`
	TaskQueue string      `json:"task_queue,omitempty"`
	Patch     model.State `json:"patch"`
}

// New constructs a gateway Server and registers routes.
func New(st store.Store, pub bus.Publisher) *Server {
	s := &Server{
		store: st,
		pub:   pub,
		mux:   http.NewServeMux(),
	}
	s.mux.HandleFunc("GET /healthz", s.handleHealth)
	s.mux.HandleFunc("GET /entities", s.handleListEntities)
	s.mux.HandleFunc("GET /entities/{id}", s.handleGetEntity)
	s.mux.HandleFunc("GET /entities/{id}/plans", s.handleListPlans)
	s.mux.HandleFunc("PUT /entities/{id}/intent", s.handlePutIntent)
	s.mux.HandleFunc("POST /entities/{id}/intent", s.handlePutIntent)
	return s
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleListEntities(w http.ResponseWriter, r *http.Request) {
	entities, err := s.store.ListEntities(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, entities)
}

func (s *Server) handleGetEntity(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	entity, err := s.store.GetEntity(r.Context(), id)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "entity not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, entity)
}

func (s *Server) handleListPlans(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	plans, err := s.store.ListPlansByEntity(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, plans)
}

func (s *Server) handlePutIntent(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		writeError(w, http.StatusBadRequest, "missing entity id")
		return
	}

	var req IntentRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json body: "+err.Error())
		return
	}
	if len(req.Patch) == 0 {
		writeError(w, http.StatusBadRequest, "patch must not be empty")
		return
	}

	// Fill missing kind/vendor from existing entity when submitting follow-up patches.
	if req.Kind == "" || req.Vendor == "" {
		if existing, err := s.store.GetEntity(r.Context(), id); err == nil {
			if req.Kind == "" {
				req.Kind = existing.Kind
			}
			if req.Vendor == "" {
				req.Vendor = existing.Vendor
			}
			if req.TaskQueue == "" {
				req.TaskQueue = existing.TaskQueue
			}
		}
	}

	if req.Kind == "" {
		writeError(w, http.StatusBadRequest, "kind is required for new entities")
		return
	}
	if req.Vendor == "" {
		req.Vendor = "mock"
	}

	ev := events.IntentRequestedEvent{
		EntityID:    id,
		Kind:        req.Kind,
		Vendor:      req.Vendor,
		TaskQueue:   req.TaskQueue,
		Patch:       req.Patch,
		RequestedAt: time.Now().UTC(),
	}
	if err := s.pub.PublishJSON(r.Context(), events.IntentSubject(req.Kind, req.Vendor, id), ev); err != nil {
		writeError(w, http.StatusInternalServerError, "publish intent: "+err.Error())
		return
	}

	writeJSON(w, http.StatusAccepted, map[string]any{
		"accepted":  true,
		"entity_id": id,
		"kind":      req.Kind,
		"vendor":    req.Vendor,
	})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
