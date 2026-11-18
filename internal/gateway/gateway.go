package gateway

import (
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"time"

	"orch/internal/aspects"
	"orch/internal/bus"
	"orch/internal/events"
	"orch/internal/model"
	"orch/internal/store"
)

// Server exposes the HTTP API for submitting configuration intents, inspecting state/plans,
// and managing/reloading the device profile catalog.
type Server struct {
	store        store.Store
	pub          bus.Publisher
	bcast        bus.Broadcaster
	profilesPath string
	mux          *http.ServeMux
}

// IntentRequest is the HTTP body for submitting a full or partial target config.
type IntentRequest struct {
	Kind      string      `json:"kind"`
	Vendor    string      `json:"vendor"`
	TaskQueue string      `json:"task_queue,omitempty"`
	Patch     model.State `json:"patch"`
	Replace   bool        `json:"replace,omitempty"`
}

// New constructs a gateway Server and registers routes.
// If webDir is non-empty and exists, static UI files are served under /ui/ (with / redirecting to /ui/).
func New(st store.Store, pub bus.Publisher, bcast bus.Broadcaster, profilesPath string, webDir ...string) *Server {
	s := &Server{
		store:        st,
		pub:          pub,
		bcast:        bcast,
		profilesPath: profilesPath,
		mux:          http.NewServeMux(),
	}
	s.mux.HandleFunc("GET /healthz", s.handleHealth)
	s.mux.HandleFunc("GET /entities", s.handleListEntities)
	s.mux.HandleFunc("GET /entities/{id}", s.handleGetEntity)
	s.mux.HandleFunc("GET /entities/{id}/plans", s.handleListPlans)
	s.mux.HandleFunc("PUT /entities/{id}/intent", s.handlePutIntent)
	s.mux.HandleFunc("POST /entities/{id}/intent", s.handlePutIntent)

	s.mux.HandleFunc("GET /catalog/aspects", s.handleListAspects)
	s.mux.HandleFunc("GET /catalog/profiles", s.handleListProfiles)
	s.mux.HandleFunc("GET /catalog/profiles/{kind}", s.handleGetProfile)
	s.mux.HandleFunc("PUT /catalog/profiles/{kind}", s.handlePutProfile)
	s.mux.HandleFunc("DELETE /catalog/profiles/{kind}", s.handleDeleteProfile)
	s.mux.HandleFunc("POST /catalog/reload", s.handleReloadCatalog)

	if len(webDir) > 0 && webDir[0] != "" {
		if info, err := os.Stat(webDir[0]); err == nil && info.IsDir() {
			fs := http.StripPrefix("/ui/", http.FileServer(http.Dir(webDir[0])))
			s.mux.Handle("GET /ui/", fs)
			s.mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
				http.Redirect(w, r, "/ui/", http.StatusFound)
			})
		}
	}
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
		Replace:     req.Replace,
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

func (s *Server) handleListAspects(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, aspects.Schemas())
}

func (s *Server) handleListProfiles(w http.ResponseWriter, r *http.Request) {
	profiles, err := s.store.ListProfiles(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, profiles)
}

func (s *Server) handleGetProfile(w http.ResponseWriter, r *http.Request) {
	kind := r.PathValue("kind")
	p, err := s.store.GetProfile(r.Context(), kind)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "profile not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, p)
}

func (s *Server) handlePutProfile(w http.ResponseWriter, r *http.Request) {
	kind := r.PathValue("kind")
	if kind == "" {
		writeError(w, http.StatusBadRequest, "missing kind")
		return
	}
	var p model.DeviceProfile
	if err := json.NewDecoder(r.Body).Decode(&p); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json body: "+err.Error())
		return
	}
	p.Kind = kind
	if len(p.Resources) == 0 {
		writeError(w, http.StatusBadRequest, "resources must not be empty")
		return
	}
	saved, err := s.store.UpsertProfile(r.Context(), p)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if err := s.notifyCatalogReload("api"); err != nil {
		writeError(w, http.StatusInternalServerError, "broadcast catalog reload: "+err.Error())
		return
	}
	writeJSON(w, http.StatusOK, saved)
}

func (s *Server) handleDeleteProfile(w http.ResponseWriter, r *http.Request) {
	kind := r.PathValue("kind")
	err := s.store.DeleteProfile(r.Context(), kind)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "profile not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if err := s.notifyCatalogReload("api"); err != nil {
		writeError(w, http.StatusInternalServerError, "broadcast catalog reload: "+err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"deleted": true, "kind": kind})
}

func (s *Server) handleReloadCatalog(w http.ResponseWriter, r *http.Request) {
	if s.profilesPath == "" {
		writeError(w, http.StatusBadRequest, "profiles path is not configured")
		return
	}
	count, err := store.SyncProfilesFromFile(r.Context(), s.store, s.profilesPath)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if err := s.notifyCatalogReload("file"); err != nil {
		writeError(w, http.StatusInternalServerError, "broadcast catalog reload: "+err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"reloaded": true,
		"source":   s.profilesPath,
		"count":    count,
	})
}

func (s *Server) notifyCatalogReload(source string) error {
	if s.bcast == nil {
		return nil
	}
	return s.bcast.PublishBroadcast(events.SubjectCatalogReloaded, events.CatalogReloadedEvent{
		Source:     source,
		ReloadedAt: time.Now().UTC(),
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
