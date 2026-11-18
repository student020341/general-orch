package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"orch/internal/events"
	"orch/internal/model"
	"orch/internal/store/memory"
)

type gwFakeBus struct {
	published  []string
	broadcasts []string
}

func (f *gwFakeBus) PublishJSON(_ context.Context, subject string, _ any) error {
	f.published = append(f.published, subject)
	return nil
}

func (f *gwFakeBus) PublishBroadcast(subject string, _ any) error {
	f.broadcasts = append(f.broadcasts, subject)
	return nil
}

func TestGatewayCatalogCRUDAndReload(t *testing.T) {
	tmpDir := t.TempDir()
	profilesFile := filepath.Join(tmpDir, "profiles.json")
	seed := []model.DeviceProfile{{
		Kind:              "beacon_bot",
		Aspects:           []string{"lights", "rotation"},
		DefaultQueues:     map[string]string{"lights": "lighting-workers", "rotation": "motion-workers"},
		FetchStateQueue:   "motion-workers",
		SupportsLiveState: true,
	}}
	rawSeed, _ := json.Marshal(seed)
	if err := os.WriteFile(profilesFile, rawSeed, 0o600); err != nil {
		t.Fatal(err)
	}

	st := memory.New()
	fb := &gwFakeBus{}
	srv := New(st, fb, fb, profilesFile)

	// 1. Reload from file.
	req := httptest.NewRequest(http.MethodPost, "/catalog/reload", nil)
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("reload status = %d: %s", rec.Code, rec.Body.String())
	}
	if len(fb.broadcasts) != 1 || fb.broadcasts[0] != events.SubjectCatalogReloaded {
		t.Fatalf("expected catalog reload broadcast, got %v", fb.broadcasts)
	}

	// 2. PUT a new profile.
	body := bytes.NewBufferString(`{"aspects":["lights"],"default_queues":{"lights":"pi-workers"},"serial_execution":true}`)
	req = httptest.NewRequest(http.MethodPut, "/catalog/profiles/desk_lamp", body)
	rec = httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("put profile status = %d: %s", rec.Code, rec.Body.String())
	}

	// 3. List profiles -> 2 entries.
	req = httptest.NewRequest(http.MethodGet, "/catalog/profiles", nil)
	rec = httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("list profiles status = %d", rec.Code)
	}
	var listed []model.DeviceProfile
	if err := json.Unmarshal(rec.Body.Bytes(), &listed); err != nil {
		t.Fatal(err)
	}
	if len(listed) != 2 {
		t.Fatalf("expected 2 profiles, got %d", len(listed))
	}

	// 4. DELETE custom profile.
	req = httptest.NewRequest(http.MethodDelete, "/catalog/profiles/desk_lamp", nil)
	rec = httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("delete profile status = %d: %s", rec.Code, rec.Body.String())
	}
	if len(fb.broadcasts) != 3 {
		t.Fatalf("expected 3 catalog reload broadcasts, got %d", len(fb.broadcasts))
	}
}
