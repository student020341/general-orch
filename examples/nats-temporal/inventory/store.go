package inventory

import (
	"maps"
	"sync"

	"orch/examples/nats-temporal/domain"
)

// DeploymentStore simulates the persisted deployment and resource reservation database.
type DeploymentStore struct {
	mu      sync.RWMutex
	records map[string]domain.DeploymentRecord
}

// NewDeploymentStore creates an empty deployment store.
func NewDeploymentStore() *DeploymentStore {
	return &DeploymentStore{
		records: make(map[string]domain.DeploymentRecord),
	}
}

// Get returns a copy of the deployment record for accountID, if one exists.
func (s *DeploymentStore) Get(accountID string) (domain.DeploymentRecord, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	rec, ok := s.records[accountID]
	if !ok {
		return domain.DeploymentRecord{}, false
	}
	return cloneRecord(rec), true
}

// Save stores a copy of rec keyed by rec.AccountID.
func (s *DeploymentStore) Save(rec domain.DeploymentRecord) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.records[rec.AccountID] = cloneRecord(rec)
}

// IntendedPortDemand returns the total active speed (Mbps) across all deployments
// routed through (hubID, port), covering both direct connections and splitters.
func (s *DeploymentStore) IntendedPortDemand(hubID string, port int) int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	total := 0
	for _, rec := range s.records {
		if !rec.Active || rec.Topology == nil {
			continue
		}
		if rec.Topology.HubID == hubID && rec.Topology.HubPort == port {
			total += rec.TargetSpeed
		}
	}
	return total
}

// IntendedHubDemand returns the total active speed (Mbps) across all deployments
// connected to hubID (both direct customers and all downstream splitters).
func (s *DeploymentStore) IntendedHubDemand(hubID string) int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	total := 0
	for _, rec := range s.records {
		if !rec.Active || rec.Topology == nil {
			continue
		}
		if rec.Topology.HubID == hubID {
			total += rec.TargetSpeed
		}
	}
	return total
}

func cloneRecord(rec domain.DeploymentRecord) domain.DeploymentRecord {
	if rec.Topology != nil {
		topoCopy := *rec.Topology
		rec.Topology = &topoCopy
	}
	if rec.JobStates != nil {
		rec.JobStates = maps.Clone(rec.JobStates)
	}
	return rec
}
