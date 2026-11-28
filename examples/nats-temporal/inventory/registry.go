package inventory

import (
	"fmt"
	"sync"

	"orch/examples/nats-temporal/domain"
)

// UnitRecord describes a home unit in the external inventory registry.
type UnitRecord struct {
	Serial     string
	Model      string // "ssh" or "auto"
	UpstreamID string // splitter ID or hub ID
	PortOrSlot int    // splitter slot or hub port
}

// SplitterRecord describes an optional intermediate splitter wired to a hub port.
type SplitterRecord struct {
	ID      string
	HubID   string
	HubPort int
}

// HubRecord describes a multi-port distribution hub in a cabinet.
type HubRecord struct {
	ID      string
	Cabinet string
}

// ChannelRule maps (cabinet, hub_id, partner_slug) to VLAN/channel allocations.
type ChannelRule struct {
	Cabinet     string
	HubID       string
	PartnerSlug string
	ServiceVLAN int
	UtilityVLAN int
}

// Registry simulates an external physical wiring registry (NetBox analog).
type Registry struct {
	mu        sync.RWMutex
	units     map[string]UnitRecord
	splitters map[string]SplitterRecord
	hubs      map[string]HubRecord
	rules     map[string]ChannelRule
}

// NewRegistry creates an empty external wiring registry.
func NewRegistry() *Registry {
	return &Registry{
		units:     make(map[string]UnitRecord),
		splitters: make(map[string]SplitterRecord),
		hubs:      make(map[string]HubRecord),
		rules:     make(map[string]ChannelRule),
	}
}

func (r *Registry) AddHub(h HubRecord) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.hubs[h.ID] = h
}

func (r *Registry) AddSplitter(s SplitterRecord) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.splitters[s.ID] = s
}

func (r *Registry) AddUnit(u UnitRecord) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.units[u.Serial] = u
}

func (r *Registry) AddChannelRule(rule ChannelRule) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.rules[ruleKey(rule.Cabinet, rule.HubID, rule.PartnerSlug)] = rule
}

// HasUnit reports whether a unit serial exists in the registry.
func (r *Registry) HasUnit(serial string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.units[serial]
	return ok
}

// TraceCircuit crawls cables upstream from unitSerial to discover splitter (if any), hub, and VLANs.
func (r *Registry) TraceCircuit(unitSerial, partnerSlug string) (domain.CircuitTopology, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	unit, ok := r.units[unitSerial]
	if !ok {
		return domain.CircuitTopology{}, fmt.Errorf("unit serial %q not found in inventory", unitSerial)
	}

	topo := domain.CircuitTopology{
		UnitSerial: unit.Serial,
		UnitModel:  unit.Model,
	}

	if splitter, isSplitter := r.splitters[unit.UpstreamID]; isSplitter {
		topo.SplitterID = splitter.ID
		topo.SplitterSlot = unit.PortOrSlot
		topo.HubID = splitter.HubID
		topo.HubPort = splitter.HubPort
	} else {
		topo.HubID = unit.UpstreamID
		topo.HubPort = unit.PortOrSlot
	}

	hub, ok := r.hubs[topo.HubID]
	if !ok {
		return domain.CircuitTopology{}, fmt.Errorf("hub %q not found for unit %q", topo.HubID, unitSerial)
	}
	topo.Cabinet = hub.Cabinet

	rule, ok := r.rules[ruleKey(hub.Cabinet, hub.ID, partnerSlug)]
	if !ok {
		return domain.CircuitTopology{}, fmt.Errorf("no channel rule for cabinet=%q hub=%q partner=%q", hub.Cabinet, hub.ID, partnerSlug)
	}
	topo.ServiceVLAN = rule.ServiceVLAN
	topo.UtilityVLAN = rule.UtilityVLAN
	return topo, nil
}

func ruleKey(cabinet, hubID, partner string) string {
	return cabinet + "|" + hubID + "|" + partner
}
