package resolver

import (
	"encoding/json"
	"fmt"

	"orch/examples/nats-temporal/domain"
	"orch/examples/nats-temporal/inventory"
	"orch/examples/nats-temporal/providers/controller"
	"orch/examples/nats-temporal/providers/wired"
	"orch/sdk"
)

// Resolver translates declarative service specs into dependency-ordered orch jobs.
type Resolver struct {
	inv   *inventory.Registry
	store *inventory.DeploymentStore
}

func New(inv *inventory.Registry, store *inventory.DeploymentStore) *Resolver {
	return &Resolver{inv: inv, store: store}
}

// Resolve inspects inventory and deployment state to build an ExecutionPlan.
func (r *Resolver) Resolve(spec domain.ProvisionSpec) (domain.ExecutionPlan, error) {
	if spec.AccountID == "" || spec.SubscriptionID == "" {
		return domain.ExecutionPlan{}, sdk.Terminal(fmt.Errorf("account_id and subscription_id are required"))
	}

	switch spec.Platform {
	case domain.PlatformController:
		return r.resolveController(spec)
	case domain.PlatformWired:
		return r.resolveWired(spec)
	default:
		return domain.ExecutionPlan{}, sdk.Terminal(fmt.Errorf("unsupported platform %q", spec.Platform))
	}
}

func (r *Resolver) resolveController(spec domain.ProvisionSpec) (domain.ExecutionPlan, error) {
	rec, exists := r.store.Get(spec.AccountID)
	if !spec.Active && !exists {
		// Nothing to decommission; avoid autovivifying a stuck deployment record.
		return domain.ExecutionPlan{
			AccountID:      spec.AccountID,
			SubscriptionID: spec.SubscriptionID,
			Platform:       spec.Platform,
			CorrelationID:  spec.CorrelationID,
			TargetActive:   false,
		}, nil
	}

	if spec.Active && spec.TargetSpeedMbps <= 0 {
		return domain.ExecutionPlan{}, sdk.Terminal(fmt.Errorf("target_speed_mbps must be positive"))
	}

	resJSON, err := json.Marshal(controller.EntryResource{SubscriptionID: spec.SubscriptionID})
	if err != nil {
		return domain.ExecutionPlan{}, err
	}

	// Calculate controller template variables (e.g. VLAN from tier + partner).
	vlan := 200
	if spec.ConfigTier == "exclusive" {
		vlan = 250
	}
	tgtJSON, err := json.Marshal(controller.EntryTarget{
		Active:      spec.Active,
		UnitSerial:  spec.UnitSerial,
		ServiceVLAN: vlan,
		SpeedMbps:   spec.TargetSpeedMbps,
		ConfigTier:  spec.ConfigTier,
	})
	if err != nil {
		return domain.ExecutionPlan{}, err
	}

	rec = domain.DeploymentRecord{
		AccountID:      spec.AccountID,
		SubscriptionID: spec.SubscriptionID,
		Platform:       domain.PlatformController,
		Active:         spec.Active,
		TargetSpeed:    spec.TargetSpeedMbps,
		JobStates:      map[string]domain.JobStatus{"controller-entry": domain.JobPending},
	}
	r.store.Save(rec)

	return domain.ExecutionPlan{
		AccountID:      spec.AccountID,
		SubscriptionID: spec.SubscriptionID,
		Platform:       domain.PlatformController,
		CorrelationID:  spec.CorrelationID,
		TargetActive:   spec.Active,
		Jobs: []domain.ResourceJob{
			{
				ID: "controller-entry",
				Request: sdk.Request{
					Provider: "controller",
					Kind:     "entry",
					Resource: resJSON,
					Target:   tgtJSON,
				},
			},
		},
	}, nil
}

func (r *Resolver) resolveWired(spec domain.ProvisionSpec) (domain.ExecutionPlan, error) {
	rec, exists := r.store.Get(spec.AccountID)
	if !spec.Active && !exists && !r.inv.HasUnit(spec.UnitSerial) {
		// Clean teardown when neither DB record nor inventory serial exists.
		return domain.ExecutionPlan{
			AccountID:      spec.AccountID,
			SubscriptionID: spec.SubscriptionID,
			Platform:       spec.Platform,
			CorrelationID:  spec.CorrelationID,
			TargetActive:   false,
		}, nil
	}

	var topo domain.CircuitTopology
	if exists && rec.Topology != nil && !spec.Active {
		topo = *rec.Topology
	} else {
		var err error
		topo, err = r.inv.TraceCircuit(spec.UnitSerial, spec.PartnerSlug)
		if err != nil {
			return domain.ExecutionPlan{}, err
		}
	}

	plan := domain.ExecutionPlan{
		AccountID:      spec.AccountID,
		SubscriptionID: spec.SubscriptionID,
		Platform:       domain.PlatformWired,
		CorrelationID:  spec.CorrelationID,
		TargetActive:   spec.Active,
	}

	hubRes, err := json.Marshal(wired.HubResource{HubID: topo.HubID, Port: topo.HubPort})
	if err != nil {
		return domain.ExecutionPlan{}, err
	}
	uplinkRes, err := json.Marshal(wired.HubUplinkResource{HubID: topo.HubID})
	if err != nil {
		return domain.ExecutionPlan{}, err
	}
	unitRes, err := json.Marshal(wired.UnitResource{Serial: topo.UnitSerial})
	if err != nil {
		return domain.ExecutionPlan{}, err
	}

	if spec.Active && spec.TargetSpeedMbps <= 0 {
		return domain.ExecutionPlan{}, sdk.Terminal(fmt.Errorf("target_speed_mbps must be positive"))
	}

	// Persist this account's intent before computing shared hub port and uplink demand.
	nextRec := domain.DeploymentRecord{
		AccountID:      spec.AccountID,
		SubscriptionID: spec.SubscriptionID,
		Platform:       domain.PlatformWired,
		Active:         spec.Active,
		UtilityReady:   rec.UtilityReady,
		PartnerAcked:   rec.PartnerAcked,
		TargetSpeed:    spec.TargetSpeedMbps,
		Topology:       &topo,
	}
	if !spec.Active {
		nextRec.TargetSpeed = 0
	}
	r.store.Save(nextRec)

	portDemand := r.store.IntendedPortDemand(topo.HubID, topo.HubPort)
	hubDemand := r.store.IntendedHubDemand(topo.HubID)

	if !spec.Active {
		// Clear unit and splitter slot; scale down or clear shared hub port and uplink based on remaining intent.
		plan.Jobs = append(plan.Jobs, domain.ResourceJob{
			ID: "unit",
			Request: sdk.Request{
				Provider: "wired",
				Kind:     "unit",
				Resource: unitRes,
				Target:   mustJSON(wired.UnitTarget{Active: ptr(false)}),
			},
		})
		if topo.SplitterID != "" {
			splitRes := mustJSON(wired.SplitterResource{SplitterID: topo.SplitterID, Slot: topo.SplitterSlot})
			plan.Jobs = append(plan.Jobs, domain.ResourceJob{
				ID: "splitter",
				Request: sdk.Request{
					Provider: "wired",
					Kind:     "splitter",
					Resource: splitRes,
					Target:   mustJSON(wired.SplitterTarget{Active: ptr(false)}),
				},
			})
		}
		hubTarget := wired.HubTarget{Active: ptr(false)}
		if portDemand > 0 {
			hubTarget = wired.HubTarget{Active: ptr(true), SpeedMbps: ptr(portDemand)}
		}
		plan.Jobs = append(plan.Jobs, domain.ResourceJob{
			ID: "hub",
			Request: sdk.Request{
				Provider: "wired",
				Kind:     "hub",
				Resource: hubRes,
				Target:   mustJSON(hubTarget),
			},
		})
		uplinkTarget := wired.HubUplinkTarget{Active: ptr(false)}
		if hubDemand > 0 {
			uplinkTarget = wired.HubUplinkTarget{Active: ptr(true), SpeedMbps: ptr(hubDemand)}
		}
		plan.Jobs = append(plan.Jobs, domain.ResourceJob{
			ID: "hub-uplink",
			Request: sdk.Request{
				Provider: "wired",
				Kind:     "hub-uplink",
				Resource: uplinkRes,
				Target:   mustJSON(uplinkTarget),
			},
		})
	} else {
		// Conditional pre-stage for utility service type.
		if spec.ServiceType == domain.ServiceUtility && !rec.UtilityReady {
			plan.PreJobs = append(plan.PreJobs, domain.ResourceJob{
				ID: "hub-utility",
				Request: sdk.Request{
					Provider: "wired",
					Kind:     "hub",
					Resource: hubRes,
					Target: mustJSON(wired.HubTarget{
						Active:      ptr(true),
						SpeedMbps:   ptr(100),
						UtilityVLAN: ptr(topo.UtilityVLAN),
					}),
				},
			})
		}

		plan.RequiresPartnerAck = spec.RequiresPartnerAck && !rec.PartnerAcked

		hubTarget := wired.HubTarget{
			Active:      ptr(true),
			SpeedMbps:   ptr(portDemand),
			ServiceVLAN: ptr(topo.ServiceVLAN),
		}
		if spec.ServiceType == domain.ServiceUtility {
			hubTarget.UtilityVLAN = ptr(topo.UtilityVLAN)
		}

		plan.Jobs = append(plan.Jobs, domain.ResourceJob{
			ID: "hub",
			Request: sdk.Request{
				Provider: "wired",
				Kind:     "hub",
				Resource: hubRes,
				Target:   mustJSON(hubTarget),
			},
		})
		plan.Jobs = append(plan.Jobs, domain.ResourceJob{
			ID: "hub-uplink",
			Request: sdk.Request{
				Provider: "wired",
				Kind:     "hub-uplink",
				Resource: uplinkRes,
				Target: mustJSON(wired.HubUplinkTarget{
					Active:    ptr(true),
					SpeedMbps: ptr(hubDemand),
				}),
			},
		})

		if topo.SplitterID != "" {
			splitRes := mustJSON(wired.SplitterResource{SplitterID: topo.SplitterID, Slot: topo.SplitterSlot})
			plan.Jobs = append(plan.Jobs, domain.ResourceJob{
				ID: "splitter",
				Request: sdk.Request{
					Provider: "wired",
					Kind:     "splitter",
					Resource: splitRes,
					Target: mustJSON(wired.SplitterTarget{
						Active:      ptr(true),
						SpeedMbps:   ptr(spec.TargetSpeedMbps),
						ServiceVLAN: ptr(topo.ServiceVLAN),
						ConfigTier:  ptr(spec.ConfigTier),
					}),
				},
			})
		}

		plan.Jobs = append(plan.Jobs, domain.ResourceJob{
			ID: "unit",
			Request: sdk.Request{
				Provider: "wired",
				Kind:     "unit",
				Resource: unitRes,
				Target: mustJSON(wired.UnitTarget{
					Active:     ptr(true),
					SpeedMbps:  ptr(spec.TargetSpeedMbps),
					ConfigTier: ptr(spec.ConfigTier),
					IPMode:     ptr(spec.IPMode),
				}),
			},
		})
	}

	jobStates := make(map[string]domain.JobStatus, len(plan.Jobs))
	for _, j := range plan.Jobs {
		jobStates[j.ID] = domain.JobPending
	}
	nextRec.JobStates = jobStates
	r.store.Save(nextRec)

	return plan, nil
}

func ptr[T any](v T) *T { return &v }

func mustJSON(v any) json.RawMessage {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}
