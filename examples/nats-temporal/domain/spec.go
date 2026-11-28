package domain

import "orch/sdk"

// Platform identifies which provisioning architecture handles a service request.
type Platform string

const (
	PlatformWired      Platform = "wired"
	PlatformController Platform = "controller"
)

// ServiceType distinguishes standard service traffic from utility pre-provisioning.
type ServiceType string

const (
	ServiceData    ServiceType = "data"
	ServiceUtility ServiceType = "utility"
)

// DemarcType describes whether the unit connects directly or through a shared splitter.
type DemarcType string

const (
	DemarcDirect DemarcType = "direct"
	DemarcMulti  DemarcType = "multi"
)

// ProvisionSpec is the declarative target state submitted for an account subscription.
type ProvisionSpec struct {
	AccountID          string      `json:"account_id"`
	SubscriptionID     string      `json:"subscription_id"`
	Active             bool        `json:"active"`
	Platform           Platform    `json:"platform"`
	ServiceType        ServiceType `json:"service_type"`
	DemarcType         DemarcType  `json:"demarc_type"`
	IPMode             string      `json:"ip_mode"`
	ConfigTier         string      `json:"config_tier"`
	PartnerSlug        string      `json:"partner_slug"`
	RequiresPartnerAck bool        `json:"requires_partner_ack"`
	UnitSerial         string      `json:"unit_serial"`
	TargetSpeedMbps    int         `json:"target_speed_mbps"`
	CorrelationID      string      `json:"correlation_id"`
}

// ResourceJob wraps a single orch sdk.Request with job-level dependencies.
type ResourceJob struct {
	ID        string      `json:"id"`
	Request   sdk.Request `json:"request"`
	DependsOn []string    `json:"depends_on,omitempty"`
}

// ExecutionPlan is the Stage 1 Resolver output executed by a runner or Temporal workflow.
type ExecutionPlan struct {
	AccountID          string        `json:"account_id"`
	SubscriptionID     string        `json:"subscription_id"`
	Platform           Platform      `json:"platform"`
	CorrelationID      string        `json:"correlation_id"`
	PreJobs            []ResourceJob `json:"pre_jobs,omitempty"`
	RequiresPartnerAck bool          `json:"requires_partner_ack"`
	Jobs               []ResourceJob `json:"jobs,omitempty"`
	TargetActive       bool          `json:"target_active"`
}

// IsNoOp reports whether the resolved plan has nothing to execute.
func (p ExecutionPlan) IsNoOp() bool {
	return len(p.PreJobs) == 0 && len(p.Jobs) == 0
}

// JobStatus tracks per-resource convergence state within a deployment.
type JobStatus string

const (
	JobPending   JobStatus = "pending"
	JobConverged JobStatus = "converged"
	JobFailed    JobStatus = "failed"
)

// CircuitTopology records the physical path discovered in the inventory registry.
type CircuitTopology struct {
	UnitSerial   string `json:"unit_serial"`
	UnitModel    string `json:"unit_model"`
	SplitterID   string `json:"splitter_id,omitempty"`
	SplitterSlot int    `json:"splitter_slot,omitempty"`
	HubID        string `json:"hub_id"`
	HubPort      int    `json:"hub_port"`
	Cabinet      string `json:"cabinet"`
	ServiceVLAN  int    `json:"service_vlan"`
	UtilityVLAN  int    `json:"utility_vlan"`
}

// DeploymentRecord represents persisted deployment and reservation state for an account.
type DeploymentRecord struct {
	AccountID      string               `json:"account_id"`
	SubscriptionID string               `json:"subscription_id"`
	Platform       Platform             `json:"platform"`
	Active         bool                 `json:"active"`
	UtilityReady   bool                 `json:"utility_ready"`
	PartnerAcked   bool                 `json:"partner_acked"`
	TargetSpeed    int                  `json:"target_speed_mbps"`
	Topology       *CircuitTopology     `json:"topology,omitempty"`
	JobStates      map[string]JobStatus `json:"job_states,omitempty"`
}
