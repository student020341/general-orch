package diy

import (
	"orch/examples/diy/lightspinner"
	"orch/sdk"
)

// Provider groups DIY hardware drivers under the "diy" provider namespace.
type Provider struct {
	LightSpinner *lightspinner.MockStore
}

// NewProvider creates a DIY provider backed by a shared mock light-spinner store.
func NewProvider() *Provider {
	return &Provider{
		LightSpinner: lightspinner.NewMockStore(),
	}
}

func (p *Provider) Name() string { return "diy" }

func (p *Provider) Drivers() []sdk.Driver {
	return []sdk.Driver{
		lightspinner.NewDriver(p.LightSpinner),
	}
}
