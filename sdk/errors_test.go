package sdk_test

import (
	"errors"
	"fmt"
	"testing"

	"orch/sdk"
)

func TestTerminalError(t *testing.T) {
	if sdk.Terminal(nil) != nil {
		t.Fatal("expected Terminal(nil) to return nil")
	}
	if sdk.IsTerminal(nil) {
		t.Fatal("expected IsTerminal(nil) to be false")
	}

	base := errors.New("invalid vlan")
	if sdk.IsTerminal(base) {
		t.Fatal("expected plain error not to be terminal")
	}

	term := sdk.Terminal(base)
	if !sdk.IsTerminal(term) {
		t.Fatal("expected IsTerminal to be true")
	}
	if !errors.Is(term, base) {
		t.Fatal("expected Terminal to preserve base error in chain")
	}
	if !errors.Is(term, sdk.ErrTerminal) {
		t.Fatal("expected errors.Is(term, ErrTerminal) to be true")
	}

	// Avoid double-wrapping and survive further fmt.Errorf wrapping.
	if got := sdk.Terminal(term); got != term {
		t.Fatal("expected Terminal on an already-terminal error to return it unchanged")
	}
	wrapped := fmt.Errorf("apply failed: %w", term)
	if !sdk.IsTerminal(wrapped) || !errors.Is(wrapped, base) {
		t.Fatal("expected wrapped terminal error to preserve both terminal and base checks")
	}
}
