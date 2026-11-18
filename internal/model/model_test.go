package model

import (
	"reflect"
	"testing"
)

func TestMergeState(t *testing.T) {
	base := State{
		"light1": State{"r": float64(255), "g": float64(0), "b": float64(0)},
		"light2": State{"r": float64(0), "g": float64(255), "b": float64(0)},
		"rotation": float64(90),
	}
	patch := State{
		"light1": State{"b": float64(128)},
		"light2": nil,
	}

	got := MergeState(base, patch)
	want := State{
		"light1": State{"r": float64(255), "g": float64(0), "b": float64(128)},
		"rotation": float64(90),
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("MergeState mismatch:\ngot:  %#v\nwant: %#v", got, want)
	}
	if _, ok := base["light2"]; !ok {
		t.Fatalf("base map should not be mutated")
	}
}
