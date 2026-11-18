package rotation

import "testing"

func TestShortestTurnDelta(t *testing.T) {
	cases := []struct {
		from float64
		to   float64
		want float64
	}{
		{350, 10, 20},
		{10, 350, -20},
		{0, 180, 180},
		{90, 90, 0},
		{270, 45, 135},
		{45, 270, -135},
	}
	for _, tc := range cases {
		got := ShortestTurnDelta(tc.from, tc.to)
		if got != tc.want {
			t.Errorf("ShortestTurnDelta(%v, %v) = %v, want %v", tc.from, tc.to, got, tc.want)
		}
	}
}
