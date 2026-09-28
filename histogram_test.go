package totallytics

import (
	"math"
	"testing"
)

func TestBucket(t *testing.T) {
	cases := []struct {
		ms   float64
		want int
	}{
		{math.NaN(), 0},
		{math.Inf(-1), 0},
		{-5, 0},
		{0, 0},
		{0.5, 0},
		{1, 0},
		{1.08, 1},
		{1.09, 2},
		{100, 60},
		{1000, 90},
		{math.Inf(1), maxBucket},
		{1e300, maxBucket},
	}
	for _, tc := range cases {
		if got := bucket(tc.ms); got != tc.want {
			t.Errorf("bucket(%v) = %d, want %d", tc.ms, got, tc.want)
		}
	}
}
