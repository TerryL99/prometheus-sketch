package promsketch 

import (
	"testing"
)

func TestCountMinSketch(t *testing.T) {
	cases := []struct {
		key	string
		cnt	int64
	}{
		{"notfound", 1},
		{"hello", 1},
		{"count", 3},
		{"min", 4},
		{"world", 10},
		{"cheatcheat", 3},
		{"cheatcheat", 7},
		{"min", 2},
		{"hello", 2},
		{"tigger", 34},
		{"flow", 9},
		{"miss", 4},
		{"hello", 30},
		{"world", 10},
		{"hello", 10},
	}

	expected := []struct {
		key	string
		cnt	int64
	}{
		{"notfound", 1},
		{"hello", 43},
		{"count", 3},
		{"min", 6},
		{"world", 20},
		{"cheatcheat", 10},
		{"tigger", 34},
		{"flow", 9},
		{"miss", 4},
	}

	s, _ := NewCountMinSketch(50, 10000)

	for _, c := range cases {
		s.CMProcessing(c.key, c.cnt)
	}

	for i, c := range expected {
		res := s.EstimateString(c.key)
		if c.cnt != res {
			t.Logf("case %d '%s' result %d, expect %d", i, c.key, res, c.cnt)
		}
	}
}