package promsketch

import (
	"testing"
	"fmt"
	"math/rand"
	"time"
)

func TestUnivSketch(t *testing.T) {

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

	/*
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
	*/

	seed1 := make([]uint32, 5)
	seed2 := make([]uint32, 5)
	rand.Seed(time.Now().UnixNano())
	for r := 0; r < 5; r++ {
		seed1[r] = rand.Uint32()
		seed2[r] = rand.Uint32()
	}

	s, _ := NewUnivSketch(TOPK_SIZE, 5, 10000, 5, seed1, seed2)

	for _, c := range cases {
		s.univmon_processing(c.key, c.cnt)

	}

	result := s.calcL1()
	fmt.Println("result =", result)
	if result != 130 {
		t.Logf("result %f, expect %d", result, 130)
	}
}