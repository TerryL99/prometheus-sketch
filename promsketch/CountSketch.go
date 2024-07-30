package promsketch

import (
	"hash"
	"hash/fnv"
	// "bytes"
	// "encoding/binary"
	// "encoding/json"
	// "io"
	"math"
	// "os"
	"errors"
	// "fmt"
	"sort"
	"github.com/spaolacci/murmur3"
	"math/rand"
	"time"
)


type CountSketch struct {
	row		int
	col		int
	count 	[][]int64
	hasher 	hash.Hash64
	seeds 	[]uint32
	sign_seeds 	[]uint32
	sums		[]int64
	bucket_size int
}

func NewCountSketch(row int, col int, seed1, seed2 []uint32,) (s *CountSketch, err error) {
	if row <= 0 || col <= 0 {
		return nil, errors.New("CountSketch New: values of row and col should be positive")
	}

	s = &CountSketch{
		row: row,
		col: col,
		hasher: fnv.New64(),
	}
	
	s.count = make([][]int64, row)
	for r := 0; r < row; r++ {
		s.count[r] = make([]int64, col)
		for c := 0; c < col; c++ {
			s.count[r][c] = 0
		}
	}
	s.sums = make([]int64, row)

	s.seeds = make([]uint32, row)
	s.sign_seeds = make([]uint32, row)
	for r := 0; r < row; r++ {
		s.seeds[r] = seed1[r]
		s.sign_seeds[r] = seed2[r]
	}

	return s, nil
}

// NewWithEstimates creates a new Count Sketch with given error rate and condifence. 
// Accuracy guarantees will be made in terms of a pair of user specified parameters, 
// ε and δ, meaning that the error in answering a query is within a factor of ε with
// probability δ.
func NewCountSketchWithEstimates(epsilon, delta float64) (s *CountSketch, err error) {
	if epsilon <= 0 || epsilon >= 1 {
		return nil, errors.New("CountSketch NewWithEstiamtes: value of epsilon should be in range (0,1)")
	}
	if delta <= 0 || delta >= 1 {
		return nil, errors.New("CountSketch NewWithEstimates: value of delta should be in range (0,1)")
	}

	row := int(math.Ceil(2.72 / epsilon / epsilon))
	col := int(math.Ceil(math.Log(delta) / math.Log(0.5))) // e.g., delta = 0.05

	seed1 := make([]uint32, row)
	seed2 := make([]uint32, row)
	rand.Seed(time.Now().UnixNano())
	for r := 0; r < row; r++ {
		seed1[r] = rand.Uint32()
		seed2[r] = rand.Uint32()
	}

	return NewCountSketch(row, col, seed1, seed2)
}

// Row returns the number of rows (hash functions)
func (s *CountSketch) Row() int { return s.row }

// Col returns the number of colums
func (s *CountSketch) Col() int { return s.col }

// computes the positions in the count sketch and the signs for updating the counts based on the input key.
func (s *CountSketch) position_and_sign(key []byte) (pos []int32, sign []int32) {
	pos = make([]int32, s.row)
	sign = make([]int32, s.row)
	var hash1, hash2 uint32
	for i := uint32(0); i < uint32(s.row); i++ {
		hash1 = murmur3.Sum32WithSeed(key, s.seeds[i])
		hash2 = murmur3.Sum32WithSeed(key, hash1)
		pos[i] = int32((hash1 + i * hash2) % uint32(s.col))
		sign[i] = int32(murmur3.Sum32WithSeed(key, s.sign_seeds[i]) % 2)
		sign[i] = sign[i] * 2 - 1
	}
	return pos, sign
}

// Update the count sketch with the given key and count.
func (s *CountSketch) UpdateString(key string, count int64) {
	pos, sign := s.position_and_sign([]byte(key))
	for r, c := range pos {
		cur_count := s.count[r][c]
		s.count[r][c] += int64(sign[r]) * count
		s.sums[r] += s.count[r][c] * s.count[r][c] - cur_count * cur_count
	}
}

// Estimate the count of the given key.
func (s *CountSketch) EstimateString(key string) int64 {
	pos, sign := s.position_and_sign([]byte(key))
	counters := make([]int64, s.row)
	for r, c := range pos {
		counters[r] = int64(sign[r]) * s.count[r][c]
	}

	sort.Slice(counters, func(i, j int) bool { return counters[i] < counters[j] })
	median := counters[s.row / 2]
	if median <= 0 {
		return 1
	}
	return median
}

// Update the count sketch with the given key and count.
func (s *CountSketch) UpdateAndEstimateString(key string, count int64) int64 {
	pos, sign := s.position_and_sign([]byte(key))
	for r, c := range pos {
		s.count[r][c] += int64(sign[r]) * count
	}
	
	counters := make([]int64, s.row)
	for r, c := range pos {
		counters[r] = int64(sign[r]) * s.count[r][c]
	}

	sort.Slice(counters, func(i, j int) bool { return counters[i] < counters[j] })
	median := counters[s.row / 2]
	if median <= 0 {
		return 1
	}
	return median
}

// Computes an approximation of the L2 norm of the count sketch
func (s *CountSketch) cs_l2() float64 {	
	sos := make([]int64, CS_ROW_NO)
	for i := 0; i < CS_ROW_NO; i++ {
		sos[i] = s.sums[i]
	}

	sort.Slice(sos, func(i, j int) bool { return sos[i] < sos[j] })
	f2_value := sos[CS_ROW_NO / 2]
	
	return math.Sqrt(float64(f2_value))
}


func (s *CountSketch) UpdateInt(key uint32, count int64) {
	pos, sign := s.position_and_sign(i32tob(key))
	for r, c := range pos {
		cur_count := s.count[r][c]
		s.count[r][c] += int64(sign[r]) * count
		s.sums[r] += s.count[r][c] * s.count[r][c] - cur_count * cur_count
	}
}


func (s *CountSketch) EstimateInt(key uint32) int64 { 
	pos, sign := s.position_and_sign(i32tob(key))
	counters := make([]int64, s.row)
	for r, c := range pos {
		counters[r] = int64(sign[r]) * s.count[r][c]
	}

	sort.Slice(counters, func(i, j int) bool { return counters[i] < counters[j] })
	median := counters[s.row / 2]
	if median <= 0 {
		return 1
	}
	return median
}