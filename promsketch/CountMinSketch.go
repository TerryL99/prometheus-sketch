package promsketch 

import (
	"math"
	"math/rand"
	"hash"
	"hash/fnv"
	"errors"
	"time"
	"github.com/spaolacci/murmur3"
)

type CountMinSketch struct {
	row		int
	col		int
	seed1 	[]uint32
	count 	[][]int64
	hasher hash.Hash64
}

func NewCountMinSketch(row, col int) (s *CountMinSketch, err error) {
	if row <= 0 || col <= 0 {
		return nil, errors.New("CountMinSketch New: values of row and col should be positive")
	}

	s = &CountMinSketch{
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

	s.seed1 = make([]uint32, row)
	rand.Seed(time.Now().UnixNano())
	for r := 0; r < row; r++ {
		s.seed1[r] = rand.Uint32()
	}

	return s, nil
}

// Row returns the number of rows (hash functions)
func (s *CountMinSketch) Row() int { return s.row }

// Col returns the number of colums
func (s *CountMinSketch) Col() int { return s.col }

func (s *CountMinSketch) position(key []byte) (pos []int) {
	pos = make([]int, s.row)
	for i := 0; i < s.row; i++ {
		pos[i] = int(murmur3.Sum32WithSeed(key, s.seed1[i]) % uint32(s.col))
	}
	return pos
}

func (s *CountMinSketch) CMProcessing(key string, value int64) {
	// line_to_udpate := s.line_to_udpate
	// col_loc := xxhash.Sum64String(key) % uint64(CM_COL_NO)
	// s.count[line_to_udpate][col_loc] += value // value is 1 for frequency
	pos := s.position([]byte(key))
	for r, c := range pos {
		s.count[r][c] += value
	}
}

func (s *CountMinSketch) EstimateString(key string) int64 {
	pos := s.position([]byte(key))
	var res int64 = math.MaxInt64
	for r, c := range pos {
		if res > s.count[r][c] {
			res = s.count[r][c]
		}
	}
	return res
}