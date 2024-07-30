package promsketch

import (
	"hash"
	"hash/fnv"
	"math"

	"github.com/cespare/xxhash/v2"
)

/*
Can be used for Prometheus functions: 
- count_over_time
- entropy_over_time (newly added)
- hh(topk)_over_time (newly added)
- card_over_time (newly added)
- sum_over_time
- avg_over_time
- stddev_over_time
- stdvar_over_time
- min_over_time
- max_over_time
*/

// HHLayerStruct represents the heavy hitters layer structure used in the sketch.
type HHLayerStruct struct {
	topK	*TopKHeap
}

// NewHHLayerStruct initializes a new HHLayerStruct with a given top K value.
func NewHHLayerStruct(k int) (hh_layer_s *HHLayerStruct, err error) {
	topkheap := NewTopKHeap(k)
	hh_layer_s = &HHLayerStruct{
		topK: topkheap,
	}
	
	return hh_layer_s, nil
}

// UnivSketch represents a universal sketch with various configurations and layers.
type UnivSketch struct {
	k			int  // topK
	row			int
	col			int
	layer 		int
	hasher		hash.Hash64
	cs_layers 	[]*CountSketch
	HH_layers 	[]*HHLayerStruct
	bucket_size int // for sliding window model; per sketch
}

// New create a new Universal Sketch with row hashing funtions and col counters per row of a Count Sketch.
func NewUnivSketch(k, row, col, layer int, seed1, seed2 []uint32) (us *UnivSketch, err error) {
	us = &UnivSketch{
		k:		k,
		row:	row,
		col:	col,
		layer:	layer,
		hasher: fnv.New64(),
	}

	// Initialize count sketches and heavy hitter layers.
	us.cs_layers = make([]*CountSketch, layer)
	us.HH_layers = make([]*HHLayerStruct, layer)
	for i := 0; i < layer; i++ {
		us.cs_layers[i], _ = NewCountSketch(row, col, seed1, seed2)
		us.HH_layers[i], _ = NewHHLayerStruct(k)
	}

	return us, nil
}


// find the last possible layer for each key
func (us *UnivSketch) findBottomLayerNum(hash uint64, layer int) (int){
	// optimization -- hash only once
	// if hash mod 2 == 1, go down
	for l := 0; l < layer - 1; l++ {
		if ((hash >> l) & 1) == 0 {
			return l
		}
	}
	return layer - 1
}

// update multiple layers from top to bottom_layer_num
// insert a key into Universal Sketch
func (us *UnivSketch) update(key string, value int64, bottom_layer_num int) {
	for l := 0; l <= bottom_layer_num; l++ {
		median_count := us.cs_layers[l].UpdateAndEstimateString(key, value) // add item key to the layer	
		us.HH_layers[l].topK.Update(key, median_count)
	}	
}

func (us *UnivSketch) univmon_processing(key string, value int64) {
	hash := xxhash.Sum64String(key)
	bottom_layer_num := us.findBottomLayerNum(hash, CS_LVLS)
	us.update(key, value, bottom_layer_num)
}

// Query Universal Sketch
func (us *UnivSketch) calcGSumHeuristic(g func(float64) float64) float64 {
	Y := make([]float64, us.layer)
	var coe float64 = 1
	var hash float64 = 0.0
	var tmp float64 = 0 

	Y[us.layer-1] = 0

	for _, item := range us.HH_layers[us.layer-1].topK.heap {
		tmp += g(float64(item.count))
	}
	Y[us.layer-1] = tmp
	
	for i := (us.layer - 2); i >= 0; i-- {
		tmp = 0
		for _, item := range us.HH_layers[i].topK.heap {
			hash = 0.0
			for _, next_layer_item := range us.HH_layers[i+1].topK.heap {
				if item.key == next_layer_item.key {
					hash = 1.0
					break
				}
			}
			coe = 1 - 2 * hash
			tmp += coe * g(float64(item.count))
		}
		Y[i] = 2 * Y[i+1] + tmp
	}

	return Y[0]
}

func (us *UnivSketch) calcGSum(g func(float64) float64) float64 {
	return us.calcGSumHeuristic(g)
}

func (us *UnivSketch) calcL1() float64 {
	return us.calcGSum(func(x float64) float64 { return x })
}

func (us *UnivSketch) calcEntropy() float64 {
	return us.calcGSum(func(x float64) float64 { return x * math.Log(x) / math.Log(2) })
}

func (us *UnivSketch) calcCard() float64 {
	return us.calcGSum(func(x float64) float64 { return 1 })
}
