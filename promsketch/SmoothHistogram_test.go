package promsketch

import (
	"fmt"
	"math"
	"math/rand"
	"testing"
	"time"
)

func TestSmoothHistogram(t *testing.T) {
	fmt.Println("Hello TestSmoothHistogram")

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


	// packet number based interval, not time-based intervals

	seed1 := make([]uint32, CS_ROW_NO)
	seed2 := make([]uint32, CS_ROW_NO)
	rand.Seed(time.Now().UnixNano())
	for r := 0; r < CS_ROW_NO; r++ {
		seed1[r] = rand.Uint32()
		seed2[r] = rand.Uint32()
	}

	s, _ := NewUnivSketch(TOPK_SIZE, CS_ROW_NO, CS_COL_NO, CS_LVLS, seed1, seed2)
	s1, _ := NewUnivSketch(TOPK_SIZE, CS_ROW_NO, CS_COL_NO, CS_LVLS, seed1, seed2)
	s2, _ := NewUnivSketch(TOPK_SIZE, CS_ROW_NO, CS_COL_NO, CS_LVLS, seed1, seed2)


	beta := 1.0
	shu := smooth_init_univmon(beta)

	// fmt.Println("successfully init universal sketch")

	t1 := int(130/3)
	t2 := int(130/3 *2)
	pkt := int(0)
	card := make(map[string]int)
	for _, c := range cases {
		for i := 0; i < int(c.cnt); i++ {
			shu.smooth_update_univmon(c.key)
			pkt += 1
			// ground truth
			if pkt >= t1 && pkt <= t2 {
				s.univmon_processing(c.key, 1)
				// fmt.Println(c.key)
				if _, ok := card[c.key]; ok {
					card[c.key] += 1
				} else {
					card[c.key] = 1
				}
			}
			if pkt <= 3 {
				s1.univmon_processing(c.key, 1)
			}
			if pkt <= 130 {
				s2.univmon_processing(c.key, 1)
			}
		}
	}

	var entropy_norm float64 = 0
	for _, v := range card {
		tmp_count := float64(v)
		entropy_norm += tmp_count * math.Log(tmp_count) / math.Log(2)
	}
	m := float64(t2 - t1)
	entropy := math.Log(m) / math.Log(2) - entropy_norm / m

	
	merged_univ := shu.query_interval_merge(t1, t2, 130)
	entropyNormEstimate := merged_univ.calcEntropy()
	entropyEstimate := math.Log(m) / math.Log(2) - entropyNormEstimate / m
	cardEstimate := merged_univ.calcCard()
	fmt.Println("entropyNormEstimate =", entropyNormEstimate)
	fmt.Println("entropyEstimate =", entropyEstimate)
	fmt.Println("cardEstimate =", cardEstimate)
	fmt.Println("cardEstimate =", query_univmon_distinct(merged_univ))

	fmt.Println("deterministic ground truth: card:", len(card), "entropy_norm:", entropy_norm, "entropy:", entropy)
	

	singleUnivMonEntropyNorm := s.calcEntropy()
	singleUnivMonEntropy := math.Log(m) / math.Log(2) - singleUnivMonEntropyNorm / m
	singleUnivMonCard := s.calcCard()
	fmt.Println("single univmon: card:", singleUnivMonCard, "entropy_norm:", singleUnivMonEntropyNorm, "entropy:", singleUnivMonEntropy)
	
	// t1 = 3, t2 = 130
	pkt = 0
	card1 := make(map[string]int)
	for _, c := range cases {
		for i := 0; i < int(c.cnt); i++ {
			pkt += 1
			if pkt > 3 && pkt <= 130 {
				if _, ok := card1[c.key]; ok {
					card1[c.key] += 1
				} else {
					card1[c.key] = 1
				}
			}
		}
	}

	var entropy_norm1 float64 = 0
	for k, v := range card1 {
		fmt.Println(k, v)
		tmp_count1 := float64(v)
		entropy_norm1 += tmp_count1 * math.Log(tmp_count1) / math.Log(2)
	}
	m = float64(130-3)
	entropy1 := math.Log(m) / math.Log(2) - entropy_norm1 / m
	fmt.Println(entropy1)
	fmt.Println(len(card1))


	// s2 - s1
	merged_univ_precise, _ := NewUnivSketch(TOPK_SIZE, CS_ROW_NO, CS_COL_NO, CS_LVLS, seed1, seed2) // new && init; seed1 and seed2 should be the same as other UnivSketch
	for i := 0; i < CS_LVLS; i++ {
		for j := 0; j < CS_ROW_NO; j++ {
			for k := 0; k < CS_COL_NO; k++ {
				merged_univ_precise.cs_layers[i].count[j][k] = s2.cs_layers[i].count[j][k] - s1.cs_layers[i].count[j][k]
			}
		}

		merged_univ_precise.HH_layers[i].topK = s2.HH_layers[i].topK
		for _, item := range merged_univ_precise.HH_layers[i].topK.heap {
			item.count = merged_univ_precise.cs_layers[i].EstimateString(item.key)
		}
	}
	fmt.Println("merged_univ_precise card =", merged_univ_precise.calcCard())
	fmt.Println("merged_univ_precise entropy_norm =", merged_univ_precise.calcEntropy())


	// t.Logf("got %f, expect %d", got, 130)
}