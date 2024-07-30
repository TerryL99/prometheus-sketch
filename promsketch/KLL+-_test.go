package promsketch

import (
	"fmt"
	"math"
	"math/rand"
	"sort"
	"testing"
	/*
		"gonum.org/v1/plot"
		"gonum.org/v1/plot/vg"
		"gonum.org/v1/plot/vg/draw"
		"gonum.org/v1/plot/vg/vgimg"
	*/)

func TestKLLPlusMinus(t *testing.T) {
	
	inserts := 1000
	deletes := int(0.2 * float64(inserts))
	s1 := make([]float64, 0)
	s2 := make([]float64, 0)

	r := rand.New(rand.NewSource(99))
	for i := 0; i < inserts; i++ {
		item := int(r.NormFloat64() * 200) + 200
		for item <= 0 {
			item = int(r.NormFloat64() * 200) + 200
		}
		s1 = append(s1, float64(item))
	}

	for i := 0; i < deletes; i++ {
		item := int(r.NormFloat64() * 100) + 100
		for item <= 0 {
			item = int(r.NormFloat64() * 100) + 100
		}
		s2 = append(s2, float64(item))
	}


	kll_no_deletion := NewKLL(256, 2.0/3.0, true, true, false)
	kll_with_deletion := NewKLL(256, 2.0/3.0, true, true, true)


	for _, item := range s1 {
		kll_no_deletion.Update(item, 1, true)
	}

	for _, item := range s2 {
		kll_with_deletion.Update(item, 1, true)
	}
	for _, item := range s1 {
		kll_with_deletion.Update(item, 1, true)
	}
	for _, item := range s2 {
		kll_with_deletion.Delete(item)
	}

	x_no := make([]float64, 0)
	y_no := make([]float64, 0)
	cdf_no := kll_no_deletion.cdf()
	for _, pair := range cdf_no {
		x_no = append(x_no, pair.item)
		y_no = append(y_no, pair.weight)
	}


	x := make([]float64, 0)
	y := make([]float64, 0)
	cdf := kll_with_deletion.cdf()
	for _, pair := range cdf {
		x = append(x, pair.item)
		y = append(y, pair.weight)
	}

	fmt.Println("With Deletion:")
	fmt.Println(x[:15])
	fmt.Println(y[:15])



	// ground truth
	x_gt := make([]float64, 0)
	y_gt := make([]float64, 0)
	sort.Slice(s1, func(i, j int) bool { return s1[i] < s1[j] })
	gt := make(map[float64]float64)
	for _, item := range s1 {
		if len(x_gt) > 0 && x_gt[len(x_gt)-1] == item {
			y_gt[len(y_gt)-1] += 1
		} else {
			x_gt = append(x_gt, item)
			y_gt = append(y_gt, 1.0)
		}
		if _, ok := gt[item]; ok {
			gt[item] += 1
		} else {
			gt[item] = 1
		}
	}

	// compute CDF ground truth
	for i, value := range y_gt {
		if i > 0 {
			y_gt[i] = y_gt[i-1] + value / float64(len(s1))
		} else {
			y_gt[i] = value / float64(len(s1))
		}
	}
	
	fmt.Println("True Dist:")
	fmt.Println(x_gt[:15])
	fmt.Println(y_gt[:15])
}



func TestKLLQuantile(t *testing.T) {
	values := []float64{1.0, 0.9, 0.8, 0.7, 0.6, 0.5, 0.4, 0.3, 0.2, 0.1}
	kll := NewKLL(256, 2.0/3.0, true, true, false)
	for _, item := range values {
		kll.Update(item, 1, true)
	}
	cdf := kll.cdf()
	items := make([]float64, 0)
	weights := make([]float64, 0)
	for _, pair := range cdf {
		items = append(items, pair.item)
		weights = append(weights, pair.weight)
	}
	fmt.Printf("items = %v\n", items)
	fmt.Printf("weights = %v\n", weights)
	quantile := 0.5
	n := float64(len(weights))
	rank := quantile * (n-1)
	lowerIndex := math.Max(0, math.Floor(rank))
	upperIndex := math.Min(n-1, lowerIndex+1)
	weight := rank - math.Floor(rank)
	result := float64(items[int(lowerIndex)])*(1-weight) + float64(items[int(upperIndex)])*weight
	fmt.Printf("The %v quantile is %v\n", quantile, result)
}