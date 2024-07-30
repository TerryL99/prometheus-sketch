package promsketch

import (
	"fmt"
	"math"
	"math/rand"
	"sort"
	"time"
)

/*
Can be used for Prometheus functions:
	- quantile_over_time
*/

type KLLPair struct {
	item float64
	weight int64
}

type estPair struct {
	item float64
	weight float64
}

type KLLSampler struct {
	h int
	weight int64
	item float64
}

func NewKLLSampler() (s *KLLSampler) {
	s = &KLLSampler{
		h: 0,
		weight: 0,
		item: 0,
	}
	return s
}

func (s *KLLSampler) UpdateHeight() {
	s.h += 1
}

// Handles the reception of an item with a given weight into the KLLSampler.
func (s *KLLSampler) ReceiveItem(item float64, item_weight int64) (float64, bool) {
	// default value of item_weight = 1
	if s.h == 0 {
		return item, true
	} else {
		s.weight += item_weight
		if s.weight > int64(math.Pow(2, float64(s.h))) {
			// keep the lighter item
			if item_weight < s.weight - item_weight {
				heavy := s.weight - item_weight
				heavy_item := s.item
				s.item = item
				s.weight = item_weight
				if rand.Float64() < float64(heavy) / math.Pow(2, float64(s.h)) {
					return heavy_item, true
				}
			} else {
				s.weight -= item_weight
				if rand.Float64() < float64(item_weight) / math.Pow(2, float64(s.h)) {
					return item, true
				}
			}
		}

		if rand.Float64() < float64(item_weight) / float64(s.weight) {
			s.item = item
		}
		if s.weight == int64(math.Pow(2, float64(s.h))) {
			s.weight = 0
			return item, true
		}
	}
	return 0, false
}

type Compactor struct {
	list []float64
	numCompaction int
	offset int 
	alternate bool
	numRemove int
}

func NewCompactor(alternate bool) (c *Compactor) {
	c = &Compactor{
		numCompaction: 0,
		offset: 0,
		alternate: alternate,
		numRemove: 0,
	}
	c.list = make([]float64, 0)
	return c
}

// Performs compaction on the Compactor's list of items
func (c *Compactor) compact(deletion bool) ([]float64) {
	// if  list is empty, return nil
	if len(c.list) == 0 {
		return nil
	}

	rand.Seed(time.Now().UnixNano())

	// discard(e_i, -e_i)
	number_of_remove := 0
	dic := make(map[float64]int)
	if deletion == true {
		for _, i := range c.list {
			if _, ok := dic[-1*i]; ok {
				dic[-1*i] -= 1
				if dic[-1 * i] == 0 {
					delete(dic, -1 * i)
					number_of_remove += 1
				}	
			} else {
				if _, ok := dic[i]; ok {
					dic[i] = dic[i] + 1
				} else {
					dic[i] = 1
				}
			}	
		}

		if number_of_remove > 0 {
			// return since paris to discard are found
			c.numRemove += number_of_remove
			c.list = c.list[:0]
			for k, v := range dic {
				for iter := 0; iter < v; iter++ {
					c.list = append(c.list, k) // re-append the remaining items
				}
			}
			sort.Slice(c.list, func(i, j int) bool { return c.list[i] < c.list[j] }) 
			return nil
		}
	}

	// push(E_i, e_i+1)
	sort.Slice(c.list, func(i, j int) bool { return c.list[i] < c.list[j] }) 
	if c.alternate == true && c.numCompaction % 2 == 1 {
		c.offset = 1 - c.offset
	} else {
		if rand.Float32() < 0.5 {
			c.offset = 1 
		} else {
			c.offset = 0
		}
	}

	remains := make([]float64, 0)
	pushed := make([]float64, 0)
	i := 0
	for i < len(c.list) {
		if i + 1 == len(c.list) {
			// last item has no pair
			remains = append(remains, c.list[i])
		} else if SignFloat64(c.list[i]) != SignFloat64(c.list[i+1]) {
			// can not push (delete a, insert b)
			remains = append(remains, c.list[i])
			remains = append(remains, c.list[i+1])
		} else {
			pushed = append(pushed, c.list[i + c.offset])
		}
		i += 2
	}

	c.list = c.list[:len(remains)]
	copy(c.list, remains)
	
	c.numCompaction += 1

	return pushed
}

type KLL struct {
	k int
	c float64
	lazy bool
	alternate bool
	compactors []*Compactor
	H int
	size uint64
	maxSize uint64
	with_deletion bool
	insert_sampler *KLLSampler
	delete_sampler *KLLSampler
	max_H int
}

func NewKLL(k int, c float64, lazy bool, alternate bool, with_deletion bool) (kll *KLL) {
	kll = &KLL{
		k: k, // capacity of the H level, largest one
		c: c, // default = 2.0 / 3.0
		lazy: lazy, // default = true
		alternate: alternate, // default = true
		compactors: make([]*Compactor, 0),
		H: 0,
		size: 0,
		maxSize: 0,
		with_deletion: with_deletion, // default = true
	}
	kll.insert_sampler = NewKLLSampler()
	kll.delete_sampler = NewKLLSampler()
	kll.max_H = int(math.Floor(math.Log(1.0 / float64(k)) / (math.Log(c))))

	kll.grow()
	return kll
}

// increase the height (H) of the KLL sketch and adjusts its capacity accordingly.
func (kll *KLL) grow() (string) {
	if kll.H > kll.max_H {
		kll.insert_sampler.UpdateHeight()
		kll.delete_sampler.UpdateHeight()
		kll.H += 1
		totCapacity := uint64(0)
		for h := 0; h < kll.max_H + 1; h++ {
			totCapacity += kll.capacity(h)
		}
		kll.maxSize = totCapacity
		return "limit"
	} else {
		tmp_c := NewCompactor(kll.alternate)
		kll.compactors = append(kll.compactors, tmp_c)
		kll.H += 1
		if kll.H != len(kll.compactors) {
			fmt.Println("[KLL Error] kll.H != len(kll.compactors)")
		}

		totCapacity := uint64(0)
		for h := 0; h < kll.H; h++ {
			totCapacity += kll.capacity(h)
		}
		kll.maxSize = totCapacity
		return "appending"
	}
}

// calculates the capacity of the KLL sketch at a given height
func (kll *KLL) capacity(height int) (uint64){
	if height > kll.max_H {
		return 0
	}
	depth := MinInt(kll.H, kll.max_H) - height - 1
	return uint64(math.Ceil(math.Pow(kll.c, float64(depth)) * float64(kll.k))) + 2
}


func (kll *KLL) Update(item float64, weight int64, use_sampler bool) {
	// default values: weight int = 1, use_sampler bool = true
	var element float64
	var ele_exist bool 
	if use_sampler == true {
		if SignFloat64(item) == 1 {
			element, ele_exist = kll.insert_sampler.ReceiveItem(item, weight)
		} else {
			element, ele_exist = kll.delete_sampler.ReceiveItem(item, weight)
		}
	} else {
		element = item
		ele_exist = true
	}

	if ele_exist == true {
		kll.compactors[0].list = append(kll.compactors[0].list, element)
		kll.size += 1
		if kll.size >= kll.maxSize {
			kll.compress()
		}
		if kll.size >= kll.maxSize {
			fmt.Println("[KLL Error] kll.size is not smaller than kll.maxSize.")
		}
	}
}


func (kll *KLL) Delete(item float64) {
	kll.Update(-1 * item, 1, true)
}


func (kll *KLL) compress() {
	status := "None"

	if len(kll.compactors[len(kll.compactors) - 1].list) >= kll.k {
		// need to compact the topmost compactor
		status = kll.grow()
	}

	if status == "limit" {
		// cannot add a new compactor
		// need to compact all compactor since height increased by 1
		kll.compactors[1].list = append(kll.compactors[1].list, kll.compactors[0].compact(kll.with_deletion)...)
		left_ := make([]float64, 0)
		copy(left_, kll.compactors[0].list)
		if len(left_) > 3 {
			fmt.Println("[KLL Error] length after compaction is larger than 3.")
			return 
		}

		kll.compactors = kll.compactors[1:]
		tmp_c := NewCompactor(kll.alternate)
		kll.compactors = append(kll.compactors, tmp_c)
		for _, item := range left_ {
			kll.Update(item, int64(math.Pow(2, float64(kll.insert_sampler.h-1))), true) 
		}

		total_size := uint64(0)
		for _, c := range kll.compactors {
			total_size += uint64(len(c.list))
		}
		kll.size = total_size
		return 
	} else {
		for h := 0; h < len(kll.compactors); h++ {
			// fmt.Println("len =", uint64(len(kll.compactors[h].list)), kll.capacity(h))
			if uint64(len(kll.compactors[h].list)) >= kll.capacity(h) {
				/*
				fmt.Println("h+1 =", h+1)
				fmt.Println("before compact:", kll.compactors[h+1].list)
				fmt.Println("before compact:", kll.compactors[h].list)
				*/
				kll.compactors[h+1].list = append(kll.compactors[h+1].list, kll.compactors[h].compact(kll.with_deletion)...)
				/*
				fmt.Println("after compact:", kll.compactors[h+1].list)
				fmt.Println("after compact:", kll.compactors[h].list)
				*/
				if kll.lazy == true {
					break
				}
			}
		}

		total_size := uint64(0)
		for _, c := range kll.compactors {
			total_size += uint64(len(c.list))
		}
		kll.size = total_size
		return 
	}
}


func (kll *KLL) merge(other * KLL) {
	// Grow until kll has at least as many compactors as other
	for kll.H < other.H {
		kll.grow()
	}

	// Append the items in the same height compactors
	for h := 0; h < MinInt(other.H, other.max_H); h++ {
		kll.compactors[h].list = append(kll.compactors[h].list, other.compactors[h].list...)
	}

	kll.Update(other.insert_sampler.item, other.insert_sampler.weight, true)
	kll.Update(other.delete_sampler.item, other.delete_sampler.weight, true)

	sum_len := uint64(0)
	for _, c := range kll.compactors {
		sum_len += uint64(len(c.list))
	}
	kll.size = sum_len
	
	// Keep compresing until the size constraint is met
	for kll.size >= kll.maxSize {
		kll.compress()
	}
	
	if !(kll.size < kll.maxSize) {
		fmt.Println("[KLL Error] KLL size is not smaller than maxSize.")
	}
}


func (kll *KLL) rank(value float64) (r int) {
	r = 0
	for h, c := range kll.compactors {
		for _, item := range c.list {
			if AbsFloat64(item) <= value {
				r += SignFloat64(item) * int(math.Pow(2, float64(kll.insert_sampler.h + h)))
			}
		}
	}
	return r
}


func (kll *KLL) cdf() (cdf []estPair) {
	cdf = make([]estPair, 0)
	itemsAndWeights := make([]estPair, 0)
	sampler_h := kll.H - kll.max_H - 1
	if sampler_h < 0 {
		sampler_h = 0
	}

	if kll.insert_sampler.h != sampler_h {
		fmt.Println("[KLL Error] KLL insert_sampler.h not equal to sampler_h.")
		return 
	}

	for h, items := range kll.compactors {
		tmp_items_and_weights := make([]estPair, 0)
		for _, item := range items.list {
			tmp_items_and_weights = append(tmp_items_and_weights, estPair{item: AbsFloat64(item), weight: float64(SignFloat64(item)) * (math.Pow(2, float64(h+sampler_h))),})
		}
		itemsAndWeights = append(itemsAndWeights, tmp_items_and_weights...)
	}
	totWeight := float64(0)
	for _, pair := range itemsAndWeights {
		totWeight += pair.weight
	}
	sort.Slice(itemsAndWeights, func(i, j int) bool { return itemsAndWeights[i].item < itemsAndWeights[j].item }) // sort based on item 
	cumWeight := float64(0)
	cdf = make([]estPair, 0)

	hashmap := make(map[float64]float64)

	for _, pair := range itemsAndWeights {
		if _, ok := hashmap[pair.item]; !ok {
			hashmap[pair.item] = pair.weight
		} else {
			hashmap[pair.item] += pair.weight
		}
		if hashmap[pair.item] == 0 {
			delete(hashmap, pair.item)
		}
	}
	itemsAndWeights = itemsAndWeights[:0]
	for key,value := range hashmap {
		itemsAndWeights = append(itemsAndWeights, estPair{item: key, weight: value, })
	}

	sort.Slice(itemsAndWeights, func(i, j int) bool { return itemsAndWeights[i].item < itemsAndWeights[j].item }) // sort based on item 
	for _, pair := range itemsAndWeights {
		cumWeight += pair.weight
		if cumWeight > 0 && cumWeight <= totWeight {
			cdf = append(cdf, estPair{item: pair.item, weight: float64(cumWeight) / float64(totWeight), })
		}
	}

	return cdf
}


func (kll *KLL) ranks() (ranks []estPair){
	itemsAndWeights := make([]estPair, 0)
	sampler_h := kll.H - kll.max_H - 1
	if sampler_h < 0 {
		sampler_h = 0
	}

	if kll.insert_sampler.h != sampler_h {
		fmt.Println("[KLL Error] KLL insert_sampler.h not equal to sampler_h.")
		return 
	}

	for h, items := range kll.compactors {
		tmp_items_and_weights := make([]estPair, 0)
		for _, item := range items.list {
			tmp_items_and_weights = append(tmp_items_and_weights, estPair{item: AbsFloat64(item), weight: float64(SignFloat64(item)) * math.Pow(2, float64(h+sampler_h)),})
		}
		itemsAndWeights = append(itemsAndWeights, tmp_items_and_weights...)
	}
	totWeight := float64(0)
	for _, pair := range itemsAndWeights {
		totWeight += pair.weight
	}
    sort.Slice(itemsAndWeights, func(i, j int) bool { return itemsAndWeights[i].item < itemsAndWeights[j].item }) // sort based on item 
	cumWeight := float64(0)
	ranks = make([]estPair, 0)

	hashmap := make(map[float64]float64)

	for _, pair := range itemsAndWeights {
		if _, ok := hashmap[pair.item]; !ok {
			hashmap[pair.item] = pair.weight
		} else {
			hashmap[pair.item] += pair.weight
		}
		if hashmap[pair.item] == 0 {
			delete(hashmap, pair.item)
		}
	}
	itemsAndWeights = itemsAndWeights[:0]
	for key, value := range hashmap {
		itemsAndWeights = append(itemsAndWeights, estPair{item: key, weight: value, })
	}

	sort.Slice(itemsAndWeights, func(i, j int) bool { return itemsAndWeights[i].item < itemsAndWeights[j].item }) // sort based on item 
	for _, pair := range itemsAndWeights {
		cumWeight += pair.weight
		if cumWeight > 0 && cumWeight <= totWeight {
			ranks = append(ranks, estPair{item: pair.item, weight: float64(cumWeight),})
		}
	}

	return ranks
}


func (kll *KLL) frequency() (itemsAndWeights map[float64]float64) {
	itemsAndWeights = make(map[float64]float64)
	sampler_h := kll.H - kll.max_H - 1
	if sampler_h < 0 {
		sampler_h = 0
	}
	if kll.insert_sampler.h != sampler_h {
		fmt.Println("[KLL Error] KLL insert_sampler.h not equal to sampler_h.")
		return 
	}

	for h, items := range kll.compactors {
		for _, item := range items.list {
			if _, ok := itemsAndWeights[AbsFloat64(item)]; ok {
				itemsAndWeights[AbsFloat64(item)] = itemsAndWeights[AbsFloat64(item)] + float64(SignFloat64(item)) * math.Pow(2, float64(h+sampler_h))
			} else {
				itemsAndWeights[AbsFloat64(item)] = float64(SignFloat64(item)) * (math.Pow(2, float64(h+sampler_h)))
			}
		}
	}
	
	return itemsAndWeights
}


func (kll *KLL) pmf() (pmf []estPair) {
	itemsAndWeights := make([]estPair, 0)
	sampler_h := kll.H - kll.max_H - 1
	if sampler_h < 0 {
		sampler_h = 0
	}
	if kll.insert_sampler.h != sampler_h {
		fmt.Println("[KLL Error] KLL insert_sampler.h not equal to sampler_h.")
		return 
	}

	for h, items := range kll.compactors {
		tmp_items_and_weights := make([]estPair, 0)
		for _, item := range items.list {
			tmp_items_and_weights = append(tmp_items_and_weights, estPair{item: AbsFloat64(item), weight: float64(SignFloat64(item)) * math.Pow(2, float64(h+sampler_h)),})
		}
		itemsAndWeights = append(itemsAndWeights, tmp_items_and_weights...)
	}
	totWeight := float64(0)
	for _, pair := range itemsAndWeights {
		totWeight += pair.weight
	}
    sort.Slice(itemsAndWeights, func(i, j int) bool { return itemsAndWeights[i].item < itemsAndWeights[j].item }) // sort based on item 
	cumWeight := float64(0)
	pmf = make([]estPair, 0)

	hashmap := make(map[float64]float64)

	for _, pair := range itemsAndWeights {
		if _, ok := hashmap[pair.item]; !ok {
			hashmap[pair.item] = pair.weight
		} else {
			hashmap[pair.item] += pair.weight
		}
		if hashmap[pair.item] == 0 {
			delete(hashmap, pair.item)
		}
	}
	itemsAndWeights = itemsAndWeights[:0]
	for key, value := range hashmap {
		itemsAndWeights = append(itemsAndWeights, estPair{item: key, weight: value, })
	}

	sort.Slice(itemsAndWeights, func(i, j int) bool { return itemsAndWeights[i].item < itemsAndWeights[j].item })
	for _, pair := range itemsAndWeights {
		cumWeight += pair.weight
		if cumWeight > 0 && cumWeight <= totWeight {
			pmf = append(pmf, estPair{item: pair.item, weight: float64(pair.weight) / float64(totWeight),})
		}
	}
	 
	return pmf
}