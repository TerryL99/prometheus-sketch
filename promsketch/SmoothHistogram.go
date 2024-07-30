package promsketch

import (
	"fmt"
	"math"
	"math/rand"
	"github.com/cespare/xxhash/v2"
	"sort"
	"time"
)

// TODO: change bucket to timestamp-based

type CountBucket struct {
	counter int64
	bucketsize int
}

type HeavyHitter struct {
	hhs []Item
}

type HHCountSketch struct {
	cs 		*CountSketch
	topK	*TopKHeap
}

type SmoothHistogram struct {
	seed1 []uint32
	seed2 []uint32
	s_count int // sketch count
	cs_instances []*CountSketch
	beta float64
}

type SmoothHistogramCount struct {
	stored_keys []string
	keys map[string]int
	excluded_zero int
	zero_count []int
	s_count []int
	given_key_size int
	buckets [][]CountBucket

	beta float64
}

type SmoothHistogramHH struct {
	seed1 []uint32
	seed2 []uint32
	update_count_freq int // how many data points per Counting check
	cur_time int
	s_count int
	h_count int
	instances []*HHCountSketch 
	shc *SmoothHistogramCount
	beta float64
	epsilon float64
}



type SmoothHistogramUnivMon struct {
	cs_seed1 	[]uint32
	cs_seed2 	[]uint32
	s_count 	int // sketch count
	univs 		[]*UnivSketch // each bucket is a univsketch
	beta	 	float64
	total_num_pkt 	int
}

/*-----------------------------------------------------
			Smooth Histogram for UnivMon
-------------------------------------------------------*/

func smooth_init_univmon(beta float64) (shu *SmoothHistogramUnivMon) {
	shu = &SmoothHistogramUnivMon{
		s_count: 	0,
		beta:		beta,
		total_num_pkt: 0,
	}

	shu.cs_seed1 = make([]uint32, CS_ROW_NO)
	shu.cs_seed2 = make([]uint32, CS_ROW_NO)
	rand.Seed(time.Now().UnixNano())
	for i := 0; i < CS_ROW_NO; i++ {
		shu.cs_seed1[i] = rand.Uint32()
		shu.cs_seed2[i] = rand.Uint32()
	}
	
	/*
	shu.seed1 = make([]uint32, CS_LVLS)
	for i := 0; i < CS_LVLS; i++ {
		shu.seed1[i] = rand.Uint32()
	}
	*/

	shu.univs = make([]*UnivSketch, shu.s_count)
	
	fmt.Println("UnivMon smooth histogram init is done.")

	return shu
}


func (shu * SmoothHistogramUnivMon) smooth_update_univmon(key string) {
	shu.total_num_pkt += 1

	for i := 0; i < shu.s_count; i++ {
		shu.univs[i].univmon_processing(key, 1)
		shu.univs[i].bucket_size = shu.univs[i].bucket_size + 1
	}

	// init new UnivMon
	tmp, err := NewUnivSketch(TOPK_SIZE, CS_ROW_NO, CS_COL_NO, CS_LVLS, shu.cs_seed1, shu.cs_seed2) // New && init UnivMon
	if err != nil {
		fmt.Println("[Error] failed to allocate memory for new bucket of UnivMon...")
	}
	tmp.bucket_size = 1
	shu.univs = append(shu.univs, tmp)
	shu.univs[shu.s_count].univmon_processing(key, 1)
	shu.s_count++

	for i := 0; i <= shu.s_count - 3; i++ {
		maxj := i + 1
		var compare_value float64 = float64(1.0 - 0.5 * shu.beta) * (shu.univs[i].cs_layers[0].cs_l2()) 

		for j := i+1; j <= shu.s_count - 3; j++ {
			if (maxj < j) && (float64(shu.univs[j].cs_layers[0].cs_l2()) >= compare_value) {
				maxj = j
			}
		}
		
		shift := maxj - i - 1 // offset to shift
		if shift > 0 { // need to shift
			shu.s_count = shu.s_count - shift
			shu.univs = append(shu.univs[:i+1], shu.univs[maxj:]...)
		}
		
	}

	removed := 0
	for i := 0; i < shu.s_count; i++ {
		if shu.univs[i].bucket_size > WINDOW_SIZE + 1 {
			removed++
		} else {
			break
		}
	}

	if removed > 0 {
		fmt.Println("previous len(univs) =", len(shu.univs))
		shu.s_count = shu.s_count - removed
		shu.univs = shu.univs[removed:]
		fmt.Println("removed =", removed, "len(univs) =", len(shu.univs))
	} 
}

func (shu * SmoothHistogramUnivMon) print_buckets_univmon() {
	for i := 0; i < shu.s_count; i++ {
		fmt.Printf("No. %d bucket: size is %d\n", i, shu.univs[i].bucket_size)
	}
}

/*------------------------------------------------------------------------------
			Smooth Histogram for Heavy Hitters 
--------------------------------------------------------------------------------*/

func (shh *SmoothHistogramHH) smooth_init_hh(beta float64, epsilon float64, seed1, seed2 []uint32, update_count_freq int) {
	shh.beta = beta
	shh.epsilon = epsilon

	shh.s_count = 0
	shh.h_count = 0
	shh.update_count_freq = update_count_freq

	shh.seed1 = make([]uint32, CS_ROW_NO)
	shh.seed2 = make([]uint32, CS_ROW_NO)
	copy(shh.seed1, seed1)
	copy(shh.seed2, seed2)

	shh.instances = make([]*HHCountSketch, shh.s_count)
	shh.shc = smooth_init_count(nil, 0, shh.beta)

	fmt.Println("hh smooth historgram init is done.")
}

func (shh *SmoothHistogramHH) smooth_update_hh(key string) {
	for i := 0; i < shh.s_count; i++ {
		median_count := shh.instances[i].cs.UpdateAndEstimateString(key, 1)
		shh.instances[i].topK.Update(key, median_count)
		shh.instances[i].cs.bucket_size++
	}

	// init new CountSketch
	tmp, err := NewCountSketch(CS_ROW_NO, CS_COL_NO, shh.seed1, shh.seed2) // new && init a count sketch
	if err != nil {
		fmt.Println("[Error] failed to allocate memory for new count Sketch bucket...")
	}
	tmp_topk := NewTopKHeap(TOPK_SIZE)
	shh.instances = append(shh.instances, &HHCountSketch{tmp, tmp_topk})
	shh.instances[shh.s_count].cs.bucket_size = 1
	median_count := shh.instances[shh.s_count].cs.UpdateAndEstimateString(key, 1)
	shh.instances[shh.s_count].topK.Update(key, median_count)
	shh.s_count++

	for i := 0; i <= shh.s_count - 3; i++ {
		maxj := i + 1
		var compare_value float64 = float64(1.0 - 0.5 * shh.beta) * (shh.instances[i].cs.cs_l2())
		for j := i + 1; j <= shh.s_count - 3; j++ {
			if (maxj < j) && (float64(shh.instances[j].cs.cs_l2()) >= compare_value) {
				maxj = j
			}
		}
		
		shift := maxj - i - 1 // offsert to shift
		if shift > 0 { // need to shift
			// for j := i+1; j < maxj; j++ {
				// clean the memory for the CS heap -- auto-managed by garbage collector
			// }

			shh.s_count -= shift
			shh.instances = shh.instances[shift:]
		}
	}

	removed := 0
	for i := 0; i < shh.s_count; i++ {
		if shh.instances[i].cs.bucket_size > WINDOW_SIZE + 1 {
			removed++
		} else {
			break
		}
	}

	if removed > 0 {
		// for j := 0; j < removed; j++ {
			// deleteMinHeap(&shh->CSInstances[j].topK); // clean the memory for the CS heap
		// }
		shh.s_count -= removed
		shh.instances = shh.instances[removed: ]
	}

	if shh.cur_time % shh.update_count_freq == 0 {
		threshold := shh.epsilon * shh.epsilon * 3.0 / 4.0
		for i := 0; i < shh.s_count; i++ {
			for _, item := range shh.instances[i].topK.heap {
				if item.count > int64(threshold * shh.instances[i].cs.cs_l2()){
					shh.shc.smooth_insert_count(item.key)
				}
			}
		}
	}

}

/*--------------------------------------------------------------------
				Smooth Histogram for Counting 
--------------------------------------------------------------------*/

func smooth_init_count(given_keys []string, given_key_size int, beta float64) (shc *SmoothHistogramCount) {
	shc = &SmoothHistogramCount{
		beta: beta,
		excluded_zero: 0,
		given_key_size: given_key_size,
	}

	shc.keys = make(map[string]int)

	if given_keys != nil && given_key_size != 0 {
		for i := 0; i < given_key_size; i++ {
			shc.keys[given_keys[i]] = i
		}
	}

	shc.stored_keys = make([]string, given_key_size)
	shc.s_count = make([]int, given_key_size)
	shc.zero_count = make([]int, given_key_size)
	shc.buckets = make([][]CountBucket, given_key_size)

	for i := 0; i < given_key_size; i++ {
		shc.stored_keys[i] = given_keys[i]
		shc.s_count[i] = 0
		shc.zero_count[i] = 0
		shc.buckets[i] = make([]CountBucket, shc.s_count[i])
	}

	fmt.Println("Init the Smooth Histogram for Counting is done.")

	return shc
}

func (shc *SmoothHistogramCount) smooth_insert_count(insert_key string) int {

	_, ok := shc.keys[insert_key]

	if !ok {
		if shc.excluded_zero > 0 {
			for i := 0; i < shc.given_key_size; i++ {
				shc.zero_count[i] += shc.excluded_zero
			}
			shc.excluded_zero = 0
		}
		shc.given_key_size++
		shc.keys[insert_key] = shc.given_key_size - 1

		shc.stored_keys = append(shc.stored_keys, insert_key)
		shc.s_count = append(shc.s_count, 0)
		shc.zero_count = append(shc.zero_count, 0)
		tmp_countbucket := make([]CountBucket, shc.s_count[shc.given_key_size-1])
		shc.buckets = append(shc.buckets, tmp_countbucket)
	} else {
		fmt.Println("[Warning] The key to insert is already tracked in the SmoothHistogram.")
		return -1
	}
	
	return 1
}

func (shc *SmoothHistogramCount) smooth_delete_count(delete_key string) {

	found, ok := shc.keys[delete_key]

	if ok {
		index := found
		shc.given_key_size--
		shc.stored_keys = append(shc.stored_keys[:index], shc.stored_keys[index + 1:]...)
		shc.s_count = append(shc.s_count[:index], shc.s_count[index + 1:]...)
		shc.zero_count = append(shc.zero_count[:index], shc.zero_count[index + 1:]...)
		shc.buckets = append(shc.buckets[:index], shc.buckets[index + 1:]...)
	} else {
		fmt.Println("[Warning] the key to delete is not found in the SmoothHistogram...")
	}
}

func (shc *SmoothHistogramCount) smooth_update_count(key string) {
	
	found, ok := shc.keys[key]

	if ok {
		index := found
		for i := 0; i < shc.given_key_size; i++ {
			shc.zero_count[i] += shc.excluded_zero + 1
		}
		shc.excluded_zero = 0
		
		for i := 0; i < shc.s_count[index]; i++ {
			shc.buckets[index][i].counter++
			shc.buckets[index][i].bucketsize += shc.zero_count[index]
		}
		shc.zero_count[index] = 0 // reset zero counter after bucket updates
		tmp_countbucket := CountBucket{
			bucketsize : 1,
			counter : 1,
		}
		shc.buckets[index] = append(shc.buckets[index], tmp_countbucket)
		shc.s_count[index]++

		for i := 0; i < shc.s_count[index] - 2; i++ {
			maxj := i + 1
			var compare_value float64 = (1.0 - 0.5 * shc.beta) * (float64(shc.buckets[index][i].counter))
			for j := i+1; j < shc.s_count[index] - 2; j++ {
				if (maxj < j) && (float64(shc.buckets[index][j].counter) >= compare_value) {
					maxj = j
				}
			}
			shift := maxj - i - 1
			if shift > 0 {
				shc.buckets[index] = append(shc.buckets[index][:i+1], shc.buckets[index][maxj:]...)
				shc.s_count[index] = shc.s_count[index] - shift
			}
		}

		removed := 0
		for i := 0; i < shc.s_count[index]; i++ {
			if shc.buckets[index][i].bucketsize > WINDOW_SIZE + 1 {
				removed ++
			} else {
				break
			}
		}

		if removed > 0 {
			shc.s_count[index] = shc.s_count[index] - removed
			shc.buckets[index] = shc.buckets[index][removed:]
		}
	} else {
		shc.excluded_zero++
	}
}

func (shc *SmoothHistogramCount) print_buckets_count() {
	for i := 0; i < shc.given_key_size; i++ {
		for j := 0; j < shc.s_count[i]; j++ {
			fmt.Printf("item#%d, bucket %d, coutner %d, bucketsize %d\n", i, j, shc.buckets[i][j].counter, shc.buckets[i][j].bucketsize)
		}
	}
}

/*----------------------------------------------------------
				Smooth Histogram for L2
----------------------------------------------------------*/

func smooth_init_l2(beta float64, seed1, seed2 []uint32) (sh *SmoothHistogram) {
	sh = &SmoothHistogram{
		beta: beta,
		s_count: 0,
	}
	sh.seed1 = make([]uint32, CS_ROW_NO)
	sh.seed2 = make([]uint32, CS_ROW_NO)
	copy(sh.seed1, seed1)
	copy(sh.seed2, seed2)
	
	sh.cs_instances = make([]*CountSketch, sh.s_count)

	fmt.Println("l2 smooth histogram init is done.")

	return sh
}

func (sh *SmoothHistogram) smooth_udpate_l2(key string) {
	for i := 0; i < sh.s_count; i++ {
		sh.cs_instances[i].UpdateString(key, 1)
		sh.cs_instances[i].bucket_size++
	}

	tmp, err := NewCountSketch(CS_ROW_NO, CS_COL_NO, sh.seed1, sh.seed2) // new && init count sketch
	if err != nil {
		fmt.Println("[Error] failed to allocate memory for count sketch...")
	}
	sh.cs_instances = append(sh.cs_instances, tmp)

	sh.cs_instances[sh.s_count].UpdateString(key, 1)
	sh.s_count++

	fmt.Println("Adding new CS bucket is done")

	for i := 0; i < sh.s_count - 2; i++ {
		maxj := i + 1
		var compare_value float64 = float64(1.0 - 0.5 * sh.beta) * (sh.cs_instances[i].cs_l2())

		for j := i + 1; j < sh.s_count - 2; j++ {
			if (maxj < j) && (float64(sh.cs_instances[j].cs_l2()) >= compare_value) {
				maxj = j
			}
		}

		shift := maxj - i - 1
		if shift > 0 {
			sh.s_count = sh.s_count - shift
			sh.cs_instances = sh.cs_instances[shift:]
		}
	}

	removed := 0
	for i := 0; i < sh.s_count; i++ {
		if sh.cs_instances[i].bucket_size > WINDOW_SIZE + 1 {
			removed ++
		} else {
			break
		}
	}
	if removed > 0 {
		sh.s_count -= removed
		sh.cs_instances = sh.cs_instances[removed:]
	}
}


func (sh *SmoothHistogram) print_buckets_l2() {
	for i := 0; i < sh.s_count; i++ {
		fmt.Printf("No.%d bucket: L2 is %f, size if %d\n", i, sh.cs_instances[i].cs_l2(), sh.cs_instances[i].bucket_size)
	}
}


/*----------------------------------------------------------
				Interval Query functions
----------------------------------------------------------*/
/*
	Merge the universal sketches to the interval of t2 to t1;
*/
func (shu *SmoothHistogramUnivMon) query_interval_merge(t1, t2, cur_t int) (*UnivSketch) {
	var diff1, diff2 int = math.MaxInt, math.MaxInt
	var from_bucket, to_bucket int = 0, 0

	for i := 0; i < shu.s_count; i++ {
		fmt.Println("i =", i, "bucket_size =", shu.univs[i].bucket_size)
		curdiff1 := AbsInt((cur_t - t1) - shu.univs[i].bucket_size)
		curdiff2 := AbsInt((cur_t - t2) - shu.univs[i].bucket_size)
		if curdiff1 < diff1 {
			diff1 = curdiff1
			from_bucket = i
		}
		if curdiff2 < diff2 {
			diff2 = curdiff2
			to_bucket = i
		}
	}

	fmt.Println("from_bucket =", from_bucket)
	fmt.Println("to_bucket =", to_bucket)

	merged_univ, err := NewUnivSketch(TOPK_SIZE, CS_ROW_NO, CS_COL_NO, CS_LVLS, shu.cs_seed1, shu.cs_seed2) // new && init; seed1 and seed2 should be the same as other UnivSketch
	if err != nil {
		fmt.Println("[Error] failed to allocate memory for new bucket of UnivMon...")
	}

	for i := 0; i < CS_LVLS; i++ {
		for j := 0; j < CS_ROW_NO; j++ {
			for k := 0; k < CS_COL_NO; k++ {
				merged_univ.cs_layers[i].count[j][k] = shu.univs[from_bucket].cs_layers[i].count[j][k] - shu.univs[to_bucket].cs_layers[i].count[j][k]
			}
		}

		merged_univ.HH_layers[i].topK = shu.univs[from_bucket].HH_layers[i].topK
		for _, item := range merged_univ.HH_layers[i].topK.heap {
			item.count = merged_univ.cs_layers[i].EstimateString(item.key)
		}
	}

	merged_univ.bucket_size = shu.univs[from_bucket].bucket_size - shu.univs[to_bucket].bucket_size
	fmt.Println("merged_univ.bucket_size =", merged_univ.bucket_size)

	return merged_univ
}


/*
	Query the universal sketches to the interval of t2 to t1;
*/
func query_univmon_distinct(univ *UnivSketch) float64 {
	l2_value := univ.cs_layers[CS_LVLS-1].cs_l2()
	var threshold int64 = int64(l2_value * 0.01)
	var y_bottom float64 = 0.0
	
	for _, item := range univ.HH_layers[CS_LVLS-1].topK.heap {
		if item.count >= threshold {
			y_bottom += 1.0
		}
	}

	var y_1, y_2 float64 = 0.0, 0.0
	y_2 = y_bottom
	var indSum float64 = 0.0
	var hash float64 = 0.0
	for i := int(CS_LVLS - 2); i >= 0; i-- {
		indSum = 0.0
		l2_value = univ.cs_layers[i].cs_l2()
		threshold = int64(l2_value * 0.01)
		for _, item := range univ.HH_layers[i].topK.heap {
			if item.count >= threshold {
				hash = float64(xxhash.Sum64String(item.key) % 2) // TODO: xxhash with a seed
				indSum += (1.0 - 2.0 * hash)
			}
		}
		y_1 = 2.0 * y_2 + indSum
		y_2 = y_1
	}

	return y_1
}

func query_univmon_entropy(univ *UnivSketch) float64 {
	if univ.bucket_size == 0 {
		return 0.0
	}

	var y_bottom float64 = 0.0
	var y_1, y_2 float64 = 0.0, 0.0
	var w float64
	for _, item := range univ.HH_layers[CS_LVLS-1].topK.heap {
		w = float64(item.count) 
		if w >= 0.0 {
			y_bottom += w * math.Log(w) / math.Log(2)
		}
	}

	y_2 = y_bottom
	var indSum float64 = 0.0
	var hash float64 = 0.0
	for i := int(CS_LVLS - 2); i >= 0; i-- {
		indSum = 0.0
		for _, item := range univ.HH_layers[i].topK.heap {
			w = float64(item.count) 
			if w >= 0.0 {
				hash = float64(xxhash.Sum64String(item.key) % 2) // TODO: xxhash with a seed
				indSum += (1.0 - 2.0 * hash) * w * math.Log(w) / math.Log(2)
			}
		}
		y_1 = 2.0 * y_2 + indSum
		y_2 = y_1
	}
	
	var entropy float64 = math.Log(float64(univ.bucket_size)) / math.Log(2) - y_1 / (float64(univ.bucket_size)) 
	return entropy
}

/*
Query the L2 on the interval of t1 to 0;
*/

func query_interval_l2(sh *SmoothHistogram, interval_size int) float64 {
	diff := math.MaxInt
	var return_bucket int = 0
	for i := 0; i < sh.s_count; i++ {
		curdiff := AbsInt(interval_size - sh.cs_instances[i].bucket_size)
		if curdiff < diff {
			diff = curdiff
			return_bucket = i
		}
	}
	return sh.cs_instances[return_bucket].cs_l2()
}

func (shc *SmoothHistogramCount) query_interval_count(t2, t1 int, query_key string) int64 {
	diff1, diff2 := math.MaxInt32, math.MaxInt32
	var from_bucket, to_bucket int = 0, 0

	found, ok := shc.keys[query_key]
	var index int

	if ok {
		index = found
	} else {
		return -1
	}

	for i := 0; i < shc.s_count[index]; i++ {
		curdiff1 := AbsInt(t1 - (shc.buckets[index][i].bucketsize + shc.zero_count[index] + shc.excluded_zero))
		curdiff2 := AbsInt(t2 - (shc.buckets[index][i].bucketsize + shc.zero_count[index] + shc.excluded_zero))
		if curdiff1 < diff1 {
			diff1 = curdiff1
			to_bucket = i
		} 

		if curdiff2	< diff2 {
			diff2 = curdiff2
			from_bucket = i
		}
	}
	interval_count := shc.buckets[index][from_bucket].counter - shc.buckets[index][to_bucket].counter
	return interval_count
}


/*
Query the frequency of a given key in the interval t1 to 0.
*/
func (sh * SmoothHistogram) query_interval_frequency(key string, interval_size int) float64 {
	diff := math.MaxInt32
	var return_bucket int = 0
	for i := 0; i < sh.s_count; i++ {
		curdiff := AbsInt(interval_size - sh.cs_instances[i].bucket_size)
		if curdiff < diff {
			diff = curdiff
			return_bucket = i
		}
	}

	sos := make([]int64, CS_ROW_NO)
	var hash uint64 
	for i := 0; i < CS_ROW_NO; i++ {
		hash = xxhash.Sum64String(key) // TODO: xxhash with a seed
		sos[i] = AbsInt64(sh.cs_instances[return_bucket].count[i][hash])
	}

	sort.Slice(sos, func(i, j int) bool { return sos[i] < sos[j] })
	median := sos[CS_ROW_NO / 2]

	return float64(median)
}

/*
Query the Heavy Hitters >=threshold in the interval t2 to t1.
*/
func (shc *SmoothHistogramCount) query_interval_hh(t2, t1 int, threshold int64) (hh *HeavyHitter) {
	hh = &HeavyHitter{
		hhs: make([]Item, 0),
	}
	for i := 0; i < shc.given_key_size; i++ {
		key_count := shc.query_interval_count(t1, t2, shc.stored_keys[i])
		if key_count >= threshold {
			hh.hhs = append(hh.hhs, Item{
				key: shc.stored_keys[i],
				count: key_count,
			})
		}
	}

	return hh
}	

func (sh * SmoothHistogram) query_T1T2interval_l2(t2, t1 int) float64 {
	diff1 := math.MaxInt
	diff2 := math.MaxInt
	var from_bucket, to_bucket int = 0, 0
	for i := 0; i < sh.s_count; i++ {
		curdiff2 := AbsInt(t2 - sh.cs_instances[i].bucket_size)
		if curdiff2 < diff2 {
			diff2 = curdiff2
			from_bucket = i
		}
		curdiff1 := AbsInt(t1 - sh.cs_instances[i].bucket_size)
		if curdiff1 < diff1 {
			diff1 = curdiff1
			to_bucket = i
		}
	}


	sos := make([]int64, CS_ROW_NO)
	for j := 0; j < CS_ROW_NO; j++ {
		sos[j] = 0
	}
	for i := 0; i < CS_ROW_NO; i++ {
		for j := 0; j < CS_COL_NO; j++ {
			var temp_dif int64 = AbsInt64(sh.cs_instances[from_bucket].count[i][j] - sh.cs_instances[to_bucket].count[i][j]);
			sos[i] += temp_dif * temp_dif
		}
	}

	sort.Slice(sos, func(i, j int) bool { return sos[i] < sos[j] })
	median := float64(sos[CS_ROW_NO / 2])
	return math.Sqrt(median)
}

func (sh * SmoothHistogram) query_T1T2interval_frequency(key string, t2, t1 int) float64 {
	return sh.query_interval_frequency(key, t2) - sh.query_interval_frequency(key, t1)
}