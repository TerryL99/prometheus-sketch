package promsketch

import (
	"fmt"
  	"container/heap"
)

// https://pkg.go.dev/container/heap#pkg-constants

type Item struct {
	key			string
	count    	int64 	// The value of the item (arbitrary)
	index 		int 	// The index of the item in the heap
}

type minHeap []*Item

func (pq minHeap) Len() int { return len(pq) }

func (pq minHeap) Less(i, j int) bool {
	return pq[i].count < pq[j].count
}

func (pq minHeap) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *minHeap) Push(x any) {
	n := len(*pq)
	item := x.(*Item)
	item.index = n
	*pq = append(*pq, item)
}

func (pq *minHeap) Pop() any {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil  	// avoid memory leak
	item.index = -1 	// for safety
	*pq = old[0 : n-1]
	return item
}

type TopKHeap struct {
	heap	minHeap
	k		int
	key_index	map[string]int 	// map a key to index in the heap
}

func (topkheap *TopKHeap) InitKeyIndex() {
	topkheap.key_index = make(map[string]int)
	for i, item := range topkheap.heap {
		topkheap.key_index[item.key] = i
	}
}

func NewTopKHeap(k int) (topkheap * TopKHeap){
	topkheap = &TopKHeap{
		heap:	make(minHeap, 0, k),
		k:	k,
	}
	return topkheap
}

func NewTopKHeapWithItems(k int, items map[string]int64) (topkheap * TopKHeap) {
	topkheap = &TopKHeap{
		heap:	make(minHeap, len(items)),
		k:	k,
		key_index: make(map[string]int),
	}
	i := 0
	for key, count := range items {
		topkheap.heap[i] = &Item{
			key:    key,
			count: count,
			index:    i,
		}
		i++
	}
	heap.Init(&topkheap.heap)
	topkheap.InitKeyIndex()
	return topkheap
}

func (topkheap *TopKHeap) Print() {
	for _, item := range topkheap.heap {
		fmt.Println(item.key, ":", item.count)
	}
}

func (topkheap *TopKHeap) Update(key string, count int64) bool {
	var find bool = false
	var index int = -1
	for _, item := range topkheap.heap {
		if item.key == key {
			find = true
			index = item.index
			break
		}
	}
	
	if find == true {
		item := topkheap.heap[index]
		item.count = count
		heap.Fix(&topkheap.heap, item.index)
		return true
	} else {
		topkheap.Insert(key, count)
		return true
	}
}


func (topkheap *TopKHeap) Insert(key string, count int64) {
	if int(len(topkheap.heap)) < topkheap.k {
		heap.Push(&topkheap.heap, &Item{
			key:	key,
			count:	count,
		})
	} else {
		if topkheap.heap[0].count < count {
			topkheap.heap[0].count = count
			topkheap.heap[0].key = key
			if topkheap.k > 1 {
				heap.Fix(&topkheap.heap, 0)
			}
		}
	}
}
