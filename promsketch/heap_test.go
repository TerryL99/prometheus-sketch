package promsketch

import (
	"fmt"
	"testing"
	"container/heap"
)

// This example creates a minHeap with some items, adds and manipulates an item,
// and then removes the items in priority order.
func TestHeap(t *testing.T) {
	// Some items and their priorities.
	items := map[string]int64{
		"banana": 3, "apple": 2, "pear": 4,
	}

	// Create a priority queue, put the items in it, and
	// establish the priority queue (heap) invariants.
	pq := make(minHeap, len(items))
	i := 0
	for key, count := range items {
		pq[i] = &Item{
			key:    key,
			count: count,
			index:    i,
		}
		i++
	}
	heap.Init(&pq)

	// Insert a new item and then modify its count.
	item := &Item{
		key:    "orange",
		count: 10,
	}
	heap.Push(&pq, item)

	// Take the items out; they arrive in decreasing count order.
	for pq.Len() > 0 {
		_ = heap.Pop(&pq).(*Item)
	}
}

func TestTopKHeap(t *testing.T) {
	fmt.Println("Hello TestTopKHeap")
	items := map[string]int64{
		"banana": 3, "apple": 2, "pear": 4,
	}
	topkheap := NewTopKHeapWithItems(5, items)
	
	topkheap.Print()
	/*
	topkheap.Insert("orange", 1)
	fmt.Println("after insert orange:")
	topkheap.Print()

	topkheap.Insert("mango", 10)
	fmt.Println("after insert mango:")
	topkheap.Print()

	topkheap.Insert("lemon", 8)
	fmt.Println("after insert lemon:")
	topkheap.Print()

	topkheap.Insert("kiwi", 11)
	fmt.Println("after insert kiwi:")
	topkheap.Print()
	*/
}
