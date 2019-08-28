package limiters

import (
	"container/heap"
	"time"
)

// pqItem is an item in the priority queue.
type pqItem struct {
	limiter Limiter
	exp     time.Time
	index   int
	key     string
}

type limitersPQ []*pqItem

func (pq limitersPQ) Len() int { return len(pq) }

func (pq limitersPQ) Less(i, j int) bool {
	return pq[i].exp.Before(pq[j].exp)
}

func (pq limitersPQ) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *limitersPQ) Push(x interface{}) {
	n := len(*pq)
	item := x.(*pqItem)
	item.index = n
	*pq = append(*pq, item)
}

func (pq *limitersPQ) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	item.index = -1 // for safety
	*pq = old[0 : n-1]
	return item
}

// Registry is a garbage-collectable registry of Limiters.
type Registry struct {
	pq *limitersPQ
	m  map[string]*pqItem
}

// NewRegistry creates a new instance of Registry.
func NewRegistry() *Registry {
	pq := make(limitersPQ, 0)
	return &Registry{pq: &pq, m: make(map[string]*pqItem)}
}

// GetOrCreate gets an existing Limiter by key and updates its expiration time.
// If the key lookup fails it creates a new limiter by calling the provided limiter closure and puts in on the queue.
func (r *Registry) GetOrCreate(key string, limiter func() Limiter, ttl time.Duration, now time.Time) Limiter {
	item, ok := r.m[key]
	if ok {
		// Update the expiration time.
		item.exp = now.Add(ttl)
		heap.Fix(r.pq, item.index)
	} else {
		item = &pqItem{
			limiter: limiter(),
			exp:     now.Add(ttl),
			key:     key,
		}
		heap.Push(r.pq, item)
		r.m[key] = item
	}

	return item.limiter
}

// DeleteExpired deletes expired items from the registry and returns the number of deleted items.
func (r *Registry) DeleteExpired(now time.Time) int {
	c := 0
	for {
		if len(*r.pq) == 0 {
			break
		}
		item := (*r.pq)[0]
		if now.Before(item.exp) {
			break
		}
		delete(r.m, item.key)
		heap.Pop(r.pq)
		c++
	}
	return c
}

// Exists returns true if an item with the given key exists in the registry.
func (r *Registry) Exists(key string) bool {
	_, ok := r.m[key]
	return ok
}
