package limiters

import (
	"container/heap"
	"sync"
	"time"
)

// pqItem is an item in the priority queue.
type pqItem struct {
	value interface{}
	exp   time.Time
	index int
	key   string
}

// gcPq is a priority queue.
type gcPq []*pqItem

func (pq gcPq) Len() int { return len(pq) }

func (pq gcPq) Less(i, j int) bool {
	return pq[i].exp.Before(pq[j].exp)
}

func (pq gcPq) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *gcPq) Push(x interface{}) {
	n := len(*pq)
	item := x.(*pqItem)
	item.index = n
	*pq = append(*pq, item)
}

func (pq *gcPq) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	item.index = -1 // for safety
	*pq = old[0 : n-1]

	return item
}

// Registry is a thread-safe garbage-collectable registry of values.
type Registry struct {
	// Guards all the fields below it.
	mx sync.Mutex
	pq *gcPq
	m  map[string]*pqItem
}

// NewRegistry creates a new instance of Registry.
func NewRegistry() *Registry {
	pq := make(gcPq, 0)

	return &Registry{pq: &pq, m: make(map[string]*pqItem)}
}

// GetOrCreate gets an existing value by key and updates its expiration time.
// If the key lookup fails it creates a new value by calling the provided value closure and puts it on the queue.
func (r *Registry) GetOrCreate(key string, value func() interface{}, ttl time.Duration, now time.Time) interface{} {
	r.mx.Lock()
	defer r.mx.Unlock()
	item, ok := r.m[key]
	if ok {
		// Update the expiration time.
		item.exp = now.Add(ttl)
		heap.Fix(r.pq, item.index)
	} else {
		item = &pqItem{
			value: value(),
			exp:   now.Add(ttl),
			key:   key,
		}
		heap.Push(r.pq, item)
		r.m[key] = item
	}

	return item.value
}

// DeleteExpired deletes expired items from the registry and returns the number of deleted items.
func (r *Registry) DeleteExpired(now time.Time) int {
	r.mx.Lock()
	defer r.mx.Unlock()
	c := 0
	for len(*r.pq) != 0 {
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

// Delete deletes an item from the registry.
func (r *Registry) Delete(key string) {
	r.mx.Lock()
	defer r.mx.Unlock()
	item, ok := r.m[key]
	if !ok {
		return
	}
	delete(r.m, key)
	heap.Remove(r.pq, item.index)
}

// Exists returns true if an item with the given key exists in the registry.
func (r *Registry) Exists(key string) bool {
	r.mx.Lock()
	defer r.mx.Unlock()
	_, ok := r.m[key]

	return ok
}

// Len returns the number of items in the registry.
func (r *Registry) Len() int {
	r.mx.Lock()
	defer r.mx.Unlock()

	return len(*r.pq)
}
