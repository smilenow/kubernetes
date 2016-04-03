package cache

import (
	"sync"
	"k8s.io/kubernetes/pkg/api"
	heap "k8s.io/kubernetes/pkg/client/cache/heap"
)

// Queue is exactly like a Store, but has a Pop() method too.
// Declared in fifo.go
/*
type Queue interface {
	Store

	// Pop blocks until it has something to return.
	Pop() interface{}

	// AddIfNotPresent adds a value previously
	// returned by Pop back into the queue as long
	// as nothing else (presumably more recent)
	// has since been added.
	AddIfNotPresent(interface{}) error

	// Return true if the first batch of items has been popped
	HasSynced() bool
}
*/

type priorityQueueItem struct {
	// id identify the index of PQ.items
	id		string

	// priority identify the priority of Pod.Spec.priority
	priority	int64

	// The index of the item in the heap
	index		int
}

type PriorityQueue []*priorityQueueItem

func (pq PriorityQueue) Len() int {
	return len(pq)
}

func (pq PriorityQueue) Less(i, j int) bool {
	return pq[i].priority > pq[j].priority
}

func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index, pq[j].index = i, j
}

func (pq *PriorityQueue) Push(x interface{}) {
	n := len(*pq)
	item := x.(*priorityQueueItem)
	item.index = n
	*pq = append(*pq, item)
}

func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	item.index = -1
	*pq = old[0:n-1]
	return item
}

func (pq *PriorityQueue) update(item *priorityQueueItem, id string, priority int64) {
	item.id = id
	item.priority = priority
	heap.FixValue(pq, item.index)
}

// PQ means Priority Queue, and we will use PQ to represent it
// PQ receives adds and updates from a Reflector, and puts them in a priority
// queue for pod.Spec.Priority decreasing order processing. If multiple adds/updates
// of a single item happen while an item is in the queue before it has been processed,
// it will only be processed once, and when it is processed, the most recent version
// will be processed. This can't be done with a channel.
//
// PQ solves this use case:
//  * You want to process every object (exactly) once.
//  * You want to process the most recent version of the object when you process it.
//  * You want to process the object according to their priority decreasingly.
//  * You do not want to process deleted objects, they should be removed from the queue.
//  * You do not want to periodically reprocess objects.
// Compare with FIFO and DeltaFIFO for other use cases.
type PQ struct {
	lock sync.RWMutex
	cond sync.Cond
	// We depend on the property that items in the set are in the queue and vice versa.
	items map[string]interface{}
	queue PriorityQueue

	// populated is true if the first batch of items inserted by Replace() has been populated
	// or Delete/Add/Update was called first.
	populated bool
	// initialPopulationCount is the number of items inserted by the first call of Replace()
	initialPopulationCount int

	// keyFunc is used to make the key used for queued item insertion and retrieval, and
	// should be deterministic.
	keyFunc KeyFunc
}

var (
	_ = Queue(&PQ{})	// PQ is a Queue
)

// Return true if an Add/Update/Delete/AddIfNotPresent are called first,
// or an Update called first but the first batch of items inserted by Replace() has been popped
func (f *PQ) HasSynced() bool {
	f.lock.Lock()
	defer f.lock.Unlock()
	return f.populated && f.initialPopulationCount == 0
}

// Add inserts an item, and puts it in the queue. The item is only enqueued
// if it doesn't already exist in the set.
func (f *PQ) Add(obj interface{}) error {
	id, err := f.keyFunc(obj)
	if err != nil {
		return KeyError{obj, err}
	}
	f.lock.Lock()
	defer f.lock.Unlock()
	f.populated = true
	if _, exists := f.items[id]; !exists {
		currentPod := obj.(*api.Pod)
		newPriorityQueueItem := &priorityQueueItem{
			id: id,
			priority: *currentPod.Spec.Priority,
		}
		heap.Push(&f.queue, newPriorityQueueItem)
	}
	f.items[id] = obj
	f.cond.Broadcast()
	return nil
}

// AddIfNotPresent inserts an item, and puts it in the queue. If the item is already
// present in the set, it is neither enqueued nor added to the set.
//
// This is useful in a single producer/consumer scenario so that the consumer can
// safely retry items without contending with the producer and potentially enqueueing
// stale items.
func (f *PQ) AddIfNotPresent(obj interface{}) error {
	id, err := f.keyFunc(obj)
	if err != nil {
		return KeyError{obj, err}
	}
	f.lock.Lock()
	defer f.lock.Unlock()
	f.populated = true
	if _, exists := f.items[id]; exists {
		return nil
	}

	currentPod := obj.(*api.Pod)
	newPriorityQueueItem := &priorityQueueItem{
		id: id,
		priority: *currentPod.Spec.Priority,
	}
	heap.Push(&f.queue, newPriorityQueueItem)
	f.items[id] = obj
	f.cond.Broadcast()
	return nil
}

// Update is the same as Add in this implementation.
// Assume Pod.Spec.Priority will not be change when this pod isn't failed
func (f *PQ) Update(obj interface{}) error {
	return f.Add(obj)
}

// Delete removes an item. It doesn't add it to the queue, because
// this implementation assumes the consumer only cares about the objects,
// not the order in which they were created/added.
func (f *PQ) Delete(obj interface{}) error {
	id, err := f.keyFunc(obj)
	if err != nil {
		return KeyError{obj, err}
	}
	f.lock.Lock()
	defer f.lock.Unlock()
	f.populated = true
	delete(f.items, id)
	return err
}

// List returns a list of all the items.
func (f *PQ) List() []interface{} {
	f.lock.RLock()
	defer f.lock.RUnlock()
	list := make([]interface{}, 0, len(f.items))
	for _, item := range f.items {
		list = append(list, item)
	}
	return list
}

// ListKeys returns a list of all the keys of the objects currently
// in the PQ.
func (f *PQ) ListKeys() []string {
	f.lock.RLock()
	defer f.lock.RUnlock()
	list := make([]string, 0, len(f.items))
	for key := range f.items {
		list = append(list, key)
	}
	return list
}

// Get returns the requested item, or sets exists=false.
func (f *PQ) Get(obj interface{}) (item interface{}, exists bool, err error) {
	key, err := f.keyFunc(obj)
	if err != nil {
		return nil, false, KeyError{obj, err}
	}
	return f.GetByKey(key)
}

// GetByKey returns the requested item, or sets exists=false.
func (f *PQ) GetByKey(key string) (item interface{}, exists bool, err error) {
	f.lock.RLock()
	defer f.lock.RUnlock()
	item, exists = f.items[key]
	return item, exists, nil
}

// Pop waits until an item is ready and returns it. If multiple items are
// ready, they are returned in the order according to their priority key.
// The item is removed from the queue (and the store) before it is returned,
// so if you don't successfully process it, you need to add it back with
// AddIfNotPresent().
func (f *PQ) Pop() interface{} {
	f.lock.Lock()
	defer f.lock.Unlock()
	for {
		for len(f.queue) == 0 {
			f.cond.Wait()
		}
		popPriorityQueueItem := heap.Pop(&f.queue).(*priorityQueueItem)
		id := popPriorityQueueItem.id
		if f.initialPopulationCount > 0 {
			f.initialPopulationCount--
		}
		item, ok := f.items[id]
		if !ok {
			// Item may have been deleted subsequently.
			continue
		}
		delete(f.items, id)
		return item
	}
}

// Replace will delete the contents of 'f', using instead the given map.
// 'f' takes ownership of the map, you should not reference the map again
// after calling this function. f's queue is reset, too; upon return, it
// will contain the items in the map, in no particular order.
func (f *PQ) Replace(list []interface{}, resourceVersion string) error {
	items := map[string]interface{}{}
	for _, item := range list {
		key, err := f.keyFunc(item)
		if err != nil {
			return KeyError{item, err}
		}
		items[key] = item
	}

	f.lock.Lock()
	defer f.lock.Unlock()

	if !f.populated {
		f.populated = true
		f.initialPopulationCount = len(items)
	}

	f.items = items
	f.queue = f.queue[:0]
	for id, obj := range items {
		currentPod := obj.(*api.Pod)
		newPriorityQueueItem := &priorityQueueItem{
			id: id,
			priority: *currentPod.Spec.Priority,
		}
		heap.Push(&f.queue, newPriorityQueueItem)
	}
	if len(f.queue) > 0 {
		f.cond.Broadcast()
	}
	return nil
}

// NewPQ returns a Store which can be used to queue up items to process.
func NewPQ(keyFunc KeyFunc) *PQ {
	f := &PQ{
		items: 		map[string]interface{}{},
		queue:		PriorityQueue{},
		keyFunc: 	keyFunc,
	}
	f.cond.L = &f.lock
	return f
}
