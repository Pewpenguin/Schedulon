package scheduler

import (
	"container/heap"
	"time"
)

const (
	priorityAgingInterval = time.Minute
	maxAgingSteps         = int32(64)
)

type pqItem struct {
	task *Task
	seq  uint64
}

func effectivePriority(clock time.Time, t *Task) int32 {
	if t == nil {
		return 0
	}
	wait := clock.Sub(t.SubmittedAt)
	if wait < 0 {
		wait = 0
	}
	steps := int32(wait / priorityAgingInterval)
	if steps > maxAgingSteps {
		steps = maxAgingSteps
	}
	return t.Priority + steps
}

// prioritizedHeap is a min-heap: root is the task that should run next (highest effective priority, then FIFO).
type prioritizedHeap struct {
	clock time.Time
	arr   []*pqItem
}

func (h prioritizedHeap) Len() int { return len(h.arr) }

func (h prioritizedHeap) Less(i, j int) bool {
	ei := effectivePriority(h.clock, h.arr[i].task)
	ej := effectivePriority(h.clock, h.arr[j].task)
	if ei != ej {
		return ei > ej
	}
	return h.arr[i].seq < h.arr[j].seq
}

func (h prioritizedHeap) Swap(i, j int) { h.arr[i], h.arr[j] = h.arr[j], h.arr[i] }

func (h *prioritizedHeap) Push(x interface{}) {
	h.arr = append(h.arr, x.(*pqItem))
}

func (h *prioritizedHeap) Pop() interface{} {
	old := h.arr
	n := len(old)
	x := old[n-1]
	h.arr = old[0 : n-1]
	return x
}

type priorityQueue struct {
	inner prioritizedHeap
	seq   uint64
}

func newPriorityQueue() *priorityQueue {
	return &priorityQueue{
		inner: prioritizedHeap{arr: make([]*pqItem, 0)},
	}
}

func (pq *priorityQueue) len() int {
	return len(pq.inner.arr)
}

func (pq *priorityQueue) enqueue(task *Task) {
	if task == nil {
		return
	}
	it := &pqItem{task: task, seq: pq.seq}
	pq.seq++
	heap.Push(&pq.inner, it)
}

// dequeueBest selects the best pending task for the given worker using scoreFn.
// Other tasks are re-inserted. If worker is nil, the highest-priority ready task is returned without worker filters.
func (pq *priorityQueue) dequeueBest(clock time.Time, worker *Worker, scoreFn func(*Worker, *Task, int) float64, tenantOnWorker func(*Worker, string) int) *Task {
	if len(pq.inner.arr) == 0 {
		return nil
	}
	pq.inner.clock = clock
	heap.Init(&pq.inner)

	tmp := make([]*pqItem, 0, len(pq.inner.arr))
	for len(pq.inner.arr) > 0 {
		tmp = append(tmp, heap.Pop(&pq.inner).(*pqItem))
	}

	var best *pqItem
	bestScore := -1.0e18
	for _, it := range tmp {
		t := it.task
		if t == nil {
			continue
		}
		if !taskReadyForSchedule(t, clock) {
			continue
		}
		if worker != nil && !isSchedulableForWorker(t, worker) {
			continue
		}
		var sc float64
		if scoreFn != nil && worker != nil {
			same := 0
			if tenantOnWorker != nil {
				same = tenantOnWorker(worker, t.Tenant)
			}
			sc = scoreFn(worker, t, same)
		} else {
			sc = float64(effectivePriority(clock, t))*1e6 - float64(it.seq)*1e-3
		}
		if sc > bestScore {
			bestScore = sc
			best = it
		}
	}

	for _, it := range tmp {
		if it == best {
			continue
		}
		heap.Push(&pq.inner, it)
	}
	if best != nil {
		return best.task
	}
	return nil
}

func taskReadyForSchedule(t *Task, clock time.Time) bool {
	if t == nil {
		return false
	}
	if t.NotBefore.IsZero() {
		return true
	}
	return !clock.Before(t.NotBefore)
}
