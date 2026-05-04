package scheduler

import (
	"sync"
	"time"

	pb "github.com/training-scheduler/proto"
)

type TaskQueue struct {
	mu               sync.Mutex
	pq               *priorityQueue
	scoreFn          func(*Worker, *Task, int) float64
	tenantOnWorkerFn func(*Worker, string) int
}

func NewTaskQueue() *TaskQueue {
	return &TaskQueue{
		pq: newPriorityQueue(),
	}
}

// SetScoring configures worker–task scoring and per-tenant load on a worker (optional).
func (q *TaskQueue) SetScoring(scoreFn func(*Worker, *Task, int) float64, tenantOnWorker func(*Worker, string) int) {
	if q == nil {
		return
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	q.scoreFn = scoreFn
	q.tenantOnWorkerFn = tenantOnWorker
}

func (q *TaskQueue) Enqueue(task *Task) {
	if task == nil {
		return
	}

	q.mu.Lock()
	defer q.mu.Unlock()
	q.pq.enqueue(task)
}

func (q *TaskQueue) Dequeue(worker *Worker) *Task {
	q.mu.Lock()
	defer q.mu.Unlock()

	return q.pq.dequeueBest(time.Now(), worker, q.scoreFn, q.tenantOnWorkerFn)
}

func (q *TaskQueue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.pq.len()
}

func (q *TaskQueue) Snapshot() []*Task {
	q.mu.Lock()
	defer q.mu.Unlock()

	out := make([]*Task, 0, len(q.pq.inner.arr))
	for _, it := range q.pq.inner.arr {
		if it != nil && it.task != nil {
			out = append(out, it.task)
		}
	}
	return out
}

func (q *TaskQueue) Reset() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.pq = newPriorityQueue()
}

// Requeue resets a task for scheduling and appends it to the pending queue.
// Used when a worker is lost or a lease expires; caller must detach the task from workers and GPUs first.
func (q *TaskQueue) Requeue(task *Task) {
	if task == nil {
		return
	}

	task.Status = pb.TaskStatus_PENDING
	task.WorkerID = ""
	task.AssignedGPUs = nil
	task.LeaseOwner = ""
	task.LeaseExpiresAt = time.Time{}
	task.Progress = 0
	task.NotBefore = time.Time{}

	q.Enqueue(task)
}

func isSchedulableForWorker(task *Task, worker *Worker) bool {
	if task == nil || worker == nil {
		return false
	}

	if task.RequiredGPUMemory > 0 && worker.GPUMemoryFree < task.RequiredGPUMemory {
		return false
	}

	reqModel := task.RequiredGPUModel
	if reqModel != "" {
		if worker.GPUModel == "" || worker.GPUModel != reqModel {
			return false
		}
	}

	available := uint32(0)
	for _, gpu := range worker.GPUDevices {
		if !gpu.Available {
			continue
		}
		if gpu.MemoryMB < task.MinGPUMemory {
			continue
		}
		if reqModel != "" && gpu.Model != reqModel {
			continue
		}
		available++
		if available >= task.RequiredGPUs {
			return true
		}
	}

	return false
}
