package scheduler

import (
	"sync"
	"time"

	pb "github.com/training-scheduler/proto"
)

type TaskQueue struct {
	mu      sync.Mutex
	pending []*Task
}

func NewTaskQueue() *TaskQueue {
	return &TaskQueue{
		pending: make([]*Task, 0),
	}
}

func (q *TaskQueue) Enqueue(task *Task) {
	if task == nil {
		return
	}

	q.mu.Lock()
	defer q.mu.Unlock()
	q.pending = append(q.pending, task)
}

func (q *TaskQueue) Dequeue(worker *Worker) *Task {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.pending) == 0 {
		return nil
	}

	for i, task := range q.pending {
		if worker != nil && !isSchedulableForWorker(task, worker) {
			continue
		}

		q.pending = append(q.pending[:i], q.pending[i+1:]...)
		return task
	}

	return nil
}

func (q *TaskQueue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.pending)
}

func (q *TaskQueue) Snapshot() []*Task {
	q.mu.Lock()
	defer q.mu.Unlock()

	out := make([]*Task, len(q.pending))
	copy(out, q.pending)
	return out
}

func (q *TaskQueue) Reset() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.pending = make([]*Task, 0)
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

	q.Enqueue(task)
}

func isSchedulableForWorker(task *Task, worker *Worker) bool {
	if task == nil || worker == nil {
		return false
	}

	available := uint32(0)
	for _, gpu := range worker.GPUDevices {
		if gpu.Available && gpu.MemoryMB >= task.MinGPUMemory {
			available++
			if available >= task.RequiredGPUs {
				return true
			}
		}
	}

	return false
}
