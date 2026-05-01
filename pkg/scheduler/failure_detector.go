package scheduler

import (
	"context"
	"time"

	pb "github.com/training-scheduler/proto"
)

// StartFailureDetector runs startFailureDetector in a goroutine until ctx is cancelled.
func (s *Scheduler) StartFailureDetector(ctx context.Context) {
	go s.startFailureDetector(ctx)
}

func (s *Scheduler) startFailureDetector(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.runFailureChecks()
		}
	}
}

func (s *Scheduler) effectiveWorkerTimeout() time.Duration {
	if s != nil && s.workerTimeout > 0 {
		return s.workerTimeout
	}
	return 60 * time.Second
}

func (s *Scheduler) runFailureChecks() {
	s.mu.Lock()
	now := time.Now()
	workerTO := s.effectiveWorkerTimeout()

	for id, worker := range s.workers {
		if worker.Status == pb.WorkerStatus_OFFLINE {
			continue
		}
		if worker.LastHeartbeat.IsZero() {
			worker.LastHeartbeat = now
			continue
		}
		if now.Sub(worker.LastHeartbeat) > workerTO {
			s.requeueTasksForOfflineWorkerLocked(id)
			s.logger.Warn("Worker marked offline (heartbeat timeout)", map[string]interface{}{
				"worker_id":        id,
				"last_heartbeat":   worker.LastHeartbeat,
				"worker_timeout":   workerTO.String(),
				"elapsed_since_hb": now.Sub(worker.LastHeartbeat).String(),
			})
			worker.Status = pb.WorkerStatus_OFFLINE
		}
	}

	for _, task := range s.tasks {
		if task == nil || task.Status != pb.TaskStatus_RUNNING {
			continue
		}
		if task.LeaseExpiresAt.IsZero() {
			continue
		}
		if !task.LeaseExpiresAt.Before(now) {
			continue
		}

		s.logger.Info("Task reclaimed due to expired lease", map[string]interface{}{
			"task_id":            task.ID,
			"previous_worker_id": task.WorkerID,
			"lease_expired_at":   task.LeaseExpiresAt,
		})
		s.reclaimExpiredTaskLocked(task)
	}

	metricsSnapshot := s.metricsSnapshotLocked()
	s.updatePersistentStateLocked()
	s.mu.Unlock()

	s.updateMetrics(metricsSnapshot.activeTasks, metricsSnapshot.pendingTasks, metricsSnapshot.activeWorkers)
	s.UpdatePersistentState()
}

// reclaimExpiredTaskLocked moves a RUNNING task back to the pending queue. Caller must hold s.mu.
func (s *Scheduler) reclaimExpiredTaskLocked(task *Task) {
	if task == nil {
		return
	}

	workerID := task.WorkerID
	if workerID != "" {
		if worker, ok := s.workers[workerID]; ok {
			delete(worker.Tasks, task.ID)
			for _, gpu := range worker.GPUDevices {
				for _, aid := range task.AssignedGPUs {
					if gpu.ID == aid {
						gpu.Available = true
					}
				}
			}
			if len(worker.Tasks) == 0 && worker.Status != pb.WorkerStatus_OFFLINE {
				worker.Status = pb.WorkerStatus_IDLE
			}
		}
	}

	s.taskQueue.Requeue(task)
}

// requeueTasksForOfflineWorkerLocked releases GPUs and requeues all RUNNING tasks for the worker.
// Caller must hold s.mu.
func (s *Scheduler) requeueTasksForOfflineWorkerLocked(workerID string) bool {
	worker := s.workers[workerID]
	if worker == nil {
		return false
	}

	ids := make([]string, 0, len(worker.Tasks))
	for id := range worker.Tasks {
		ids = append(ids, id)
	}

	any := false
	for _, tid := range ids {
		task := s.tasks[tid]
		if task == nil || task.Status != pb.TaskStatus_RUNNING {
			continue
		}

		delete(worker.Tasks, tid)
		for _, gpu := range worker.GPUDevices {
			for _, aid := range task.AssignedGPUs {
				if gpu.ID == aid {
					gpu.Available = true
				}
			}
		}

		s.logger.Info("Task requeued (worker offline)", map[string]interface{}{
			"task_id":   task.ID,
			"worker_id": workerID,
		})
		s.taskQueue.Requeue(task)
		any = true
	}

	return any
}
