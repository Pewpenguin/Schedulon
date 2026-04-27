package scheduler

import (
	"time"

	"github.com/training-scheduler/pkg/metrics"
	pb "github.com/training-scheduler/proto"
)

func (s *Scheduler) SetMetrics(metrics *metrics.SchedulerMetrics) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.metrics = metrics
}

func (s *Scheduler) updateMetrics() {
	if s.metrics == nil {
		return
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	activeCount := 0
	for _, task := range s.tasks {
		if task.Status == pb.TaskStatus_RUNNING {
			activeCount++
		}
	}

	s.metrics.SetActiveTasks(activeCount)
	s.metrics.SetPendingTasks(s.taskQueue.Len())
	s.metrics.SetActiveWorkers(len(s.workers))
}

func (s *Scheduler) recordTaskSubmission() {
	if s.metrics == nil {
		return
	}

	s.metrics.IncrementTotalTasks()
	s.updateMetrics()
}

func (s *Scheduler) recordTaskCompletion(task *Task) {
	if s.metrics == nil {
		return
	}

	duration := time.Since(task.StartTime).Seconds()
	s.metrics.ObserveTaskDuration(task.Name, duration)

	if task.Status == pb.TaskStatus_COMPLETED {
		s.metrics.IncrementCompletedTasks()
	} else if task.Status == pb.TaskStatus_FAILED {
		s.metrics.IncrementFailedTasks()
	}

	s.updateMetrics()
}

func (s *Scheduler) recordWorkerRegistration() {
	if s.metrics == nil {
		return
	}

	s.updateMetrics()
}

func (s *Scheduler) recordWorkerStatusChange() {
	if s.metrics == nil {
		return
	}

	s.updateMetrics()
}
