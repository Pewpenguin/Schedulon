package scheduler

import (
	"time"

	"github.com/training-scheduler/pkg/metrics"
	pb "github.com/training-scheduler/proto"
)

func (s *Scheduler) SetMetrics(metrics *metrics.SchedulerMetrics) {
	s.mu.Lock()
	s.metrics = metrics
	snapshot := s.metricsSnapshotLocked()
	s.mu.Unlock()

	s.updateMetrics(snapshot.activeTasks, snapshot.pendingTasks, snapshot.activeWorkers)
}

type schedulerMetricsSnapshot struct {
	activeTasks   int
	pendingTasks  int
	activeWorkers int
}

func (s *Scheduler) metricsSnapshotLocked() schedulerMetricsSnapshot {
	activeCount := 0
	for _, task := range s.tasks {
		if task.Status == pb.TaskStatus_RUNNING {
			activeCount++
		}
	}

	return schedulerMetricsSnapshot{
		activeTasks:   activeCount,
		pendingTasks:  s.taskQueue.Len(),
		activeWorkers: len(s.workers),
	}
}

// updateMetrics publishes scheduler gauges from a precomputed snapshot.
// It intentionally avoids touching scheduler mutexes; callers must gather
// state while holding locks and then invoke this method after unlocking.
func (s *Scheduler) updateMetrics(activeTasks, pendingTasks, activeWorkers int) {
	if s.metrics == nil {
		return
	}

	s.metrics.SetActiveTasks(activeTasks)
	s.metrics.SetPendingTasks(pendingTasks)
	s.metrics.SetActiveWorkers(activeWorkers)
}

func (s *Scheduler) recordTaskSubmission() {
	if s.metrics == nil {
		return
	}

	s.metrics.IncrementTotalTasks()
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
}

func (s *Scheduler) recordWorkerRegistration() {
	if s.metrics == nil {
		return
	}
}

func (s *Scheduler) recordWorkerStatusChange() {
	if s.metrics == nil {
		return
	}
}
