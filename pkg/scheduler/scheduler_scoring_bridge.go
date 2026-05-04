package scheduler

import (
	"strings"

	"github.com/training-scheduler/pkg/scheduler/policies"
)

func (s *Scheduler) scoreAssignment(w *Worker, t *Task, sameTenantOnWorker int) float64 {
	return policies.Score(workerViewFrom(w), taskViewFrom(t), sameTenantOnWorker)
}

func (s *Scheduler) sameTenantRunningOnWorker(w *Worker, tenant string) int {
	if w == nil || strings.TrimSpace(tenant) == "" {
		return 0
	}
	n := 0
	for _, rt := range w.Tasks {
		if rt != nil && rt.Tenant == tenant {
			n++
		}
	}
	return n
}
