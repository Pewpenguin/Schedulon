package scheduler

import (
	"math"

	"github.com/training-scheduler/pkg/scheduler/policies"
)

func recomputeWorkerGPUAggregates(w *Worker) {
	if w == nil {
		return
	}
	var totalMiB, freeMiB int64
	for _, g := range w.GPUDevices {
		if g == nil {
			continue
		}
		totalMiB += int64(g.MemoryMB)
		if g.Available {
			freeMiB += int64(g.MemoryMB)
		}
	}
	w.GPUMemoryTotal = clampInt32MiB(totalMiB)
	w.GPUMemoryFree = clampInt32MiB(freeMiB)
	w.GPUModel = homogeneousGPUModel(w.GPUDevices)
}

func clampInt32MiB(v int64) int32 {
	if v > int64(math.MaxInt32) {
		return math.MaxInt32
	}
	if v < int64(math.MinInt32) {
		return math.MinInt32
	}
	return int32(v)
}

func homogeneousGPUModel(gpus []*GPU) string {
	if len(gpus) == 0 {
		return ""
	}
	m := gpus[0].Model
	for _, g := range gpus[1:] {
		if g.Model != m {
			return ""
		}
	}
	return m
}

func workerViewFrom(w *Worker) policies.WorkerView {
	if w == nil {
		return policies.WorkerView{}
	}
	avail := 0
	for _, g := range w.GPUDevices {
		if g != nil && g.Available {
			avail++
		}
	}
	return policies.WorkerView{
		ID:                w.ID,
		AvailableGPUCount: avail,
		TotalGPUCount:     len(w.GPUDevices),
		GPUMemoryFreeMiB:  int64(w.GPUMemoryFree),
		GPUMemoryTotalMiB: int64(w.GPUMemoryTotal),
		GPUModel:          w.GPUModel,
		RunningTaskCount:  len(w.Tasks),
	}
}

func taskViewFrom(t *Task) policies.TaskView {
	if t == nil {
		return policies.TaskView{}
	}
	return policies.TaskView{
		ID:                     t.ID,
		Priority:               t.Priority,
		RequiredGPUs:         int(t.RequiredGPUs),
		RequiredGPUMemoryMiB: t.RequiredGPUMemory,
		RequiredGPUModel:     t.RequiredGPUModel,
		Tenant:               t.Tenant,
		RequiredPerGPUMemoryMB: t.MinGPUMemory,
	}
}
