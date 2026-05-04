package policies

import (
	"hash/fnv"
	"math"
)

// WorkerView is a scheduling snapshot of a worker (no live references to scheduler types).
type WorkerView struct {
	ID                string
	AvailableGPUCount int
	TotalGPUCount     int
	GPUMemoryFreeMiB  int64
	GPUMemoryTotalMiB int64
	GPUModel          string
	RunningTaskCount  int
}

// TaskView captures task requirements used for scoring.
type TaskView struct {
	ID                     string
	Priority               int32
	RequiredGPUs           int
	RequiredGPUMemoryMiB   int32
	RequiredGPUModel       string
	Tenant                 string
	RequiredPerGPUMemoryMB uint64 // minimum per-GPU memory (scheduler MinGPUMemory)
}

// Score ranks a (worker, task) pair. Higher is better.
// score = (gpu_fit_score * 0.5) + (load_balance_score * 0.3) + (fairness_penalty * -0.2) + jitter
func Score(w WorkerView, t TaskView, sameTenantRunningOnWorker int) float64 {
	gpuFit := gpuFitScore(w, t)
	loadBal := loadBalanceScore(w)
	fairPen := fairnessPenalty(sameTenantRunningOnWorker)
	jit := deterministicJitter(w.ID, t.ID)
	// Large weight so ordering matches business priority when worker-fit terms tie.
	pri := float64(t.Priority) * 100.0

	return gpuFit*0.5 + loadBal*0.3 + fairPen*(-0.2) + jit + pri
}

func gpuFitScore(w WorkerView, t TaskView) float64 {
	if t.RequiredGPUs <= 0 {
		return 0
	}
	if w.AvailableGPUCount < t.RequiredGPUs {
		return 0
	}
	// Prefer a tighter GPU-count fit (use fraction of GPUs consumed).
	use := float64(t.RequiredGPUs) / float64(max(1, w.AvailableGPUCount))
	memFit := 1.0
	if t.RequiredGPUMemoryMiB > 0 && w.GPUMemoryFreeMiB > 0 {
		ratio := float64(t.RequiredGPUMemoryMiB) / float64(w.GPUMemoryFreeMiB)
		if ratio > 1 {
			memFit = 0
		} else {
			memFit = ratio // tighter request vs free -> higher score
		}
	}
	return math.Min(1, use)*0.65 + math.Min(1, memFit)*0.35
}

func loadBalanceScore(w WorkerView) float64 {
	if w.TotalGPUCount <= 0 {
		return 0.5
	}
	busyFrac := float64(w.RunningTaskCount) / float64(max(1, w.TotalGPUCount*2))
	if busyFrac > 1 {
		busyFrac = 1
	}
	return 1 - busyFrac
}

func fairnessPenalty(sameTenantRunningOnWorker int) float64 {
	if sameTenantRunningOnWorker <= 0 {
		return 0
	}
	// Penalty grows with concentration of the same tenant on this worker.
	return float64(sameTenantRunningOnWorker)
}

func deterministicJitter(workerID, taskID string) float64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(workerID))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(taskID))
	v := float64(h.Sum64()%10000) / 1e6 // [0, 0.01)
	return v
}
