package scheduler

import (
	"errors"
	"sort"

	pb "github.com/training-scheduler/proto"
)

type SimpleWorkloadPolicy struct{}

func NewSimpleWorkloadPolicy() *SimpleWorkloadPolicy {
	return &SimpleWorkloadPolicy{}
}

func (p *SimpleWorkloadPolicy) AssignTask(workers map[string]*Worker, task *Task) (string, []string, error) {
	type workerScore struct {
		id    string
		score float64
		gpus  []string
	}

	var candidates []workerScore

	for id, worker := range workers {
		if worker.Status != pb.WorkerStatus_IDLE {
			continue
		}

		var availableGPUs []string
		for _, gpu := range worker.GPUDevices {
			if gpu.Available && gpu.MemoryMB >= task.MinGPUMemory {
				availableGPUs = append(availableGPUs, gpu.ID)
			}
		}

		if uint32(len(availableGPUs)) >= task.RequiredGPUs {
			totalMemory := uint64(0)
			for _, gpu := range worker.GPUDevices {
				if gpu.Available {
					totalMemory += gpu.MemoryMB
				}
			}

			excessGPUs := float64(len(availableGPUs)) - float64(task.RequiredGPUs)
			excessMemory := float64(totalMemory) - float64(task.MinGPUMemory*uint64(task.RequiredGPUs))

			score := excessGPUs*0.7 + excessMemory*0.3

			candidates = append(candidates, workerScore{
				id:    id,
				score: score,
				gpus:  availableGPUs[:task.RequiredGPUs],
			})
		}
	}

	if len(candidates) == 0 {
		return "", nil, errors.New("no suitable worker found")
	}

	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].score < candidates[j].score
	})

	return candidates[0].id, candidates[0].gpus, nil
}

type BalancedWorkloadPolicy struct{}

func NewBalancedWorkloadPolicy() *BalancedWorkloadPolicy {
	return &BalancedWorkloadPolicy{}
}

func (p *BalancedWorkloadPolicy) AssignTask(workers map[string]*Worker, task *Task) (string, []string, error) {
	type workerScore struct {
		id            string
		score         float64
		gpus          []string
		taskCount     int
		totalGPUs     int
		availableGPUs int
	}

	var candidates []workerScore

	for id, worker := range workers {
		if worker.Status == pb.WorkerStatus_OFFLINE || worker.Status == pb.WorkerStatus_ERROR {
			continue
		}

		var availableGPUs []string
		for _, gpu := range worker.GPUDevices {
			if gpu.Available && gpu.MemoryMB >= task.MinGPUMemory {
				availableGPUs = append(availableGPUs, gpu.ID)
			}
		}

		if uint32(len(availableGPUs)) >= task.RequiredGPUs {
			taskCount := len(worker.Tasks)
			totalGPUs := worker.GPUs

			loadFactor := float64(taskCount) / float64(totalGPUs)
			availabilityFactor := float64(len(availableGPUs)) / float64(totalGPUs)

			score := loadFactor - availabilityFactor

			candidates = append(candidates, workerScore{
				id:            id,
				score:         score,
				gpus:          availableGPUs[:task.RequiredGPUs],
				taskCount:     taskCount,
				totalGPUs:     totalGPUs,
				availableGPUs: len(availableGPUs),
			})
		}
	}

	if len(candidates) == 0 {
		return "", nil, errors.New("no suitable worker found")
	}

	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].score < candidates[j].score
	})

	return candidates[0].id, candidates[0].gpus, nil
}
