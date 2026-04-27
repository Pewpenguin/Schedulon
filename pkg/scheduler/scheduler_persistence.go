package scheduler

import (
	"fmt"
	"time"

	"github.com/training-scheduler/pkg/persistence"
	pb "github.com/training-scheduler/proto"
)

func (s *Scheduler) EnablePersistence(config persistence.Config) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	manager, err := persistence.NewManager(config, s.logger)
	if err != nil {
		return fmt.Errorf("failed to create persistence manager: %v", err)
	}

	s.persistenceManager = manager

	manager.Start()

	s.mu.Unlock()
	err = s.LoadState()
	s.mu.Lock()

	if err != nil {
		s.logger.Warn("Failed to load existing state", map[string]interface{}{"error": err.Error()})
	}

	if config.AutoSave {
		s.logger.Info("Automatic state saving enabled", map[string]interface{}{"interval": config.SaveInterval})
	}

	return nil
}

func (s *Scheduler) DisablePersistence() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.persistenceManager != nil {
		s.mu.Unlock()
		_ = s.SaveState()
		s.mu.Lock()

		s.persistenceManager.Stop()
		s.persistenceManager = nil
		s.logger.Info("Persistence disabled", nil)
	}
}

func (s *Scheduler) SaveState() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.persistenceManager == nil {
		return fmt.Errorf("persistence manager not initialized")
	}

	state := &persistence.SchedulerState{
		Workers:      make(map[string]*persistence.WorkerState),
		Tasks:        make(map[string]*persistence.TaskState),
		PendingTasks: make([]string, 0, s.taskQueue.Len()),
	}

	for id, worker := range s.workers {
		gpuStates := make([]*persistence.GPUState, 0, len(worker.GPUs))
		for _, gpu := range worker.GPUs {
			gpuStates = append(gpuStates, &persistence.GPUState{
				ID:        gpu.ID,
				Model:     gpu.Model,
				MemoryMB:  gpu.MemoryMB,
				Available: gpu.Available,
			})
		}

		taskIDs := make([]string, 0, len(worker.Tasks))
		for taskID := range worker.Tasks {
			taskIDs = append(taskIDs, taskID)
		}

		state.Workers[id] = &persistence.WorkerState{
			ID:      id,
			GPUs:    gpuStates,
			Address: worker.Address,
			Status:  worker.Status,
			Tasks:   taskIDs,
		}
	}

	for id, task := range s.tasks {
		var startTime int64 = 0
		if !task.StartTime.IsZero() {
			startTime = task.StartTime.Unix()
		}

		state.Tasks[id] = &persistence.TaskState{
			ID:            id,
			Name:          task.Name,
			RequiredGPUs:  task.RequiredGPUs,
			MinGPUMemory:  task.MinGPUMemory,
			Configuration: task.Configuration,
			Status:        task.Status,
			WorkerID:      task.WorkerID,
			AssignedGPUs:  task.AssignedGPUs,
			StartTime:     startTime,
			Progress:      task.Progress,
		}
	}

	for _, task := range s.taskQueue.Snapshot() {
		state.PendingTasks = append(state.PendingTasks, task.ID)
	}

	err := s.persistenceManager.SaveState(state)
	if err != nil {
		s.logger.Error("Failed to save scheduler state", map[string]interface{}{"error": err.Error()})
		return err
	}

	s.logger.Info("Scheduler state saved successfully", nil)
	return nil
}

func (s *Scheduler) LoadState() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.persistenceManager == nil {
		return fmt.Errorf("persistence manager not initialized")
	}

	state, err := s.persistenceManager.LoadState()
	if err != nil {
		s.logger.Error("Failed to load scheduler state", map[string]interface{}{"error": err.Error()})
		return err
	}

	if state == nil {
		s.logger.Info("No previous state found, starting with empty state", nil)
		return nil
	}

	s.logger.Info("Loading scheduler state", map[string]interface{}{"saved_at": state.SavedAt})

	s.workers = make(map[string]*Worker)
	s.tasks = make(map[string]*Task)
	s.taskQueue = NewTaskQueue()

	for id, taskState := range state.Tasks {
		startTime := time.Time{}
		if taskState.StartTime > 0 {
			startTime = time.Unix(taskState.StartTime, 0)
		}

		task := &Task{
			ID:            id,
			Name:          taskState.Name,
			RequiredGPUs:  taskState.RequiredGPUs,
			MinGPUMemory:  taskState.MinGPUMemory,
			Configuration: taskState.Configuration,
			Status:        taskState.Status,
			WorkerID:      taskState.WorkerID,
			AssignedGPUs:  taskState.AssignedGPUs,
			StartTime:     startTime,
			Progress:      taskState.Progress,
		}

		s.tasks[id] = task
	}

	for id, workerState := range state.Workers {
		gpus := make([]*GPU, 0, len(workerState.GPUs))
		for _, gpuState := range workerState.GPUs {
			gpus = append(gpus, &GPU{
				ID:        gpuState.ID,
				Model:     gpuState.Model,
				MemoryMB:  gpuState.MemoryMB,
				Available: gpuState.Available,
			})
		}

		worker := &Worker{
			ID:      id,
			GPUs:    gpus,
			Address: workerState.Address,
			Status:  workerState.Status,
			Tasks:   make(map[string]*Task),
		}

		s.workers[id] = worker

		for _, taskID := range workerState.Tasks {
			task, exists := s.tasks[taskID]
			if !exists {
				s.logger.Warn("Task referenced by worker not found", map[string]interface{}{
					"task_id":   taskID,
					"worker_id": id,
				})
				continue
			}

			worker.Tasks[taskID] = task
		}
	}

	for _, taskID := range state.PendingTasks {
		task, exists := s.tasks[taskID]
		if !exists {
			s.logger.Warn("Pending task not found", map[string]interface{}{"task_id": taskID})
			continue
		}

		if task.Status != pb.TaskStatus_PENDING {
			if err := ValidateTransition(task.Status.String(), pb.TaskStatus_PENDING.String()); err != nil {
				s.logger.Warn("Skipping task from pending list due to invalid state transition", map[string]interface{}{
					"task_id": taskID,
					"status":  task.Status.String(),
					"error":   err.Error(),
				})
				continue
			}
			s.logger.Warn("Task marked as pending but has different status", map[string]interface{}{
				"task_id": taskID,
				"status":  task.Status.String(),
			})
			task.Status = pb.TaskStatus_PENDING
		}

		s.taskQueue.Enqueue(task)
	}

	s.logger.Info("Scheduler state loaded successfully", map[string]interface{}{
		"workers":       len(s.workers),
		"tasks":         len(s.tasks),
		"pending_tasks": s.taskQueue.Len(),
	})

	return nil
}
