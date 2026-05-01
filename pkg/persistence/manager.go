package persistence

import (
	"fmt"
	"sync"
	"time"

	"github.com/training-scheduler/pkg/logging"
	pb "github.com/training-scheduler/proto"
)

type PersistenceType string

const (
	DatabasePersistence PersistenceType = "database"
	FilePersistence     PersistenceType = "file"
)

type Config struct {
	Type         PersistenceType
	Database     DatabaseConfig
	SaveInterval int
	AutoSave     bool
}

func DefaultConfig() Config {
	return Config{
		Type:         DatabasePersistence,
		Database:     DefaultSQLiteConfig(),
		SaveInterval: 60,
		AutoSave:     true,
	}
}

type SchedulerState struct {
	Workers      map[string]*WorkerState `json:"workers"`
	Tasks        map[string]*TaskState   `json:"tasks"`
	PendingTasks []string                `json:"pending_tasks"`
	SavedAt      int64                   `json:"saved_at"`
}

type WorkerState struct {
	ID      string          `json:"id"`
	GPUs    []*GPUState     `json:"gpus"`
	Address string          `json:"address"`
	Status  pb.WorkerStatus `json:"status"`
	Tasks   []string        `json:"tasks"`
}

type GPUState struct {
	ID        string `json:"id"`
	Model     string `json:"model"`
	MemoryMB  uint64 `json:"memory_mb"`
	Available bool   `json:"available"`
}

type TaskState struct {
	ID            string        `json:"id"`
	Name          string        `json:"name"`
	RequiredGPUs  uint32        `json:"required_gpus"`
	MinGPUMemory  uint64        `json:"min_gpu_memory"`
	Configuration []byte        `json:"configuration"`
	Status        pb.TaskStatus `json:"status"`
	WorkerID      string        `json:"worker_id"`
	AssignedGPUs  []string      `json:"assigned_gpus"`
	StartTime     int64         `json:"start_time"`
	Progress      float32       `json:"progress"`
}

type Manager struct {
	config      Config
	dbManager   *DatabaseManager
	mu          sync.RWMutex
	stopCh      chan struct{}
	saveTrigger chan struct{}
	saveHandler func() error
	logger      *logging.Logger
}

func NewManager(config Config, logger *logging.Logger) (*Manager, error) {
	manager := &Manager{
		config:      config,
		stopCh:      make(chan struct{}),
		saveTrigger: make(chan struct{}, 1),
		logger:      logger,
	}

	if config.Type == DatabasePersistence {
		dbManager, err := NewDatabaseManager(config.Database)
		if err != nil {
			return nil, fmt.Errorf("failed to create database manager: %v", err)
		}
		manager.dbManager = dbManager
	}

	return manager, nil
}

func (m *Manager) Start() {
	go m.saveConsumerLoop()

	if m.config.AutoSave {
		go m.autoSaveLoop()
	}
}

func (m *Manager) Stop() {
	close(m.stopCh)
	if m.dbManager != nil {
		_ = m.dbManager.Close()
	}
}

func (m *Manager) autoSaveLoop() {
	ticker := time.NewTicker(time.Duration(m.config.SaveInterval) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			select {
			case m.saveTrigger <- struct{}{}:
			default:
			}
		case <-m.stopCh:
			return
		}
	}
}

func (m *Manager) TriggerSave() {
	select {
	case m.saveTrigger <- struct{}{}:
	default:
	}
}

func (m *Manager) SetSaveHandler(handler func() error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.saveHandler = handler
}

func (m *Manager) saveConsumerLoop() {
	for {
		select {
		case <-m.stopCh:
			m.logger.Info("Persistence save consumer stopped", nil)
			return
		case <-m.saveTrigger:
			m.mu.RLock()
			handler := m.saveHandler
			m.mu.RUnlock()

			if handler == nil {
				m.logger.Warn("Persistence trigger received without save handler", nil)
				continue
			}

			m.logger.Info("Processing persistence save trigger", nil)
			start := time.Now()
			if err := handler(); err != nil {
				m.logger.Error("Persistence save trigger failed", map[string]interface{}{
					"error": err.Error(),
				})
				continue
			}

			m.logger.Info("Persistence save trigger completed", map[string]interface{}{
				"duration_ms": time.Since(start).Milliseconds(),
			})
		}
	}
}

func (m *Manager) SaveState(state *SchedulerState) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	state.SavedAt = time.Now().Unix()
	m.logger.Info("Flushing scheduler state", map[string]interface{}{
		"workers":       len(state.Workers),
		"tasks":         len(state.Tasks),
		"pending_tasks": len(state.PendingTasks),
		"target":        m.config.Type,
	})

	if m.config.Type == DatabasePersistence {
		if err := m.saveToDatabase(state); err != nil {
			return err
		}

		m.logger.Info("Scheduler state flush completed", map[string]interface{}{
			"target":    m.config.Type,
			"timestamp": state.SavedAt,
		})
		return nil
	}

	return fmt.Errorf("unsupported persistence type: %s", m.config.Type)
}

func (m *Manager) LoadState() (*SchedulerState, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.config.Type == DatabasePersistence {
		return m.loadFromDatabase()
	}

	return nil, fmt.Errorf("unsupported persistence type: %s", m.config.Type)
}

func (m *Manager) saveToDatabase(state *SchedulerState) error {
	if m.dbManager == nil {
		return fmt.Errorf("database manager not initialized")
	}

	if err := m.dbManager.ClearAllState(); err != nil {
		return fmt.Errorf("failed to clear existing state: %v", err)
	}

	for id, worker := range state.Workers {
		workerModel := &WorkerModel{
			ID:        id,
			Address:   worker.Address,
			Status:    int(worker.Status),
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		}

		gpuModels := make([]*GPUModel, 0, len(worker.GPUs))
		for _, gpu := range worker.GPUs {
			gpuModels = append(gpuModels, &GPUModel{
				ID:        gpu.ID,
				WorkerID:  id,
				Model:     gpu.Model,
				MemoryMB:  gpu.MemoryMB,
				Available: gpu.Available,
				CreatedAt: time.Now(),
				UpdatedAt: time.Now(),
			})
		}

		if err := m.dbManager.SaveWorker(workerModel, gpuModels); err != nil {
			return fmt.Errorf("failed to save worker %s: %v", id, err)
		}
	}

	for id, task := range state.Tasks {
		startTime := time.Unix(task.StartTime, 0)
		if task.StartTime == 0 {
			startTime = time.Time{}
		}

		taskModel := &TaskModel{
			ID:            id,
			Name:          task.Name,
			RequiredGPUs:  task.RequiredGPUs,
			MinGPUMemory:  task.MinGPUMemory,
			Configuration: task.Configuration,
			Status:        int(task.Status),
			WorkerID:      task.WorkerID,
			StartTime:     startTime,
			Progress:      task.Progress,
			CreatedAt:     time.Now(),
			UpdatedAt:     time.Now(),
		}

		if err := m.dbManager.SaveTask(taskModel, task.AssignedGPUs); err != nil {
			return fmt.Errorf("failed to save task %s: %v", id, err)
		}
	}

	for _, taskID := range state.PendingTasks {
		if err := m.dbManager.SavePendingTask(taskID); err != nil {
			return fmt.Errorf("failed to save pending task %s: %v", taskID, err)
		}
	}

	m.logger.Info("Scheduler state saved to database", map[string]interface{}{
		"workers":       len(state.Workers),
		"tasks":         len(state.Tasks),
		"pending_tasks": len(state.PendingTasks),
		"timestamp":     state.SavedAt,
	})

	return nil
}

func (m *Manager) loadFromDatabase() (*SchedulerState, error) {
	if m.dbManager == nil {
		return nil, fmt.Errorf("database manager not initialized")
	}

	state := &SchedulerState{
		Workers:      make(map[string]*WorkerState),
		Tasks:        make(map[string]*TaskState),
		PendingTasks: make([]string, 0),
		SavedAt:      time.Now().Unix(),
	}

	workerModels, err := m.dbManager.GetWorkers()
	if err != nil {
		return nil, fmt.Errorf("failed to load workers: %v", err)
	}

	for _, workerModel := range workerModels {
		var gpuModels []*GPUModel
		gpuModels, err = m.dbManager.GetWorkerGPUs(workerModel.ID)
		if err != nil {
			return nil, fmt.Errorf("failed to load GPUs for worker %s: %v", workerModel.ID, err)
		}

		gpus := make([]*GPUState, 0, len(gpuModels))
		for _, gpuModel := range gpuModels {
			gpus = append(gpus, &GPUState{
				ID:        gpuModel.ID,
				Model:     gpuModel.Model,
				MemoryMB:  gpuModel.MemoryMB,
				Available: gpuModel.Available,
			})
		}

		state.Workers[workerModel.ID] = &WorkerState{
			ID:      workerModel.ID,
			GPUs:    gpus,
			Address: workerModel.Address,
			Status:  pb.WorkerStatus(workerModel.Status),
			Tasks:   make([]string, 0),
		}
	}

	taskModels, err := m.dbManager.GetTasks()
	if err != nil {
		return nil, fmt.Errorf("failed to load tasks: %v", err)
	}

	for _, taskModel := range taskModels {
		var gpuIDs []string
		gpuIDs, err = m.dbManager.GetTaskGPUs(taskModel.ID)
		if err != nil {
			return nil, fmt.Errorf("failed to load GPU assignments for task %s: %v", taskModel.ID, err)
		}

		startTime := taskModel.StartTime.Unix()
		if taskModel.StartTime.IsZero() {
			startTime = 0
		}

		state.Tasks[taskModel.ID] = &TaskState{
			ID:            taskModel.ID,
			Name:          taskModel.Name,
			RequiredGPUs:  taskModel.RequiredGPUs,
			MinGPUMemory:  taskModel.MinGPUMemory,
			Configuration: taskModel.Configuration,
			Status:        pb.TaskStatus(taskModel.Status),
			WorkerID:      taskModel.WorkerID,
			AssignedGPUs:  gpuIDs,
			StartTime:     startTime,
			Progress:      taskModel.Progress,
		}

		if taskModel.WorkerID != "" {
			if worker, exists := state.Workers[taskModel.WorkerID]; exists {
				worker.Tasks = append(worker.Tasks, taskModel.ID)
			}
		}
	}

	pendingTaskIDs, err := m.dbManager.GetPendingTasks()
	if err != nil {
		return nil, fmt.Errorf("failed to load pending tasks: %v", err)
	}

	state.PendingTasks = pendingTaskIDs

	m.logger.Info("Scheduler state loaded from database", map[string]interface{}{
		"workers":       len(state.Workers),
		"tasks":         len(state.Tasks),
		"pending_tasks": len(state.PendingTasks),
	})

	return state, nil
}
