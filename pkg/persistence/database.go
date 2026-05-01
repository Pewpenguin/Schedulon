package persistence

import (
	"fmt"
	"time"

	"gorm.io/driver/postgres"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type DatabaseType string

const (
	SQLite     DatabaseType = "sqlite"
	PostgreSQL DatabaseType = "postgres"
)

type DatabaseConfig struct {
	Type             DatabaseType
	ConnectionString string
	AutoMigrate      bool
	LogMode          bool
}

func DefaultSQLiteConfig() DatabaseConfig {
	return DatabaseConfig{
		Type:             SQLite,
		ConnectionString: "scheduler.db",
		AutoMigrate:      true,
		LogMode:          false,
	}
}

func DefaultPostgresConfig() DatabaseConfig {
	return DatabaseConfig{
		Type:             PostgreSQL,
		ConnectionString: "host=localhost user=postgres password=postgres dbname=scheduler port=5432 sslmode=disable",
		AutoMigrate:      true,
		LogMode:          false,
	}
}

type WorkerModel struct {
	ID        string `gorm:"primaryKey"`
	Address   string
	Status    int
	CreatedAt time.Time
	UpdatedAt time.Time
}

type GPUModel struct {
	ID        string `gorm:"primaryKey"`
	WorkerID  string `gorm:"index"`
	Model     string
	MemoryMB  uint64
	Available bool
	CreatedAt time.Time
	UpdatedAt time.Time
}

type TaskModel struct {
	ID            string `gorm:"primaryKey"`
	Name          string
	RequiredGPUs  uint32
	MinGPUMemory  uint64
	Configuration []byte
	Status        int
	WorkerID      string
	StartTime     time.Time
	Progress      float32
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

type TaskGPUModel struct {
	TaskID string `gorm:"primaryKey"`
	GPUID  string `gorm:"primaryKey"`
}

type PendingTaskModel struct {
	TaskID    string `gorm:"primaryKey"`
	CreatedAt time.Time
}

type IdempotencyKeyModel struct {
	Key       string `gorm:"primaryKey"`
	TaskID    string `gorm:"index"`
	CreatedAt time.Time
	UpdatedAt time.Time
}

type DatabaseManager struct {
	db     *gorm.DB
	config DatabaseConfig
}

func NewDatabaseManager(config DatabaseConfig) (*DatabaseManager, error) {
	var db *gorm.DB
	var err error

	logLevel := logger.Silent
	if config.LogMode {
		logLevel = logger.Info
	}

	gormConfig := &gorm.Config{
		Logger: logger.Default.LogMode(logLevel),
	}

	switch config.Type {
	case SQLite:
		db, err = gorm.Open(sqlite.Open(config.ConnectionString), gormConfig)
	case PostgreSQL:
		db, err = gorm.Open(postgres.Open(config.ConnectionString), gormConfig)
	default:
		return nil, fmt.Errorf("unsupported database type: %s", config.Type)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %v", err)
	}

	manager := &DatabaseManager{
		db:     db,
		config: config,
	}

	if config.AutoMigrate {
		if err := manager.MigrateSchema(); err != nil {
			return nil, fmt.Errorf("failed to migrate database schema: %v", err)
		}
	}

	return manager, nil
}

func (m *DatabaseManager) MigrateSchema() error {
	return m.db.AutoMigrate(
		&WorkerModel{},
		&GPUModel{},
		&TaskModel{},
		&TaskGPUModel{},
		&PendingTaskModel{},
		&IdempotencyKeyModel{},
	)
}

func (m *DatabaseManager) SaveWorker(worker *WorkerModel, gpus []*GPUModel) error {
	return m.db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Save(worker).Error; err != nil {
			return err
		}

		for _, gpu := range gpus {
			gpu.WorkerID = worker.ID
			if err := tx.Save(gpu).Error; err != nil {
				return err
			}
		}

		return nil
	})
}

func (m *DatabaseManager) SaveTask(task *TaskModel, assignedGPUs []string) error {
	return m.db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Save(task).Error; err != nil {
			return err
		}

		if err := tx.Where("task_id = ?", task.ID).Delete(&TaskGPUModel{}).Error; err != nil {
			return err
		}

		for _, gpuID := range assignedGPUs {
			assignment := &TaskGPUModel{
				TaskID: task.ID,
				GPUID:  gpuID,
			}
			if err := tx.Create(assignment).Error; err != nil {
				return err
			}
		}

		return nil
	})
}

func (m *DatabaseManager) SavePendingTask(taskID string) error {
	pendingTask := &PendingTaskModel{
		TaskID:    taskID,
		CreatedAt: time.Now(),
	}
	return m.db.Create(pendingTask).Error
}

func (m *DatabaseManager) RemovePendingTask(taskID string) error {
	return m.db.Where("task_id = ?", taskID).Delete(&PendingTaskModel{}).Error
}

func (m *DatabaseManager) SaveIdempotencyKey(key, taskID string) error {
	record := &IdempotencyKeyModel{
		Key:       key,
		TaskID:    taskID,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}
	return m.db.Save(record).Error
}

func (m *DatabaseManager) GetWorkers() ([]*WorkerModel, error) {
	var workers []*WorkerModel
	if err := m.db.Find(&workers).Error; err != nil {
		return nil, err
	}
	return workers, nil
}

func (m *DatabaseManager) GetWorkerGPUs(workerID string) ([]*GPUModel, error) {
	var gpus []*GPUModel
	if err := m.db.Where("worker_id = ?", workerID).Find(&gpus).Error; err != nil {
		return nil, err
	}
	return gpus, nil
}

func (m *DatabaseManager) GetTasks() ([]*TaskModel, error) {
	var tasks []*TaskModel
	if err := m.db.Find(&tasks).Error; err != nil {
		return nil, err
	}
	return tasks, nil
}

func (m *DatabaseManager) GetTaskGPUs(taskID string) ([]string, error) {
	var assignments []*TaskGPUModel
	if err := m.db.Where("task_id = ?", taskID).Find(&assignments).Error; err != nil {
		return nil, err
	}

	gpuIDs := make([]string, len(assignments))
	for i, assignment := range assignments {
		gpuIDs[i] = assignment.GPUID
	}

	return gpuIDs, nil
}

func (m *DatabaseManager) GetPendingTasks() ([]string, error) {
	var pendingTasks []*PendingTaskModel
	if err := m.db.Find(&pendingTasks).Error; err != nil {
		return nil, err
	}

	taskIDs := make([]string, len(pendingTasks))
	for i, pendingTask := range pendingTasks {
		taskIDs[i] = pendingTask.TaskID
	}

	return taskIDs, nil
}

func (m *DatabaseManager) GetIdempotencyKeys() (map[string]string, error) {
	var records []*IdempotencyKeyModel
	if err := m.db.Find(&records).Error; err != nil {
		return nil, err
	}

	out := make(map[string]string, len(records))
	for _, record := range records {
		out[record.Key] = record.TaskID
	}

	return out, nil
}

func (m *DatabaseManager) ClearAllState() error {
	return m.db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Exec("DELETE FROM pending_task_models").Error; err != nil {
			return err
		}
		if err := tx.Exec("DELETE FROM idempotency_key_models").Error; err != nil {
			return err
		}
		if err := tx.Exec("DELETE FROM task_gpu_models").Error; err != nil {
			return err
		}
		if err := tx.Exec("DELETE FROM task_models").Error; err != nil {
			return err
		}
		if err := tx.Exec("DELETE FROM gpu_models").Error; err != nil {
			return err
		}
		if err := tx.Exec("DELETE FROM worker_models").Error; err != nil {
			return err
		}
		return nil
	})
}

func (m *DatabaseManager) Close() error {
	sqlDB, err := m.db.DB()
	if err != nil {
		return err
	}
	return sqlDB.Close()
}
