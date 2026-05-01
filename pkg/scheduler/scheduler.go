package scheduler

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/training-scheduler/pkg/logging"
	"github.com/training-scheduler/pkg/metrics"
	"github.com/training-scheduler/pkg/persistence"
	pb "github.com/training-scheduler/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Scheduler struct {
	pb.UnimplementedTrainingSchedulerServer

	mu                 sync.RWMutex
	workers            map[string]*Worker
	tasks              map[string]*Task
	taskQueue          *TaskQueue
	workloadPolicy     WorkloadPolicy
	leaseDuration      time.Duration
	workerTimeout      time.Duration
	metrics            *metrics.SchedulerMetrics
	logger             *logging.Logger
	persistenceManager *persistence.Manager
}

type Worker struct {
	ID            string
	GPUs          int
	GPUDevices    []*GPU
	Address       string
	Status        pb.WorkerStatus
	Tasks         map[string]*Task
	LastHeartbeat time.Time
	RunningTasks  int
}

type GPU struct {
	ID        string
	Model     string
	MemoryMB  uint64
	Available bool
}

type Task struct {
	ID             string
	Name           string
	RequiredGPUs   uint32
	MinGPUMemory   uint64
	Configuration  []byte
	Status         pb.TaskStatus
	WorkerID       string
	AssignedGPUs   []string
	StartTime      time.Time
	Progress       float32
	Metrics        []*pb.Metric
	LeaseOwner     string
	LeaseExpiresAt time.Time
}

type WorkloadPolicy interface {
	AssignTask(workers map[string]*Worker, task *Task) (string, []string, error)
}

func NewScheduler(policy WorkloadPolicy) *Scheduler {
	// Create default logger
	loggerConfig := logging.Config{
		Level:     logging.InfoLevel,
		Component: "scheduler",
	}
	logger, err := logging.NewLogger(loggerConfig)
	if err != nil {
		logger, _ = logging.NewLogger(logging.Config{Level: logging.InfoLevel})
	}

	return &Scheduler{
		workers:        make(map[string]*Worker),
		tasks:          make(map[string]*Task),
		taskQueue:      NewTaskQueue(),
		workloadPolicy: policy,
		leaseDuration:  30 * time.Second,
		workerTimeout:  60 * time.Second,
		logger:         logger,
	}
}

func (s *Scheduler) effectiveLeaseDuration() time.Duration {
	if s != nil && s.leaseDuration > 0 {
		return s.leaseDuration
	}
	return 30 * time.Second
}

// grantLeaseLocked sets lease ownership and expiry. Caller must hold s.mu.
func (s *Scheduler) grantLeaseLocked(task *Task, workerID string) {
	if task == nil || workerID == "" {
		return
	}
	task.LeaseOwner = workerID
	task.LeaseExpiresAt = time.Now().Add(s.effectiveLeaseDuration())
}

func (s *Scheduler) RegisterWorker(ctx context.Context, req *pb.RegisterWorkerRequest) (*pb.RegisterWorkerResponse, error) {
	s.mu.Lock()

	workerID := req.WorkerId
	if workerID == "" {
		workerID = fmt.Sprintf("worker-%d", len(s.workers)+1)
	}

	if _, exists := s.workers[workerID]; exists {
		s.mu.Unlock()
		return &pb.RegisterWorkerResponse{
			Success:    false,
			Message:    "Worker ID already registered",
			AssignedId: workerID,
		}, nil
	}

	gpus := make([]*GPU, 0, len(req.Gpus))
	for _, g := range req.Gpus {
		gpus = append(gpus, &GPU{
			ID:        g.Id,
			Model:     g.Model,
			MemoryMB:  g.MemoryMb,
			Available: g.Available,
		})
	}

	worker := &Worker{
		ID:            workerID,
		GPUs:          len(gpus),
		GPUDevices:    gpus,
		Address:       req.Address,
		Status:        pb.WorkerStatus_IDLE,
		Tasks:         make(map[string]*Task),
		LastHeartbeat: time.Now(),
	}

	s.workers[workerID] = worker

	s.logger.Info("Worker registered", map[string]interface{}{
		"worker_id": workerID,
		"gpu_count": len(gpus),
	})

	s.recordWorkerRegistration()
	s.updatePersistentStateLocked()
	s.assignPendingTasks()
	metricsSnapshot := s.metricsSnapshotLocked()
	s.mu.Unlock()
	s.updateMetrics(metricsSnapshot.activeTasks, metricsSnapshot.pendingTasks, metricsSnapshot.activeWorkers)

	return &pb.RegisterWorkerResponse{
		Success:    true,
		Message:    "Worker registered successfully",
		AssignedId: workerID,
	}, nil
}

func (s *Scheduler) RequestTask(ctx context.Context, req *pb.TaskRequest) (*pb.Task, error) {
	s.mu.Lock()

	if req == nil {
		s.mu.Unlock()
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}

	workerID := req.WorkerId
	if strings.TrimSpace(workerID) == "" {
		s.mu.Unlock()
		return nil, status.Error(codes.InvalidArgument, "worker_id is required")
	}

	// RequestTask is worker-polling only; clients must use SubmitTask.
	if req.Task != nil {
		s.mu.Unlock()
		return nil, status.Error(codes.InvalidArgument, "task submission via RequestTask is not allowed; use SubmitTask")
	}

	worker, exists := s.workers[workerID]
	if !exists {
		s.mu.Unlock()
		return nil, status.Errorf(codes.NotFound, "Worker not registered: %s", workerID)
	}

	if len(req.AvailableGpuIds) > 0 {
		availableByID := make(map[string]struct{}, len(req.AvailableGpuIds))
		for _, gpuID := range req.AvailableGpuIds {
			availableByID[gpuID] = struct{}{}
		}
		for _, gpu := range worker.GPUDevices {
			_, available := availableByID[gpu.ID]
			gpu.Available = available
		}
	}

	task := s.taskQueue.Dequeue(worker)
	if task == nil {
		s.mu.Unlock()
		return nil, status.Error(codes.NotFound, "No pending tasks available")
	}

	if task.Status != pb.TaskStatus_PENDING {
		s.taskQueue.Enqueue(task)
		s.mu.Unlock()
		return nil, status.Error(codes.FailedPrecondition, "task is not in PENDING state")
	}

	assignedGPUIDs := selectGPUIDsForTask(worker, task)
	if len(assignedGPUIDs) < int(task.RequiredGPUs) {
		s.taskQueue.Enqueue(task)
		s.mu.Unlock()
		return nil, status.Error(codes.ResourceExhausted, "No suitable tasks for this worker")
	}

	if err := ValidateTransition(task.Status.String(), pb.TaskStatus_RUNNING.String()); err != nil {
		s.taskQueue.Enqueue(task)
		s.mu.Unlock()
		return nil, status.Errorf(codes.FailedPrecondition, "invalid task state transition: %v", err)
	}
	task.Status = pb.TaskStatus_RUNNING
	task.WorkerID = workerID
	task.AssignedGPUs = assignedGPUIDs
	task.StartTime = time.Now()
	s.grantLeaseLocked(task, workerID)

	worker.Status = pb.WorkerStatus_BUSY
	s.recordWorkerStatusChange()
	worker.Tasks[task.ID] = task

	for _, gpu := range worker.GPUDevices {
		for _, id := range assignedGPUIDs {
			if gpu.ID == id {
				gpu.Available = false
			}
		}
	}

	pbTask := &pb.Task{
		Id:            task.ID,
		Name:          task.Name,
		RequiredGpus:  task.RequiredGPUs,
		MinGpuMemory:  task.MinGPUMemory,
		Configuration: task.Configuration,
		Status:        task.Status,
	}

	s.logger.Info("Task assigned to worker", map[string]interface{}{
		"task_id":   task.ID,
		"worker_id": workerID,
	})

	s.updatePersistentStateLocked()
	metricsSnapshot := s.metricsSnapshotLocked()
	s.mu.Unlock()
	s.updateMetrics(metricsSnapshot.activeTasks, metricsSnapshot.pendingTasks, metricsSnapshot.activeWorkers)

	return pbTask, nil
}

func selectGPUIDsForTask(worker *Worker, task *Task) []string {
	if worker == nil || task == nil {
		return nil
	}

	assigned := make([]string, 0, task.RequiredGPUs)
	for _, gpu := range worker.GPUDevices {
		if gpu.Available && gpu.MemoryMB >= task.MinGPUMemory {
			assigned = append(assigned, gpu.ID)
			if len(assigned) == int(task.RequiredGPUs) {
				break
			}
		}
	}

	return assigned
}

func (s *Scheduler) SubmitTask(ctx context.Context, req *pb.SubmitTaskRequest) (*pb.SubmitTaskResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "submit task request is required")
	}
	exec := req.GetExec()
	if exec == nil {
		return nil, status.Error(codes.InvalidArgument, "exec is required")
	}
	if strings.TrimSpace(exec.GetImage()) == "" {
		return nil, status.Error(codes.InvalidArgument, "image is required")
	}
	if len(exec.GetCommand()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "command must contain at least one argument")
	}
	if req.RequiredGpus <= 0 {
		return nil, status.Error(codes.InvalidArgument, "required_gpus must be greater than 0")
	}
	if req.Priority < 0 {
		return nil, status.Error(codes.InvalidArgument, "priority must be non-negative")
	}

	taskID := fmt.Sprintf("task-%d", time.Now().UnixNano())

	// Keep the original submit payload in configuration for worker execution.
	configBytes, err := json.Marshal(map[string]interface{}{
		"image":           exec.GetImage(),
		"command":         exec.GetCommand(),
		"priority":        req.Priority,
		"idempotency_key": req.IdempotencyKey,
	})
	if err != nil {
		s.logger.Error("Failed to marshal submitted task configuration", map[string]interface{}{
			"error": err.Error(),
		})
		return nil, status.Error(codes.Internal, "failed to serialize task configuration")
	}

	task := &Task{
		ID:            taskID,
		Name:          exec.GetImage(),
		RequiredGPUs:  uint32(req.RequiredGpus),
		MinGPUMemory:  0,
		Configuration: configBytes,
		Status:        pb.TaskStatus_PENDING,
	}

	s.logger.Info("Received task submission", map[string]interface{}{
		"task_id":         taskID,
		"image":           exec.GetImage(),
		"required_gpus":   req.RequiredGpus,
		"priority":        req.Priority,
		"idempotency_key": req.IdempotencyKey,
	})

	s.AddTask(task)

	return &pb.SubmitTaskResponse{
		TaskId: task.ID,
		Status: task.Status.String(),
	}, nil
}

func (s *Scheduler) ReportTaskStatus(ctx context.Context, update *pb.TaskStatusUpdate) (*pb.TaskStatusResponse, error) {
	s.mu.Lock()

	if update == nil {
		s.mu.Unlock()
		return nil, status.Error(codes.InvalidArgument, "status update is required")
	}

	taskID := update.TaskId
	reportingWorkerID := strings.TrimSpace(update.WorkerId)
	if reportingWorkerID == "" {
		s.mu.Unlock()
		return nil, status.Error(codes.InvalidArgument, "worker_id is required")
	}

	task, exists := s.tasks[taskID]
	if !exists {
		s.mu.Unlock()
		return nil, status.Errorf(codes.NotFound, "Task not found: %s", taskID)
	}

	worker, exists := s.workers[reportingWorkerID]
	if !exists {
		s.mu.Unlock()
		return nil, status.Errorf(codes.NotFound, "Worker not found: %s", reportingWorkerID)
	}

	leaseHolder := strings.TrimSpace(task.LeaseOwner)
	if leaseHolder == "" {
		leaseHolder = strings.TrimSpace(task.WorkerID)
	}
	if leaseHolder == "" {
		s.mu.Unlock()
		return nil, status.Errorf(codes.FailedPrecondition, "task %s has no lease owner", taskID)
	}
	if reportingWorkerID != leaseHolder {
		s.mu.Unlock()
		return nil, status.Errorf(codes.PermissionDenied, "worker %q is not the lease holder for task %s", reportingWorkerID, taskID)
	}

	if err := ValidateTransition(task.Status.String(), update.Status.String()); err != nil {
		s.mu.Unlock()
		return nil, status.Errorf(codes.FailedPrecondition, "invalid task state transition: %v", err)
	}
	task.Status = update.Status
	task.Progress = update.Progress
	task.Metrics = update.Metrics

	if task.Status == pb.TaskStatus_RUNNING {
		s.grantLeaseLocked(task, reportingWorkerID)
	} else {
		task.LeaseOwner = ""
		task.LeaseExpiresAt = time.Time{}
	}

	if update.Status == pb.TaskStatus_COMPLETED || update.Status == pb.TaskStatus_FAILED {
		s.recordTaskCompletion(task)
		for _, gpu := range worker.GPUDevices {
			for _, id := range task.AssignedGPUs {
				if gpu.ID == id {
					gpu.Available = true
				}
			}
		}

		delete(worker.Tasks, taskID)

		if len(worker.Tasks) == 0 {
			worker.Status = pb.WorkerStatus_IDLE
			s.recordWorkerStatusChange()
		}

		s.assignPendingTasks()
	}

	s.logger.Info("Task status updated", map[string]interface{}{
		"task_id":  taskID,
		"status":   update.Status.String(),
		"progress": update.Progress * 100,
	})

	s.updatePersistentStateLocked()
	metricsSnapshot := s.metricsSnapshotLocked()
	s.mu.Unlock()
	s.updateMetrics(metricsSnapshot.activeTasks, metricsSnapshot.pendingTasks, metricsSnapshot.activeWorkers)

	return &pb.TaskStatusResponse{
		Acknowledged: true,
		Message:      "Status update received",
	}, nil
}

func (s *Scheduler) Heartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}

	workerID := strings.TrimSpace(req.WorkerId)
	if workerID == "" {
		return nil, status.Error(codes.InvalidArgument, "worker_id is required")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	worker, exists := s.workers[workerID]
	if !exists {
		return nil, status.Errorf(codes.NotFound, "unknown worker: %s", workerID)
	}

	now := time.Now()
	worker.LastHeartbeat = now
	worker.RunningTasks = int(req.RunningTasks)

	for _, t := range worker.Tasks {
		if t != nil && t.Status == pb.TaskStatus_RUNNING {
			s.grantLeaseLocked(t, workerID)
		}
	}

	s.logger.Info("Worker heartbeat updated", map[string]interface{}{
		"worker_id":      workerID,
		"running_tasks":  worker.RunningTasks,
		"last_heartbeat": now,
	})

	return &pb.HeartbeatResponse{Ok: true}, nil
}

func (s *Scheduler) MonitorWorker(req *pb.WorkerStatusRequest, stream pb.TrainingScheduler_MonitorWorkerServer) error {
	workerID := req.WorkerId

	s.mu.RLock()
	_, exists := s.workers[workerID]
	s.mu.RUnlock()

	if !exists {
		return status.Errorf(codes.NotFound, "Worker not found: %s", workerID)
	}

	for {
		s.mu.RLock()
		worker, stillExists := s.workers[workerID]
		if !stillExists {
			s.mu.RUnlock()
			return status.Errorf(codes.NotFound, "Worker no longer exists: %s", workerID)
		}

		pbGPUs := make([]*pb.GPU, 0, len(worker.GPUDevices))
		for _, gpu := range worker.GPUDevices {
			pbGPUs = append(pbGPUs, &pb.GPU{
				Id:        gpu.ID,
				Model:     gpu.Model,
				MemoryMb:  gpu.MemoryMB,
				Available: gpu.Available,
			})
		}

		activeTasks := make([]*pb.TaskSummary, 0, len(worker.Tasks))
		for _, task := range worker.Tasks {
			activeTasks = append(activeTasks, &pb.TaskSummary{
				Id:       task.ID,
				Name:     task.Name,
				Status:   task.Status,
				Progress: task.Progress,
			})
		}

		response := &pb.WorkerStatusResponse{
			WorkerId:    workerID,
			Status:      worker.Status,
			Gpus:        pbGPUs,
			ActiveTasks: activeTasks,
			Timestamp:   uint64(time.Now().Unix()),
		}
		s.mu.RUnlock()

		if err := stream.Send(response); err != nil {
			return err
		}

		time.Sleep(1 * time.Second)
	}
}

func (s *Scheduler) AddTask(task *Task) {
	s.mu.Lock()

	s.tasks[task.ID] = task
	s.taskQueue.Enqueue(task)

	s.logger.Info("New task added", map[string]interface{}{
		"task_id":       task.ID,
		"required_gpus": task.RequiredGPUs,
	})
	s.recordTaskSubmission()

	s.updatePersistentStateLocked()

	s.assignPendingTasks()
	metricsSnapshot := s.metricsSnapshotLocked()
	s.mu.Unlock()
	s.updateMetrics(metricsSnapshot.activeTasks, metricsSnapshot.pendingTasks, metricsSnapshot.activeWorkers)
}

func (s *Scheduler) assignPendingTasks() {
	tasksAssigned := false

	pendingCount := s.taskQueue.Len()
	for i := 0; i < pendingCount; i++ {
		task := s.taskQueue.Dequeue(nil)
		if task == nil {
			break
		}

		workerID, gpuIDs, err := s.workloadPolicy.AssignTask(s.workers, task)
		if err != nil {
			s.taskQueue.Enqueue(task)
			continue
		}

		if err := ValidateTransition(task.Status.String(), pb.TaskStatus_RUNNING.String()); err != nil {
			s.taskQueue.Enqueue(task)
			s.logger.Warn("Skipped task assignment due to invalid state transition", map[string]interface{}{
				"task_id": task.ID,
				"error":   err.Error(),
			})
			continue
		}
		task.Status = pb.TaskStatus_RUNNING
		task.WorkerID = workerID
		task.AssignedGPUs = gpuIDs
		task.StartTime = time.Now()
		s.grantLeaseLocked(task, workerID)

		worker := s.workers[workerID]
		worker.Status = pb.WorkerStatus_BUSY
		worker.Tasks[task.ID] = task

		for _, gpu := range worker.GPUDevices {
			for _, id := range gpuIDs {
				if gpu.ID == id {
					gpu.Available = false
				}
			}
		}

		s.logger.Info("Task assigned to worker", map[string]interface{}{
			"task_id":   task.ID,
			"worker_id": workerID,
		})

		tasksAssigned = true
	}

	if tasksAssigned {
		s.updatePersistentStateLocked()
	}
}

func (s *Scheduler) ListWorkers(ctx context.Context, req *pb.ListWorkersRequest) (*pb.ListWorkersResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	response := &pb.ListWorkersResponse{
		Workers: make([]*pb.WorkerInfo, 0, len(s.workers)),
	}

	for id, worker := range s.workers {
		response.Workers = append(response.Workers, &pb.WorkerInfo{
			WorkerId: id,
			Status:   worker.Status,
			Address:  worker.Address,
			GpuCount: uint32(worker.GPUs),
		})
	}

	return response, nil
}

func (s *Scheduler) UpdatePersistentState() {
	s.mu.RLock()
	defer s.mu.RUnlock()

	s.updatePersistentStateLocked()
}

func (s *Scheduler) updatePersistentStateLocked() {
	if s.persistenceManager != nil {
		s.persistenceManager.TriggerSave()
	}
}
