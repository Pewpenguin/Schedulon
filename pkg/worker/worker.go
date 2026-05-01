package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/training-scheduler/pkg/logging"
	"github.com/training-scheduler/pkg/metrics"
	pb "github.com/training-scheduler/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Worker struct {
	ID          string
	Address     string
	GPUs        []*pb.GPU
	Status      pb.WorkerStatus
	Client      pb.TrainingSchedulerClient
	Conn        *grpc.ClientConn
	ActiveTasks map[string]*Task
	mu          sync.RWMutex
	Paused      bool

	MetricFrequency   time.Duration
	HeartbeatInterval time.Duration
	GPUAllocation     string
	metrics           *metrics.WorkerMetrics
	logger            *logging.Logger
	runCtx            context.Context
	runCancel         context.CancelFunc
	stopOnce          sync.Once
}

type Task struct {
	ID            string
	Name          string
	Configuration []byte
	ExecutionSpec *pb.ExecutionSpec
	Status        pb.TaskStatus
	Progress      float32
	GPUIDs        []string
	Metrics       []*pb.Metric
	DoneCh        chan struct{}
	LogPath       string
	ArtifactPath  string
	StartedAt     time.Time
	CompletedAt   time.Time
	Duration      time.Duration
}

func NewWorker(schedulerAddr string, gpus []*pb.GPU) (*Worker, error) {
	conn, err := grpc.Dial(schedulerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to scheduler: %v", err)
	}

	client := pb.NewTrainingSchedulerClient(conn)

	// Create default logger
	loggerConfig := logging.Config{
		Level:     logging.InfoLevel,
		Component: "worker",
	}
	logger, err := logging.NewLogger(loggerConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create logger: %v", err)
	}

	return &Worker{
		Address:           schedulerAddr,
		GPUs:              gpus,
		Status:            pb.WorkerStatus_IDLE,
		Client:            client,
		Conn:              conn,
		ActiveTasks:       make(map[string]*Task),
		MetricFrequency:   2 * time.Second,
		HeartbeatInterval: 10 * time.Second,
		GPUAllocation:     "packed",
		logger:            logger,
	}, nil
}

func (w *Worker) Register(ctx context.Context) error {
	req := &pb.RegisterWorkerRequest{
		WorkerId: w.ID,
		Gpus:     w.GPUs,
		Address:  w.Address,
	}

	resp, err := w.Client.RegisterWorker(ctx, req)
	if err != nil {
		return fmt.Errorf("failed to register worker: %v", err)
	}

	if !resp.Success {
		return fmt.Errorf("registration failed: %s", resp.Message)
	}

	w.ID = resp.AssignedId
	w.logger.Info("Worker registered", map[string]interface{}{"worker_id": w.ID})
	return nil
}

func (w *Worker) Start(ctx context.Context) error {
	if err := w.Register(ctx); err != nil {
		return err
	}

	w.mu.Lock()
	if w.runCancel != nil {
		w.runCancel()
	}
	w.runCtx, w.runCancel = context.WithCancel(ctx)
	w.stopOnce = sync.Once{}
	runCtx := w.runCtx
	w.mu.Unlock()

	go w.pollForTasks(runCtx)

	go w.reportStatus(runCtx)

	go w.startHeartbeatLoop()

	return nil
}

func (w *Worker) startHeartbeatLoop() {
	w.mu.RLock()
	interval := w.HeartbeatInterval
	ctx := w.runCtx
	w.mu.RUnlock()

	if interval <= 0 {
		interval = 10 * time.Second
	}
	if ctx == nil {
		return
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	sendHeartbeat := func() {
		w.mu.RLock()
		workerID := w.ID
		runningTasks := int32(len(w.ActiveTasks))
		w.mu.RUnlock()

		hbCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()

		_, err := w.Client.Heartbeat(hbCtx, &pb.HeartbeatRequest{
			WorkerId:     workerID,
			RunningTasks: runningTasks,
		})
		if err != nil {
			w.logger.Warn("Worker heartbeat failed", map[string]interface{}{
				"worker_id":     workerID,
				"running_tasks": runningTasks,
				"error":         err.Error(),
			})
			return
		}

		w.logger.Info("Worker heartbeat sent", map[string]interface{}{
			"worker_id":     workerID,
			"running_tasks": runningTasks,
		})
	}

	sendHeartbeat()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			sendHeartbeat()
		}
	}
}

func (w *Worker) pollForTasks(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			w.mu.RLock()
			if w.Paused {
				w.mu.RUnlock()
				w.logger.Info("Worker is paused, not requesting new tasks", nil)
				time.Sleep(5 * time.Second)
				continue
			}

			availableGPUIDs := make([]string, 0)
			for _, gpu := range w.GPUs {
				if gpu.Available {
					availableGPUIDs = append(availableGPUIDs, gpu.Id)
				}
			}

			w.mu.RUnlock()

			if len(availableGPUIDs) == 0 {
				time.Sleep(5 * time.Second)
				continue
			}

			req := &pb.TaskRequest{
				WorkerId:        w.ID,
				AvailableGpuIds: availableGPUIDs,
			}

			task, err := w.Client.RequestTask(ctx, req)
			if err != nil {
				time.Sleep(5 * time.Second)
				continue
			}

			w.executeTask(ctx, task)
		}
	}
}

func (w *Worker) executeTask(ctx context.Context, pbTask *pb.Task) {
	w.logger.Info("Executing task", map[string]interface{}{"task_id": pbTask.Id})

	w.mu.Lock()
	assignedGPUIDs := w.allocateGPUs(pbTask)

	task := &Task{
		ID:            pbTask.Id,
		Name:          pbTask.Name,
		Configuration: pbTask.Configuration,
		ExecutionSpec: parseExecutionSpec(pbTask.Configuration),
		Status:        pb.TaskStatus_RUNNING,
		Progress:      0.0,
		GPUIDs:        assignedGPUIDs,
		Metrics:       make([]*pb.Metric, 0),
		DoneCh:        make(chan struct{}),
		StartedAt:     time.Now(),
	}

	w.Status = pb.WorkerStatus_BUSY
	w.ActiveTasks[task.ID] = task
	w.mu.Unlock()

	w.recordTaskStart(task)
	w.reportTaskStatus(ctx, task)

	go func() {
		executor := NewDockerExecutor()
		result, err := executor.Run(task)

		w.mu.Lock()
		task.CompletedAt = time.Now()
		if result != nil {
			task.StartedAt = result.StartTime
			task.CompletedAt = result.EndTime
			task.Duration = result.EndTime.Sub(result.StartTime)
			task.LogPath = result.LogPath
			task.ArtifactPath = result.ArtifactPath
		} else {
			task.Duration = task.CompletedAt.Sub(task.StartedAt)
		}

		if err == nil && result != nil && result.ExitCode == 0 {
			task.Status = pb.TaskStatus_COMPLETED
			task.Progress = 1.0
		} else {
			task.Status = pb.TaskStatus_FAILED
		}
		task.Metrics = append(task.Metrics, &pb.Metric{
			Name:      "execution_duration_seconds",
			Value:     float32(task.Duration.Seconds()),
			Timestamp: uint64(time.Now().Unix()),
		})
		w.mu.Unlock()

		if task.Status == pb.TaskStatus_COMPLETED {
			w.recordTaskCompletion(task)
		}
		if task.Status == pb.TaskStatus_FAILED && err != nil {
			w.logger.Error("Task execution failed", map[string]interface{}{
				"task_id":       task.ID,
				"error":         err.Error(),
				"log_path":      task.LogPath,
				"artifact_path": task.ArtifactPath,
			})
		}
		w.reportTaskStatus(ctx, task)

		w.mu.Lock()
		delete(w.ActiveTasks, task.ID)

		for _, gpu := range w.GPUs {
			for _, id := range task.GPUIDs {
				if gpu.Id == id {
					gpu.Available = true
				}
			}
		}

		if len(w.ActiveTasks) == 0 {
			w.Status = pb.WorkerStatus_IDLE
		}
		w.mu.Unlock()

		close(task.DoneCh)
	}()
}

func (w *Worker) reportTaskStatus(ctx context.Context, task *Task) {
	w.mu.RLock()
	update := &pb.TaskStatusUpdate{
		TaskId:   task.ID,
		WorkerId: w.ID,
		Status:   task.Status,
		Progress: task.Progress,
		Message: fmt.Sprintf(
			"Task %s progress: %.2f%%, duration=%s, log=%s, artifact=%s",
			task.ID,
			task.Progress*100,
			task.Duration.String(),
			task.LogPath,
			task.ArtifactPath,
		),
		Metrics: task.Metrics,
	}
	w.mu.RUnlock()

	_, err := w.Client.ReportTaskStatus(ctx, update)
	if err != nil {
		w.logger.Error("Failed to report task status", map[string]interface{}{
			"task_id": task.ID,
			"error":   err.Error(),
		})
	}
}

func parseExecutionSpec(configuration []byte) *pb.ExecutionSpec {
	if len(configuration) == 0 {
		return nil
	}

	var spec pb.ExecutionSpec
	if err := json.Unmarshal(configuration, &spec); err != nil {
		return nil
	}

	return &spec
}

func (w *Worker) reportStatus(ctx context.Context) {
	var streamCancel context.CancelFunc
	var streamCtx context.Context
	var streamMutex sync.Mutex

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	establishStream := func() {
		streamMutex.Lock()
		defer streamMutex.Unlock()

		if streamCancel != nil {
			streamCancel()
		}

		streamCtx, streamCancel = context.WithCancel(ctx)

		req := &pb.WorkerStatusRequest{
			WorkerId: w.ID,
		}

		stream, err := w.Client.MonitorWorker(streamCtx, req)
		if err != nil {
			w.logger.Error("Failed to establish worker status monitoring", map[string]interface{}{
				"error": err.Error(),
			})
			return
		}

		w.logger.Info("Worker status monitoring established", map[string]interface{}{"worker_id": w.ID})

		go func() {
			defer streamCancel()

			for {
				resp, err := stream.Recv()
				if err != nil {
					w.logger.Error("Error receiving from status stream", map[string]interface{}{"error": err.Error()})
					return
				}

				w.logger.Info("Received status update from scheduler", map[string]interface{}{"worker_id": resp.WorkerId})

				w.processStatusUpdate(streamCtx, resp)

				if resp.Command != nil {
					w.handleCommand(streamCtx, resp.Command)
				}

				w.sendStatusUpdate()
			}
		}()
	}

	establishStream()

	reconnectTicker := time.NewTicker(2 * time.Minute)
	defer reconnectTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			streamMutex.Lock()
			if streamCancel != nil {
				streamCancel()
			}
			streamMutex.Unlock()
			return

		case <-reconnectTicker.C:
			establishStream()

		case <-ticker.C:
			w.sendStatusUpdate()
		}
	}
}

func (w *Worker) sendStatusUpdate() {
	w.mu.RLock()
	defer w.mu.RUnlock()

	activeTasks := make([]string, 0, len(w.ActiveTasks))
	for id, task := range w.ActiveTasks {
		activeTasks = append(activeTasks, fmt.Sprintf("%s(%.1f%%)", id, task.Progress*100))
	}

	availableGPUs := 0
	totalGPUs := len(w.GPUs)
	for _, gpu := range w.GPUs {
		if gpu.Available {
			availableGPUs++
		}
	}

	w.logger.Info("Worker status", map[string]interface{}{
		"worker_id":      w.ID,
		"status":         w.Status.String(),
		"available_gpus": availableGPUs,
		"total_gpus":     totalGPUs,
	})

	if len(activeTasks) > 0 {
		w.logger.Info("Worker active tasks", map[string]interface{}{
			"worker_id":    w.ID,
			"active_tasks": activeTasks,
		})
	}
}

func (w *Worker) handleCommand(ctx context.Context, command *pb.WorkerCommand) {
	w.logger.Info("Received command", map[string]interface{}{"command_type": command.Type})

	switch command.Type {
	case "PAUSE":
		w.mu.Lock()
		w.Paused = true
		w.logger.Info("Worker paused", map[string]interface{}{"worker_id": w.ID})
		w.mu.Unlock()
		w.recordStatusChange()

	case "RESUME":
		w.mu.Lock()
		w.Paused = false
		w.logger.Info("Worker resumed", map[string]interface{}{"worker_id": w.ID})
		w.mu.Unlock()
		w.recordStatusChange()

	case "STOP_TASK":
		taskID := command.Params["task_id"]
		if taskID != "" {
			w.stopTask(ctx, taskID)
		}

	case "UPDATE_CONFIG":
		config := command.Params["config"]
		if config != "" {
			w.logger.Info("Updating worker configuration", map[string]interface{}{"config": config})

			if metricFreq, exists := command.Params["metric_frequency"]; exists {
				w.logger.Info("Updating metric collection frequency", map[string]interface{}{"frequency": metricFreq})
				w.updateMetricFrequency(metricFreq)
			}

			if gpuAllocation, exists := command.Params["gpu_allocation"]; exists {
				w.logger.Info("Updating GPU allocation strategy", map[string]interface{}{"strategy": gpuAllocation})
				w.updateGPUAllocationStrategy(gpuAllocation)
			}

			if heartbeatInterval, exists := command.Params["heartbeat_interval"]; exists {
				w.logger.Info("Updating heartbeat interval", map[string]interface{}{"interval": heartbeatInterval})
				w.updateHeartbeatInterval(heartbeatInterval)
			}
		}

	case "SYNC_STATE":
		w.logger.Info("Synchronizing worker state with scheduler", nil)
		go w.reportStatus(ctx)

	default:
		w.logger.Warn("Unknown command type received", map[string]interface{}{"command_type": command.Type})
	}
}

func (w *Worker) processStatusUpdate(ctx context.Context, resp *pb.WorkerStatusResponse) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if len(resp.Gpus) > 0 {
		w.logger.Info("Synchronizing GPU status with scheduler's view", nil)
		schedulerGPUs := make(map[string]*pb.GPU)
		for _, gpu := range resp.Gpus {
			schedulerGPUs[gpu.Id] = gpu
		}

		for i, gpu := range w.GPUs {
			if schedulerGPU, exists := schedulerGPUs[gpu.Id]; exists {
				gpuInUse := false
				for _, task := range w.ActiveTasks {
					for _, gpuID := range task.GPUIDs {
						if gpuID == gpu.Id {
							gpuInUse = true
							break
						}
					}
					if gpuInUse {
						break
					}
				}

				if !gpuInUse {
					oldAvailable := w.GPUs[i].Available
					w.GPUs[i].Available = schedulerGPU.Available
					if oldAvailable != w.GPUs[i].Available {
						w.logger.Info("GPU availability changed", map[string]interface{}{
							"gpu_id":        gpu.Id,
							"old_available": oldAvailable,
							"new_available": w.GPUs[i].Available,
						})
					}
				}
			}
		}
	}

	if resp.Status != w.Status {
		w.logger.Info("Worker status updated", map[string]interface{}{
			"old_status": w.Status.String(),
			"new_status": resp.Status.String(),
		})
		w.Status = resp.Status
		w.recordStatusChange()
	}

	if len(resp.ActiveTasks) > 0 {
		schedulerTasks := make(map[string]*pb.TaskSummary)
		for _, task := range resp.ActiveTasks {
			schedulerTasks[task.Id] = task
		}

		for taskID, taskSummary := range schedulerTasks {
			if _, exists := w.ActiveTasks[taskID]; !exists {
				w.logger.Info("Task exists in scheduler but not in worker", map[string]interface{}{"task_id": taskID})
			} else {
				w.updateTaskPriority(taskID, taskSummary)
			}
		}

		for taskID, task := range w.ActiveTasks {
			if _, exists := schedulerTasks[taskID]; !exists {
				w.logger.Info("Task exists in worker but not in scheduler", map[string]interface{}{
					"task_id": taskID,
					"action":  "marking as canceled",
				})
				task.Status = pb.TaskStatus_CANCELED
				go func(taskID string, task *Task) {
					w.mu.Lock()
					delete(w.ActiveTasks, taskID)
					for _, gpu := range w.GPUs {
						for _, id := range task.GPUIDs {
							if gpu.Id == id {
								gpu.Available = true
							}
						}
					}
					w.mu.Unlock()
					w.reportTaskStatus(ctx, task)
				}(taskID, task)
			}
		}
	}

}

func (w *Worker) updateTaskPriority(taskID string, schedulerTask *pb.TaskSummary) {
	task, exists := w.ActiveTasks[taskID]
	if !exists {
		return
	}

	if task.Status != schedulerTask.Status {
		w.logger.Info("Task status updated from scheduler", map[string]interface{}{
			"task_id":    taskID,
			"old_status": task.Status.String(),
			"new_status": schedulerTask.Status.String(),
		})
		task.Status = schedulerTask.Status

		if schedulerTask.Status == pb.TaskStatus_COMPLETED ||
			schedulerTask.Status == pb.TaskStatus_FAILED ||
			schedulerTask.Status == pb.TaskStatus_CANCELED {
			go func(taskID string) {
				w.mu.Lock()
				delete(w.ActiveTasks, taskID)
				for _, gpu := range w.GPUs {
					for _, id := range task.GPUIDs {
						if gpu.Id == id {
							gpu.Available = true
						}
					}
				}
				w.mu.Unlock()
			}(taskID)
		}
	}

	if abs(float64(task.Progress-schedulerTask.Progress)) > 0.05 {
		w.logger.Info("Task progress synced with scheduler", map[string]interface{}{
			"task_id":      taskID,
			"old_progress": task.Progress,
			"new_progress": schedulerTask.Progress,
		})
		task.Progress = schedulerTask.Progress
	}
}

func abs(x float64) float64 {
	if x < 0 {
		return -x
	}
	return x
}

func (w *Worker) stopTask(ctx context.Context, taskID string) {
	w.mu.Lock()
	task, exists := w.ActiveTasks[taskID]
	if exists {
		task.Status = pb.TaskStatus_CANCELED
		w.logger.Info("Task stopped", map[string]interface{}{"task_id": taskID})
	}
	w.mu.Unlock()

	if exists {
		w.reportTaskStatus(ctx, task)
	}
}

func (w *Worker) Stop() {
	w.stopOnce.Do(func() {
		w.mu.Lock()
		cancel := w.runCancel
		w.runCancel = nil
		w.runCtx = nil
		w.mu.Unlock()

		if cancel != nil {
			cancel()
		}
	})

	w.mu.RLock()
	for _, task := range w.ActiveTasks {
		<-task.DoneCh
	}
	w.mu.RUnlock()

	if w.Conn != nil {
		w.Conn.Close()
	}

	w.logger.Info("Worker stopped", nil)
}

func (w *Worker) updateMetricFrequency(freqStr string) {
	w.mu.Lock()
	defer w.mu.Unlock()

	freq, err := time.ParseDuration(freqStr)
	if err != nil {
		w.logger.Error("Invalid metric frequency format", map[string]interface{}{"error": err.Error()})
		return
	}

	if freq < 100*time.Millisecond || freq > time.Minute {
		w.logger.Warn("Metric frequency out of range", map[string]interface{}{
			"frequency": freqStr,
			"min":       "100ms",
			"max":       "1m",
		})
		return
	}

	w.logger.Info("Setting metric frequency", map[string]interface{}{"frequency": freq.String()})
	w.MetricFrequency = freq

	for _, task := range w.ActiveTasks {
		w.logger.Info("Applied new metric frequency to task", map[string]interface{}{"task_id": task.ID})
	}
}

func (w *Worker) updateHeartbeatInterval(intervalStr string) {
	w.mu.Lock()
	defer w.mu.Unlock()

	interval, err := time.ParseDuration(intervalStr)
	if err != nil {
		w.logger.Error("Invalid heartbeat interval format", map[string]interface{}{"error": err.Error()})
		return
	}

	if interval < time.Second || interval > 5*time.Minute {
		w.logger.Warn("Heartbeat interval out of range", map[string]interface{}{
			"interval": intervalStr,
			"min":      "1s",
			"max":      "5m",
		})
		return
	}

	w.HeartbeatInterval = interval
	w.logger.Info("Heartbeat interval updated", map[string]interface{}{"interval": interval.String()})
}

func (w *Worker) updateGPUAllocationStrategy(strategy string) {
	w.mu.Lock()
	defer w.mu.Unlock()

	validStrategies := map[string]bool{
		"packed":      true,
		"spread":      true,
		"memory":      true,
		"performance": true,
	}

	strategy = strings.ToLower(strings.TrimSpace(strategy))
	if !validStrategies[strategy] {
		w.logger.Warn("Invalid GPU allocation strategy", map[string]interface{}{"strategy": strategy})
		return
	}

	w.logger.Info("Setting GPU allocation strategy", map[string]interface{}{"strategy": strategy})
	w.GPUAllocation = strategy
}

func (w *Worker) allocateGPUs(task *pb.Task) []string {
	assignedGPUIDs := make([]string, 0, task.RequiredGpus)
	availableGPUs := make([]*pb.GPU, 0)

	for _, gpu := range w.GPUs {
		if gpu.Available {
			availableGPUs = append(availableGPUs, gpu)
		}
	}

	if uint32(len(availableGPUs)) < task.RequiredGpus {
		w.logger.Warn("Not enough available GPUs for task", map[string]interface{}{
			"task_id":        task.Id,
			"required_gpus":  task.RequiredGpus,
			"available_gpus": len(availableGPUs),
		})
		return assignedGPUIDs
	}

	switch w.GPUAllocation {
	case "packed":
		for i := 0; i < int(task.RequiredGpus) && i < len(availableGPUs); i++ {
			availableGPUs[i].Available = false
			assignedGPUIDs = append(assignedGPUIDs, availableGPUs[i].Id)
		}

	case "spread":
		for i, gpu := range availableGPUs {
			if i%2 == 0 && uint32(len(assignedGPUIDs)) < task.RequiredGpus {
				gpu.Available = false
				assignedGPUIDs = append(assignedGPUIDs, gpu.Id)
			}
		}

		for i, gpu := range availableGPUs {
			if i%2 == 1 && uint32(len(assignedGPUIDs)) < task.RequiredGpus {
				gpu.Available = false
				assignedGPUIDs = append(assignedGPUIDs, gpu.Id)
			}
		}

	case "memory":
		sort.Slice(availableGPUs, func(i, j int) bool {
			return availableGPUs[i].MemoryMb > availableGPUs[j].MemoryMb
		})
		for i := 0; i < int(task.RequiredGpus) && i < len(availableGPUs); i++ {
			availableGPUs[i].Available = false
			assignedGPUIDs = append(assignedGPUIDs, availableGPUs[i].Id)
		}

	case "performance":
		sort.Slice(availableGPUs, func(i, j int) bool {
			if availableGPUs[i].CudaCores > 0 && availableGPUs[j].CudaCores > 0 {
				return availableGPUs[i].CudaCores > availableGPUs[j].CudaCores
			}
			return availableGPUs[i].MemoryBandwidth > availableGPUs[j].MemoryBandwidth
		})
		for i := 0; i < int(task.RequiredGpus) && i < len(availableGPUs); i++ {
			availableGPUs[i].Available = false
			assignedGPUIDs = append(assignedGPUIDs, availableGPUs[i].Id)
		}

	default:
		for i := 0; i < int(task.RequiredGpus) && i < len(availableGPUs); i++ {
			availableGPUs[i].Available = false
			assignedGPUIDs = append(assignedGPUIDs, availableGPUs[i].Id)
		}
	}

	w.logger.Info("Allocated GPUs to task", map[string]interface{}{
		"task_id":       task.Id,
		"gpu_count":     len(assignedGPUIDs),
		"strategy":      w.GPUAllocation,
		"allocated_ids": assignedGPUIDs,
	})
	return assignedGPUIDs
}
