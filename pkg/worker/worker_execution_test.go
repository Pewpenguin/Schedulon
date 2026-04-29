package worker

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	pb "github.com/training-scheduler/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type testSchedulerServer struct {
	pb.UnimplementedTrainingSchedulerServer

	mu sync.Mutex

	task             *pb.Task
	workerID         string
	finalStatus      pb.TaskStatus
	finalMessage     string
	finalTaskMetrics []*pb.Metric
	finalUpdateSeen  chan struct{}
}

func newTestSchedulerServer(task *pb.Task) *testSchedulerServer {
	return &testSchedulerServer{
		task:            task,
		finalStatus:     pb.TaskStatus_PENDING,
		finalUpdateSeen: make(chan struct{}),
	}
}

func (s *testSchedulerServer) RegisterWorker(_ context.Context, req *pb.RegisterWorkerRequest) (*pb.RegisterWorkerResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.workerID = req.WorkerId
	if s.workerID == "" {
		s.workerID = "test-worker-1"
	}
	return &pb.RegisterWorkerResponse{
		Success:    true,
		Message:    "ok",
		AssignedId: s.workerID,
	}, nil
}

func (s *testSchedulerServer) SubmitTask(context.Context, *pb.SubmitTaskRequest) (*pb.SubmitTaskResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not used in this test")
}

func (s *testSchedulerServer) RequestTask(_ context.Context, req *pb.TaskRequest) (*pb.Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.workerID != "" && req.WorkerId != s.workerID {
		return nil, status.Error(codes.NotFound, "worker mismatch")
	}
	if s.task == nil {
		return nil, status.Error(codes.NotFound, "no tasks available")
	}
	task := s.task
	s.task = nil
	return task, nil
}

func (s *testSchedulerServer) ReportTaskStatus(_ context.Context, update *pb.TaskStatusUpdate) (*pb.TaskStatusResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.finalStatus = update.Status
	s.finalMessage = update.Message
	s.finalTaskMetrics = update.Metrics
	if update.Status == pb.TaskStatus_COMPLETED || update.Status == pb.TaskStatus_FAILED {
		select {
		case <-s.finalUpdateSeen:
		default:
			close(s.finalUpdateSeen)
		}
	}
	return &pb.TaskStatusResponse{Acknowledged: true, Message: "ok"}, nil
}

func (s *testSchedulerServer) MonitorWorker(req *pb.WorkerStatusRequest, stream pb.TrainingScheduler_MonitorWorkerServer) error {
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()

	for {
		resp := &pb.WorkerStatusResponse{
			WorkerId: req.WorkerId,
			Status:   pb.WorkerStatus_IDLE,
			Gpus:     nil,
		}
		if err := stream.Send(resp); err != nil {
			return err
		}

		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		case <-ticker.C:
		}
	}
}

func (s *testSchedulerServer) ListWorkers(context.Context, *pb.ListWorkersRequest) (*pb.ListWorkersResponse, error) {
	return &pb.ListWorkersResponse{}, nil
}

func TestWorkerExecution_DockerRun_CompletesAndWritesLogs(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("docker execution integration test is skipped on windows")
	}
	if _, err := os.Stat("/var/run/docker.sock"); err != nil {
		t.Skip("docker socket is not available")
	}

	taskID := "integration-task-" + strings.ReplaceAll(time.Now().Format("150405.000000"), ".", "")
	execSpec := &pb.ExecutionSpec{
		Image: "python:3.10",
		Command: []string{
			"python",
		},
		Args: []string{
			"-c",
			"print('worker integration execution')",
		},
		TimeoutSeconds: 120,
	}

	configBytes, err := json.Marshal(execSpec)
	if err != nil {
		t.Fatalf("marshal execution spec: %v", err)
	}

	server := grpc.NewServer()
	mock := newTestSchedulerServer(&pb.Task{
		Id:            taskID,
		Name:          "worker-exec-test",
		RequiredGpus:  0,
		Configuration: configBytes,
		Status:        pb.TaskStatus_PENDING,
	})
	pb.RegisterTrainingSchedulerServer(server, mock)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer lis.Close()

	go func() {
		_ = server.Serve(lis)
	}()
	defer server.GracefulStop()

	w, err := NewWorker(lis.Addr().String(), []*pb.GPU{
		{
			Id:        "gpu-0",
			Model:     "test-gpu",
			MemoryMb:  8192,
			Available: true,
		},
	})
	if err != nil {
		t.Fatalf("create worker: %v", err)
	}
	defer w.Stop()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := w.Start(ctx); err != nil {
		t.Fatalf("start worker: %v", err)
	}

	select {
	case <-mock.finalUpdateSeen:
	case <-time.After(3 * time.Minute):
		t.Fatal("timed out waiting for terminal task status")
	}

	if mock.finalStatus != pb.TaskStatus_COMPLETED {
		t.Fatalf("expected final task status COMPLETED, got %s (message=%q)", mock.finalStatus.String(), mock.finalMessage)
	}

	logPath := filepath.Join("/var/lib/scheduler-worker/tasks", taskID, "logs.txt")
	if _, err := os.Stat(logPath); err != nil {
		t.Fatalf("expected logs file to exist at %s: %v", logPath, err)
	}

	logBytes, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("read logs: %v", err)
	}
	if !strings.Contains(string(logBytes), "worker integration execution") {
		t.Fatalf("expected logs to contain command output, got: %s", string(logBytes))
	}

	// Success terminal status from worker implies executor exit code == 0.
	if mock.finalStatus != pb.TaskStatus_COMPLETED {
		t.Fatal("expected successful exit code mapped to COMPLETED status")
	}

	cancel()
}

func TestWorkerExecution_DockerUnavailable_Skips(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("docker unavailable check test is skipped on windows")
	}
	_, err := os.Stat("/var/run/docker.sock")
	if err == nil {
		return
	}
	if !errors.Is(err, os.ErrNotExist) {
		t.Skip("cannot determine docker availability")
	}
	t.Skip("docker is unavailable in this environment")
}
