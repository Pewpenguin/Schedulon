package scheduler

import (
	"context"
	"testing"
	"time"

	pb "github.com/training-scheduler/proto"
)

// TestWorkerFailure_LeaseExpiryReassignsAndCompletes verifies that after a task is running on a
// worker that stops renewing its lease, the failure detector requeues it and a new worker can
// complete it. Lifecycle: PENDING → RUNNING → PENDING → RUNNING → COMPLETED.
func TestWorkerFailure_LeaseExpiryReassignsAndCompletes(t *testing.T) {
	ctx := context.Background()
	s := NewScheduler(&noAutoAssignPolicy{})

	// Short lease so tests finish quickly; long worker heartbeat timeout so reclaim is lease-driven.
	s.leaseDuration = 40 * time.Millisecond
	s.workerTimeout = 24 * time.Hour

	taskID := submitFailureRecoveryTask(t, s, ctx)
	lifecycle := []pb.TaskStatus{pb.TaskStatus_PENDING}

	worker1 := "worker-failure-1"
	registerFailureRecoveryWorker(t, s, ctx, worker1)

	_, err := s.RequestTask(ctx, &pb.TaskRequest{
		WorkerId:        worker1,
		AvailableGpuIds: []string{"gpu-0"},
	})
	if err != nil {
		t.Fatalf("RequestTask (worker1): %v", err)
	}
	if s.tasks[taskID].Status != pb.TaskStatus_RUNNING {
		t.Fatalf("after first assignment: want RUNNING, got %s", s.tasks[taskID].Status.String())
	}
	lifecycle = append(lifecycle, pb.TaskStatus_RUNNING)

	// Simulate worker crash: no heartbeats / no status updates; lease will expire.
	waitForTaskStatus(t, s, taskID, pb.TaskStatus_PENDING, 3*time.Second)
	lifecycle = append(lifecycle, pb.TaskStatus_PENDING)

	worker2 := "worker-failure-2"
	registerFailureRecoveryWorker(t, s, ctx, worker2)

	got, err := s.RequestTask(ctx, &pb.TaskRequest{
		WorkerId:        worker2,
		AvailableGpuIds: []string{"gpu-0"},
	})
	if err != nil {
		t.Fatalf("RequestTask (worker2): %v", err)
	}
	if got.GetId() != taskID {
		t.Fatalf("expected same task %q reassigned, got %q", taskID, got.GetId())
	}
	if s.tasks[taskID].Status != pb.TaskStatus_RUNNING {
		t.Fatalf("after second assignment: want RUNNING, got %s", s.tasks[taskID].Status.String())
	}
	lifecycle = append(lifecycle, pb.TaskStatus_RUNNING)

	_, err = s.ReportTaskStatus(ctx, &pb.TaskStatusUpdate{
		TaskId:   taskID,
		WorkerId: worker2,
		Status:   pb.TaskStatus_COMPLETED,
		Progress: 1.0,
		Message:  "done",
	})
	if err != nil {
		t.Fatalf("ReportTaskStatus: %v", err)
	}
	lifecycle = append(lifecycle, pb.TaskStatus_COMPLETED)

	want := []pb.TaskStatus{
		pb.TaskStatus_PENDING,
		pb.TaskStatus_RUNNING,
		pb.TaskStatus_PENDING,
		pb.TaskStatus_RUNNING,
		pb.TaskStatus_COMPLETED,
	}
	if len(lifecycle) != len(want) {
		t.Fatalf("lifecycle length: got %d want %d (got %#v)", len(lifecycle), len(want), lifecycle)
	}
	for i := range want {
		if lifecycle[i] != want[i] {
			t.Fatalf("lifecycle[%d]: got %s want %s (full=%v)",
				i, lifecycle[i].String(), want[i].String(), lifecycle)
		}
	}
}

func submitFailureRecoveryTask(t *testing.T, s *Scheduler, ctx context.Context) string {
	t.Helper()
	submitResp, err := s.SubmitTask(ctx, &pb.SubmitTaskRequest{
		Exec: &pb.ExecutionSpec{
			Image:   "busybox:latest",
			Command: []string{"sh"},
		},
		RequiredGpus: 1,
		Priority:     0,
	})
	if err != nil {
		t.Fatalf("SubmitTask: %v", err)
	}
	if submitResp.TaskId == "" {
		t.Fatal("empty task_id")
	}
	if s.tasks[submitResp.TaskId].Status != pb.TaskStatus_PENDING {
		t.Fatalf("want PENDING after submit, got %s", s.tasks[submitResp.TaskId].Status.String())
	}
	return submitResp.TaskId
}

func registerFailureRecoveryWorker(t *testing.T, s *Scheduler, ctx context.Context, workerID string) {
	t.Helper()
	_, err := s.RegisterWorker(ctx, &pb.RegisterWorkerRequest{
		WorkerId: workerID,
		Address:  "127.0.0.1:0",
		Gpus: []*pb.GPU{
			{
				Id:        "gpu-0",
				Model:     "test",
				MemoryMb:  8192,
				Available: true,
			},
		},
	})
	if err != nil {
		t.Fatalf("RegisterWorker(%s): %v", workerID, err)
	}
}

func waitForTaskStatus(t *testing.T, s *Scheduler, taskID string, want pb.TaskStatus, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		s.runFailureChecks()
		if s.tasks[taskID].Status == want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for task %s status %s (last=%s)",
		taskID, want.String(), s.tasks[taskID].Status.String())
}
