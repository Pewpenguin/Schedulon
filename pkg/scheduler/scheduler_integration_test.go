package scheduler

import (
	"context"
	"testing"

	pb "github.com/training-scheduler/proto"
)

type noAutoAssignPolicy struct{}

func (p *noAutoAssignPolicy) AssignTask(workers map[string]*Worker, task *Task) (string, []string, error) {
	return "", nil, context.DeadlineExceeded
}

func TestSchedulerTaskLifecycle_SubmitPollComplete(t *testing.T) {
	ctx := context.Background()
	s := NewScheduler(&noAutoAssignPolicy{})

	submitResp, err := s.SubmitTask(ctx, &pb.SubmitTaskRequest{
		Exec: &pb.ExecutionSpec{
			Image:   "pytorch/pytorch:2.2",
			Command: []string{"python", "train.py"},
		},
		RequiredGpus: 1,
		Priority:     1,
	})
	if err != nil {
		t.Fatalf("SubmitTask failed: %v", err)
	}
	if submitResp.TaskId == "" {
		t.Fatal("expected non-empty task_id")
	}

	taskID := submitResp.TaskId
	task, exists := s.tasks[taskID]
	if !exists {
		t.Fatalf("task %q not found in scheduler storage", taskID)
	}
	if task.Status != pb.TaskStatus_PENDING {
		t.Fatalf("expected task status PENDING after submit, got %s", task.Status.String())
	}

	workerID := "worker-integration-1"
	_, err = s.RegisterWorker(ctx, &pb.RegisterWorkerRequest{
		WorkerId: workerID,
		Address:  "localhost:9000",
		Gpus: []*pb.GPU{
			{
				Id:        "gpu-0",
				Model:     "A100",
				MemoryMb:  80 * 1024,
				Available: true,
			},
		},
	})
	if err != nil {
		t.Fatalf("RegisterWorker failed: %v", err)
	}

	assignedTask, err := s.RequestTask(ctx, &pb.TaskRequest{
		WorkerId:        workerID,
		AvailableGpuIds: []string{"gpu-0"},
	})
	if err != nil {
		t.Fatalf("RequestTask failed: %v", err)
	}
	if assignedTask.GetId() != taskID {
		t.Fatalf("expected assigned task %q, got %q", taskID, assignedTask.GetId())
	}

	if s.tasks[taskID].Status != pb.TaskStatus_RUNNING {
		t.Fatalf("expected task status RUNNING after polling, got %s", s.tasks[taskID].Status.String())
	}

	_, err = s.ReportTaskStatus(ctx, &pb.TaskStatusUpdate{
		TaskId:   taskID,
		WorkerId: workerID,
		Status:   pb.TaskStatus_COMPLETED,
		Progress: 1.0,
		Message:  "done",
	})
	if err != nil {
		t.Fatalf("ReportTaskStatus failed: %v", err)
	}

	if s.tasks[taskID].Status != pb.TaskStatus_COMPLETED {
		t.Fatalf("expected task status COMPLETED after report, got %s", s.tasks[taskID].Status.String())
	}
}
