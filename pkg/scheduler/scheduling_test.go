package scheduler

import (
	"context"
	"testing"
	"time"

	pb "github.com/training-scheduler/proto"
)

func TestPriorityQueue_HigherPriorityFirst(t *testing.T) {
	ctx := context.Background()
	s := NewScheduler()

	_, err := s.SubmitTask(ctx, &pb.SubmitTaskRequest{
		Exec:          &pb.ExecutionSpec{Image: "img-low", Command: []string{"sh", "-c", "true"}},
		RequiredGpus:  1,
		Priority:      1,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.SubmitTask(ctx, &pb.SubmitTaskRequest{
		Exec:          &pb.ExecutionSpec{Image: "img-high", Command: []string{"sh", "-c", "true"}},
		RequiredGpus:  1,
		Priority:      10,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.SubmitTask(ctx, &pb.SubmitTaskRequest{
		Exec:          &pb.ExecutionSpec{Image: "img-mid", Command: []string{"sh", "-c", "true"}},
		RequiredGpus:  1,
		Priority:      5,
	})
	if err != nil {
		t.Fatal(err)
	}

	wid := "w-priority"
	_, err = s.RegisterWorker(ctx, &pb.RegisterWorkerRequest{
		WorkerId: wid,
		Address:  "localhost:1",
		Gpus: []*pb.GPU{
			{Id: "g0", Model: "A100", MemoryMb: 40 * 1024, Available: true},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	t1, err := s.RequestTask(ctx, &pb.TaskRequest{WorkerId: wid, AvailableGpuIds: []string{"g0"}})
	if err != nil {
		t.Fatal(err)
	}
	if t1.GetName() != "img-high" {
		t.Fatalf("expected highest priority task first, got %q", t1.GetName())
	}

	_, _ = s.ReportTaskStatus(ctx, &pb.TaskStatusUpdate{TaskId: t1.GetId(), WorkerId: wid, Status: pb.TaskStatus_COMPLETED, Progress: 1})

	t2, err := s.RequestTask(ctx, &pb.TaskRequest{WorkerId: wid, AvailableGpuIds: []string{"g0"}})
	if err != nil {
		t.Fatal(err)
	}
	if t2.GetName() != "img-mid" {
		t.Fatalf("expected mid priority second, got %q", t2.GetName())
	}
	_, _ = s.ReportTaskStatus(ctx, &pb.TaskStatusUpdate{TaskId: t2.GetId(), WorkerId: wid, Status: pb.TaskStatus_COMPLETED, Progress: 1})

	t3, err := s.RequestTask(ctx, &pb.TaskRequest{WorkerId: wid, AvailableGpuIds: []string{"g0"}})
	if err != nil {
		t.Fatal(err)
	}
	if t3.GetName() != "img-low" {
		t.Fatalf("expected lowest priority last, got %q", t3.GetName())
	}
}

func TestRetryOnFailureThenSuccess(t *testing.T) {
	ctx := context.Background()
	s := NewScheduler()

	sub, err := s.SubmitTask(ctx, &pb.SubmitTaskRequest{
		Exec:         &pb.ExecutionSpec{Image: "retry-img", Command: []string{"sh"}},
		RequiredGpus: 1,
		Priority:     0,
		MaxRetries:   2,
	})
	if err != nil {
		t.Fatal(err)
	}
	tid := sub.TaskId

	wid := "w-retry"
	_, err = s.RegisterWorker(ctx, &pb.RegisterWorkerRequest{
		WorkerId: wid,
		Address:  "localhost:2",
		Gpus: []*pb.GPU{
			{Id: "g0", Model: "A100", MemoryMb: 40 * 1024, Available: true},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = s.RequestTask(ctx, &pb.TaskRequest{WorkerId: wid, AvailableGpuIds: []string{"g0"}})
	if err != nil {
		t.Fatal(err)
	}

	_, err = s.ReportTaskStatus(ctx, &pb.TaskStatusUpdate{
		TaskId: tid, WorkerId: wid, Status: pb.TaskStatus_FAILED, Progress: 0,
	})
	if err != nil {
		t.Fatal(err)
	}
	task := s.tasks[tid]
	if task.Status != pb.TaskStatus_PENDING {
		t.Fatalf("expected PENDING after retryable failure, got %v", task.Status)
	}
	if task.RetryCount != 1 {
		t.Fatalf("expected retry_count 1, got %d", task.RetryCount)
	}
	if task.NotBefore.IsZero() {
		t.Fatal("expected NotBefore backoff set")
	}

	task.NotBefore = time.Now().Add(-time.Minute)

	_, err = s.RequestTask(ctx, &pb.TaskRequest{WorkerId: wid, AvailableGpuIds: []string{"g0"}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.ReportTaskStatus(ctx, &pb.TaskStatusUpdate{
		TaskId: tid, WorkerId: wid, Status: pb.TaskStatus_COMPLETED, Progress: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if s.tasks[tid].Status != pb.TaskStatus_COMPLETED {
		t.Fatalf("expected COMPLETED, got %v", s.tasks[tid].Status)
	}
}

func TestRetryExhaustedMarksFailed(t *testing.T) {
	ctx := context.Background()
	s := NewScheduler()

	sub, err := s.SubmitTask(ctx, &pb.SubmitTaskRequest{
		Exec:         &pb.ExecutionSpec{Image: "fail-img", Command: []string{"sh"}},
		RequiredGpus: 1,
		MaxRetries:   0,
	})
	if err != nil {
		t.Fatal(err)
	}
	tid := sub.TaskId

	wid := "w-fail"
	_, err = s.RegisterWorker(ctx, &pb.RegisterWorkerRequest{
		WorkerId: wid,
		Gpus:     []*pb.GPU{{Id: "g0", Model: "A100", MemoryMb: 8192, Available: true}},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, _ = s.RequestTask(ctx, &pb.TaskRequest{WorkerId: wid, AvailableGpuIds: []string{"g0"}})
	_, err = s.ReportTaskStatus(ctx, &pb.TaskStatusUpdate{
		TaskId: tid, WorkerId: wid, Status: pb.TaskStatus_FAILED,
	})
	if err != nil {
		t.Fatal(err)
	}
	if s.tasks[tid].Status != pb.TaskStatus_FAILED {
		t.Fatalf("expected FAILED, got %v", s.tasks[tid].Status)
	}
}

func TestFairnessViolationOnThirdSameTenant(t *testing.T) {
	ctx := context.Background()
	s := NewScheduler()

	wid := "w-fair"
	_, err := s.RegisterWorker(ctx, &pb.RegisterWorkerRequest{
		WorkerId: wid,
		Gpus: []*pb.GPU{
			{Id: "g0", Model: "A100", MemoryMb: 8192, Available: true},
			{Id: "g1", Model: "A100", MemoryMb: 8192, Available: true},
			{Id: "g2", Model: "A100", MemoryMb: 8192, Available: true},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 3; i++ {
		_, err := s.SubmitTask(ctx, &pb.SubmitTaskRequest{
			Exec:         &pb.ExecutionSpec{Image: "job", Command: []string{"sh"}},
			RequiredGpus: 1,
			Tenant:       "tenant-a",
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	// Assign three concurrent single-GPU tasks on the same worker.
	seen := map[string]struct{}{}
	for i := 0; i < 3; i++ {
		avail := []string{"g0", "g1", "g2"}
		tk, err := s.RequestTask(ctx, &pb.TaskRequest{WorkerId: wid, AvailableGpuIds: avail})
		if err != nil {
			t.Fatalf("RequestTask %d: %v", i, err)
		}
		seen[tk.GetId()] = struct{}{}
	}
	if len(seen) != 3 {
		t.Fatalf("expected 3 distinct assignments, got %d", len(seen))
	}
}

func TestResourceFilterGpuModel(t *testing.T) {
	ctx := context.Background()
	s := NewScheduler()

	_, err := s.SubmitTask(ctx, &pb.SubmitTaskRequest{
		Exec:               &pb.ExecutionSpec{Image: "need-rtx", Command: []string{"sh"}},
		RequiredGpus:       1,
		RequiredGpuModel:   "RTX4090",
		RequiredGpuMemory:  1024,
	})
	if err != nil {
		t.Fatal(err)
	}

	wA100 := "w-a100"
	_, err = s.RegisterWorker(ctx, &pb.RegisterWorkerRequest{
		WorkerId: wA100,
		Gpus:     []*pb.GPU{{Id: "g0", Model: "A100", MemoryMb: 40 * 1024, Available: true}},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.RequestTask(ctx, &pb.TaskRequest{WorkerId: wA100, AvailableGpuIds: []string{"g0"}})
	if err == nil {
		t.Fatal("expected no schedulable task on wrong GPU model")
	}

	wRTX := "w-rtx"
	_, err = s.RegisterWorker(ctx, &pb.RegisterWorkerRequest{
		WorkerId: wRTX,
		Gpus:     []*pb.GPU{{Id: "g0", Model: "RTX4090", MemoryMb: 24 * 1024, Available: true}},
	})
	if err != nil {
		t.Fatal(err)
	}
	tk, err := s.RequestTask(ctx, &pb.TaskRequest{WorkerId: wRTX, AvailableGpuIds: []string{"g0"}})
	if err != nil {
		t.Fatal(err)
	}
	if tk.GetRequiredGpuModel() != "RTX4090" {
		t.Fatalf("proto task model: %q", tk.GetRequiredGpuModel())
	}
}

func TestRetryDelayDoubling(t *testing.T) {
	d1 := RetryDelay(1)
	d2 := RetryDelay(2)
	if d2 < d1 {
		t.Fatalf("expected backoff to grow: %v vs %v", d1, d2)
	}
}
