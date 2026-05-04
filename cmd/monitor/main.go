package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/training-scheduler/pkg/security"
	pb "github.com/training-scheduler/proto"
	"google.golang.org/grpc"
)

func main() {
	schedulerAddr := flag.String("scheduler", "localhost:50051", "Address of the scheduler server")
	workerID := flag.String("worker", "", "Worker ID to monitor (leave empty to monitor all)")
	refreshRate := flag.Int("refresh", 1, "Refresh rate in seconds")
	tlsCert := flag.String("tls-cert", "", "Path to client TLS certificate (PEM) for mTLS")
	tlsKey := flag.String("tls-key", "", "Path to client TLS private key (PEM) for mTLS")
	tlsCA := flag.String("tls-ca", "", "Path to CA bundle (PEM) to verify the scheduler; enables TLS when set (optionally with --tls-cert and --tls-key for mTLS)")
	flag.Parse()

	dialOpts, err := security.ClientGRPCDialOptions(*tlsCert, *tlsKey, *tlsCA)
	if err != nil {
		log.Fatalf("TLS: %v", err)
	}
	conn, err := grpc.NewClient(*schedulerAddr, dialOpts...)
	if err != nil {
		log.Fatalf("Failed to connect to scheduler: %v", err)
	}
	defer conn.Close()

	client := pb.NewTrainingSchedulerClient(conn)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		cancel()
		os.Exit(0)
	}()

	if *workerID != "" {
		monitorWorker(ctx, client, *workerID, *refreshRate)
	} else {
		monitorAllWorkers(ctx, client, *refreshRate)
	}
}

func monitorWorker(ctx context.Context, client pb.TrainingSchedulerClient, workerID string, refreshRate int) {
	req := &pb.WorkerStatusRequest{
		WorkerId: workerID,
	}

	stream, err := client.MonitorWorker(ctx, req)
	if err != nil {
		log.Fatalf("Failed to start monitoring: %v", err)
	}

	fmt.Printf("Monitoring worker: %s\n\n", workerID)

	for {
		resp, err := stream.Recv()
		if err != nil {
			log.Fatalf("Error receiving status update: %v", err)
		}

		fmt.Print("\033[H\033[2J")

		displayWorkerStatus(resp)

		time.Sleep(time.Duration(refreshRate) * time.Second)
	}
}

func displayWorkerStatus(status *pb.WorkerStatusResponse) {
	fmt.Printf("Worker ID: %s\n", status.WorkerId)
	fmt.Printf("Status: %s\n", getWorkerStatusString(status.Status))
	fmt.Printf("Timestamp: %s\n\n", time.Unix(int64(status.Timestamp), 0).Format(time.RFC3339))

	fmt.Println("GPUs:")
	fmt.Println("-----")
	for i, gpu := range status.Gpus {
		fmt.Printf("GPU %d:\n", i+1)
		fmt.Printf("  ID: %s\n", gpu.Id)
		fmt.Printf("  Model: %s\n", gpu.Model)
		fmt.Printf("  Memory: %d MB\n", gpu.MemoryMb)
		fmt.Printf("  Available: %t\n\n", gpu.Available)
	}

	fmt.Println("Active Tasks:")
	fmt.Println("------------")
	if len(status.ActiveTasks) == 0 {
		fmt.Println("No active tasks")
	} else {
		for i, task := range status.ActiveTasks {
			fmt.Printf("Task %d:\n", i+1)
			fmt.Printf("  ID: %s\n", task.Id)
			fmt.Printf("  Name: %s\n", task.Name)
			fmt.Printf("  Status: %s\n", getTaskStatusString(task.Status))
			fmt.Printf("  Progress: %.2f%%\n\n", task.Progress*100)
		}
	}
}

func getWorkerStatusString(status pb.WorkerStatus) string {
	switch status {
	case pb.WorkerStatus_IDLE:
		return "IDLE"
	case pb.WorkerStatus_BUSY:
		return "BUSY"
	case pb.WorkerStatus_OFFLINE:
		return "OFFLINE"
	case pb.WorkerStatus_ERROR:
		return "ERROR"
	default:
		return "UNKNOWN"
	}
}

func getTaskStatusString(status pb.TaskStatus) string {
	switch status {
	case pb.TaskStatus_PENDING:
		return "PENDING"
	case pb.TaskStatus_RUNNING:
		return "RUNNING"
	case pb.TaskStatus_COMPLETED:
		return "COMPLETED"
	case pb.TaskStatus_FAILED:
		return "FAILED"
	case pb.TaskStatus_CANCELED:
		return "CANCELED"
	default:
		return "UNKNOWN"
	}
}

func monitorAllWorkers(ctx context.Context, client pb.TrainingSchedulerClient, refreshRate int) {
	fmt.Println("Monitoring all workers...")

	for {
		resp, err := client.ListWorkers(ctx, &pb.ListWorkersRequest{})
		if err != nil {
			log.Fatalf("Failed to list workers: %v", err)
		}

		fmt.Print("\033[H\033[2J")
		fmt.Println("All Workers Status:")
		fmt.Println("------------------")
		fmt.Printf("Total Workers: %d\n\n", len(resp.Workers))

		fmt.Printf("%-20s %-10s %-20s %-10s\n", "WORKER ID", "STATUS", "ADDRESS", "GPU COUNT")
		fmt.Println("--------------------------------------------------------------------")

		if len(resp.Workers) == 0 {
			fmt.Println("No workers registered")
		} else {
			for _, worker := range resp.Workers {
				fmt.Printf("%-20s %-10s %-20s %-10d\n",
					worker.WorkerId,
					getWorkerStatusString(worker.Status),
					worker.Address,
					worker.GpuCount)
			}
		}

		fmt.Println("\nPress Ctrl+C to exit")
		time.Sleep(time.Duration(refreshRate) * time.Second)
	}
}
