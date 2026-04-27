package main

import (
	"context"
	"flag"
	"log"
	"strings"
	"time"

	pb "github.com/training-scheduler/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	schedulerAddr := flag.String("scheduler", "localhost:50051", "Address of the scheduler server")
	image := flag.String("image", "", "Container image to run")
	command := flag.String("command", "", "Command to execute (for example: \"python train.py\")")
	requiredGPUs := flag.Uint("gpus", 1, "Number of GPUs required for the task")
	priority := flag.Int("priority", 0, "Task priority (higher means more important)")
	flag.Parse()

	if strings.TrimSpace(*image) == "" {
		log.Fatal("--image is required")
	}
	if strings.TrimSpace(*command) == "" {
		log.Fatal("--command is required")
	}
	if *requiredGPUs == 0 {
		log.Fatal("--gpus must be greater than 0")
	}
	if *priority < 0 {
		log.Fatal("--priority must be non-negative")
	}

	conn, err := grpc.NewClient(*schedulerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect to scheduler: %v", err)
	}
	defer conn.Close()

	client := pb.NewTrainingSchedulerClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	req := &pb.SubmitTaskRequest{
		Image:        *image,
		Command:      strings.Fields(*command),
		RequiredGpus: int32(*requiredGPUs),
		Priority:     int32(*priority),
	}

	resp, err := client.SubmitTask(ctx, req)
	if err != nil {
		log.Fatalf("Failed to submit task: %v", err)
	}

	log.Printf("Task submitted successfully")
	log.Printf("  Task ID: %s", resp.TaskId)
	log.Printf("  Status: %s", resp.Status)
}
