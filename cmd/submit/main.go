package main

import (
	"context"
	"flag"
	"log"
	"strings"
	"time"

	"github.com/training-scheduler/pkg/security"
	pb "github.com/training-scheduler/proto"
	"google.golang.org/grpc"
)

func main() {
	schedulerAddr := flag.String("scheduler", "localhost:50051", "Address of the scheduler server")
	image := flag.String("image", "", "Container image to run")
	command := flag.String("command", "", "Command to execute (for example: \"python train.py\")")
	requiredGPUs := flag.Uint("gpus", 1, "Number of GPUs required for the task")
	priority := flag.Int("priority", 0, "Task priority (higher means more important)")
	idempotencyKey := flag.String("idempotency-key", "", "Optional idempotency key for duplicate submission handling")
	tlsCert := flag.String("tls-cert", "", "Path to client TLS certificate (PEM) for mTLS")
	tlsKey := flag.String("tls-key", "", "Path to client TLS private key (PEM) for mTLS")
	tlsCA := flag.String("tls-ca", "", "Path to CA bundle (PEM) to verify the scheduler; enables TLS when set (optionally with --tls-cert and --tls-key for mTLS)")
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

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	req := &pb.SubmitTaskRequest{
		Exec: &pb.ExecutionSpec{
			Image:   *image,
			Command: strings.Fields(*command),
		},
		RequiredGpus:   int32(*requiredGPUs),
		Priority:       int32(*priority),
		IdempotencyKey: *idempotencyKey,
	}

	resp, err := client.SubmitTask(ctx, req)
	if err != nil {
		log.Fatalf("Failed to submit task: %v", err)
	}

	log.Printf("Task submitted successfully")
	log.Printf("  Task ID: %s", resp.TaskId)
	log.Printf("  Status: %s", resp.Status)
}
