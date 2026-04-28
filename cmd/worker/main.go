package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	"github.com/training-scheduler/pkg/logging"
	"github.com/training-scheduler/pkg/metrics"
	"github.com/training-scheduler/pkg/worker"
	pb "github.com/training-scheduler/proto"
)

func main() {
	schedulerAddr := flag.String("scheduler", "localhost:50051", "Address of the scheduler server")
	gpuIDs := flag.String("gpus", "", "Comma-separated list of GPU IDs")
	gpuModels := flag.String("models", "", "Comma-separated list of GPU models")
	gpuMemories := flag.String("memories", "", "Comma-separated list of GPU memory sizes in MB")
	workerAddr := flag.String("addr", "localhost:0", "Address of this worker")
	metricsPort := flag.Int("metrics-port", 9092, "The metrics server port")
	logLevel := flag.String("log-level", "info", "Log level (debug, info, warn, error)")
	logDir := flag.String("log-dir", "/var/log/worker", "Directory for log files")
	workDir := flag.String("work-dir", "/var/lib/scheduler-worker", "Worker runtime data directory")
	dockerSocket := flag.String("docker-socket", "/var/run/docker.sock", "Path to Docker socket")
	artifactDir := flag.String("artifact-dir", "/var/lib/scheduler-worker/artifacts", "Directory for task artifacts")
	flag.Parse()

	requiredDirs := []string{
		*workDir,
		filepath.Join(*workDir, "tasks"),
		*artifactDir,
	}
	for _, dir := range requiredDirs {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			log.Fatalf("Failed to create required directory %s: %v", dir, err)
		}
	}

	dockerSocketDir := filepath.Dir(*dockerSocket)
	if dockerSocketDir != "" && dockerSocketDir != "." {
		if err := os.MkdirAll(dockerSocketDir, 0o755); err != nil {
			log.Fatalf("Failed to create docker socket directory %s: %v", dockerSocketDir, err)
		}
	}

	// Initialize logger
	loggerConfig := logging.Config{
		Level:     logging.LogLevel(*logLevel),
		Component: "worker",
		LogDir:    *logDir,
		LogFile:   "worker.log",
	}

	logger, err := logging.NewLogger(loggerConfig)
	if err != nil {
		log.Fatalf("Failed to initialize logger: %v", err)
	}

	ids := strings.Split(*gpuIDs, ",")
	models := strings.Split(*gpuModels, ",")
	memories := strings.Split(*gpuMemories, ",")

	if len(ids) == 0 || ids[0] == "" {
		logger.Fatal("At least one GPU ID must be specified", nil)
	}

	if len(models) != len(ids) {
		logger.Fatal("Number of GPU models must match number of GPU IDs", nil)
	}

	if len(memories) != len(ids) {
		logger.Fatal("Number of GPU memory sizes must match number of GPU IDs", nil)
	}

	gpus := make([]*pb.GPU, 0, len(ids))
	for i, id := range ids {
		memory, parseErr := strconv.ParseUint(memories[i], 10, 64)
		if parseErr != nil {
			logger.Fatal("Invalid GPU memory size", map[string]interface{}{"memory": memories[i], "error": parseErr.Error()})
		}

		gpus = append(gpus, &pb.GPU{
			Id:        id,
			Model:     models[i],
			MemoryMb:  memory,
			Available: true,
		})
	}

	metricsServer := metrics.NewMetricsServer(fmt.Sprintf(":%d", *metricsPort))

	logger.Info("Starting metrics server", map[string]interface{}{"port": *metricsPort})
	go func() {
		if startErr := metricsServer.Start(); startErr != nil {
			logger.Error("Failed to start metrics server", map[string]interface{}{"error": startErr.Error()})
		}
	}()

	worker, initErr := worker.NewWorker(*schedulerAddr, gpus)
	if initErr != nil {
		logger.Fatal("Failed to create worker", map[string]interface{}{"error": initErr.Error()})
	}

	workerMetrics := metrics.NewWorkerMetrics(worker.ID)
	worker.SetMetrics(workerMetrics)

	worker.Address = *workerAddr

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := worker.Start(ctx); err != nil {
		logger.Fatal("Failed to start worker", map[string]interface{}{"error": err.Error()})
	}

	logger.Info("Worker started", map[string]interface{}{
		"gpu_count":      len(gpus),
		"scheduler_addr": *schedulerAddr,
		"work_dir":       *workDir,
		"docker_socket":  *dockerSocket,
		"artifact_dir":   *artifactDir,
	})

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	logger.Info("Shutting down worker", nil)
	cancel()
	worker.Stop()
	logger.Info("Worker stopped", nil)
}
