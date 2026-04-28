# Distributed Training Scheduler Usage Guide

This document provides instructions for setting up and using the distributed training scheduler system.

## System Architecture

The distributed training scheduler consists of the following components:

1. **Scheduler Server**: Central coordinator that distributes ML training tasks to worker nodes
2. **Worker Clients**: Nodes with GPUs that execute training tasks
3. **Task Submission Client**: Utility for submitting training tasks to the scheduler
4. **Monitoring Client**: Utility for monitoring worker and task status

## Prerequisites

- Go 1.21 or later
- Protocol Buffers compiler (protoc)
- Go plugins for Protocol Buffers:
  - `go install google.golang.org/protobuf/cmd/protoc-gen-go@latest`
  - `go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest`

## Building the System

To build all components of the system, run:

```bash
make all
```

This will generate the Protocol Buffers code and build the binaries for the scheduler server, worker client, task submission utility, and monitoring utility.

## Running the System

### 1. Start the Scheduler Server

```bash
make server
# or
go run cmd/server/main.go --port=50051 --policy=balanced
```

Options:
- `--port`: Port to listen on (default: 50051)
- `--policy`: Workload distribution policy ("simple" or "balanced", default: "balanced")

### 2. Start Worker Clients

Start one or more worker clients on nodes with GPUs:

```bash
make worker
# or
go run cmd/worker/main.go --scheduler=localhost:50051 --gpus=0,1 --models=RTX3090,RTX3090 --memories=24576,24576
```

Options:
- `--scheduler`: Address of the scheduler server (default: "localhost:50051")
- `--gpus`: Comma-separated list of GPU IDs
- `--models`: Comma-separated list of GPU models
- `--memories`: Comma-separated list of GPU memory sizes in MB
- `--addr`: Address of this worker (default: "localhost:0")

### 3. Submit Training Tasks

Submit training tasks to the scheduler:

```bash
make submit
# or
go run cmd/submit/main.go --scheduler=localhost:50051 --name="ResNet50 Training" --gpus=2 --memory=8192
# or using a configuration file
go run cmd/submit/main.go --scheduler=localhost:50051 --config=examples/sample_task.json
```

Example `examples/sample_task.json`:

```json
{
  "exec": {
    "image": "python:3.10",
    "command": ["python"],
    "args": ["-c", "print('training simulation')"]
  },
  "required_gpus": 0,
  "priority": 1
}
```

Options:
- `--scheduler`: Address of the scheduler server (default: "localhost:50051")
- `--name`: Name of the training task
- `--gpus`: Number of GPUs required for the task (default: 1)
- `--memory`: Minimum GPU memory required in MB (default: 4096)
- `--config`: Path to task configuration file (JSON)

### 4. Monitor Workers and Tasks

Monitor the status of workers and tasks:

```bash
make monitor
# or
go run cmd/monitor/main.go --scheduler=localhost:50051 --worker=worker-1 --refresh=1
```

Options:
- `--scheduler`: Address of the scheduler server (default: "localhost:50051")
- `--worker`: Worker ID to monitor
- `--refresh`: Refresh rate in seconds (default: 1)

## Task Configuration

Training tasks can be configured using JSON files. For container-based execution, define an `exec` block:

```json
{
  "exec": {
    "image": "python:3.10",
    "command": ["python"],
    "args": ["-c", "print('training simulation')"]
  },
  "required_gpus": 0,
  "priority": 1
}
```

## Workload Distribution Policies

The scheduler supports two workload distribution policies:

1. **Simple Policy**: Assigns tasks to workers based on GPU availability and memory requirements, optimizing for minimal resource waste.

2. **Balanced Policy**: Distributes tasks across workers to balance the load, considering the number of active tasks and available GPUs on each worker.

## Extending the System

### Adding Custom Workload Policies

To implement a custom workload distribution policy, create a new type that implements the `WorkloadPolicy` interface in `pkg/scheduler/policy.go`:

```go
type WorkloadPolicy interface {
    AssignTask(workers map[string]*Worker, task *Task) (string, []string, error)
}
```

### Implementing Real Training Tasks

The current implementation simulates task execution. To implement real training tasks, modify the `executeTask` method in `pkg/worker/worker.go` to execute actual ML training code.