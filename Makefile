# Makefile for the Distributed Training Scheduler

.PHONY: all proto build clean server worker submit monitor

all: proto build

# Generate protobuf code
proto:
	protoc \
		--go_out=proto \
		--go-grpc_out=proto \
		proto/scheduler.proto

# Build all components
build: proto
	go build -o bin/server cmd/server/main.go
	go build -o bin/worker cmd/worker/main.go
	go build -o bin/submit cmd/submit/main.go
	go build -o bin/monitor cmd/monitor/main.go

# Run the scheduler server
server:
	go run cmd/server/main.go

# Run a worker
worker:
	go run cmd/worker/main.go --gpus=0,1 --models=RTX3090,RTX3090 --memories=24576,24576

# Submit a sample task
submit:
	go run cmd/submit/main.go --name="Sample Training Task" --gpus=2 --memory=8192

# Monitor a worker
monitor:
	go run cmd/monitor/main.go --worker=worker-1

# Monitor stack
monitor-up:
	docker-compose -f monitoring/docker-compose.monitoring.yml up -d

monitor-down:
	docker-compose -f monitoring/docker-compose.monitoring.yml down

# Clean build artifacts
clean:
	rm -rf bin/
	rm -f proto/*.pb.go