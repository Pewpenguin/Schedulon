package worker

import "time"

// Executor defines how a worker executes an assigned task.
// Different implementations (e.g. local process, container runtime) can satisfy this contract.
type Executor interface {
	Run(task *Task) (*ExecutionResult, error)
}

// ExecutionResult captures execution metadata returned by an Executor implementation.
type ExecutionResult struct {
	ExitCode     int
	StartTime    time.Time
	EndTime      time.Time
	LogPath      string
	ArtifactPath string
}
