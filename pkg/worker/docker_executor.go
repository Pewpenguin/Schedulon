package worker

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"

	pb "github.com/training-scheduler/proto"
)

const (
	defaultTaskLogRoot      = "/var/lib/scheduler-worker/tasks"
	defaultLogFileName      = "logs.txt"
	defaultTimeoutExitCode  = -1
)

// DockerExecutor runs task workloads through the docker CLI.
// It intentionally uses os/exec for simplicity.
type DockerExecutor struct {
	TaskLogRoot string
}

func NewDockerExecutor() *DockerExecutor {
	return &DockerExecutor{
		TaskLogRoot: defaultTaskLogRoot,
	}
}

// Run executes a task using "docker run" and records logs to disk.
func (e *DockerExecutor) Run(task *Task) (*ExecutionResult, error) {
	if task == nil {
		return nil, errors.New("task is nil")
	}
	if task.ExecutionSpec == nil {
		return nil, fmt.Errorf("task %s has no execution spec", task.ID)
	}
	if strings.TrimSpace(task.ExecutionSpec.Image) == "" {
		return nil, fmt.Errorf("task %s execution image is required", task.ID)
	}

	startTime := time.Now()

	taskDir := filepath.Join(e.taskLogRoot(), task.ID)
	if err := os.MkdirAll(taskDir, 0o755); err != nil {
		return nil, fmt.Errorf("create task log dir: %w", err)
	}

	logPath := filepath.Join(taskDir, defaultLogFileName)
	logFile, err := os.Create(logPath)
	if err != nil {
		return nil, fmt.Errorf("create task log file: %w", err)
	}
	defer logFile.Close()

	artifactHostPath, artifactContainerPath, mountArtifacts, err := PrepareArtifactStorage(task.ID, task.ExecutionSpec)
	if err != nil {
		return nil, err
	}

	dockerArgs := e.buildDockerArgs(task.ExecutionSpec, artifactHostPath, artifactContainerPath, mountArtifacts)

	cmdCtx := context.Background()
	var cancel context.CancelFunc
	if task.ExecutionSpec.TimeoutSeconds > 0 {
		cmdCtx, cancel = context.WithTimeout(cmdCtx, time.Duration(task.ExecutionSpec.TimeoutSeconds)*time.Second)
		defer cancel()
	}

	cmd := exec.CommandContext(cmdCtx, "docker", dockerArgs...)
	cmd.Stdout = logFile
	cmd.Stderr = logFile

	runErr := cmd.Run()
	endTime := time.Now()

	result := &ExecutionResult{
		ExitCode:     0,
		StartTime:    startTime,
		EndTime:      endTime,
		LogPath:      logPath,
		ArtifactPath: artifactHostPath,
	}

	if errors.Is(cmdCtx.Err(), context.DeadlineExceeded) {
		result.ExitCode = defaultTimeoutExitCode
		return result, fmt.Errorf("docker execution timed out after %d seconds", task.ExecutionSpec.TimeoutSeconds)
	}

	if runErr != nil {
		var exitErr *exec.ExitError
		if errors.As(runErr, &exitErr) {
			result.ExitCode = exitErr.ExitCode()
			return result, fmt.Errorf("docker run failed with exit code %d", result.ExitCode)
		}
		result.ExitCode = defaultTimeoutExitCode
		return result, fmt.Errorf("docker run invocation failed: %w", runErr)
	}

	return result, nil
}

func (e *DockerExecutor) taskLogRoot() string {
	if strings.TrimSpace(e.TaskLogRoot) == "" {
		return defaultTaskLogRoot
	}
	return e.TaskLogRoot
}

func (e *DockerExecutor) buildDockerArgs(spec *pb.ExecutionSpec, artifactHostPath string, artifactContainerPath string, mountArtifacts bool) []string {
	args := []string{"run", "--rm"}

	if spec == nil {
		return args
	}

	envKeys := make([]string, 0, len(spec.Env))
	for key := range spec.Env {
		envKeys = append(envKeys, key)
	}
	sort.Strings(envKeys)
	for _, key := range envKeys {
		args = append(args, "-e", fmt.Sprintf("%s=%s", key, spec.Env[key]))
	}

	if mountArtifacts {
		args = append(args, "-v", fmt.Sprintf("%s:%s", artifactHostPath, artifactContainerPath))
	}

	if strings.TrimSpace(spec.WorkingDir) != "" {
		args = append(args, "-w", spec.WorkingDir)
	}

	args = append(args, spec.Image)
	args = append(args, spec.Command...)
	args = append(args, spec.Args...)

	return args
}
