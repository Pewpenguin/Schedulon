package worker

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	pb "github.com/training-scheduler/proto"
)

const defaultArtifactRoot = "/var/lib/scheduler-worker/artifacts"

// PrepareArtifactStorage creates a local artifact directory for a task when artifact output is requested.
// It returns host/container paths and whether mounting is required.
func PrepareArtifactStorage(taskID string, spec *pb.ExecutionSpec) (hostPath string, containerPath string, enabled bool, err error) {
	if spec == nil || strings.TrimSpace(spec.ArtifactPath) == "" {
		return "", "", false, nil
	}
	if strings.TrimSpace(taskID) == "" {
		return "", "", false, fmt.Errorf("task id is required for artifact storage")
	}

	hostPath = filepath.Join(defaultArtifactRoot, taskID)
	if err := os.MkdirAll(hostPath, 0o755); err != nil {
		return "", "", false, fmt.Errorf("create artifact directory: %w", err)
	}

	return hostPath, spec.ArtifactPath, true, nil
}
