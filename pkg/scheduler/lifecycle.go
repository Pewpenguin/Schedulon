package scheduler

import "fmt"

const (
	TaskPending   = "PENDING"
	TaskRunning   = "RUNNING"
	TaskCompleted = "COMPLETED"
	TaskFailed    = "FAILED"
	TaskCanceled  = "CANCELED"
)

func ValidateTransition(oldState, newState string) error {
	if oldState == newState {
		return nil
	}

	switch oldState {
	case TaskPending:
		if newState != TaskRunning {
			return fmt.Errorf("invalid task transition: %s -> %s", oldState, newState)
		}
	case TaskRunning:
		switch newState {
		case TaskCompleted, TaskFailed, TaskCanceled, TaskPending:

		default:
			return fmt.Errorf("invalid task transition: %s -> %s", oldState, newState)
		}
	case TaskCompleted, TaskFailed, TaskCanceled:
		return fmt.Errorf("invalid task transition: %s -> %s", oldState, newState)
	default:
		return fmt.Errorf("invalid current task state: %s", oldState)
	}

	return nil
}
