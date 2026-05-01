package scheduler

import "fmt"

const (
	TaskPending   = "PENDING"
	TaskRunning   = "RUNNING"
	TaskCompleted = "COMPLETED"
	TaskFailed    = "FAILED"
)

func ValidateTransition(oldState, newState string) error {
	if oldState == newState {
		return nil
	}

	allowedTransitions := map[string]map[string]bool{
		TaskPending: {
			TaskRunning: true,
		},
		TaskRunning: {
			TaskCompleted: true,
			TaskFailed:    true,
			TaskPending:   true, // lease reclaim / failure detector
		},
	}

	nextStates, knownState := allowedTransitions[oldState]
	if !knownState {
		return fmt.Errorf("invalid current task state: %s", oldState)
	}
	if !nextStates[newState] {
		return fmt.Errorf("invalid task transition: %s -> %s", oldState, newState)
	}

	return nil
}
