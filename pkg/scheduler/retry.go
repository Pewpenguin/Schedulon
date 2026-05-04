package scheduler

import "time"

const (
	retryBackoffMin = 500 * time.Millisecond
	retryBackoffMax = 60 * time.Second
)

// RetryDelay returns exponential backoff duration for the given 1-based retry attempt.
func RetryDelay(retryAttempt int) time.Duration {
	if retryAttempt < 1 {
		retryAttempt = 1
	}
	d := retryBackoffMin
	for i := 1; i < retryAttempt; i++ {
		next := d * 2
		if next > retryBackoffMax {
			return retryBackoffMax
		}
		d = next
	}
	if d > retryBackoffMax {
		return retryBackoffMax
	}
	return d
}
