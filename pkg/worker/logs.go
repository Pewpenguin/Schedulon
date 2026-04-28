package worker

import (
	"bufio"
	"io"
	"os"
	"path/filepath"
	"sync"
)

const (
	defaultWorkerDataRoot = "/var/lib/scheduler-worker"
	defaultTaskLogsName   = "logs.txt"
)

// CreateTaskLogDir creates the standard task log directory and returns its path.
// If creation fails, an empty string is returned.
func CreateTaskLogDir(taskID string) string {
	taskDir := filepath.Join(defaultWorkerDataRoot, "tasks", taskID)
	if err := os.MkdirAll(taskDir, 0o755); err != nil {
		return ""
	}
	return taskDir
}

// WriteLogs continuously copies stdout and stderr streams into a single log file.
// The file is flushed on each write so logs are available during execution.
func WriteLogs(stdout io.Reader, stderr io.Reader, logFile string) error {
	f, err := os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()

	buffered := bufio.NewWriterSize(f, 4096)
	syncWriter := &flushingWriter{
		file:   f,
		writer: buffered,
	}

	var wg sync.WaitGroup
	errCh := make(chan error, 2)

	copyStream := func(r io.Reader) {
		defer wg.Done()
		if r == nil {
			return
		}
		_, copyErr := io.Copy(syncWriter, r)
		if copyErr != nil {
			errCh <- copyErr
		}
	}

	wg.Add(2)
	go copyStream(stdout)
	go copyStream(stderr)
	wg.Wait()
	close(errCh)

	if flushErr := syncWriter.Flush(); flushErr != nil {
		return flushErr
	}

	for copyErr := range errCh {
		if copyErr != nil {
			return copyErr
		}
	}

	return nil
}

type flushingWriter struct {
	mu     sync.Mutex
	file   *os.File
	writer *bufio.Writer
}

func (w *flushingWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	n, err := w.writer.Write(p)
	if err != nil {
		return n, err
	}
	if err := w.writer.Flush(); err != nil {
		return n, err
	}
	if err := w.file.Sync(); err != nil {
		return n, err
	}
	return n, nil
}

func (w *flushingWriter) Flush() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.writer.Flush(); err != nil {
		return err
	}
	return w.file.Sync()
}

// DefaultTaskLogFile returns the default logs.txt path for a task.
func DefaultTaskLogFile(taskID string) string {
	return filepath.Join(defaultWorkerDataRoot, "tasks", taskID, defaultTaskLogsName)
}
