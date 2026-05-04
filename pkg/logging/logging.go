package logging

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/metadata"
)

// Standard JSON field names for cross-service correlation.
const (
	FieldTaskID    = "task_id"
	FieldWorkerID  = "worker_id"
	FieldTraceID   = "trace_id"
	FieldComponent = "component"
)

type traceIDKey struct{}

// WithTraceID returns a child context carrying a trace id for logging.TraceIDFromContext / emit.
func WithTraceID(ctx context.Context, traceID string) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if traceID == "" {
		return ctx
	}
	return context.WithValue(ctx, traceIDKey{}, traceID)
}

// TraceIDFromContext returns the trace id previously stored with WithTraceID.
func TraceIDFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	v, _ := ctx.Value(traceIDKey{}).(string)
	return v
}

// TraceIDFromIncomingContext returns trace_id from gRPC incoming metadata (keys: trace_id, x-request-id)
// or from WithTraceID on ctx.
func TraceIDFromIncomingContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	if v := TraceIDFromContext(ctx); v != "" {
		return v
	}
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}
	if v := md.Get(FieldTraceID); len(v) > 0 && v[0] != "" {
		return v[0]
	}
	if v := md.Get("x-request-id"); len(v) > 0 && v[0] != "" {
		return v[0]
	}
	return ""
}

// Fields builds a map with task_id and worker_id for use with Info / WithFields.
// trace_id is filled from context when using InfoCtx / WarnCtx / etc.
func Fields(taskID, workerID string) map[string]interface{} {
	return map[string]interface{}{
		FieldTaskID:   taskID,
		FieldWorkerID: workerID,
	}
}

// Logger is a wrapper around logrus.Logger
type Logger struct {
	*logrus.Logger
	Component string
}

type LogLevel string

const (
	DebugLevel LogLevel = "debug"
	InfoLevel  LogLevel = "info"
	WarnLevel  LogLevel = "warn"
	ErrorLevel LogLevel = "error"
	FatalLevel LogLevel = "fatal"
	PanicLevel LogLevel = "panic"
)

type Config struct {
	Level     LogLevel
	Component string
	LogDir    string
	LogFile   string
}

func NewLogger(config Config) (*Logger, error) {
	logLevel, err := logrus.ParseLevel(string(config.Level))
	if err != nil {
		logLevel = logrus.InfoLevel
	}

	logger := logrus.New()
	logger.SetLevel(logLevel)

	logger.SetFormatter(&logrus.JSONFormatter{
		TimestampFormat: time.RFC3339,
		FieldMap: logrus.FieldMap{
			logrus.FieldKeyTime:  "timestamp",
			logrus.FieldKeyLevel: "level",
			logrus.FieldKeyMsg:   "message",
		},
	})

	if config.LogDir != "" && config.LogFile != "" {
		if err := os.MkdirAll(config.LogDir, 0755); err != nil {
			return nil, fmt.Errorf("failed to create log directory: %v", err)
		}

		logFilePath := filepath.Join(config.LogDir, config.LogFile)
		file, err := os.OpenFile(logFilePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
		if err != nil {
			return nil, fmt.Errorf("failed to open log file: %v", err)
		}

		mw := io.MultiWriter(os.Stdout, file)
		logger.SetOutput(mw)
	}

	return &Logger{
		Logger:    logger,
		Component: config.Component,
	}, nil
}

func (l *Logger) mergeStandardFields(ctx context.Context, fields map[string]interface{}) logrus.Fields {
	out := make(map[string]interface{}, 24)
	if fields != nil {
		for k, v := range fields {
			out[k] = v
		}
	}
	if _, ok := out[FieldTaskID]; !ok {
		out[FieldTaskID] = ""
	}
	if _, ok := out[FieldWorkerID]; !ok {
		out[FieldWorkerID] = ""
	}
	trace := ""
	if v, ok := out[FieldTraceID]; ok && v != nil {
		trace, _ = v.(string)
	}
	if trace == "" {
		trace = TraceIDFromIncomingContext(ctx)
	}
	out[FieldTraceID] = trace
	out[FieldComponent] = l.Component
	return logrus.Fields(out)
}

func (l *Logger) emit(ctx context.Context, level logrus.Level, msg string, fields map[string]interface{}) {
	out := l.mergeStandardFields(ctx, fields)
	if _, file, line, ok := runtime.Caller(3); ok {
		out["file"] = filepath.Base(file)
		out["line"] = line
	}
	l.Logger.WithFields(out).Log(level, msg)
}

// WithFields returns a logrus entry including standard fields (task_id, worker_id, trace_id, component).
func (l *Logger) WithFields(fields map[string]interface{}) *logrus.Entry {
	out := l.mergeStandardFields(context.Background(), fields)
	if _, file, line, ok := runtime.Caller(2); ok {
		out["file"] = filepath.Base(file)
		out["line"] = line
	}
	return l.Logger.WithFields(out)
}

func (l *Logger) Debug(msg string, fields map[string]interface{}) {
	l.emit(context.Background(), logrus.DebugLevel, msg, fields)
}

func (l *Logger) Info(msg string, fields map[string]interface{}) {
	l.emit(context.Background(), logrus.InfoLevel, msg, fields)
}

func (l *Logger) Warn(msg string, fields map[string]interface{}) {
	l.emit(context.Background(), logrus.WarnLevel, msg, fields)
}

func (l *Logger) Error(msg string, fields map[string]interface{}) {
	l.emit(context.Background(), logrus.ErrorLevel, msg, fields)
}

func (l *Logger) Fatal(msg string, fields map[string]interface{}) {
	l.emit(context.Background(), logrus.FatalLevel, msg, fields)
}

func (l *Logger) Panic(msg string, fields map[string]interface{}) {
	l.emit(context.Background(), logrus.PanicLevel, msg, fields)
}

func (l *Logger) DebugCtx(ctx context.Context, msg string, fields map[string]interface{}) {
	l.emit(ctx, logrus.DebugLevel, msg, fields)
}

func (l *Logger) InfoCtx(ctx context.Context, msg string, fields map[string]interface{}) {
	l.emit(ctx, logrus.InfoLevel, msg, fields)
}

func (l *Logger) WarnCtx(ctx context.Context, msg string, fields map[string]interface{}) {
	l.emit(ctx, logrus.WarnLevel, msg, fields)
}

func (l *Logger) ErrorCtx(ctx context.Context, msg string, fields map[string]interface{}) {
	l.emit(ctx, logrus.ErrorLevel, msg, fields)
}

func (l *Logger) FatalCtx(ctx context.Context, msg string, fields map[string]interface{}) {
	l.emit(ctx, logrus.FatalLevel, msg, fields)
}
