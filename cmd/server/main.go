package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/training-scheduler/pkg/logging"
	"github.com/training-scheduler/pkg/metrics"
	"github.com/training-scheduler/pkg/persistence"
	"github.com/training-scheduler/pkg/scheduler"
	pb "github.com/training-scheduler/proto"
	"google.golang.org/grpc"
)

func main() {
	port := flag.Int("port", 50051, "The server port")
	metricsPort := flag.Int("metrics-port", 9091, "The metrics server port")
	logLevel := flag.String("log-level", "info", "Log level (debug, info, warn, error)")
	logDir := flag.String("log-dir", "/var/log/scheduler", "Directory for log files")

	enablePersistence := flag.Bool("persistence", true, "Enable state persistence")
	persistenceType := flag.String("persistence-type", "database", "Persistence type (database or file)")
	databaseType := flag.String("db-type", "sqlite", "Database type (sqlite or postgres)")
	databaseConnection := flag.String("db-connection", "scheduler.db", "Database connection string")
	autoSave := flag.Bool("auto-save", true, "Enable automatic state saving")
	saveInterval := flag.Int("save-interval", 60, "Interval between automatic state saves (seconds)")
	flag.Parse()

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()

	metricsServer := metrics.NewMetricsServer(fmt.Sprintf(":%d", *metricsPort))
	schedulerMetrics := metrics.NewSchedulerMetrics()

	schedulerService := scheduler.NewScheduler()
	schedulerService.SetMetrics(schedulerMetrics)

	detectorCtx, stopFailureDetector := context.WithCancel(context.Background())
	defer stopFailureDetector()
	schedulerService.StartFailureDetector(detectorCtx)

	pb.RegisterTrainingSchedulerServer(grpcServer, schedulerService)

	// Initialize logger
	loggerConfig := logging.Config{
		Level:     logging.LogLevel(*logLevel),
		Component: "scheduler",
		LogDir:    *logDir,
		LogFile:   "scheduler.log",
	}

	logger, err := logging.NewLogger(loggerConfig)
	if err != nil {
		log.Fatalf("Failed to initialize logger: %v", err)
	}

	logger.Info("Starting scheduler server", map[string]interface{}{
		"port":         *port,
		"metrics_port": *metricsPort,
	})

	logger.Info("Starting metrics server", map[string]interface{}{"port": *metricsPort})
	go func() {
		if err := metricsServer.Start(); err != nil {
			logger.Error("Failed to start metrics server", map[string]interface{}{"error": err.Error()})
		}
	}()

	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			logger.Fatal("Failed to serve", map[string]interface{}{"error": err.Error()})
		}
	}()

	schedulerMetrics.SetActiveTasks(0)
	schedulerMetrics.SetQueueDepth(0)
	schedulerMetrics.SetSchedulerActiveWorkers(0)

	if *enablePersistence {
		var persistenceConfig persistence.Config
		if *persistenceType == "database" {
			dbConfig := persistence.DatabaseConfig{
				Type:             persistence.DatabaseType(*databaseType),
				ConnectionString: *databaseConnection,
				AutoMigrate:      true,
				LogMode:          *logLevel == "debug",
			}
			persistenceConfig = persistence.Config{
				Type:         persistence.DatabasePersistence,
				Database:     dbConfig,
				SaveInterval: *saveInterval,
				AutoSave:     *autoSave,
			}
		} else {
			logger.Warn("File persistence is not implemented, using default database persistence", nil)
			persistenceConfig = persistence.DefaultConfig()
		}

		logger.Info("Enabling state persistence", map[string]interface{}{
			"type":          persistenceConfig.Type,
			"auto_save":     persistenceConfig.AutoSave,
			"save_interval": persistenceConfig.SaveInterval,
		})

		err = schedulerService.EnablePersistence(persistenceConfig)
		if err != nil {
			logger.Error("Failed to enable persistence", map[string]interface{}{"error": err.Error()})
		}
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	logger.Info("Shutting down server", nil)

	stopFailureDetector()

	if *enablePersistence {
		schedulerService.DisablePersistence()
	}

	grpcServer.GracefulStop()
	logger.Info("Server stopped", nil)
}
