package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/training-scheduler/pkg/logging"
	"github.com/training-scheduler/pkg/metrics"
	"github.com/training-scheduler/pkg/persistence"
	"github.com/training-scheduler/pkg/scheduler"
	"github.com/training-scheduler/pkg/security"
	pb "github.com/training-scheduler/proto"
	"google.golang.org/grpc"
)

type authTokenFlag []string

func (a *authTokenFlag) String() string {
	if a == nil || len(*a) == 0 {
		return ""
	}
	return strings.Join(*a, ",")
}

func (a *authTokenFlag) Set(value string) error {
	*a = append(*a, strings.TrimSpace(value))
	return nil
}

func main() {
	configPath := flag.String("config", "configs/scheduler.yaml", "Path to scheduler YAML (optional; defaults apply if file is missing)")
	port := flag.Int("port", 0, "gRPC listen port (0 = use config file or default 50051)")
	metricsPort := flag.Int("metrics-port", 9091, "The metrics server port")
	logLevel := flag.String("log-level", "info", "Log level (debug, info, warn, error)")
	logDir := flag.String("log-dir", "/var/log/scheduler", "Directory for log files")

	enablePersistence := flag.Bool("persistence", true, "Enable state persistence")
	persistenceType := flag.String("persistence-type", "database", "Persistence type (database or file)")
	databaseType := flag.String("db-type", "sqlite", "Database type (sqlite or postgres)")
	databaseConnection := flag.String("db-connection", "scheduler.db", "Database connection string")
	autoSave := flag.Bool("auto-save", true, "Enable automatic state saving")
	saveInterval := flag.Int("save-interval", 60, "Interval between automatic state saves (seconds)")
	tlsCert := flag.String("tls-cert", "", "Path to server TLS certificate (PEM); enables TLS when set with --tls-key")
	tlsKey := flag.String("tls-key", "", "Path to server TLS private key (PEM); enables TLS when set with --tls-cert")
	tlsCA := flag.String("tls-ca", "", "Optional path to CA bundle (PEM); if set, requires and verifies client certificates (mTLS)")
	var authTokenList authTokenFlag
	flag.Var(&authTokenList, "auth-token", "Static bearer token allowed for unary RPCs (repeatable); when at least one is set, clients must send metadata authorization (Bearer <token> or raw token)")
	flag.Parse()

	cfg := newSchedulerViper(*configPath)
	listenPort := schedulerListenPort(cfg, *port)
	leaseDur := schedulerLeaseDuration(cfg)
	workerTO := schedulerWorkerTimeout(cfg)

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", listenPort))
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	var grpcOpts []grpc.ServerOption
	if *tlsCert != "" || *tlsKey != "" {
		if *tlsCert == "" || *tlsKey == "" {
			log.Fatal("TLS enabled: both --tls-cert and --tls-key are required")
		}
		creds, tlsErr := security.LoadTLSCredentials(*tlsCert, *tlsKey, *tlsCA)
		if tlsErr != nil {
			log.Fatalf("TLS: %v", tlsErr)
		}
		grpcOpts = append(grpcOpts, grpc.Creds(creds))
	}

	validTokens := make(map[string]bool)
	for _, t := range authTokenList {
		t = strings.TrimSpace(t)
		if t != "" {
			validTokens[t] = true
		}
	}
	grpcOpts = append(grpcOpts, grpc.UnaryInterceptor(security.AuthInterceptor(validTokens)))

	grpcServer := grpc.NewServer(grpcOpts...)

	metricsServer := metrics.NewMetricsServer(fmt.Sprintf(":%d", *metricsPort))
	schedulerMetrics := metrics.NewSchedulerMetrics()

	schedulerService := scheduler.NewScheduler()
	schedulerService.SetTiming(leaseDur, workerTO)
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

	logFields := map[string]interface{}{
		"port":            listenPort,
		"metrics_port":    *metricsPort,
		"lease_duration":  leaseDur.String(),
		"worker_timeout":  workerTO.String(),
		"config_path":     *configPath,
	}
	if cfg.ConfigFileUsed() != "" {
		logFields["config_file"] = cfg.ConfigFileUsed()
	}
	logger.Info("Starting scheduler server", logFields)

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
