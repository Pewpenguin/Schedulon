package main

import (
	"time"

	"github.com/spf13/viper"
)

const (
	defaultSchedulerPort          = 50051
	defaultLeaseDuration          = 30 * time.Second
	defaultWorkerTimeout          = 60 * time.Second
	defaultLeaseDurationString    = "30s"
	defaultWorkerTimeoutString    = "60s"
)

func newSchedulerViper(configPath string) *viper.Viper {
	v := viper.New()
	v.SetDefault("scheduler.port", defaultSchedulerPort)
	v.SetDefault("scheduler.lease_duration", defaultLeaseDurationString)
	v.SetDefault("scheduler.worker_timeout", defaultWorkerTimeoutString)

	v.SetConfigFile(configPath)
	_ = v.ReadInConfig()

	return v
}

func schedulerListenPort(v *viper.Viper, flagPort int) int {
	if flagPort > 0 {
		return flagPort
	}
	p := v.GetInt("scheduler.port")
	if p > 0 {
		return p
	}
	return defaultSchedulerPort
}

func schedulerLeaseDuration(v *viper.Viper) time.Duration {
	return parseDurationSetting(v, "scheduler.lease_duration", defaultLeaseDuration)
}

func schedulerWorkerTimeout(v *viper.Viper) time.Duration {
	return parseDurationSetting(v, "scheduler.worker_timeout", defaultWorkerTimeout)
}

func parseDurationSetting(v *viper.Viper, key string, fallback time.Duration) time.Duration {
	s := v.GetString(key)
	if s == "" {
		return fallback
	}
	d, err := time.ParseDuration(s)
	if err != nil || d <= 0 {
		return fallback
	}
	return d
}
