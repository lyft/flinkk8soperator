package config

import (
	"github.com/lyft/flytestdlib/config"
)

//go:generate pflags Config

const AppName = "flinkK8sOperator"
const configSectionKey = "operator"

var ConfigSection = config.MustRegisterSection(configSectionKey, &Config{})

type Config struct {
	ResyncPeriod          config.Duration `json:"resyncPeriod" pflag:"\"30s\",Determines the resync period for all watchers."`
	LimitNamespace        string          `json:"limitNamespace" pflag:"\"\",Namespaces to watch for by flink operator"`
	MetricsPrefix         string          `json:"metricsPrefix" pflag:"\"flinkk8soperator\",Prefix for metrics propagated to prometheus"`
	ProfilerPort          config.Port     `json:"profilerPort" pflag:"\"10254\",Profiler port"`
	FlinkIngressURLFormat string          `json:"ingressUrlFormat"`
	UseProxy              bool            `json:"useKubectlProxy"`
	ProxyPort             config.Port     `json:"proxyPort" pflag:"\"8001\",The port at which flink cluster runs locally"`
	ContainerNameFormat   string          `json:"containerNameFormat"`
	Workers               int             `json:"workers" pflag:"4,Number of routines to process custom resource"`
	BaseBackoffDuration   config.Duration `json:"baseBackoffDuration" pflag:"\"100ms\",Determines the base backoff for exponential retries."`
	MaxBackoffDuration    config.Duration `json:"maxBackoffDuration" pflag:"\"30s\",Determines the max backoff for exponential retries."`
	MaxErrDuration        config.Duration `json:"maxErrDuration" pflag:"\"5m\",Determines the max time to wait on errors."`
}

func GetConfig() *Config {
	return ConfigSection.GetConfig().(*Config)
}

func SetConfig(c *Config) error {
	return ConfigSection.SetConfig(c)
}
