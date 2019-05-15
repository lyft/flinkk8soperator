package config

import (
	"github.com/lyft/flytestdlib/config"
)

//go:generate pflags Config

const configSectionKey = "operator"

var ConfigSection = config.MustRegisterSection(configSectionKey, &Config{})

type Config struct {
	ResyncPeriod                  config.Duration `json:"resyncPeriod" pflag:"\"10s\",Determines the resync period for all watchers."`
	LimitNamespace                string          `json:"limitNamespace" pflag:"\"\",Namespaces to watch for by flink operator"`
	MetricsPrefix                 string          `json:"metricsPrefix" pflag:"\"flinkk8soperator\",Prefix for metrics propagated to prometheus"`
	ProfilerPort                  config.Port     `json:"prof-port" pflag:"\"10254\",Profiler port"`
	FlinkIngressURLFormat         string          `json:"ingressUrlFormat"`
	UseProxy                      bool            `json:"useKubectlProxy"`
	ProxyPort                     config.Port     `json:"ProxyPort" pflag:"\"8001\",The port at which flink cluster runs locally"`
	ContainerNameFormat           string          `json:"containerNameFormat"`
	StatemachineStalenessDuration config.Duration `json:"statemachineStalenessDuration" pflag:"\"5m\",Duration for statemachine staleness."`
}

func GetConfig() *Config {
	return ConfigSection.GetConfig().(*Config)
}

func SetConfig(c *Config) error {
	return ConfigSection.SetConfig(c)
}
