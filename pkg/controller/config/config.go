package config

import (
	"github.com/lyft/flytestdlib/config"
)

//go:generate pflags Config

const configSectionKey = "operator"

// MustRegister Section will panic
var ConfigSection = config.MustRegisterSection(configSectionKey, &Config{})

type Config struct {
	FlinkIngressUrlFormat         string          `json:"ingressUrlFormat"`
	UseProxy                      bool            `json:"useKubectlProxy"`
	ProxyPort                     config.Port     `json:"ProxyPort" pflag:"\"8001\",The port at which flink cluster runs locally"`
	ContainerNameFormat           string          `json:"containerNameFormat"`
	StatemachineStalenessDuration config.Duration `json:"statemachineStalenessDuration" pflag:"\"10m\",Duration for statemachine staleness."`
}

func GetConfig() *Config {
	return ConfigSection.GetConfig().(*Config)
}
