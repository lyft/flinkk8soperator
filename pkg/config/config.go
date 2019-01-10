package config

import (
	"fmt"

	"github.com/spf13/viper"
)

var (
	FlinkIngressUrlFormat string
	UseProxy              bool
	ProxyPort             int
)

func Init(cfgFile string) {
	viper.SetConfigType("yaml")
	viper.SetConfigName("flinkk8soperator_config") // name of config file (without extension)
	viper.AddConfigPath(".")
	viper.AddConfigPath("/etc/flinkk8soperator/config")
	viper.AddConfigPath("$GOPATH/src/github.com/lyft/flinkk8soperator")

	viper.SetDefault("useKubectlProxy", false)
	viper.SetDefault("proxyPort", 8001)

	if cfgFile != "" {
		// Override the above if a specific one was given.  Must happen after
		fmt.Printf("Overriding the configuration file with %v\n", cfgFile)
		viper.SetConfigFile(cfgFile)
	}

	viper.AutomaticEnv() // read in environment variables that match
	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err == nil {
		fmt.Println("Using config file:", viper.ConfigFileUsed())
	} else {
		panic(err)
	}

	// Watch config files to pick up on file changes without requiring a full application restart.
	// This call must occur after *all* config paths have been added.
	viper.WatchConfig()

	// The url regex is used to create custom ingress endpoint for each flink cluster
	FlinkIngressUrlFormat = viper.GetString("ingressUrlFormat")

	UseProxy = viper.GetBool("useKubectlProxy")
	ProxyPort = viper.GetInt("proxyPort")
}
