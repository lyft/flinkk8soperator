package cmd

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/lyft/flytestdlib/config/viper"
	"github.com/lyft/flytestdlib/version"
	"github.com/operator-framework/operator-sdk/pkg/sdk"

	"github.com/lyft/flytestdlib/config"
	"github.com/lyft/flytestdlib/logger"
	"github.com/spf13/pflag"

	"github.com/lyft/flinkk8soperator/pkg/apis/app/v1alpha1"
	"github.com/lyft/flinkk8soperator/pkg/controller/common"
	"github.com/spf13/cobra"

	"github.com/lyft/flinkk8soperator/pkg/controller"
	controller_config "github.com/lyft/flinkk8soperator/pkg/controller/config"

	"time"

	"github.com/lyft/flytestdlib/promutils"
	"github.com/lyft/flytestdlib/promutils/labeled"
	"github.com/pkg/errors"
	"github.com/lyft/flytestdlib/profutils"
)

const (
	appName = "flinkoperator"
)

var (
	cfgFile        string
	configAccessor = viper.NewAccessor(config.Options{})
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "flinkoperator",
	Short: "Operator for running Flink applications in kubernetes",
	PreRunE: func(cmd *cobra.Command, args []string) error {
		return initConfig(cmd.Flags())
	},
	Run: func(cmd *cobra.Command, args []string) {
		executeRootCmd(controller_config.GetConfig())
	},
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	version.LogBuildInformation(appName)
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	// See https://gist.github.com/nak3/78a32817a8a3950ae48f239a44cd3663
	// allows `$ flinkoperator --logtostderr` to work
	pflag.CommandLine.AddGoFlagSet(flag.CommandLine)
	err := flag.CommandLine.Parse([]string{})
	if err != nil {
		logAndExit(err)
	}

	// Here you will define your flags and configuration settings. Cobra supports persistent flags, which, if defined
	// here, will be global for your application.
	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "",
		"config file path to load configuration")

	configAccessor.InitializePflags(rootCmd.PersistentFlags())
}

func initConfig(flags *pflag.FlagSet) error {
	configAccessor = viper.NewAccessor(config.Options{
		SearchPaths: []string{cfgFile},
	})

	configAccessor.InitializePflags(flags)
	err := configAccessor.UpdateConfig(context.Background())
	if err != nil {
		return err
	}
	return nil
}

func logAndExit(err error) {
	logger.Error(context.Background(), err)
	os.Exit(-1)
}

func executeRootCmd(cfg *controller_config.Config) {
	ctx := context.Background()
	resource := v1alpha1.SchemeGroupVersion.String()
	kind := v1alpha1.FlinkApplicationKind

	if cfg.MetricsPrefix == "" {
		logAndExit(errors.New("Invalid config: Metric prefix empty"))
	}
	operatorScope := promutils.NewScope(cfg.MetricsPrefix)

	go func() {
		err := profutils.StartProfilingServerWithDefaultHandlers(ctx, cfg.ProfilerPort.Port, nil)
		if err != nil {
			logger.Panicf(ctx, "Failed to Start profiling and metrics server. Error: %v", err)
		}
	}()

	watch(ctx, resource, kind, cfg.LimitNamespace, cfg.ResyncPeriod.Duration)
	labeled.SetMetricKeys(common.GetValidLabelNames()...)
	sdk.Handle(controller.NewHandler(operatorScope))
	sdk.Run(ctx)
}

func watch(ctx context.Context, resource, kind, namespace string, resyncPeriod time.Duration) {
	namespaceForLogging := namespace
	if namespaceForLogging == "" {
		namespaceForLogging = "*"
	}

	logger.Infof(ctx, "Watching [Resource: %s] [Kind: %s] [Namespace: %s] [SyncPeriod: %v]",
		resource, kind, namespaceForLogging, resyncPeriod)
	// Passing empty string as namespace indicates informer to watch all namespaces.
	// "*" does NOT work as namespace. We are just logging "*" for readability
	sdk.Watch(resource, kind, namespace, resyncPeriod)
}
