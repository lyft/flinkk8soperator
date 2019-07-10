package cmd

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"

	"k8s.io/klog"

	"sigs.k8s.io/controller-runtime/pkg/cache"

	"github.com/lyft/flytestdlib/config/viper"
	"github.com/lyft/flytestdlib/version"

	"github.com/lyft/flytestdlib/config"
	"github.com/lyft/flytestdlib/logger"
	"github.com/spf13/pflag"

	"github.com/lyft/flinkk8soperator/pkg/controller/common"
	"github.com/spf13/cobra"

	"github.com/lyft/flinkk8soperator/pkg/controller"
	controllerConfig "github.com/lyft/flinkk8soperator/pkg/controller/config"
	ctrlRuntimeConfig "sigs.k8s.io/controller-runtime/pkg/client/config"

	"github.com/kubernetes-sigs/controller-runtime/pkg/runtime/signals"
	apis "github.com/lyft/flinkk8soperator/pkg/apis/app"
	"github.com/lyft/flytestdlib/profutils"
	"github.com/lyft/flytestdlib/promutils"
	"github.com/lyft/flytestdlib/promutils/labeled"
	"github.com/pkg/errors"
	"sigs.k8s.io/controller-runtime/pkg/manager"
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
	RunE: func(cmd *cobra.Command, args []string) error {
		return executeRootCmd(controllerConfig.GetConfig())
	},
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	version.LogBuildInformation(controllerConfig.AppName)
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func Run(config *controllerConfig.Config) error {
	if err := controllerConfig.SetConfig(config); err != nil {
		logger.Errorf(context.Background(), "Failed to set config: %v", err)
		return err
	}

	return executeRootCmd(controllerConfig.GetConfig())
}

func init() {
	// See https://gist.github.com/nak3/78a32817a8a3950ae48f239a44cd3663
	// allows `$ flinkoperator --logtostderr` to work
	klog.InitFlags(nil)
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

func executeRootCmd(controllerCfg *controllerConfig.Config) error {
	ctx, cancelNow := context.WithCancel(context.Background())

	labeled.SetMetricKeys(common.GetValidLabelNames()...)

	logger.Infof(ctx, "%+v\n", controllerCfg)

	if controllerCfg.MetricsPrefix == "" {
		logAndExit(errors.New("Invalid config: Metric prefix empty"))
	}
	operatorScope := promutils.NewScope(controllerCfg.MetricsPrefix)

	go func() {
		err := profutils.StartProfilingServerWithDefaultHandlers(ctx, controllerCfg.ProfilerPort.Port, nil)
		if err != nil {
			logger.Panicf(ctx, "Failed to Start profiling and metrics server. Error: %v", err)
		}
	}()

	stopCh, err := operatorEntryPoint(ctx, operatorScope, controllerCfg)
	if err != nil {
		cancelNow()
		return err
	}

	for {
		select {
		case <-stopCh:
			cancelNow()
			os.Exit(0)
		case <-ctx.Done():
			cancelNow()
		}
	}
}

func operatorEntryPoint(ctx context.Context, metricsScope promutils.Scope,
	controllerCfg *controllerConfig.Config) (stopCh <-chan struct{}, err error) {

	// Get a config to talk to the apiserver
	cfg, err := ctrlRuntimeConfig.GetConfig()
	if err != nil {
		return nil, err
	}

	limitNameSpace := strings.TrimSpace(controllerCfg.LimitNamespace)
	var mgr manager.Manager

	if limitNameSpace == "" {
		mgr, err = manager.New(cfg, manager.Options{
			SyncPeriod: &controllerCfg.ResyncPeriod.Duration,
		})
	} else {
		namespaceList := strings.Split(limitNameSpace, ",")
		mgr, err = manager.New(cfg, manager.Options{
			NewCache:   cache.MultiNamespacedCacheBuilder(namespaceList),
			SyncPeriod: &controllerCfg.ResyncPeriod.Duration,
		})
	}

	if err != nil {
		return nil, err
	}

	logger.Infof(ctx, "Registering Components.")

	// Setup Scheme for all resources
	if err := apis.AddToScheme(mgr.GetScheme()); err != nil {
		return nil, err
	}

	// Setup all Controllers
	logger.Infof(ctx, "Adding controllers.")
	if err := controller.AddToManager(ctx, mgr, controllerConfig.RuntimeConfig{
		MetricsScope: metricsScope,
	}); err != nil {
		return nil, err
	}

	// Start the Cmd
	logger.Infof(ctx, "Starting the Cmd.")
	stopCh = signals.SetupSignalHandler()
	return stopCh, mgr.Start(stopCh)
}
