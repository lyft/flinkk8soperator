package flink

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/lyft/flinkk8soperator/pkg/apis/app/v1alpha1"
	"github.com/lyft/flinkk8soperator/pkg/controller/common"
	"github.com/lyft/flinkk8soperator/pkg/controller/k8"
	"github.com/spf13/viper"
	"k8s.io/api/core/v1"
)

const (
	HighAvailability                 = "HIGH_AVAILABILITY"
	StorageDirectory                 = "STORAGE_DIR"
	ClusterId                        = "CLUSTER_ID"
	ZookeeperQuorum                  = "ZOOKEEPER_QUORUM"
	ZookeeperPathRoot                = "ZOOKEEPER_PATH_ROOT"
	SavepointDir                     = "SAVEPOINT_DIR"
	CheckpointDir                    = "CHECKPOINT_DIR"
	ExternalizedCheckpointDir        = "EXTERNALIZED_CHECKPOINT_DIR"
	FlinkRpcPort                     = "FLINK_RPC_PORT"
	AwsMetadataServiceTimeoutKey     = "AWS_METADATA_SERVICE_TIMEOUT"
	AwsMetadataServiceNumAttemptsKey = "AWS_METADATA_SERVICE_NUM_ATTEMPTS"
	AwsMetadataServiceTimeout        = "5"
	AwsMetadataServiceNumAttempts    = "20"
)

func getZookeeperPathRoot() (string, error) {
	if c := viper.GetString("zookeeperPathRoot"); c != "" {
		return c, nil
	}
	return "", errors.New("zookeeper path unavailable")
}
func getHighAvailabilityStorageDir(appName string) (string, error) {
	if c := viper.GetString("highAvailabilityStorageDir"); c != "" {
		return fmt.Sprintf(c, appName), nil
	}
	return "", errors.New("highAvailabilityStorageDir path unavailable")
}

func getFlinkSavepointDir(appName string) (string, error) {
	if c := viper.GetString("flinkSavepointDir"); c != "" {
		return fmt.Sprintf(c, appName), nil
	}
	return "", errors.New("flinkSavepointDir path unavailable")
}
func getFlinkExternalizedCheckpointDir(appName string) (string, error) {
	if c := viper.GetString("flinkExternalizedCheckpointDir"); c != "" {
		return fmt.Sprintf(c, appName), nil
	}
	return "", errors.New("flinkExternalizedCheckpointDir path unavailable")
}

func getFlinkCheckpointDir(appName string) (string, error) {
	if c := viper.GetString("flinkCheckpointDir"); c != "" {
		return fmt.Sprintf(c, appName), nil
	}
	return "", errors.New("flinkCheckpointDir path unavailable")
}

func getZookeeperQuorumStr(zookeperHosts []string) string {
	return strings.Join(zookeperHosts[:], ",")
}

func containerPort(name string, optionalPort *int32, defaultPort int32) v1.ContainerPort {
	if optionalPort == nil {
		return v1.ContainerPort{
			Name:          name,
			ContainerPort: defaultPort,
		}
	}
	return v1.ContainerPort{
		Name:          name,
		ContainerPort: *optionalPort,
	}
}

func getCommonAppLabels(app v1alpha1.FlinkApplication) map[string]string {
	appLabels := k8.GetAppLabel(app.Name)
	appLabels = common.CopyMap(appLabels, k8.GetImageLabel(k8.GetImageKey(app.Spec.Image)))
	return appLabels
}

func getJobManagerServiceName(app v1alpha1.FlinkApplication) string {
	return fmt.Sprintf(JobManagerServiceNameFormat, app.Name)
}

func GetAWSServiceEnv() []v1.EnvVar {
	return []v1.EnvVar{
		{
			Name:  AwsMetadataServiceTimeoutKey,
			Value: AwsMetadataServiceTimeout,
		},
		{
			Name:  AwsMetadataServiceNumAttemptsKey,
			Value: AwsMetadataServiceNumAttempts,
		},
	}
}

func GetZookeeperEnv(application v1alpha1.FlinkApplication) ([]v1.EnvVar, error) {
	zookeeperConfig := application.Spec.ZookeeperConfig
	if zookeeperConfig == nil || len(zookeeperConfig.HostAddresses) == 0 {
		return nil, errors.New("invalid zookeeper config")
	}
	storageDirectory, err := getHighAvailabilityStorageDir(application.Name)
	if err != nil {
		return nil, err
	}

	zookeeperPathRoot, err := getZookeeperPathRoot()
	if err != nil {
		return nil, err
	}
	return []v1.EnvVar{
		{
			Name:  HighAvailability,
			Value: string(v1alpha1.Zookeeper),
		},
		{
			Name:  ZookeeperQuorum,
			Value: getZookeeperQuorumStr(zookeeperConfig.HostAddresses),
		},
		{
			Name:  StorageDirectory,
			Value: storageDirectory,
		},
		{
			Name:  ClusterId,
			Value: application.Name,
		},
		{
			Name:  ZookeeperPathRoot,
			Value: zookeeperPathRoot,
		},
		{
			Name:  FlinkRpcPort,
			Value: strconv.Itoa(FlinkRpcDefaultPort),
		},
	}, nil
}

func getFlinkEnv(app v1alpha1.FlinkApplication) ([]v1.EnvVar, error) {
	// Ignore errors. Em
	savePointDirValue, err := getFlinkSavepointDir(app.Name)
	if err != nil {
		return nil, err
	}
	checkpointDirValue, err := getFlinkCheckpointDir(app.Name)
	if err != nil {
		return nil, err
	}
	extCheckpointDirValue, err := getFlinkExternalizedCheckpointDir(app.Name)
	if err != nil {
		return nil, err
	}

	return []v1.EnvVar{
		{
			Name:  SavepointDir,
			Value: savePointDirValue,
		},
		{
			Name:  CheckpointDir,
			Value: checkpointDirValue,
		},
		{
			Name:  ExternalizedCheckpointDir,
			Value: extCheckpointDirValue,
		},
	}, nil
}

func GetFlinkContainerEnv(app v1alpha1.FlinkApplication) ([]v1.EnvVar, error) {
	env := []v1.EnvVar{
		{
			Name:  JobManagerServiceEnvVar,
			Value: getJobManagerServiceName(app),
		},
	}
	env = append(env, GetAWSServiceEnv()...)
	flinkEnv, err := getFlinkEnv(app)
	if err == nil {
		env = append(env, flinkEnv...)
	}
	jobManagerConfig := app.Spec.JobManagerConfig
	if jobManagerConfig != nil && jobManagerConfig.HighAvailability == v1alpha1.Zookeeper {
		zookeeperEnv, err := GetZookeeperEnv(app)
		if err != nil {
			return nil, err
		}
		env = append(env, zookeeperEnv...)
	}
	return env, nil
}
