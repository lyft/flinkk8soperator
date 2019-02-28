package flink

import (
	"fmt"
	"github.com/pkg/errors"
	"gopkg.in/yaml.v2"

	"strconv"

	"github.com/lyft/flinkk8soperator/pkg/apis/app/v1alpha1"
	"github.com/lyft/flinkk8soperator/pkg/controller/common"
	"github.com/lyft/flinkk8soperator/pkg/controller/k8"
	"github.com/spf13/viper"
	"k8s.io/api/core/v1"
)

const (
	AppName                          = "APP_NAME"
	ContainerNameFormat              = "containerNameFormat"
	StorageDirPrefixKey              = "storageDirPrefix"
	StorageDirEnvName                = "storageDirEnvName"
	ClusterId                        = "CLUSTER_ID"
	FlinkRpcPort                     = "FLINK_RPC_PORT"
	FlinkMetricsQueryPort            = "FLINK_METRICS_QUERY_PORT"
	JobManagerServiceEnvVar          = "JOB_MANAGER_SERVICE"
	TaskManagerHostname              = "TASKMANAGER_HOSTNAME"
	FlinkConfigOverrides             = "FLINK_CONFIG_OVERRIDES"
	AwsMetadataServiceTimeoutKey     = "AWS_METADATA_SERVICE_TIMEOUT"
	AwsMetadataServiceNumAttemptsKey = "AWS_METADATA_SERVICE_NUM_ATTEMPTS"
	AwsMetadataServiceTimeout        = "5"
	AwsMetadataServiceNumAttempts    = "20"
)

func getFlinkContainerName(containerName string) string {
	if c := viper.GetString(ContainerNameFormat); c != "" {
		return fmt.Sprintf(c, containerName)
	}
	return containerName
}

func getFlinkStorageDirPrefix(appName string) (string, error) {
	if c := viper.GetString(StorageDirPrefixKey); c != "" {
		return fmt.Sprintf(c, appName), nil
	}
	return "", errors.New("StorageDirPrefix unavailable")
}

func getFlinkStorageEnvName() (string, error) {
	if c := viper.GetString(StorageDirEnvName); c != "" {
		return c, nil
	}
	return "", errors.New("StorageDirEnvName unavailable")
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

// Renders the flink configuration overrides stored in FlinkApplication.FlinkConfig into a
// YAML string suitable for interpolating into flink-conf.yaml.
func renderFlinkConfigOverrides(app v1alpha1.FlinkApplication) (string, error) {
	if app.Spec.FlinkConfig == nil {
		return "", nil
	} else {
		b, err := yaml.Marshal(app.Spec.FlinkConfig)
		if err != nil {
			return "", err
		}
		return string(b), nil
	}
}

func getFlinkEnv(app v1alpha1.FlinkApplication) ([]v1.EnvVar, error) {
	env := []v1.EnvVar{}
	appName := app.Name
	flinkStorageEnvName, err := getFlinkStorageEnvName()
	// Do not fail on errors as applications can pass their own env
	if err == nil {
		flinkStorageDirPrefix, err := getFlinkStorageDirPrefix(appName)
		if err == nil {
			env = append(env, v1.EnvVar{
				Name:  flinkStorageEnvName,
				Value: flinkStorageDirPrefix,
			})
		}
	}

	configOverrides, err := renderFlinkConfigOverrides(app)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to serialize flink configuration")
	}

	env = append(env, []v1.EnvVar{
		{
			Name:  JobManagerServiceEnvVar,
			Value: getJobManagerServiceName(app),
		},
		{
			Name:  AppName,
			Value: appName,
		},
		{
			Name:  FlinkRpcPort,
			Value: strconv.Itoa(FlinkRpcDefaultPort),
		},
		{
			Name:  FlinkMetricsQueryPort,
			Value: strconv.Itoa(FlinkMetricsQueryDefaultPort),
		},
		{
			Name:  TaskManagerSlots,
			Value: strconv.Itoa(getTaskmanagerSlots(&app)),
		},
		{
			Name: TaskManagerHostname,
			ValueFrom: &v1.EnvVarSource{
				FieldRef: &v1.ObjectFieldSelector{
					FieldPath: "status.podIP",
				},
			},
		},
		{
			Name:  FlinkConfigOverrides,
			Value: configOverrides,
		},
	}...)
	return env, nil
}

func GetFlinkContainerEnv(app v1alpha1.FlinkApplication) ([]v1.EnvVar, error) {
	env := []v1.EnvVar{}
	env = append(env, GetAWSServiceEnv()...)
	flinkEnv, err := getFlinkEnv(app)
	if err == nil {
		env = append(env, flinkEnv...)
	}
	return env, nil
}
