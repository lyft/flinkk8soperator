package flink

import (
	"fmt"
	"github.com/lyft/flinkk8soperator/pkg/apis/app/v1alpha1"
	"github.com/lyft/flinkk8soperator/pkg/controller/common"
	"github.com/lyft/flinkk8soperator/pkg/controller/config"
	"github.com/lyft/flinkk8soperator/pkg/controller/k8"
	"github.com/pkg/errors"
	"k8s.io/api/core/v1"
)

const (
	AppName                          = "APP_NAME"
	AwsMetadataServiceTimeoutKey     = "AWS_METADATA_SERVICE_TIMEOUT"
	AwsMetadataServiceNumAttemptsKey = "AWS_METADATA_SERVICE_NUM_ATTEMPTS"
	AwsMetadataServiceTimeout        = "5"
	AwsMetadataServiceNumAttempts    = "20"
	OperatorFlinkConfig              = "OPERATOR_FLINK_CONFIG"
)

func getFlinkContainerName(containerName string) string {
	cfg := config.GetConfig()
	containerNameFormat := cfg.ContainerNameFormat
	if containerNameFormat != "" {
		return fmt.Sprintf(containerNameFormat, containerName)
	}
	return containerName
}

func getCommonAppLabels(app *v1alpha1.FlinkApplication) map[string]string {
	appLabels := k8.GetAppLabel(app.Name)
	appLabels = common.CopyMap(appLabels, k8.GetImageLabel(k8.GetImageKey(app.Spec.Image)))
	return appLabels
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

func getFlinkEnv(app *v1alpha1.FlinkApplication) ([]v1.EnvVar, error) {
	env := []v1.EnvVar{}
	appName := app.Name

	flinkConfig, err := renderFlinkConfig(app)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to serialize flink configuration")
	}

	env = append(env, []v1.EnvVar{
		{
			Name:  AppName,
			Value: appName,
		},
		{
			Name:  OperatorFlinkConfig,
			Value: flinkConfig,
		},
	}...)
	return env, nil
}

func GetFlinkContainerEnv(app *v1alpha1.FlinkApplication) ([]v1.EnvVar, error) {
	env := []v1.EnvVar{}
	env = append(env, GetAWSServiceEnv()...)
	flinkEnv, err := getFlinkEnv(app)
	if err == nil {
		env = append(env, flinkEnv...)
	}
	return env, nil
}
