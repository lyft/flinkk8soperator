package flink

import (
	"fmt"
	"github.com/lyft/flinkk8soperator/pkg/apis/app/v1alpha1"
	"gopkg.in/yaml.v2"
)

const (
	TaskManagerDefaultSlots = 16
	RpcDefaultPort          = 6123
	QueryDefaultPort        = 6124
	BlobDefaultPort         = 6125
	UiDefaultPort           = 8081
	MetricsQueryDefaultPort = 50101
)

func firstNonNil(x *int32, y int32) int32 {
	if x != nil {
		return *x
	}
	return y
}

func getTaskmanagerSlots(app *v1alpha1.FlinkApplication) int32 {
	return firstNonNil(app.Spec.TaskManagerConfig.TaskSlots, TaskManagerDefaultSlots)
}

func getRpcPort(app *v1alpha1.FlinkApplication) int32 {
	return firstNonNil(app.Spec.RpcPort, RpcDefaultPort)
}

func getUiPort(app *v1alpha1.FlinkApplication) int32 {
	return firstNonNil(app.Spec.UiPort, UiDefaultPort)
}

func getQueryPort(app *v1alpha1.FlinkApplication) int32 {
	return firstNonNil(app.Spec.QueryPort, QueryDefaultPort)
}

func getBlobPort(app *v1alpha1.FlinkApplication) int32 {
	return firstNonNil(app.Spec.BlobPort, BlobDefaultPort)
}

func getInternalMetricsQueryPort(app *v1alpha1.FlinkApplication) int32 {
	return firstNonNil(app.Spec.MetricsQueryPort, MetricsQueryDefaultPort)
}

func getJobManagerServiceName(app *v1alpha1.FlinkApplication) string {
	return fmt.Sprintf(JobManagerServiceNameFormat, app.Name)
}

// Renders the flink configuration overrides stored in FlinkApplication.FlinkConfig into a
// YAML string suitable for interpolating into flink-conf.yaml.
func renderFlinkConfig(app *v1alpha1.FlinkApplication) (string, error) {
	config := app.Spec.FlinkConfig.DeepCopy()
	if config == nil {
		config = &v1alpha1.FlinkConfig{}
	}

	(*config)["jobmanager.rpc.address"] = getJobManagerServiceName(app)
	(*config)["taskmanager.numberOfTaskSlots"] = getTaskmanagerSlots(app)
	(*config)["jobmanager.rpc.port"] = getRpcPort(app)
	(*config)["jobmanager.web.port"] = getUiPort(app)
	(*config)["query.server.port"] = getQueryPort(app)
	(*config)["blob.server.port"] = getBlobPort(app)
	(*config)["metrics.internal.query-service.port"] = getInternalMetricsQueryPort(app)

	b, err := yaml.Marshal(config)
	if err != nil {
		return "", err
	}
	return string(b), nil
}
