package flink

import (
	"fmt"
	"github.com/lyft/flinkk8soperator/pkg/apis/app/v1alpha1"
	"gopkg.in/yaml.v2"
)

const (
	JobManagerDefaultReplicaCount = 1
	TaskManagerDefaultSlots       = 16
	RpcDefaultPort                = 6123
	QueryDefaultPort              = 6124
	BlobDefaultPort               = 6125
	UiDefaultPort                 = 8081
	MetricsQueryDefaultPort       = 50101
	OffHeapMemoryDefaultFraction  = 0.5
)

func firstNonNil(x *int32, y int32) int32 {
	if x != nil {
		return *x
	}
	return y
}

func getValidFraction(x *float64, y float64) float64 {
	if x != nil && *x >= float64(0) && *x <= float64(1) {
		return *x
	}
	return y
}

func getTaskmanagerSlots(app *v1alpha1.FlinkApplication) int32 {
	return firstNonNil(app.Spec.TaskManagerConfig.TaskSlots, TaskManagerDefaultSlots)
}

func getJobmanagerReplicas(app *v1alpha1.FlinkApplication) int32 {
	return firstNonNil(app.Spec.JobManagerConfig.Replicas, JobManagerDefaultReplicaCount)
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

func getTaskManagerMemory(application *v1alpha1.FlinkApplication) int64 {
	tmResources := application.Spec.TaskManagerConfig.Resources
	if tmResources == nil {
		tmResources = &TaskManagerDefaultResources
	}
	tmMemory, _ := tmResources.Requests.Memory().AsInt64()
	return tmMemory
}

func getJobManagerMemory(application *v1alpha1.FlinkApplication) int64 {
	jmResources := application.Spec.JobManagerConfig.Resources
	if jmResources == nil {
		jmResources = &JobManagerDefaultResources
	}
	jmMemory, _ := jmResources.Requests.Memory().AsInt64()
	return jmMemory
}

func getTaskManagerHeapMemory(app *v1alpha1.FlinkApplication) float64  {
	offHeapMemoryFrac := getValidFraction( app.Spec.TaskManagerConfig.OffHeapMemoryFraction, OffHeapMemoryDefaultFraction)
	tmMemory := float64(getTaskManagerMemory(app))
	heapMemoryBytes := tmMemory - (tmMemory * offHeapMemoryFrac)
	heapMemoryMB := heapMemoryBytes / (1024 * 1024)
	return heapMemoryMB
}

func getJobManagerHeapMemory(app *v1alpha1.FlinkApplication) float64  {
	offHeapMemoryFrac := getValidFraction(app.Spec.JobManagerConfig.OffHeapMemoryFraction, OffHeapMemoryDefaultFraction)
	jmMemory := float64(getJobManagerMemory(app))
	heapMemoryBytes := jmMemory - (jmMemory * offHeapMemoryFrac)
	heapMemoryMB := heapMemoryBytes / (1024 * 1024)
	return heapMemoryMB
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
	(*config)["jobmanager.heap.size"] = getJobManagerHeapMemory(app)
	(*config)["taskmanager.heap.size"] = getTaskManagerHeapMemory(app)

	b, err := yaml.Marshal(config)
	if err != nil {
		return "", err
	}
	return string(b), nil
}
