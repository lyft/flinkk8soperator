package flink

import (
	"strings"

	"github.com/lyft/flinkk8soperator/pkg/apis/app/v1beta1"
	"gopkg.in/yaml.v2"
)

const (
	JobManagerDefaultReplicaCount = 1
	TaskManagerDefaultSlots       = 16
	RPCDefaultPort                = 6123
	QueryDefaultPort              = 6124
	BlobDefaultPort               = 6125
	UIDefaultPort                 = 8081
	MetricsQueryDefaultPort       = 50101
	OffHeapMemoryDefaultFraction  = 0.5
	HighAvailabilityKey           = "high-availability"
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

func getTaskmanagerSlots(app *v1beta1.FlinkApplication) int32 {
	return firstNonNil(app.Spec.TaskManagerConfig.TaskSlots, TaskManagerDefaultSlots)
}

func getJobmanagerReplicas(app *v1beta1.FlinkApplication) int32 {
	return firstNonNil(app.Spec.JobManagerConfig.Replicas, JobManagerDefaultReplicaCount)
}

func getRPCPort(app *v1beta1.FlinkApplication) int32 {
	return firstNonNil(app.Spec.RPCPort, RPCDefaultPort)
}

func getUIPort(app *v1beta1.FlinkApplication) int32 {
	return firstNonNil(app.Spec.UIPort, UIDefaultPort)
}

func getQueryPort(app *v1beta1.FlinkApplication) int32 {
	return firstNonNil(app.Spec.QueryPort, QueryDefaultPort)
}

func getBlobPort(app *v1beta1.FlinkApplication) int32 {
	return firstNonNil(app.Spec.BlobPort, BlobDefaultPort)
}

func getInternalMetricsQueryPort(app *v1beta1.FlinkApplication) int32 {
	return firstNonNil(app.Spec.MetricsQueryPort, MetricsQueryDefaultPort)
}

func getTaskManagerMemory(application *v1beta1.FlinkApplication) int64 {
	tmResources := application.Spec.TaskManagerConfig.Resources
	if tmResources == nil {
		tmResources = &TaskManagerDefaultResources
	}
	tmMemory, _ := tmResources.Requests.Memory().AsInt64()
	return tmMemory
}

func getJobManagerMemory(application *v1beta1.FlinkApplication) int64 {
	jmResources := application.Spec.JobManagerConfig.Resources
	if jmResources == nil {
		jmResources = &JobManagerDefaultResources
	}
	jmMemory, _ := jmResources.Requests.Memory().AsInt64()
	return jmMemory
}

func getTaskManagerHeapMemory(app *v1beta1.FlinkApplication) float64 {
	offHeapMemoryFrac := getValidFraction(app.Spec.TaskManagerConfig.OffHeapMemoryFraction, OffHeapMemoryDefaultFraction)
	tmMemory := float64(getTaskManagerMemory(app))
	heapMemoryBytes := tmMemory - (tmMemory * offHeapMemoryFrac)
	heapMemoryMB := heapMemoryBytes / (1024 * 1024)
	return heapMemoryMB
}

func getJobManagerHeapMemory(app *v1beta1.FlinkApplication) float64 {
	offHeapMemoryFrac := getValidFraction(app.Spec.JobManagerConfig.OffHeapMemoryFraction, OffHeapMemoryDefaultFraction)
	jmMemory := float64(getJobManagerMemory(app))
	heapMemoryBytes := jmMemory - (jmMemory * offHeapMemoryFrac)
	heapMemoryMB := heapMemoryBytes / (1024 * 1024)
	return heapMemoryMB
}

// Renders the flink configuration overrides stored in FlinkApplication.FlinkConfig into a
// YAML string suitable for interpolating into flink-conf.yaml.
func renderFlinkConfig(app *v1beta1.FlinkApplication) (string, error) {
	config := app.Spec.FlinkConfig.DeepCopy()
	if config == nil {
		config = &v1beta1.FlinkConfig{}
	}

	// we will fill this in later using the versioned service
	delete(*config, "jobmanager.rpc.address")

	(*config)["taskmanager.numberOfTaskSlots"] = getTaskmanagerSlots(app)
	(*config)["jobmanager.rpc.port"] = getRPCPort(app)
	(*config)["jobmanager.web.port"] = getUIPort(app)
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

func isHAEnabled(flinkConfig v1beta1.FlinkConfig) bool {
	if val, ok := flinkConfig[HighAvailabilityKey]; ok {
		value := val.(string)
		if strings.ToLower(strings.TrimSpace(value)) != "none" {
			return true
		}
	}
	return false
}
