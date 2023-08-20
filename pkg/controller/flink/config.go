package flink

import (
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/hashicorp/go-version"
	"github.com/lyft/flinkk8soperator/integ/log"
	"github.com/lyft/flinkk8soperator/pkg/apis/app/v1beta1"
)

const (
	JobManagerDefaultReplicaCount  = 1
	TaskManagerDefaultSlots        = 16
	RPCDefaultPort                 = 6123
	QueryDefaultPort               = 6124
	BlobDefaultPort                = 6125
	UIDefaultPort                  = 8081
	MetricsQueryDefaultPort        = 50101
	OffHeapMemoryDefaultFraction   = 0.5
	SystemMemoryDefaultFraction    = 0.2
	HighAvailabilityKey            = "high-availability"
	MaxCheckpointRestoreAgeSeconds = 3600
)

func firstNonNil(x *int32, y int32) int32 {
	if x != nil {
		return *x
	}
	return y
}

func getValidFraction(x *float64, d float64) float64 {
	if x != nil && *x >= float64(0) && *x <= float64(1) {
		return *x
	}
	return d
}

func getTaskmanagerSlots(app *v1beta1.FlinkApplication) int32 {
	return firstNonNil(app.Spec.TaskManagerConfig.TaskSlots, TaskManagerDefaultSlots)
}

func getJobmanagerReplicas(app *v1beta1.FlinkApplication) int32 {
	return firstNonNil(app.Spec.JobManagerConfig.Replicas, JobManagerDefaultReplicaCount)
}

func getServiceAccountName(app *v1beta1.FlinkApplication) string {
	return app.Spec.ServiceAccountName
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

func getMaxCheckpointRestoreAgeSeconds(app *v1beta1.FlinkApplication) int32 {
	return firstNonNil(app.Spec.MaxCheckpointRestoreAgeSeconds, MaxCheckpointRestoreAgeSeconds)
}

func getRequestedTaskManagerMemory(application *v1beta1.FlinkApplication) int64 {
	tmResources := application.Spec.TaskManagerConfig.Resources
	if tmResources == nil {
		tmResources = &TaskManagerDefaultResources
	}
	tmMemory, ok := tmResources.Requests.Memory().AsInt64()
	if !ok {
		tmMemory, ok = tmResources.Requests.Memory().ToDec().AsInt64()
		if !ok {
			log.Errorf("Task Manager memory couldn't be parsed, sorry: name=%s", application.Name)
			return 0
		}
	}
	return tmMemory
}

func getRequestedJobManagerMemory(application *v1beta1.FlinkApplication) int64 {
	jmResources := application.Spec.JobManagerConfig.Resources
	if jmResources == nil {
		jmResources = &JobManagerDefaultResources
	}
	jmMemory, ok := jmResources.Requests.Memory().AsInt64()
	if !ok {
		jmMemory, ok = jmResources.Requests.Memory().ToDec().AsInt64()
		if !ok {
			log.Errorf("Job Manager memory couldn't be parsed, sorry: name=%s", application.Name)
			return 0
		}
	}
	return jmMemory
}

func computeMemory(memoryInBytes float64, fraction float64) string {
	kbs := int64(math.Round(memoryInBytes-(memoryInBytes*fraction)) / 1024)
	return fmt.Sprintf("%dk", kbs)
}

// heap memory configs are used for Flink < 1.11

func getTaskManagerHeapMemory(app *v1beta1.FlinkApplication) string {
	tmMemory := float64(getRequestedTaskManagerMemory(app))
	fraction := getValidFraction(app.Spec.TaskManagerConfig.OffHeapMemoryFraction, OffHeapMemoryDefaultFraction)
	tmMemoryStr := computeMemory(tmMemory, fraction)
	return tmMemoryStr
}

func getJobManagerHeapMemory(app *v1beta1.FlinkApplication) string {
	jmMemory := float64(getRequestedJobManagerMemory(app))
	fraction := getValidFraction(app.Spec.JobManagerConfig.OffHeapMemoryFraction, OffHeapMemoryDefaultFraction)
	jmMemoryStr := computeMemory(jmMemory, fraction)
	return jmMemoryStr
}

// process memory configs are used for Flink >= 1.11

func getTaskManagerProcessMemory(app *v1beta1.FlinkApplication) string {
	tmMemory := float64(getRequestedTaskManagerMemory(app))
	fraction := getValidFraction(app.Spec.TaskManagerConfig.SystemMemoryFraction, SystemMemoryDefaultFraction)
	tmMemoryStr := computeMemory(tmMemory, fraction)
	return tmMemoryStr
}

func getJobManagerProcessMemory(app *v1beta1.FlinkApplication) string {
	jmMemory := float64(getRequestedJobManagerMemory(app))
	fraction := getValidFraction(app.Spec.JobManagerConfig.SystemMemoryFraction, SystemMemoryDefaultFraction)
	jmMemoryStr := computeMemory(jmMemory, fraction)
	return jmMemoryStr
}

func getFlinkVersion(app *v1beta1.FlinkApplication) string {
	return app.Spec.FlinkVersion
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

	appVersion, err := version.NewVersion(getFlinkVersion(app))
	v11, _ := version.NewVersion("1.11")

	if err != nil || appVersion == nil || appVersion.LessThan(v11) {
		// if process memory is specified for < 11, error out
		if app.Spec.JobManagerConfig.SystemMemoryFraction != nil || app.Spec.TaskManagerConfig.SystemMemoryFraction != nil {
			return "", fmt.Errorf("systemMemoryFraction config cannot be used with flinkVersion < 1.11', use " +
				"offHeapMemoryFraction instead")
		}

		(*config)["jobmanager.heap.size"] = getJobManagerHeapMemory(app)
		(*config)["taskmanager.heap.size"] = getTaskManagerHeapMemory(app)
	} else {
		// if heap memory is used for >= 1.11, error out
		if app.Spec.JobManagerConfig.OffHeapMemoryFraction != nil || app.Spec.TaskManagerConfig.OffHeapMemoryFraction != nil {
			return "", fmt.Errorf("offHeapMemoryFraction config cannot be used with flinkVersion >= 1.11'; " +
				"use systemMemoryFraction istead")
		}

		(*config)["jobmanager.memory.process.size"] = getJobManagerProcessMemory(app)
		(*config)["taskmanager.memory.process.size"] = getTaskManagerProcessMemory(app)
	}

	// get the keys for the map
	var keys = make([]string, len(*config))
	i := 0
	for k := range *config {
		keys[i] = k
		i++
	}

	// sort them to provide a stable iteration order
	sort.Strings(keys)

	// print them in order
	var s strings.Builder
	for _, k := range keys {
		var vStr string

		switch v := (*config)[k].(type) {
		case int, uint, int32, uint32, int64, uint64, bool, float32, float64:
			vStr = fmt.Sprintf("%v", v)
		case string:
			vStr = v
		default:
			return "", fmt.Errorf("invalid type in flink config: %T", v)
		}

		_, _ = fmt.Fprintf(&s, "%s: %s\n", k, vStr)
	}

	return s.String(), nil
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
