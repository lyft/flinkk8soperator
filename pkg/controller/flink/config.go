package flink

import (
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/hashicorp/go-version"
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

func getFraction(systemMemoryFraction *float64, offHeapMemoryFraction *float64) float64 {
	if isValidFraction(systemMemoryFraction) {
		return *systemMemoryFraction
	}
	if isValidFraction(offHeapMemoryFraction) {
		return *offHeapMemoryFraction
	}
	if offHeapMemoryFraction != nil {
		return OffHeapMemoryDefaultFraction
	}

	return SystemMemoryDefaultFraction
}

func isValidFraction(fraction *float64) bool {
	return fraction != nil && *fraction >= float64(0) && *fraction <= float64(1)
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
	tmMemory, _ := tmResources.Requests.Memory().AsInt64()
	return tmMemory
}

func getRequestedJobManagerMemory(application *v1beta1.FlinkApplication) int64 {
	jmResources := application.Spec.JobManagerConfig.Resources
	if jmResources == nil {
		jmResources = &JobManagerDefaultResources
	}
	jmMemory, _ := jmResources.Requests.Memory().AsInt64()
	return jmMemory
}

func computeMemory(memoryInBytes float64, fraction float64) string {
	kbs := int64(math.Round(memoryInBytes-(memoryInBytes*fraction)) / 1024)
	return fmt.Sprintf("%dk", kbs)
}

func getTaskManagerMemory(app *v1beta1.FlinkApplication, fraction float64) string {
	tmMemory := float64(getRequestedTaskManagerMemory(app))
	return computeMemory(tmMemory, fraction)
}

func getJobManagerMemory(app *v1beta1.FlinkApplication, fraction float64) string {
	jmMemory := float64(getRequestedJobManagerMemory(app))
	return computeMemory(jmMemory, fraction)
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

	//nolint // fall back to the old config for backwards-compatibility
	jobManagerFraction := getFraction(app.Spec.JobManagerConfig.SystemMemoryFraction, app.Spec.JobManagerConfig.OffHeapMemoryFraction)
	//nolint // fall back to the old config for backwards-compatibility
	taskManagerFraction := getFraction(app.Spec.TaskManagerConfig.SystemMemoryFraction, app.Spec.TaskManagerConfig.OffHeapMemoryFraction)

	if err != nil || appVersion == nil || appVersion.LessThan(v11) {
		(*config)["jobmanager.heap.size"] = getJobManagerMemory(app, jobManagerFraction)
		(*config)["taskmanager.heap.size"] = getTaskManagerMemory(app, taskManagerFraction)
	} else {
		(*config)["jobmanager.memory.process.size"] = getJobManagerMemory(app, jobManagerFraction)
		(*config)["taskmanager.memory.process.size"] = getTaskManagerMemory(app, taskManagerFraction)
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
