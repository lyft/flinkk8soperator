package flink

import (
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/lyft/flinkk8soperator/pkg/apis/app/v1beta1"
	"github.com/stretchr/testify/assert"
	coreV1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestRenderFlinkConfigOverrides(t *testing.T) {
	taskSlots := int32(4)
	blobPort := int32(1000)
	offHeapMemoryFrac := 0.5

	yaml, err := renderFlinkConfig(&v1beta1.FlinkApplication{
		ObjectMeta: v1.ObjectMeta{
			Name: "test-app",
		},
		Spec: v1beta1.FlinkApplicationSpec{
			FlinkConfig: map[string]interface{}{
				"akka.timeout":                            "5s",
				"taskmanager.network.memory.fraction":     0.1,
				"taskmanager.network.request-backoff.max": 5000,
				"jobmanager.rpc.address":                  "wrong-address",
				"env.java.opts.jobmanager":                "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=39000 -XX:+UseG1GC",
			},
			TaskManagerConfig: v1beta1.TaskManagerConfig{
				TaskSlots:             &taskSlots,
				OffHeapMemoryFraction: &offHeapMemoryFrac,
			},
			JobManagerConfig: v1beta1.JobManagerConfig{
				OffHeapMemoryFraction: &offHeapMemoryFrac,
			},
			BlobPort: &blobPort,
		},
		Status: v1beta1.FlinkApplicationStatus{
			Phase: v1beta1.FlinkApplicationNew,
		},
	})

	if err != nil {
		assert.NoError(t, err, "Got error producing config")
	}

	lines := strings.Split(strings.TrimSpace(yaml), "\n")
	sort.Strings(lines)

	expected := []string{
		"akka.timeout: 5s",
		fmt.Sprintf("blob.server.port: %d", blobPort),
		"env.java.opts.jobmanager: -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=39000 -XX:+UseG1GC",
		"jobmanager.heap.size: 1572864k", // defaults
		fmt.Sprintf("jobmanager.rpc.port: %d", RPCDefaultPort),
		fmt.Sprintf("jobmanager.web.port: %d", UIDefaultPort),
		fmt.Sprintf("metrics.internal.query-service.port: %d", MetricsQueryDefaultPort),
		fmt.Sprintf("query.server.port: %d", QueryDefaultPort),
		"taskmanager.heap.size: 524288k", // defaults
		"taskmanager.network.memory.fraction: 0.1",
		"taskmanager.network.request-backoff.max: 5000",
		fmt.Sprintf("taskmanager.numberOfTaskSlots: %d", taskSlots),
	}

	assert.Equal(t, expected, lines)
}

func TestGetTaskSlots(t *testing.T) {
	app1 := v1beta1.FlinkApplication{}
	assert.Equal(t, int32(TaskManagerDefaultSlots), getTaskmanagerSlots(&app1))

	app2 := v1beta1.FlinkApplication{}
	taskSlots := int32(4)
	app2.Spec.TaskManagerConfig.TaskSlots = &taskSlots
	assert.Equal(t, int32(4), getTaskmanagerSlots(&app2))
}

func TestGetJobManagerReplicas(t *testing.T) {
	app1 := v1beta1.FlinkApplication{}
	assert.Equal(t, int32(JobManagerDefaultReplicaCount), getJobmanagerReplicas(&app1))
}

func TestGetJobManagerReplicasNonZero(t *testing.T) {
	app1 := v1beta1.FlinkApplication{}
	replicas := int32(4)

	app1.Spec.JobManagerConfig.Replicas = &replicas
	assert.Equal(t, int32(4), getJobmanagerReplicas(&app1))
}

func TestGetTaskManagerMemory(t *testing.T) {
	app := v1beta1.FlinkApplication{}
	tmResources := coreV1.ResourceRequirements{
		Requests: coreV1.ResourceList{
			coreV1.ResourceCPU:    resource.MustParse("2"),
			coreV1.ResourceMemory: resource.MustParse("1Mi"),
		},
		Limits: coreV1.ResourceList{
			coreV1.ResourceCPU:    resource.MustParse("2"),
			coreV1.ResourceMemory: resource.MustParse("1Mi"),
		},
	}
	expectedResource := resource.MustParse("1Mi")
	expectedValue, _ := expectedResource.AsInt64()
	app.Spec.TaskManagerConfig.Resources = &tmResources
	assert.Equal(t, expectedValue, getTaskManagerMemory(&app))
}

func TestGetJobManagerMemory(t *testing.T) {
	app := v1beta1.FlinkApplication{}
	tmResources := coreV1.ResourceRequirements{
		Requests: coreV1.ResourceList{
			coreV1.ResourceCPU:    resource.MustParse("2"),
			coreV1.ResourceMemory: resource.MustParse("1Mi"),
		},
		Limits: coreV1.ResourceList{
			coreV1.ResourceCPU:    resource.MustParse("2"),
			coreV1.ResourceMemory: resource.MustParse("1Mi"),
		},
	}
	expectedResource := resource.MustParse("1Mi")
	expectedValue, _ := expectedResource.AsInt64()
	app.Spec.JobManagerConfig.Resources = &tmResources
	assert.Equal(t, expectedValue, getJobManagerMemory(&app))
}

func TestEnsureNoFractionalHeapMemory(t *testing.T) {
	app := v1beta1.FlinkApplication{}
	tmResources := coreV1.ResourceRequirements{
		Requests: coreV1.ResourceList{
			coreV1.ResourceCPU:    resource.MustParse("2"),
			coreV1.ResourceMemory: resource.MustParse("64Mi"),
		},
		Limits: coreV1.ResourceList{
			coreV1.ResourceCPU:    resource.MustParse("2"),
			coreV1.ResourceMemory: resource.MustParse("64Mi"),
		},
	}
	offHeapMemoryFraction := float64(0.37)
	app.Spec.TaskManagerConfig.Resources = &tmResources
	app.Spec.TaskManagerConfig.OffHeapMemoryFraction = &offHeapMemoryFraction

	assert.Equal(t, "41287k", getTaskManagerHeapMemory(&app))
}

func TestGetTaskManagerHeapMemory(t *testing.T) {
	app := v1beta1.FlinkApplication{}
	tmResources := coreV1.ResourceRequirements{
		Requests: coreV1.ResourceList{
			coreV1.ResourceCPU:    resource.MustParse("2"),
			coreV1.ResourceMemory: resource.MustParse("64Mi"),
		},
		Limits: coreV1.ResourceList{
			coreV1.ResourceCPU:    resource.MustParse("2"),
			coreV1.ResourceMemory: resource.MustParse("64Mi"),
		},
	}
	offHeapMemoryFraction := float64(0.5)
	app.Spec.TaskManagerConfig.Resources = &tmResources
	app.Spec.TaskManagerConfig.OffHeapMemoryFraction = &offHeapMemoryFraction

	assert.Equal(t, "32768k", getTaskManagerHeapMemory(&app))
}

func TestGetJobManagerHeapMemory(t *testing.T) {
	app := v1beta1.FlinkApplication{}
	jmResources := coreV1.ResourceRequirements{
		Requests: coreV1.ResourceList{
			coreV1.ResourceCPU:    resource.MustParse("2"),
			coreV1.ResourceMemory: resource.MustParse("64Mi"),
		},
		Limits: coreV1.ResourceList{
			coreV1.ResourceCPU:    resource.MustParse("2"),
			coreV1.ResourceMemory: resource.MustParse("64Mi"),
		},
	}
	offHeapMemoryFraction := float64(0.5)
	app.Spec.JobManagerConfig.Resources = &jmResources
	app.Spec.JobManagerConfig.OffHeapMemoryFraction = &offHeapMemoryFraction

	assert.Equal(t, "32768k", getJobManagerHeapMemory(&app))
}

func TestGetJobManagerProcessMemory(t *testing.T) {
	app := v1beta1.FlinkApplication{}
	jmResources := coreV1.ResourceRequirements{
		Requests: coreV1.ResourceList{
			coreV1.ResourceCPU:    resource.MustParse("2"),
			coreV1.ResourceMemory: resource.MustParse("64Mi"),
		},
		Limits: coreV1.ResourceList{
			coreV1.ResourceCPU:    resource.MustParse("2"),
			coreV1.ResourceMemory: resource.MustParse("128Mi"),
		},
	}
	app.Spec.JobManagerConfig.Resources = &jmResources

	assert.Equal(t, "65536k", getJobManagerProcessMemory(&app))
}

func TestGetTaskManagerProcessMemory(t *testing.T) {
	app := v1beta1.FlinkApplication{}
	tmResources := coreV1.ResourceRequirements{
		Requests: coreV1.ResourceList{
			coreV1.ResourceCPU:    resource.MustParse("2"),
			coreV1.ResourceMemory: resource.MustParse("64Mi"),
		},
		Limits: coreV1.ResourceList{
			coreV1.ResourceCPU:    resource.MustParse("2"),
			coreV1.ResourceMemory: resource.MustParse("128Mi"),
		},
	}
	app.Spec.TaskManagerConfig.Resources = &tmResources

	assert.Equal(t, "65536k", getTaskManagerProcessMemory(&app))
}

func MemoryConfigurationForVersion(t *testing.T, version string) []string {
	tmResources := coreV1.ResourceRequirements{
		Requests: coreV1.ResourceList{
			coreV1.ResourceCPU:    resource.MustParse("2"),
			coreV1.ResourceMemory: resource.MustParse("2Gi"),
		},
		Limits: coreV1.ResourceList{
			coreV1.ResourceCPU:    resource.MustParse("4"),
			coreV1.ResourceMemory: resource.MustParse("3Gi"),
		},
	}

	jmResources := coreV1.ResourceRequirements{
		Requests: coreV1.ResourceList{
			coreV1.ResourceCPU:    resource.MustParse("2"),
			coreV1.ResourceMemory: resource.MustParse("512Mi"),
		},
		Limits: coreV1.ResourceList{
			coreV1.ResourceCPU:    resource.MustParse("2"),
			coreV1.ResourceMemory: resource.MustParse("1Gi"),
		},
	}

	yaml, err := renderFlinkConfig(&v1beta1.FlinkApplication{
		ObjectMeta: v1.ObjectMeta{
			Name: "test-app",
		},
		Spec: v1beta1.FlinkApplicationSpec{
			TaskManagerConfig: v1beta1.TaskManagerConfig{
				Resources: &tmResources,
			},
			JobManagerConfig: v1beta1.JobManagerConfig{
				Resources: &jmResources,
			},
			FlinkVersion: version,
		},
		Status: v1beta1.FlinkApplicationStatus{
			Phase: v1beta1.FlinkApplicationNew,
		},
	})

	if err != nil {
		assert.NoError(t, err, "Got error producing config")
	}

	lines := strings.Split(strings.TrimSpace(yaml), "\n")
	sort.Strings(lines)

	return lines
}

func TestMemoryConfigurationForVersionEqualsOrAbove11(t *testing.T) {
	versions := []string{
		"1.11",
		"1.11.0",
	}

	for _, version := range versions {
		yaml := MemoryConfigurationForVersion(t, version)

		expected := []string{
			fmt.Sprintf("blob.server.port: %d", BlobDefaultPort),
			"jobmanager.memory.process.size: 524288k",
			fmt.Sprintf("jobmanager.rpc.port: %d", RPCDefaultPort),
			fmt.Sprintf("jobmanager.web.port: %d", UIDefaultPort),
			fmt.Sprintf("metrics.internal.query-service.port: %d", MetricsQueryDefaultPort),
			fmt.Sprintf("query.server.port: %d", QueryDefaultPort),
			"taskmanager.memory.process.size: 2097152k",
			fmt.Sprintf("taskmanager.numberOfTaskSlots: %d", TaskManagerDefaultSlots),
		}

		assert.Equal(t, expected, yaml)
	}
}

func TestMemoryConfigurationForVersionBelow11(t *testing.T) {
	versions := []string{
		"1.10",
		"1.10.0",
		"1.10.1",
		"1.9.0",
		"1.9",
		"1.8.0",
		"1.8",
	}

	for _, version := range versions {
		yaml := MemoryConfigurationForVersion(t, version)

		expected := []string{
			fmt.Sprintf("blob.server.port: %d", BlobDefaultPort),
			"jobmanager.heap.size: 262144k",
			fmt.Sprintf("jobmanager.rpc.port: %d", RPCDefaultPort),
			fmt.Sprintf("jobmanager.web.port: %d", UIDefaultPort),
			fmt.Sprintf("metrics.internal.query-service.port: %d", MetricsQueryDefaultPort),
			fmt.Sprintf("query.server.port: %d", QueryDefaultPort),
			"taskmanager.heap.size: 1048576k",
			fmt.Sprintf("taskmanager.numberOfTaskSlots: %d", TaskManagerDefaultSlots),
		}

		assert.Equal(t, expected, yaml)
	}
}
