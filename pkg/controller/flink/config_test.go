package flink

import (
	"fmt"
	"github.com/lyft/flinkk8soperator/pkg/apis/app/v1alpha1"
	"github.com/stretchr/testify/assert"
	coreV1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/apis/meta/v1"
	"sort"
	"strings"
	"testing"
)

func TestRenderFlinkConfigOverrides(t *testing.T) {
	taskSlots := int32(4)
	blobPort := int32(1000)
	offHeapMemoryFrac := 0.5

	yaml, err := renderFlinkConfig(&v1alpha1.FlinkApplication{
		ObjectMeta: v1.ObjectMeta{
			Name: "test-app",
		},
		Spec: v1alpha1.FlinkApplicationSpec{
			FlinkConfig: map[string]interface{}{
				"akka.timeout":                            "5s",
				"taskmanager.network.memory.fraction":     0.1,
				"taskmanager.network.request-backoff.max": 5000,
				"jobmanager.rpc.address":                  "wrong-address",
			},
			TaskManagerConfig: v1alpha1.TaskManagerConfig{
				TaskSlots:             &taskSlots,
				OffHeapMemoryFraction: &offHeapMemoryFrac,
			},
			JobManagerConfig: v1alpha1.JobManagerConfig{
				OffHeapMemoryFraction: &offHeapMemoryFrac,
			},
			BlobPort: &blobPort,
		},
		Status: v1alpha1.FlinkApplicationStatus{
			Phase: v1alpha1.FlinkApplicationNew,
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
		"jobmanager.heap.size: 1536", // defaults
		"jobmanager.rpc.address: test-app-jm",
		fmt.Sprintf("jobmanager.rpc.port: %d", RpcDefaultPort),
		fmt.Sprintf("jobmanager.web.port: %d", UiDefaultPort),
		fmt.Sprintf("metrics.internal.query-service.port: %d", MetricsQueryDefaultPort),
		fmt.Sprintf("query.server.port: %d", QueryDefaultPort),
		"taskmanager.heap.size: 512", // defaults
		"taskmanager.network.memory.fraction: 0.1",
		"taskmanager.network.request-backoff.max: 5000",
		fmt.Sprintf("taskmanager.numberOfTaskSlots: %d", taskSlots),
	}

	assert.Equal(t, expected, lines)
}

func TestGetTaskSlots(t *testing.T) {
	app1 := v1alpha1.FlinkApplication{}
	assert.Equal(t, int32(TaskManagerDefaultSlots), getTaskmanagerSlots(&app1))

	app2 := v1alpha1.FlinkApplication{}
	taskSlots := int32(4)
	app2.Spec.TaskManagerConfig.TaskSlots = &taskSlots
	assert.Equal(t, int32(4), getTaskmanagerSlots(&app2))
}

func TestGetJobManagerReplicas(t *testing.T) {
	app1 := v1alpha1.FlinkApplication{}
	assert.Equal(t, int32(JobManagerDefaultReplicaCount), getJobmanagerReplicas(&app1))
}

func TestGetJobManagerReplicasNonZero(t *testing.T) {
	app1 := v1alpha1.FlinkApplication{}
	replicas := int32(4)

	app1.Spec.JobManagerConfig.Replicas = &replicas
	assert.Equal(t, int32(4), getJobmanagerReplicas(&app1))
}

func TestGetTaskManagerMemory(t *testing.T) {
	app := v1alpha1.FlinkApplication{}
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
	app := v1alpha1.FlinkApplication{}
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

func TestGetTaskManagerHeapMemory(t *testing.T) {
	app := v1alpha1.FlinkApplication{}
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
	offHeapMemoryFraction := float64(0.5)
	app.Spec.TaskManagerConfig.Resources = &tmResources
	app.Spec.TaskManagerConfig.OffHeapMemoryFraction = &offHeapMemoryFraction

	tmMemory := float64(getTaskManagerMemory(&app))
	expectedtmHeapMemoryMB := (tmMemory - tmMemory*offHeapMemoryFraction) / (1024 * 1024)
	assert.Equal(t, expectedtmHeapMemoryMB, getTaskManagerHeapMemory(&app))
}

func TestGetJobManagerHeapMemory(t *testing.T) {
	app := v1alpha1.FlinkApplication{}
	jmResources := coreV1.ResourceRequirements{
		Requests: coreV1.ResourceList{
			coreV1.ResourceCPU:    resource.MustParse("2"),
			coreV1.ResourceMemory: resource.MustParse("1Mi"),
		},
		Limits: coreV1.ResourceList{
			coreV1.ResourceCPU:    resource.MustParse("2"),
			coreV1.ResourceMemory: resource.MustParse("1Mi"),
		},
	}
	offHeapMemoryFraction := float64(0.5)
	app.Spec.JobManagerConfig.Resources = &jmResources
	app.Spec.JobManagerConfig.OffHeapMemoryFraction = &offHeapMemoryFraction

	jmMemory := float64(getJobManagerMemory(&app))
	expectedjmHeapMemoryMB := (jmMemory - jmMemory*offHeapMemoryFraction) / (1024 * 1024)
	assert.Equal(t, expectedjmHeapMemoryMB, getJobManagerHeapMemory(&app))
}

func TestInvalidMemoryFraction(t *testing.T) {
	app := v1alpha1.FlinkApplication{}
	jmResources := coreV1.ResourceRequirements{
		Requests: coreV1.ResourceList{
			coreV1.ResourceCPU:    resource.MustParse("2"),
			coreV1.ResourceMemory: resource.MustParse("1Mi"),
		},
		Limits: coreV1.ResourceList{
			coreV1.ResourceCPU:    resource.MustParse("2"),
			coreV1.ResourceMemory: resource.MustParse("1Mi"),
		},
	}
	offHeapMemoryFraction := float64(1.5)
	app.Spec.JobManagerConfig.Resources = &jmResources
	app.Spec.JobManagerConfig.OffHeapMemoryFraction = &offHeapMemoryFraction

	jmMemory := float64(getJobManagerMemory(&app))
	expectedjmHeapMemoryMB := (jmMemory - jmMemory*OffHeapMemoryDefaultFraction) / (1024 * 1024)
	assert.Equal(t, expectedjmHeapMemoryMB, getJobManagerHeapMemory(&app))

}
