package flink

import (
	"fmt"
	"github.com/lyft/flinkk8soperator/pkg/apis/app/v1alpha1"
	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/apis/meta/v1"
	"sort"
	"strings"
	"testing"
)

func TestRenderFlinkConfigOverrides(t *testing.T) {
	taskSlots := int32(4)
	blobPort := int32(1000)

	yaml, err := renderFlinkConfig(&v1alpha1.FlinkApplication{
		ObjectMeta: v1.ObjectMeta{
			Name: "test-app",
		},
		Spec: v1alpha1.FlinkApplicationSpec{
			FlinkConfig: map[string]interface{}{
				"akka.timeout":                            "5s",
				"taskmanager.network.memory.fraction":     0.1,
				"taskmanager.network.request-backoff.max": 5000,
				"jobmanager.rpc.address": "wrong-address",
			},
			TaskManagerConfig: v1alpha1.TaskManagerConfig{
				TaskSlots: &taskSlots,
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
		"jobmanager.rpc.address: test-app-jm",
		fmt.Sprintf("jobmanager.rpc.port: %d", RpcDefaultPort),
		fmt.Sprintf("jobmanager.web.port: %d", UiDefaultPort),
		fmt.Sprintf("metrics.internal.query-service.port: %d", MetricsQueryDefaultPort),
		fmt.Sprintf("query.server.port: %d", QueryDefaultPort),
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
