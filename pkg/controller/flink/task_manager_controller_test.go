package flink

import (
	"github.com/lyft/flinkk8soperator/pkg/apis/app/v1alpha1"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestGetTaskSlots(t *testing.T) {
	app1 := v1alpha1.FlinkApplication{}
	assert.Equal(t, TaskManagerDefaultSlots, getTaskmanagerSlots(&app1))

	app2 := v1alpha1.FlinkApplication{}
	taskSlots := int32(4)
	app2.Spec.TaskManagerConfig.TaskSlots = &taskSlots
	assert.Equal(t, 4, getTaskmanagerSlots(&app2))
}

func TestComputeTaskManagerReplicas(t *testing.T) {
	app := v1alpha1.FlinkApplication{}
	taskSlots := int32(4)
	app.Spec.TaskManagerConfig.TaskSlots = &taskSlots
	app.Spec.FlinkJob.Parallelism = 9

	assert.Equal(t, int32(3), computeTaskManagerReplicas(&app))
}
