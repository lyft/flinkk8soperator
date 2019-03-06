package flink

import (
	"github.com/lyft/flinkk8soperator/pkg/apis/app/v1alpha1"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestComputeTaskManagerReplicas(t *testing.T) {
	app := v1alpha1.FlinkApplication{}
	taskSlots := int32(4)
	app.Spec.TaskManagerConfig.TaskSlots = &taskSlots
	app.Spec.FlinkJob.Parallelism = 9

	assert.Equal(t, int32(3), computeTaskManagerReplicas(&app))
}
