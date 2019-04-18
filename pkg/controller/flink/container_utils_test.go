package flink

import (
	"testing"

	"github.com/lyft/flinkk8soperator/pkg/apis/app/v1alpha1"
	"github.com/stretchr/testify/assert"
)

func TestHashForApplication(t *testing.T) {
	app := v1alpha1.FlinkApplication{}
	taskSlots := int32(8)
	app.Spec.TaskManagerConfig.TaskSlots = &taskSlots
	app.Spec.Parallelism = 4
	app.Name = "app-name"
	app.Namespace = "ns"
	app.Spec.Image = "abcdef"
	app.ObjectMeta.Labels = map[string]string{
		"label-k": "label-v",
	}
	app.ObjectMeta.Annotations = map[string]string{
		"annotation-k": "annotation-v",
	}

	h1 := HashForApplication(&app)
	assert.Equal(t, 8, len(h1))

	app.Name = "another-name"
	h2 := HashForApplication(&app)
	assert.NotEqual(t, h1, h2)

	app.Spec.Image = "zxy"
	h3 := HashForApplication(&app)
	assert.NotEqual(t, h2, h3)

	app.Labels["label-k"] = "new-v"
	h4 := HashForApplication(&app)
	assert.NotEqual(t, h3, h4)

	app.Annotations["annotation-k"] = "new-v"
	h5 := HashForApplication(&app)
	assert.NotEqual(t, h4, h5)

	app.Spec.Parallelism = 7
	h6 := HashForApplication(&app)
	assert.NotEqual(t, h5, h6)
}

func TestContainersEqual(t *testing.T) {
	app := getFlinkTestApp()
	d1 := FetchJobMangerDeploymentCreateObj(&app)
	d2 := FetchJobMangerDeploymentCreateObj(&app)

	assert.True(t, DeploymentsEqual(d1, d2))

	d1 = FetchTaskMangerDeploymentCreateObj(&app)
	d2 = FetchTaskMangerDeploymentCreateObj(&app)

	assert.True(t, DeploymentsEqual(d1, d2))

	d3 := d1.DeepCopy()
	d3.Spec.Template.Spec.Containers[0].ImagePullPolicy = "Always"
	assert.False(t, DeploymentsEqual(d3, d2))

	d3 = d1.DeepCopy()
	replicas := int32(13)
	d3.Spec.Replicas = &replicas
	assert.False(t, DeploymentsEqual(d3, d2))

	d3 = d1.DeepCopy()
	d3.Annotations[RestartNonce] = "x"
	assert.False(t, DeploymentsEqual(d3, d2))
}
