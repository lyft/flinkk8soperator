package flink

import (
	"testing"

	"github.com/lyft/flinkk8soperator/pkg/apis/app/v1beta1"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

func TestHashForApplication(t *testing.T) {
	app := v1beta1.FlinkApplication{}
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

func TestHashForDifferentResourceScales(t *testing.T) {
	app1 := v1beta1.FlinkApplication{}
	app1.Spec.TaskManagerConfig.Resources = &v1.ResourceRequirements{
		Requests: v1.ResourceList{
			v1.ResourceCPU:    resource.MustParse("0.5"),
			v1.ResourceMemory: resource.MustParse("1024Mi"),
		},
		Limits: v1.ResourceList{
			v1.ResourceCPU:    resource.MustParse("0.5"),
			v1.ResourceMemory: resource.MustParse("1024Mi"),
		},
	}

	app2 := v1beta1.FlinkApplication{}
	app2.Spec.TaskManagerConfig.Resources = &v1.ResourceRequirements{
		Requests: v1.ResourceList{
			v1.ResourceCPU:    resource.MustParse("500m"),
			v1.ResourceMemory: resource.MustParse("1024Mi"),
		},
		Limits: v1.ResourceList{
			v1.ResourceCPU:    resource.MustParse("500m"),
			v1.ResourceMemory: resource.MustParse("1024Mi"),
		},
	}

	assert.Equal(t, HashForApplication(&app1), HashForApplication(&app2))
}
