package flink

import (
	"testing"

	"github.com/lyft/flinkk8soperator/pkg/apis/app/v1beta1"

	config2 "github.com/lyft/flinkk8soperator/pkg/controller/config"
	"github.com/stretchr/testify/assert"
)

func TestReplaceJobUrl(t *testing.T) {
	assert.Equal(t,
		"ABC.lyft.xyz",
		ReplaceJobURL("{{$jobCluster}}.lyft.xyz", "ABC"))
}

func initTestConfigForIngress() error {
	return config2.ConfigSection.SetConfig(&config2.Config{
		FlinkIngressURLFormat: "{{$jobCluster}}.lyft.xyz",
	})
}
func TestGetFlinkUIIngressURL(t *testing.T) {
	err := initTestConfigForIngress()
	assert.Nil(t, err)
	assert.Equal(t,
		"ABC.lyft.xyz",
		GetFlinkUIIngressURL("ABC"))
}

func TestGetFlinkUIIngressURLBlueGreenDeployment(t *testing.T) {
	err := initTestConfigForIngress()
	assert.Nil(t, err)
	app := v1beta1.FlinkApplication{}
	app.Spec.DeploymentMode = v1beta1.DeploymentModeBlueGreen
	app.Name = "ABC"
	app.Status.UpdatingVersion = v1beta1.GreenFlinkApplication
	assert.Equal(t, "ABC-green", getIngressName(&app))
	assert.Equal(t,
		"ABC-green.lyft.xyz",
		GetFlinkUIIngressURL(getIngressName(&app)))
}
