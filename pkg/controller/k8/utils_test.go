package k8

import (
	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/apps/v1"
)

func TestGetAppLabel(t *testing.T) {
	appName := "app_name"
	appLabel := GetAppLabel(appName)
	assert.Equal(t, map[string]string{
		"flink-app": appName,
	}, appLabel)
}

func TestGetDeploymentWithName(t *testing.T) {
	name := "jm-name"
	dep := v1.Deployment{}
	dep.Name = name
	deployments := []v1.Deployment{
		dep,
	}
	actualDeployment := GetDeploymentWithName(deployments, name)
	assert.NotNil(t, actualDeployment)
	assert.Equal(t, dep, *actualDeployment)
}

func TestGetDeploymentNotExists(t *testing.T) {
	name := "jm-name"
	dep := v1.Deployment{}
	dep.Name = name
	deployments := []v1.Deployment{
		dep,
	}
	actualDeployment := GetDeploymentWithName(deployments, "random")
	assert.Nil(t, actualDeployment)
}
