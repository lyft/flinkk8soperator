package k8

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/api/apps/v1"
)

func TestGetAppLabel(t *testing.T) {
	appName := "app_name"
	appLabel := GetAppLabel(appName)
	assert.Equal(t, map[string]string{
		"app": appName,
	}, appLabel)
}

func TestImageAppLabel(t *testing.T) {
	image := "image"
	imageLabel := GetImageLabel(image)
	assert.Equal(t, map[string]string{
		"imageKey": image,
	}, imageLabel)
}

func TestImageKey(t *testing.T) {
	imageValue := "docker.io/lyft/flinkk8soperator:12345678"
	imageKey := GetImageKey(imageValue)
	assert.Equal(t, "12345", imageKey)
}

func TestImageKeyLessLength(t *testing.T) {
	imageValue := "docker.io/lyft/flinkk8soperator:1234"
	imageKey := GetImageKey(imageValue)
	assert.Equal(t, "1234", imageKey)
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

func TestMatchDeploymentsByLabel(t *testing.T) {
	nameMap := map[string]string{
		"app": "App",
	}
	dep := v1.Deployment{}
	dep.Name = "matched"
	dep2 := v1.Deployment{}
	dep2.Name = "unmatched"
	dep.Labels = nameMap
	deployments := []v1.Deployment{
		dep,
		dep2,
	}
	matched, unMatched := MatchDeploymentsByLabel(v1.DeploymentList{
		Items: deployments,
	}, nameMap)
	assert.Equal(t, matched, []v1.Deployment{dep})
	assert.Equal(t, unMatched, []v1.Deployment{dep2})
}

func TestMatchDeploymentsEmptyLabel(t *testing.T) {
	nameMap := map[string]string{}
	dep := v1.Deployment{}
	dep.Name = "matched"
	dep2 := v1.Deployment{}
	dep2.Name = "unmatched"
	deployments := []v1.Deployment{
		dep,
		dep2,
	}
	matched, unMatched := MatchDeploymentsByLabel(v1.DeploymentList{
		Items: deployments,
	}, nameMap)
	assert.Equal(t, matched, []v1.Deployment{dep, dep2})
	assert.Empty(t, unMatched)
}

func TestMatchDeploymentsNoUnmatched(t *testing.T) {
	nameMap := map[string]string{
		"app": "App",
	}
	dep := v1.Deployment{}
	dep.Labels = nameMap
	deployments := []v1.Deployment{
		dep,
	}
	matched, unMatched := MatchDeploymentsByLabel(v1.DeploymentList{
		Items: deployments,
	}, nameMap)
	assert.Equal(t, matched, []v1.Deployment{dep})
	assert.Empty(t, unMatched)
}
