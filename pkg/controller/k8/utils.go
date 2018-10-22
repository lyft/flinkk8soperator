package k8

import (
	"github.com/lyft/flinkk8soperator/pkg/controller/common"
	"k8s.io/api/apps/v1"
)

const (
	AppKey         = "App"
	ImageKey       = "ImageKey"
	ImageKeyLength = 5
)

func GetAppLabel(appName string) map[string]string {
	return map[string]string{
		AppKey: appName,
	}
}

func GetImageLabel(imageKey string) map[string]string {
	return map[string]string{
		ImageKey: imageKey,
	}
}

func GetImageKey(image string) string {
	imageTag := common.ContainerImageTag(image)
	if len(imageTag) < ImageKeyLength {
		return imageTag
	}
	return imageTag[0:ImageKeyLength]
}

func MatchDeploymentsByLabel(deployments v1.DeploymentList, matchMap map[string]string) ([]v1.Deployment, []v1.Deployment) {
	if len(deployments.Items) == 0 {
		return nil, nil
	}
	if len(matchMap) == 0 {
		return deployments.Items, nil
	}
	var matchingDeployments []v1.Deployment
	var unMatchedDeployments []v1.Deployment

	for _, deploymentEntry := range deployments.Items {
		if len(deploymentEntry.Labels) == 0 {
			unMatchedDeployments = append(unMatchedDeployments, deploymentEntry)
		} else {
			isMatch := true
			for k, v := range matchMap {
				if val, ok := deploymentEntry.Labels[k]; !ok {
					isMatch = false
				} else {
					isMatch = val == v
				}
			}
			if isMatch {
				matchingDeployments = append(matchingDeployments, deploymentEntry)
			} else {
				unMatchedDeployments = append(unMatchedDeployments, deploymentEntry)
			}
		}
	}
	return matchingDeployments, unMatchedDeployments
}
