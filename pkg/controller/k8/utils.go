package k8

import v1 "k8s.io/api/apps/v1"

const (
	AppKey = "app"
)

func GetAppLabel(appName string) map[string]string {
	return map[string]string{
		AppKey: appName,
	}
}

func GetDeploymentWithName(deployments []v1.Deployment, name string) *v1.Deployment {
	if len(deployments) == 0 {
		return nil
	}
	for _, deployment := range deployments {
		if deployment.Name == name {
			return &deployment
		}
	}
	return nil
}
