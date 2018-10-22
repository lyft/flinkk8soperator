package k8

import (
	"context"

	"github.com/lyft/flinkk8soperator/pkg/controller/logger"
	"github.com/operator-framework/operator-sdk/pkg/sdk"
	"k8s.io/api/apps/v1"
	coreV1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
)

const (
	Deployment = "Deployment"
	Pod        = "Pod"
	Service    = "Service"
	Ingress    = "Ingress"
)

type K8ClusterInterface interface {
	GetDeploymentsWithLabel(ctx context.Context, namespace string, labelMap map[string]string) (*v1.DeploymentList, error)
	DeleteDeploymentsWithLabel(ctx context.Context, namespace string, labelMap map[string]string) error
	IsAllPodsRunning(ctx context.Context, namespace string, labelMap map[string]string) (bool, error)
	CreateK8Object(ctx context.Context, object sdk.Object) error
	UpdateK8Object(ctx context.Context, object sdk.Object) error
	DeleteDeployments(ctx context.Context, deploymentList v1.DeploymentList) error
}

func NewK8Cluster() K8ClusterInterface {
	return &K8Cluster{}
}

type K8Cluster struct {
}

func (k *K8Cluster) GetPodsWithLabel(ctx context.Context, namespace string, labelMap map[string]string) (*coreV1.PodList, error) {
	podList := &coreV1.PodList{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       Pod,
		},
	}
	labelSelector := labels.SelectorFromSet(labelMap)
	options := &metav1.ListOptions{
		LabelSelector: labelSelector.String(),
	}
	err := sdk.List(namespace, podList, sdk.WithListOptions(options))
	if err != nil {
		logger.Warningf(ctx, "Failed to list pods [%v]", err)
		return nil, err
	}
	return podList, nil
}

func (k *K8Cluster) GetDeployment(ctx context.Context, namespace string, name string) (*v1.Deployment, error) {
	deployment := &v1.Deployment{
		TypeMeta: metav1.TypeMeta{
			APIVersion: v1.SchemeGroupVersion.String(),
			Kind:       Deployment,
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
	}

	err := sdk.Get(deployment)
	if err != nil {
		logger.Warningf(ctx, "Failed to get deployment [%v]", err)
		return nil, err
	}
	return deployment, nil
}

func (k *K8Cluster) GetService(ctx context.Context, namespace string, name string) (*coreV1.Service, error) {
	service := &coreV1.Service{
		TypeMeta: metav1.TypeMeta{
			APIVersion: v1.SchemeGroupVersion.String(),
			Kind:       Service,
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
	}

	err := sdk.Get(service)
	if err != nil {
		logger.Warningf(ctx, "Failed to get service [%v]", err)
		return nil, err
	}
	return service, nil
}

func (k *K8Cluster) GetDeploymentsWithLabel(ctx context.Context, namespace string, labelMap map[string]string) (*v1.DeploymentList, error) {
	deploymentList := &v1.DeploymentList{
		TypeMeta: metav1.TypeMeta{
			APIVersion: v1.SchemeGroupVersion.String(),
			Kind:       Deployment,
		},
	}
	labelSelector := labels.SelectorFromSet(labelMap)
	options := &metav1.ListOptions{
		LabelSelector: labelSelector.String(),
	}
	err := sdk.List(namespace, deploymentList, sdk.WithListOptions(options))
	if err != nil {
		logger.Errorf(ctx, "Failed to list deployments [%v]", err)
		return nil, err
	}
	return deploymentList, nil
}

func (k *K8Cluster) DeleteDeploymentsWithLabel(ctx context.Context, namespace string, labelMap map[string]string) error {
	deploymentList, err := k.GetDeploymentsWithLabel(ctx, namespace, labelMap)
	if err != nil {
		return err
	}
	return k.DeleteDeployments(ctx, *deploymentList)
}

func (k *K8Cluster) IsAllPodsRunning(ctx context.Context, namespace string, labelMap map[string]string) (bool, error) {
	podList, err := k.GetPodsWithLabel(ctx, namespace, labelMap)
	if err != nil {
		return false, err
	}
	if podList == nil || len(podList.Items) == 0 {
		return false, nil
	}

	for _, pod := range podList.Items {
		if pod.Status.Phase != coreV1.PodRunning {
			return false, nil
		}
	}
	return true, nil
}

func (k *K8Cluster) CreateK8Object(ctx context.Context, object sdk.Object) error {
	return sdk.Create(object)
}

func (k *K8Cluster) UpdateK8Object(ctx context.Context, object sdk.Object) error {
	return sdk.Update(object)
}

func (k *K8Cluster) DeleteDeployments(ctx context.Context, deploymentList v1.DeploymentList) error {
	for _, item := range deploymentList.Items {
		err := sdk.Delete(&item)
		if err != nil {
			logger.Errorf(ctx, "Failed to delete deployment [%v]", err)
			return err
		}
	}
	return nil
}
