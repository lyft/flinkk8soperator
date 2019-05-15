package mock

import (
	"context"

	"github.com/operator-framework/operator-sdk/pkg/sdk"
	v1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
)

type GetDeploymentsWithLabelFunc func(ctx context.Context, namespace string, labelMap map[string]string) (*v1.DeploymentList, error)
type GetPodsWithLabelFunc func(ctx context.Context, namespace string, labelMap map[string]string) (*corev1.PodList, error)
type CreateK8ObjectFunc func(ctx context.Context, object sdk.Object) error
type GetServiceFunc func(ctx context.Context, namespace string, name string) (*corev1.Service, error)
type UpdateK8ObjectFunc func(ctx context.Context, object sdk.Object) error
type DeleteDeploymentsFunc func(ctx context.Context, deploymentList []v1.Deployment) error

type K8Cluster struct {
	GetDeploymentsWithLabelFunc GetDeploymentsWithLabelFunc
	GetPodsWithLabelFunc        GetPodsWithLabelFunc
	GetServiceFunc              GetServiceFunc
	CreateK8ObjectFunc          CreateK8ObjectFunc
	UpdateK8ObjectFunc          UpdateK8ObjectFunc
	DeleteDeploymentsFunc       DeleteDeploymentsFunc
}

func (m *K8Cluster) GetDeploymentsWithLabel(ctx context.Context, namespace string, labelMap map[string]string) (*v1.DeploymentList, error) {
	if m.GetDeploymentsWithLabelFunc != nil {
		return m.GetDeploymentsWithLabelFunc(ctx, namespace, labelMap)
	}
	return nil, nil
}

func (m *K8Cluster) GetPodsWithLabel(ctx context.Context, namespace string, labelMap map[string]string) (*corev1.PodList, error) {
	if m.GetPodsWithLabelFunc != nil {
		return m.GetPodsWithLabelFunc(ctx, namespace, labelMap)
	}
	return nil, nil
}

func (m *K8Cluster) GetService(ctx context.Context, namespace string, name string) (*corev1.Service, error) {
	if m.GetServiceFunc != nil {
		return m.GetServiceFunc(ctx, namespace, name)
	}
	return nil, nil
}

func (m *K8Cluster) CreateK8Object(ctx context.Context, object sdk.Object) error {
	if m.CreateK8ObjectFunc != nil {
		return m.CreateK8ObjectFunc(ctx, object)
	}
	return nil
}

func (m *K8Cluster) UpdateK8Object(ctx context.Context, object sdk.Object) error {
	if m.UpdateK8ObjectFunc != nil {
		return m.UpdateK8ObjectFunc(ctx, object)
	}
	return nil
}

func (m *K8Cluster) DeleteDeployments(ctx context.Context, deployments []v1.Deployment) error {
	if m.DeleteDeploymentsFunc != nil {
		return m.DeleteDeploymentsFunc(ctx, deployments)
	}
	return nil
}
