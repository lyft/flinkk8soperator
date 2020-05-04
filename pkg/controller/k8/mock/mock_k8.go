package mock

import (
	"context"

	v1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

type GetDeploymentsWithLabelFunc func(ctx context.Context, namespace string, labelMap map[string]string) (*v1.DeploymentList, error)
type CreateK8ObjectFunc func(ctx context.Context, object runtime.Object) error
type GetServiceFunc func(ctx context.Context, namespace string, name string, version string) (*corev1.Service, error)
type GetServiceWithLabelFunc func(ctx context.Context, namespace string, labelMap map[string]string) (*corev1.ServiceList, error)
type UpdateK8ObjectFunc func(ctx context.Context, object runtime.Object) error
type UpdateStatusFunc func(ctx context.Context, object runtime.Object) error
type DeleteK8ObjectFunc func(ctx context.Context, object runtime.Object) error

type K8Cluster struct {
	GetDeploymentsWithLabelFunc GetDeploymentsWithLabelFunc
	GetServiceFunc              GetServiceFunc
	GetServicesWithLabelFunc    GetServiceWithLabelFunc
	CreateK8ObjectFunc          CreateK8ObjectFunc
	UpdateK8ObjectFunc          UpdateK8ObjectFunc
	UpdateStatusFunc            UpdateStatusFunc
	DeleteK8ObjectFunc          DeleteK8ObjectFunc
}

func (m *K8Cluster) GetDeploymentsWithLabel(ctx context.Context, namespace string, labelMap map[string]string) (*v1.DeploymentList, error) {
	if m.GetDeploymentsWithLabelFunc != nil {
		return m.GetDeploymentsWithLabelFunc(ctx, namespace, labelMap)
	}
	return nil, nil
}

func (m *K8Cluster) GetServicesWithLabel(ctx context.Context, namespace string, labelMap map[string]string) (*corev1.ServiceList, error) {
	if m.GetDeploymentsWithLabelFunc != nil {
		return m.GetServicesWithLabelFunc(ctx, namespace, labelMap)
	}
	return nil, nil
}

func (m *K8Cluster) GetService(ctx context.Context, namespace string, name string, version string) (*corev1.Service, error) {
	if m.GetServiceFunc != nil {
		return m.GetServiceFunc(ctx, namespace, name, version)
	}
	return nil, nil
}

func (m *K8Cluster) CreateK8Object(ctx context.Context, object runtime.Object) error {
	if m.CreateK8ObjectFunc != nil {
		return m.CreateK8ObjectFunc(ctx, object)
	}
	return nil
}

func (m *K8Cluster) UpdateK8Object(ctx context.Context, object runtime.Object) error {
	if m.UpdateK8ObjectFunc != nil {
		return m.UpdateK8ObjectFunc(ctx, object)
	}
	return nil
}

func (m *K8Cluster) UpdateStatus(ctx context.Context, object runtime.Object) error {
	if m.UpdateStatusFunc != nil {
		return m.UpdateStatusFunc(ctx, object)
	}
	return nil
}

func (m *K8Cluster) DeleteK8Object(ctx context.Context, object runtime.Object) error {
	if m.DeleteK8ObjectFunc != nil {
		return m.DeleteK8ObjectFunc(ctx, object)
	}
	return nil
}
