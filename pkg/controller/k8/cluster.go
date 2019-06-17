package k8

import (
	"context"

	"github.com/lyft/flytestdlib/logger"
	v1 "k8s.io/api/apps/v1"
	coreV1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
)

const (
	Deployment = "Deployment"
	Pod        = "Pod"
	Service    = "Service"
	Endpoints  = "Endpoints"
	Ingress    = "Ingress"
)

type ClusterInterface interface {
	// Tries to fetch the value from the controller runtime manager cache, if it does not exist, call API server
	GetDeploymentsWithLabel(ctx context.Context, namespace string, labelMap map[string]string) (*v1.DeploymentList, error)

	// Tries to fetch the value from the controller runtime manager cache, if it does not exist, call API server
	GetService(ctx context.Context, namespace string, name string) (*coreV1.Service, error)
	GetServicesWithLabel(ctx context.Context, namespace string, labelMap map[string]string) (*coreV1.ServiceList, error)

	CreateK8Object(ctx context.Context, object runtime.Object) error
	UpdateK8Object(ctx context.Context, object runtime.Object) error
	DeleteK8Object(ctx context.Context, object runtime.Object) error
}

func NewK8Cluster(mgr manager.Manager) ClusterInterface {
	return &Cluster{
		cache:  mgr.GetCache(),
		client: mgr.GetClient(),
	}
}

type Cluster struct {
	cache  cache.Cache
	client client.Client
}

func (k *Cluster) GetService(ctx context.Context, namespace string, name string) (*coreV1.Service, error) {
	service := &coreV1.Service{
		TypeMeta: metav1.TypeMeta{
			APIVersion: coreV1.SchemeGroupVersion.String(),
			Kind:       Service,
		},
	}
	key := types.NamespacedName{
		Name:      name,
		Namespace: namespace,
	}
	err := k.cache.Get(ctx, key, service)
	if err != nil {
		if IsK8sObjectDoesNotExist(err) {
			err := k.client.Get(ctx, key, service)
			if err != nil {
				logger.Warnf(ctx, "Failed to get service %v", err)
				return nil, err
			}
		}
		logger.Warnf(ctx, "Failed to get service from cache %v", err)
		return nil, err
	}
	return service, nil
}

func (k *Cluster) GetDeploymentsWithLabel(ctx context.Context, namespace string, labelMap map[string]string) (*v1.DeploymentList, error) {
	deploymentList := &v1.DeploymentList{
		TypeMeta: metav1.TypeMeta{
			APIVersion: v1.SchemeGroupVersion.String(),
			Kind:       Deployment,
		},
	}
	labelSelector := labels.SelectorFromSet(labelMap)
	options := &client.ListOptions{
		LabelSelector: labelSelector,
	}
	err := k.cache.List(ctx, options, deploymentList)
	if err != nil {
		if IsK8sObjectDoesNotExist(err) {
			err := k.client.List(ctx, options, deploymentList)
			if err != nil {
				logger.Warnf(ctx, "Failed to list deployments %v", err)
				return nil, err
			}
		}
		logger.Warnf(ctx, "Failed to list deployments from cache %v", err)
		return nil, err
	}
	return deploymentList, nil
}

func (k *Cluster) GetServicesWithLabel(ctx context.Context, namespace string, labelMap map[string]string) (*coreV1.ServiceList, error) {
	serviceList := &coreV1.ServiceList{
		TypeMeta: metav1.TypeMeta{
			APIVersion: coreV1.SchemeGroupVersion.String(),
			Kind:       Service,
		},
	}
	labelSelector := labels.SelectorFromSet(labelMap)
	options := &client.ListOptions{
		LabelSelector: labelSelector,
	}
	err := k.cache.List(ctx, options, serviceList)
	if err != nil {
		if IsK8sObjectDoesNotExist(err) {
			err := k.client.List(ctx, options, serviceList)
			if err != nil {
				logger.Warnf(ctx, "Failed to list services %v", err)
				return nil, err
			}
		}
		logger.Warnf(ctx, "Failed to list services from cache %v", err)
		return nil, err
	}
	return serviceList, nil
}

func (k *Cluster) CreateK8Object(ctx context.Context, object runtime.Object) error {
	objCreate := object.DeepCopyObject()
	return k.client.Create(ctx, objCreate)
}

func (k *Cluster) UpdateK8Object(ctx context.Context, object runtime.Object) error {
	objUpdate := object.DeepCopyObject()
	return k.client.Update(ctx, objUpdate)
}

func (k *Cluster) DeleteK8Object(ctx context.Context, object runtime.Object) error {
	objDelete := object.DeepCopyObject()
	return k.client.Delete(ctx, objDelete)
}
