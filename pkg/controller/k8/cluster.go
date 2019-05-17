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
	GetDeploymentsWithLabel(ctx context.Context, namespace string, labelMap map[string]string) (*v1.DeploymentList, error)
	GetService(ctx context.Context, namespace string, name string) (*coreV1.Service, error)
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
		logger.Warnf(ctx, "Failed to get service %v", err)
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
		if IsK8sObjectNotExists(err) {
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

func (k *Cluster) CreateK8Object(ctx context.Context, object runtime.Object) error {
	return k.client.Create(ctx, object)
}

func (k *Cluster) UpdateK8Object(ctx context.Context, object runtime.Object) error {
	return k.client.Update(ctx, object)
}

func (k *Cluster) DeleteK8Object(ctx context.Context, object runtime.Object) error {
	return k.client.Delete(ctx, object)
}
