package k8

import (
	"context"

	"k8s.io/apimachinery/pkg/api/errors"

	"github.com/lyft/flinkk8soperator/pkg/controller/config"
	"github.com/lyft/flytestdlib/logger"
	"github.com/lyft/flytestdlib/promutils"
	"github.com/lyft/flytestdlib/promutils/labeled"
	v1 "k8s.io/api/apps/v1"
	coreV1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
	GetService(ctx context.Context, namespace string, name string, version string) (*coreV1.Service, error)
	GetServicesWithLabel(ctx context.Context, namespace string, labelMap map[string]string) (*coreV1.ServiceList, error)

	CreateK8Object(ctx context.Context, object runtime.Object) error
	UpdateK8Object(ctx context.Context, object runtime.Object) error
	DeleteK8Object(ctx context.Context, object runtime.Object) error

	UpdateStatus(ctx context.Context, object runtime.Object) error
}

func NewK8Cluster(mgr manager.Manager, cfg config.RuntimeConfig) ClusterInterface {
	metrics := newK8ClusterMetrics(cfg.MetricsScope)
	return &Cluster{
		cache:   mgr.GetCache(),
		client:  mgr.GetClient(),
		metrics: metrics,
	}
}

func newK8ClusterMetrics(scope promutils.Scope) *k8ClusterMetrics {
	k8ClusterScope := scope.NewSubScope("k8_cluster")
	return &k8ClusterMetrics{
		scope:                  k8ClusterScope,
		createSuccess:          labeled.NewCounter("create_success", "K8 object created successfully", k8ClusterScope),
		createFailure:          labeled.NewCounter("create_failure", "K8 object creation failed", k8ClusterScope),
		updateSuccess:          labeled.NewCounter("update_success", "K8 object updated successfully", k8ClusterScope),
		updateFailure:          labeled.NewCounter("update_failure", "K8 object update failed", k8ClusterScope),
		updateConflicts:        labeled.NewCounter("update_conflict", "K8 object update failed due to a conflict", k8ClusterScope),
		updateInvalidVersion:   labeled.NewCounter("update_invalide_version", "K8 object update failed due to an invalid version", k8ClusterScope),
		deleteSuccess:          labeled.NewCounter("delete_success", "K8 object deleted successfully", k8ClusterScope),
		deleteFailure:          labeled.NewCounter("delete_failure", "K8 object deletion failed", k8ClusterScope),
		getDeploymentCacheHit:  labeled.NewCounter("get_deployment_cache_hit", "Deployment fetched from cache", k8ClusterScope),
		getDeploymentCacheMiss: labeled.NewCounter("get_deployment_cache_miss", "Deployment not present in the cache", k8ClusterScope),
		getDeploymentFailure:   labeled.NewCounter("get_deployment_failure", "Get Deployment failed", k8ClusterScope),
	}
}

type Cluster struct {
	cache   cache.Cache
	client  client.Client
	metrics *k8ClusterMetrics
}

type k8ClusterMetrics struct {
	scope                  promutils.Scope
	createSuccess          labeled.Counter
	createFailure          labeled.Counter
	updateSuccess          labeled.Counter
	updateFailure          labeled.Counter
	updateConflicts        labeled.Counter
	updateInvalidVersion   labeled.Counter
	deleteSuccess          labeled.Counter
	deleteFailure          labeled.Counter
	getDeploymentCacheHit  labeled.Counter
	getDeploymentCacheMiss labeled.Counter
	getDeploymentFailure   labeled.Counter
}

func (k *Cluster) GetService(ctx context.Context, namespace string, name string, version string) (*coreV1.Service, error) {
	serviceName := name
	if version != "" {
		serviceName = name + "-" + version
	}
	service := &coreV1.Service{
		TypeMeta: metav1.TypeMeta{
			APIVersion: coreV1.SchemeGroupVersion.String(),
			Kind:       Service,
		},
	}
	key := types.NamespacedName{
		Name:      serviceName,
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

	namespaceOpt := client.InNamespace(namespace)
	matchLabel := client.MatchingLabels(labelMap)
	err := k.cache.List(ctx, deploymentList, namespaceOpt, matchLabel)
	if err == nil {
		k.metrics.getDeploymentCacheHit.Inc(ctx)
		return deploymentList, nil
	}
	if IsK8sObjectDoesNotExist(err) {
		k.metrics.getDeploymentCacheMiss.Inc(ctx)
		err := k.client.List(ctx, deploymentList, namespaceOpt, matchLabel)
		if err != nil {
			k.metrics.getDeploymentFailure.Inc(ctx)
			logger.Warnf(ctx, "Failed to list deployments %v", err)
			return nil, err
		}
		return deploymentList, nil
	}
	logger.Warnf(ctx, "Failed to list deployments from cache %v", err)
	return nil, err
}

func (k *Cluster) GetServicesWithLabel(ctx context.Context, namespace string, labelMap map[string]string) (*coreV1.ServiceList, error) {
	serviceList := &coreV1.ServiceList{
		TypeMeta: metav1.TypeMeta{
			APIVersion: coreV1.SchemeGroupVersion.String(),
			Kind:       Service,
		},
	}
	namespaceOpt := client.InNamespace(namespace)
	matchLabel := client.MatchingLabels(labelMap)

	err := k.cache.List(ctx, serviceList, namespaceOpt, matchLabel)
	if err != nil {
		if IsK8sObjectDoesNotExist(err) {
			err := k.client.List(ctx, serviceList, namespaceOpt, matchLabel)
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
	err := k.client.Create(ctx, objCreate)
	if err != nil {
		if !errors.IsAlreadyExists(err) {
			logger.Errorf(ctx, "K8s object creation failed %v", err)
			k.metrics.createFailure.Inc(ctx)
		}

		return err
	}
	k.metrics.createSuccess.Inc(ctx)
	return nil
}

func (k *Cluster) UpdateK8Object(ctx context.Context, object runtime.Object) error {
	objUpdate := object.DeepCopyObject()
	err := k.client.Update(ctx, objUpdate)
	if err != nil {
		if errors.IsConflict(err) {
			logger.Warnf(ctx, "Conflict while updating object")
			k.metrics.updateConflicts.Inc(ctx)
		} else {
			logger.Errorf(ctx, "K8s object update failed %v", err)
			k.metrics.updateFailure.Inc(ctx)
		}
		return err
	}
	k.metrics.updateSuccess.Inc(ctx)
	return nil
}

func (k *Cluster) UpdateStatus(ctx context.Context, object runtime.Object) error {
	objectCopy := object.DeepCopyObject()
	err := k.client.Status().Update(ctx, objectCopy)
	if err != nil {
		if errors.IsInvalid(err) {
			// This is a Kubernetes bug that has been fixed in k8s 1.15
			// https://github.com/kubernetes/kubernetes/pull/78713
			// The bug prevents status sub-resources from being updated when
			// the stored version of the CRD changes
			// Example of error:
			// K8s object update failed FlinkApplication.flink.k8s.io "operator-test-app" is invalid:
			// apiVersion: Invalid value: "flink.k8s.io/v1beta1": must be flink.k8s.io/v1beta1
			// app_name=operator-test-app ns=default phase=Running src="cluster.go:209"
			// This should only ever be encountered once (per application)
			// when a new CRD version is deployed and an older version of the application exists
			// As a workaround, we try to update the entire resource instead of only the status
			// TODO Remove this block when we upgrade to k8s 1.15
			logger.Warn(ctx, "Status sub-resource update failed, attempting to update the entire resource instead")
			k.metrics.updateInvalidVersion.Inc(ctx)
			updateErr := k.client.Update(ctx, object)
			if updateErr != nil {
				logger.Errorf(ctx, "K8s object update failed %v", updateErr)
				k.metrics.updateFailure.Inc(ctx)
				return updateErr
			}
		}
		if errors.IsConflict(err) {
			logger.Warnf(ctx, "Conflict while updating status")
			k.metrics.updateConflicts.Inc(ctx)
		} else {
			logger.Errorf(ctx, "K8s object update failed %v", err)
			k.metrics.updateFailure.Inc(ctx)
		}
		return err
	}
	k.metrics.updateSuccess.Inc(ctx)
	return nil
}

func (k *Cluster) DeleteK8Object(ctx context.Context, object runtime.Object) error {
	objDelete := object.DeepCopyObject()
	err := k.client.Delete(ctx, objDelete)
	if err != nil {
		logger.Errorf(ctx, "K8s object delete failed %v", err)
		k.metrics.deleteFailure.Inc(ctx)
		return err
	}
	k.metrics.deleteSuccess.Inc(ctx)
	return nil
}
