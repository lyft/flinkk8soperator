package flink

import (
	"context"
	"fmt"

	"github.com/lyft/flinkk8soperator/pkg/apis/app/v1alpha1"
	"github.com/lyft/flinkk8soperator/pkg/controller/common"
	"github.com/lyft/flinkk8soperator/pkg/controller/k8"
	"github.com/lyft/flytestdlib/logger"
	"github.com/lyft/flytestdlib/promutils"
	"github.com/lyft/flytestdlib/promutils/labeled"
	"k8s.io/api/apps/v1"
	coreV1 "k8s.io/api/core/v1"
	k8_err "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metaV1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

const (
	JobManagerNameFormat                = "%s-%s-jm"
	JobManagerPodNameFormat             = "%s-%s-jm-pod"
	JobManagerContainerName             = "jobmanager"
	JobManagerArg                       = "jobmanager"
	JobManagerServiceNameFormat         = "%s-jm"
	JobManagerExternalServiceNameFormat = "%s-jm.%s"
	JobManagerReadinessPath             = "/config"
	JobManagerReadinessInitialDelaySec  = 10
	JobManagerReadinessTimeoutSec       = 1
	JobManagerReadinessSuccessThreshold = 1
	JobManagerReadinessFailureThreshold = 2
	JobManagerReadinessPeriodSec        = 5
	AppFrontEndKey                      = "frontend"
)

const (
	FlinkRpcPortName            = "rpc"
	FlinkQueryPortName          = "query"
	FlinkBlobPortName           = "blob"
	FlinkUIPortName             = "ui"
	FlinkInternalMetricPortName = "metrics"
)

type FlinkJobManagerControllerInterface interface {
	CreateIfNotExist(ctx context.Context, application *v1alpha1.FlinkApplication) error
}

func NewFlinkJobManagerController(scope promutils.Scope) FlinkJobManagerControllerInterface {
	metrics := newFlinkJobManagerMetrics(scope)
	return &FlinkJobManagerController{
		k8Cluster: k8.NewK8Cluster(),
		metrics:   metrics,
	}
}

func GetJobManagerExternalServiceName(app *v1alpha1.FlinkApplication) string {
	return fmt.Sprintf(JobManagerExternalServiceNameFormat, app.Name, app.Namespace)
}

type FlinkJobManagerController struct {
	k8Cluster k8.K8ClusterInterface
	metrics   *flinkJobManagerMetrics
}

func newFlinkJobManagerMetrics(scope promutils.Scope) *flinkJobManagerMetrics {
	jobManagerControllerScope := scope.NewSubScope("job_manager_controller")
	return &flinkJobManagerMetrics{
		scope:                     scope,
		deploymentCreationSuccess: labeled.NewCounter("deployment_create_success", "Job manager deployment created successfully", jobManagerControllerScope),
		deploymentCreationFailure: labeled.NewCounter("deployment_create_failure", "Job manager deployment creation failed", jobManagerControllerScope),
		serviceCreationSuccess:    labeled.NewCounter("service_create_success", "Job manager service created successfully", jobManagerControllerScope),
		serviceCreationFailure:    labeled.NewCounter("service_create_failure", "Job manager service creation failed", jobManagerControllerScope),
		ingressCreationSuccess:    labeled.NewCounter("ingress_create_success", "Job manager ingress created successfully", jobManagerControllerScope),
		ingressCreationFailure:    labeled.NewCounter("ingress_create_failure", "Job manager ingress creation failed", jobManagerControllerScope),
	}
}

type flinkJobManagerMetrics struct {
	scope                     promutils.Scope
	deploymentCreationSuccess labeled.Counter
	deploymentCreationFailure labeled.Counter
	serviceCreationSuccess    labeled.Counter
	serviceCreationFailure    labeled.Counter
	ingressCreationSuccess    labeled.Counter
	ingressCreationFailure    labeled.Counter
}

func (j *FlinkJobManagerController) CreateIfNotExist(ctx context.Context, application *v1alpha1.FlinkApplication) error {
	jobManagerDeployment, err := FetchJobMangerDeploymentCreateObj(application)
	if err != nil {
		return err
	}
	err = j.k8Cluster.CreateK8Object(ctx, jobManagerDeployment)
	if err != nil {
		if !k8_err.IsAlreadyExists(err) {
			j.metrics.deploymentCreationFailure.Inc(ctx)
			logger.Errorf(ctx, "Jobmanager deployment creation failed %v", err)
			return err
		}
		logger.Infof(ctx, "Jobmanager deployment already exists")
	} else {
		j.metrics.deploymentCreationSuccess.Inc(ctx)
	}

	jobManagerService := FetchJobManagerServiceCreateObj(application)
	err = j.k8Cluster.CreateK8Object(ctx, jobManagerService)
	if err != nil {
		if !k8_err.IsAlreadyExists(err) {
			j.metrics.serviceCreationFailure.Inc(ctx)
			logger.Errorf(ctx, "Jobmanager service creation failed %v", err)
			return err
		}
		logger.Infof(ctx, "Jobmanager service already exists")
	} else {
		j.metrics.serviceCreationSuccess.Inc(ctx)
	}
	jobManagerIngress := FetchJobManagerIngressCreateObj(application)
	err = j.k8Cluster.CreateK8Object(ctx, jobManagerIngress)
	if err != nil {
		if !k8_err.IsAlreadyExists(err) {
			j.metrics.ingressCreationFailure.Inc(ctx)
			logger.Errorf(ctx, "Jobmanager ingress creation failed %v", err)
			return err
		}
		logger.Infof(ctx, "Jobmanager ingress already exists")
	} else {
		j.metrics.ingressCreationSuccess.Inc(ctx)
	}

	return nil
}

var JobManagerDefaultResources = coreV1.ResourceRequirements{
	Requests: coreV1.ResourceList{
		coreV1.ResourceCPU:    resource.MustParse("4"),
		coreV1.ResourceMemory: resource.MustParse("3072Mi"),
	},
	Limits: coreV1.ResourceList{
		coreV1.ResourceCPU:    resource.MustParse("4"),
		coreV1.ResourceMemory: resource.MustParse("3072Mi"),
	},
}

func getJobManagerPodName(application *v1alpha1.FlinkApplication) string {
	applicationName := application.Name
	imageKey := k8.GetImageKey(application.Spec.Image)
	return fmt.Sprintf(JobManagerPodNameFormat, applicationName, imageKey)
}

func getJobManagerName(application *v1alpha1.FlinkApplication) string {
	applicationName := application.Name
	imageKey := k8.GetImageKey(application.Spec.Image)
	return fmt.Sprintf(JobManagerNameFormat, applicationName, imageKey)
}

func FetchJobManagerServiceCreateObj(app *v1alpha1.FlinkApplication) *coreV1.Service {
	jmServiceName := getJobManagerServiceName(app)
	serviceLabels := map[string]string{}

	serviceLabels[AppFrontEndKey] = jmServiceName
	return &coreV1.Service{
		TypeMeta: metaV1.TypeMeta{
			APIVersion: coreV1.SchemeGroupVersion.String(),
			Kind:       k8.Service,
		},
		ObjectMeta: metaV1.ObjectMeta{
			Name:      jmServiceName,
			Namespace: app.Namespace,
			OwnerReferences: []metaV1.OwnerReference{
				*metaV1.NewControllerRef(app, app.GroupVersionKind()),
			},
		},
		Spec: coreV1.ServiceSpec{
			Ports:    getJobManagerServicePorts(app),
			Selector: serviceLabels,
		},
	}
}

func getJobManagerServicePorts(app *v1alpha1.FlinkApplication) []coreV1.ServicePort {
	ports := getJobManagerPorts(app)
	servicePorts := make([]coreV1.ServicePort, 0, len(ports))
	for _, p := range ports {
		servicePorts = append(servicePorts, coreV1.ServicePort{
			Name: p.Name,
			Port: p.ContainerPort,
		})
	}
	return servicePorts
}

func getJobManagerPorts(app *v1alpha1.FlinkApplication) []coreV1.ContainerPort {
	return []coreV1.ContainerPort{
		{
			Name:          FlinkRpcPortName,
			ContainerPort: getRpcPort(app),
		},
		{
			Name:          FlinkBlobPortName,
			ContainerPort: getBlobPort(app),
		},
		{
			Name:          FlinkQueryPortName,
			ContainerPort: getQueryPort(app),
		},
		{
			Name:          FlinkUIPortName,
			ContainerPort: getUiPort(app),
		},
		{
			Name:          FlinkInternalMetricPortName,
			ContainerPort: getInternalMetricsQueryPort(app),
		},
	}
}

func FetchJobManagerContainerObj(application *v1alpha1.FlinkApplication) (*coreV1.Container, error) {
	jmConfig := application.Spec.JobManagerConfig
	resources := jmConfig.Resources
	if resources == nil {
		resources = &JobManagerDefaultResources
	}
	ports := getJobManagerPorts(application)
	operatorEnv, err := GetFlinkContainerEnv(application)
	if err != nil {
		return nil, err
	}
	operatorEnv = append(operatorEnv, jmConfig.Environment.Env...)

	return &coreV1.Container{
		Name:            getFlinkContainerName(JobManagerContainerName),
		Image:           application.Spec.Image,
		ImagePullPolicy: application.Spec.ImagePullPolicy,
		Resources:       *resources,
		Args:            []string{JobManagerArg},
		Ports:           ports,
		Env:             operatorEnv,
		EnvFrom:         jmConfig.Environment.EnvFrom,
		VolumeMounts:    application.Spec.VolumeMounts,
		ReadinessProbe: &coreV1.Probe{
			Handler: coreV1.Handler{
				HTTPGet: &coreV1.HTTPGetAction{
					Path: JobManagerReadinessPath,
					Port: intstr.FromInt(int(getUiPort(application))),
				},
			},
			InitialDelaySeconds: JobManagerReadinessInitialDelaySec,
			TimeoutSeconds:      JobManagerReadinessTimeoutSec,
			SuccessThreshold:    JobManagerReadinessSuccessThreshold,
			FailureThreshold:    JobManagerReadinessFailureThreshold,
			PeriodSeconds:       JobManagerReadinessPeriodSec,
		},
	}, nil
}

func FetchJobMangerDeploymentCreateObj(app *v1alpha1.FlinkApplication) (*v1.Deployment, error) {
	jmName := getJobManagerName(app)
	podName := getJobManagerPodName(app)

	podLabels := common.DuplicateMap(app.Labels)
	podLabels[AppFrontEndKey] = getJobManagerServiceName(app)
	commonLabels := getCommonAppLabels(app)
	podLabels = common.CopyMap(podLabels, commonLabels)
	podSelector := &metaV1.LabelSelector{
		MatchLabels: podLabels,
	}
	deploymentLabels := common.CopyMap(app.Labels, commonLabels)
	replicas := getJobmanagerReplicas(app)
	jobManagerContainer, err := FetchJobManagerContainerObj(app)
	if err != nil {
		return nil, err
	}
	return &v1.Deployment{
		TypeMeta: metaV1.TypeMeta{
			APIVersion: v1.SchemeGroupVersion.String(),
			Kind:       k8.Deployment,
		},
		ObjectMeta: metaV1.ObjectMeta{
			Name:        jmName,
			Namespace:   app.Namespace,
			Labels:      deploymentLabels,
			Annotations: app.Annotations,
			OwnerReferences: []metaV1.OwnerReference{
				*metaV1.NewControllerRef(app, app.GroupVersionKind()),
			},
		},
		Spec: v1.DeploymentSpec{
			Selector: podSelector,
			Strategy: v1.DeploymentStrategy{
				Type: v1.RecreateDeploymentStrategyType,
			},
			Replicas: &replicas,
			Template: coreV1.PodTemplateSpec{
				ObjectMeta: metaV1.ObjectMeta{
					Name:        podName,
					Namespace:   app.Namespace,
					Labels:      podLabels,
					Annotations: app.Annotations,
				},
				Spec: coreV1.PodSpec{
					Containers: []coreV1.Container{
						*jobManagerContainer,
					},
					Volumes:          app.Spec.Volumes,
					ImagePullSecrets: app.Spec.ImagePullSecrets,
				},
			},
		},
	}, nil
}

func getJobManagerCount(deployments []v1.Deployment, application *v1alpha1.FlinkApplication) int32 {
	jobManagerDeployment := getJobManagerDeployment(deployments, application)
	if jobManagerDeployment == nil {
		return 0
	}
	return *jobManagerDeployment.Spec.Replicas
}

func getJobManagerDeployment(deployments []v1.Deployment, application *v1alpha1.FlinkApplication) *v1.Deployment {
	jmDeploymentName := getJobManagerName(application)
	return k8.GetDeploymentWithName(deployments, jmDeploymentName)
}
