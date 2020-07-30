package flink

import (
	"context"
	"errors"
	"fmt"

	"github.com/lyft/flinkk8soperator/pkg/apis/app/v1beta1"
	"github.com/lyft/flinkk8soperator/pkg/controller/common"
	"github.com/lyft/flinkk8soperator/pkg/controller/config"
	"github.com/lyft/flinkk8soperator/pkg/controller/k8"
	"github.com/lyft/flytestdlib/logger"
	"github.com/lyft/flytestdlib/promutils"
	"github.com/lyft/flytestdlib/promutils/labeled"
	v1 "k8s.io/api/apps/v1"
	coreV1 "k8s.io/api/core/v1"
	k8_err "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metaV1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

const (
	JobManagerNameFormat                = "%s-%s-jm"
	JobManagerVersionNameFormat         = "%s-%s-%s-jm"
	JobManagerPodNameFormat             = "%s-%s-jm-pod"
	JobManagerServiceName               = "%s"
	JobManagerVersionServiceName        = "%s-%s"
	JobManagerContainerName             = "jobmanager"
	JobManagerArg                       = "jobmanager"
	JobManagerReadinessPath             = "/overview"
	JobManagerReadinessInitialDelaySec  = 10
	JobManagerReadinessTimeoutSec       = 1
	JobManagerReadinessSuccessThreshold = 1
	JobManagerReadinessFailureThreshold = 2
	JobManagerReadinessPeriodSec        = 5
)

const (
	FlinkRPCPortName            = "rpc"
	FlinkQueryPortName          = "query"
	FlinkBlobPortName           = "blob"
	FlinkUIPortName             = "ui"
	FlinkInternalMetricPortName = "metrics"
)

func VersionedJobManagerServiceName(app *v1beta1.FlinkApplication, hash string) string {
	return fmt.Sprintf("%s-%s", app.Name, hash)
}

type JobManagerControllerInterface interface {
	CreateIfNotExist(ctx context.Context, application *v1beta1.FlinkApplication) (bool, error)
}

func NewJobManagerController(k8sCluster k8.ClusterInterface, config config.RuntimeConfig) JobManagerControllerInterface {
	metrics := newJobManagerMetrics(config.MetricsScope)
	return &JobManagerController{
		k8Cluster: k8sCluster,
		metrics:   metrics,
	}
}

type JobManagerController struct {
	k8Cluster k8.ClusterInterface
	metrics   *jobManagerMetrics
}

func newJobManagerMetrics(scope promutils.Scope) *jobManagerMetrics {
	jobManagerControllerScope := scope.NewSubScope("job_manager_controller")
	return &jobManagerMetrics{
		scope:                     scope,
		deploymentCreationSuccess: labeled.NewCounter("deployment_create_success", "Job manager deployment created successfully", jobManagerControllerScope),
		deploymentCreationFailure: labeled.NewCounter("deployment_create_failure", "Job manager deployment creation failed", jobManagerControllerScope),
		serviceCreationSuccess:    labeled.NewCounter("service_create_success", "Job manager service created successfully", jobManagerControllerScope),
		serviceCreationFailure:    labeled.NewCounter("service_create_failure", "Job manager service creation failed", jobManagerControllerScope),
		ingressCreationSuccess:    labeled.NewCounter("ingress_create_success", "Job manager ingress created successfully", jobManagerControllerScope),
		ingressCreationFailure:    labeled.NewCounter("ingress_create_failure", "Job manager ingress creation failed", jobManagerControllerScope),
	}
}

type jobManagerMetrics struct {
	scope                     promutils.Scope
	deploymentCreationSuccess labeled.Counter
	deploymentCreationFailure labeled.Counter
	serviceCreationSuccess    labeled.Counter
	serviceCreationFailure    labeled.Counter
	ingressCreationSuccess    labeled.Counter
	ingressCreationFailure    labeled.Counter
}

func (j *JobManagerController) CreateIfNotExist(ctx context.Context, application *v1beta1.FlinkApplication) (bool, error) {
	hash := HashForApplication(application)
	newlyCreated := false

	jobManagerDeployment := FetchJobMangerDeploymentCreateObj(application, hash)
	podSelector := jobManagerDeployment.Spec.Selector.MatchLabels[PodDeploymentSelector]

	err := j.k8Cluster.CreateK8Object(ctx, jobManagerDeployment)
	if err != nil {
		if !k8_err.IsAlreadyExists(err) {
			j.metrics.deploymentCreationFailure.Inc(ctx)
			logger.Errorf(ctx, "Jobmanager deployment creation failed %v", err)
			return false, err
		}
		logger.Infof(ctx, "Jobmanager deployment already exists")

		labels := k8.GetAppLabel(application.Name)
		labels[FlinkAppHash] = hash

		existingDeployment, err := j.k8Cluster.GetDeploymentsWithLabel(ctx, application.Namespace, labels)
		if err != nil {
			return false, err
		}
		if len(existingDeployment.Items) == 0 {
			return false, errors.New("failed to create jobmanager deployment as it already exists, but" +
				" unable to find existing resource")
		}

		podSelector = existingDeployment.Items[0].Spec.Selector.MatchLabels[PodDeploymentSelector]
	} else {
		newlyCreated = true
		j.metrics.deploymentCreationSuccess.Inc(ctx)
	}

	// create the generic job manager service, used by the ingress to provide UI access
	// there will only be one of these across the lifetime of the application
	genericService := FetchJobManagerServiceCreateObj(application, podSelector)
	err = j.k8Cluster.CreateK8Object(ctx, genericService)
	if err != nil {
		if !k8_err.IsAlreadyExists(err) {
			j.metrics.serviceCreationFailure.Inc(ctx)
			logger.Errorf(ctx, "Jobmanager service creation failed %v", err)
			return false, err
		}
		logger.Infof(ctx, "Jobmanager service already exists")
	} else {
		newlyCreated = true
		j.metrics.serviceCreationSuccess.Inc(ctx)
	}

	// create the service for _this_ version of the flink application
	// this gives us a stable and reliable way to target a particular cluster during upgrades
	versionedJobManagerService := FetchJobManagerServiceCreateObj(application, podSelector)
	versionedJobManagerService.Name = VersionedJobManagerServiceName(application, hash)
	versionedJobManagerService.Labels[FlinkAppHash] = hash

	err = j.k8Cluster.CreateK8Object(ctx, versionedJobManagerService)
	if err != nil {
		if !k8_err.IsAlreadyExists(err) {
			j.metrics.serviceCreationFailure.Inc(ctx)
			logger.Errorf(ctx, "Versioned Jobmanager service creation failed %v", err)
			return false, err
		}
		logger.Infof(ctx, "Versioned Jobmanager service already exists")
	} else {
		newlyCreated = true
		j.metrics.serviceCreationSuccess.Inc(ctx)
	}

	if config.GetConfig().FlinkIngressURLFormat != "" {
		jobManagerIngress := FetchJobManagerIngressCreateObj(application)
		err = j.k8Cluster.CreateK8Object(ctx, jobManagerIngress)
		if err != nil {
			if !k8_err.IsAlreadyExists(err) {
				j.metrics.ingressCreationFailure.Inc(ctx)
				logger.Errorf(ctx, "Jobmanager ingress creation failed %v", err)
				return false, err
			}
			logger.Infof(ctx, "Jobmanager ingress already exists")
		} else {
			newlyCreated = true
			j.metrics.ingressCreationSuccess.Inc(ctx)
		}
	}

	return newlyCreated, nil
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

func getJobManagerPodName(application *v1beta1.FlinkApplication, hash string) string {
	applicationName := application.Name
	return fmt.Sprintf(JobManagerPodNameFormat, applicationName, hash)
}

func getJobManagerName(application *v1beta1.FlinkApplication, hash string) string {
	applicationName := application.Name
	if v1beta1.IsBlueGreenDeploymentMode(application.Status.DeploymentMode) {
		applicationVersion := application.Status.UpdatingVersion
		return fmt.Sprintf(JobManagerVersionNameFormat, applicationName, hash, applicationVersion)
	}
	return fmt.Sprintf(JobManagerNameFormat, applicationName, hash)
}

func FetchJobManagerServiceCreateObj(app *v1beta1.FlinkApplication, selector string) *coreV1.Service {
	jmServiceName := getJobManagerServiceName(app)
	serviceLabels := GetCommonAppLabels(app)
	serviceLabels[PodDeploymentSelector] = selector
	serviceLabels[FlinkDeploymentType] = FlinkDeploymentTypeJobmanager

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
			Labels: GetCommonAppLabels(app),
		},
		Spec: coreV1.ServiceSpec{
			Ports:    getJobManagerServicePorts(app),
			Selector: serviceLabels,
		},
	}
}

func getJobManagerServiceName(app *v1beta1.FlinkApplication) string {
	serviceName := app.Name
	versionName := app.Status.UpdatingVersion
	if v1beta1.IsBlueGreenDeploymentMode(app.Status.DeploymentMode) {
		return fmt.Sprintf(JobManagerVersionServiceName, serviceName, versionName)
	}
	return serviceName
}

func getJobManagerServicePorts(app *v1beta1.FlinkApplication) []coreV1.ServicePort {
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

func getJobManagerPorts(app *v1beta1.FlinkApplication) []coreV1.ContainerPort {
	return []coreV1.ContainerPort{
		{
			Name:          FlinkRPCPortName,
			ContainerPort: getRPCPort(app),
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
			ContainerPort: getUIPort(app),
		},
		{
			Name:          FlinkInternalMetricPortName,
			ContainerPort: getInternalMetricsQueryPort(app),
		},
	}
}

func FetchJobManagerContainerObj(application *v1beta1.FlinkApplication) *coreV1.Container {
	jmConfig := application.Spec.JobManagerConfig
	resources := jmConfig.Resources
	if resources == nil {
		resources = &JobManagerDefaultResources
	}

	ports := getJobManagerPorts(application)
	operatorEnv := GetFlinkContainerEnv(application)
	operatorEnv = append(operatorEnv, coreV1.EnvVar{
		Name:  FlinkDeploymentTypeEnv,
		Value: FlinkDeploymentTypeJobmanager,
	})
	operatorEnv = append(operatorEnv, jmConfig.EnvConfig.Env...)

	return &coreV1.Container{
		Name:            getFlinkContainerName(JobManagerContainerName),
		Image:           application.Spec.Image,
		ImagePullPolicy: ImagePullPolicy(application),
		Resources:       *resources,
		Args:            []string{JobManagerArg},
		Ports:           ports,
		Env:             operatorEnv,
		EnvFrom:         jmConfig.EnvConfig.EnvFrom,
		VolumeMounts:    application.Spec.VolumeMounts,
		ReadinessProbe: &coreV1.Probe{
			Handler: coreV1.Handler{
				HTTPGet: &coreV1.HTTPGetAction{
					Path: JobManagerReadinessPath,
					Port: intstr.FromInt(int(getUIPort(application))),
				},
			},
			InitialDelaySeconds: JobManagerReadinessInitialDelaySec,
			TimeoutSeconds:      JobManagerReadinessTimeoutSec,
			SuccessThreshold:    JobManagerReadinessSuccessThreshold,
			FailureThreshold:    JobManagerReadinessFailureThreshold,
			PeriodSeconds:       JobManagerReadinessPeriodSec,
		},
	}
}

func DeploymentIsJobmanager(deployment *v1.Deployment) bool {
	return deployment.Labels[FlinkDeploymentType] == FlinkDeploymentTypeJobmanager
}

// Translates a FlinkApplication into a JobManager deployment. Changes to this function must be
// made very carefully. Any new version v' that causes DeploymentsEqual(v(x), v'(x)) to be false
// will cause redeployments for all applications, and should be considered a breaking change that
// requires a new version of the CRD.
func jobmanagerTemplate(app *v1beta1.FlinkApplication) *v1.Deployment {
	labels := GetCommonAppLabels(app)
	labels = common.CopyMap(labels, app.Labels)
	labels[FlinkDeploymentType] = FlinkDeploymentTypeJobmanager

	podSelector := &metaV1.LabelSelector{
		MatchLabels: common.DuplicateMap(labels),
	}

	replicas := getJobmanagerReplicas(app)
	jobManagerContainer := FetchJobManagerContainerObj(app)

	deployment := &v1.Deployment{
		TypeMeta: metaV1.TypeMeta{
			APIVersion: v1.SchemeGroupVersion.String(),
			Kind:       k8.Deployment,
		},
		ObjectMeta: metaV1.ObjectMeta{
			Namespace:   app.Namespace,
			Labels:      labels,
			Annotations: getCommonAnnotations(app),
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
					Namespace:   app.Namespace,
					Labels:      common.DuplicateMap(labels),
					Annotations: common.DuplicateMap(app.Annotations),
				},
				Spec: coreV1.PodSpec{
					Containers: []coreV1.Container{
						*jobManagerContainer,
					},
					Volumes:          app.Spec.Volumes,
					ImagePullSecrets: app.Spec.ImagePullSecrets,
					NodeSelector:     app.Spec.JobManagerConfig.NodeSelector,
					Tolerations:      app.Spec.JobManagerConfig.Tolerations,
				},
			},
		},
	}

	serviceAccountName := getServiceAccountName(app)
	if serviceAccountName != "" {
		deployment.Spec.Template.Spec.ServiceAccountName = serviceAccountName
	}

	if app.Spec.SecurityContext != nil {
		deployment.Spec.Template.Spec.SecurityContext = app.Spec.SecurityContext
	}

	return deployment
}

func FetchJobMangerDeploymentCreateObj(app *v1beta1.FlinkApplication, hash string) *v1.Deployment {
	template := jobmanagerTemplate(app.DeepCopy())

	podDeploymentValue := RandomPodDeploymentSelector()

	template.Name = getJobManagerName(app, hash)
	template.Labels[FlinkAppHash] = hash
	template.Spec.Template.Annotations[FlinkAppHash] = hash
	template.Spec.Template.Labels[PodDeploymentSelector] = podDeploymentValue
	template.Spec.Selector.MatchLabels[PodDeploymentSelector] = podDeploymentValue
	template.Spec.Template.Name = getJobManagerPodName(app, hash)

	InjectOperatorCustomizedConfig(template, app, hash, FlinkDeploymentTypeJobmanager)

	return template
}

func JobManagerDeploymentMatches(deployment *v1.Deployment, application *v1beta1.FlinkApplication, hash string) bool {
	deploymentName := getJobManagerName(application, hash)
	return deployment.Name == deploymentName
}
