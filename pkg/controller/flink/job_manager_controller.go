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
	v1 "k8s.io/api/apps/v1"
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
	FlinkRPCPortName            = "rpc"
	FlinkQueryPortName          = "query"
	FlinkBlobPortName           = "blob"
	FlinkUIPortName             = "ui"
	FlinkInternalMetricPortName = "metrics"
)

type JobManagerControllerInterface interface {
	CreateIfNotExist(ctx context.Context, application *v1alpha1.FlinkApplication) error
}

func NewJobManagerController(scope promutils.Scope) JobManagerControllerInterface {
	metrics := newJobManagerMetrics(scope)
	return &JobManagerController{
		k8Cluster: k8.NewK8Cluster(),
		metrics:   metrics,
	}
}

func GetJobManagerExternalServiceName(app *v1alpha1.FlinkApplication) string {
	return fmt.Sprintf(JobManagerExternalServiceNameFormat, app.Name, app.Namespace)
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

func (j *JobManagerController) CreateIfNotExist(ctx context.Context, application *v1alpha1.FlinkApplication) error {
	jobManagerDeployment := FetchJobMangerDeploymentCreateObj(application)
	err := j.k8Cluster.CreateK8Object(ctx, jobManagerDeployment)
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

func getJobManagerPodName(application *v1alpha1.FlinkApplication, hash string) string {
	applicationName := application.Name
	return fmt.Sprintf(JobManagerPodNameFormat, applicationName, hash)
}

func getJobManagerName(application *v1alpha1.FlinkApplication, hash string) string {
	applicationName := application.Name
	return fmt.Sprintf(JobManagerNameFormat, applicationName, hash)
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

func FetchJobManagerContainerObj(application *v1alpha1.FlinkApplication) *coreV1.Container {
	jmConfig := application.Spec.JobManagerConfig
	resources := jmConfig.Resources
	if resources == nil {
		resources = &JobManagerDefaultResources
	}

	ports := getJobManagerPorts(application)
	operatorEnv := GetFlinkContainerEnv(application)
	operatorEnv = append(operatorEnv, jmConfig.Environment.Env...)

	return &coreV1.Container{
		Name:            getFlinkContainerName(JobManagerContainerName),
		Image:           application.Spec.Image,
		ImagePullPolicy: ImagePullPolicy(application),
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
	return deployment.Labels[FlinkDeploymentType] == "jobmanager"
}

// Translates a FlinkApplication into a JobManager deployment. Changes to this function must be
// made very carefully. Any new version v' that causes DeploymentsEqual(v(x), v'(x)) to be false
// will cause redeployments for all applications, and should be considered a breaking change that
// requires a new version of the CRD.
func jobmanagerTemplate(app *v1alpha1.FlinkApplication) *v1.Deployment {
	podLabels := common.DuplicateMap(app.Labels)
	podLabels[AppFrontEndKey] = getJobManagerServiceName(app)
	commonLabels := getCommonAppLabels(app)
	podLabels = common.CopyMap(podLabels, commonLabels)
	podSelector := &metaV1.LabelSelector{
		MatchLabels: podLabels,
	}

	deploymentLabels := common.DuplicateMap(app.Labels)
	deploymentLabels[FlinkDeploymentType] = "jobmanager"
	deploymentLabels = common.CopyMap(deploymentLabels, commonLabels)
	replicas := getJobmanagerReplicas(app)
	jobManagerContainer := FetchJobManagerContainerObj(app)

	return &v1.Deployment{
		TypeMeta: metaV1.TypeMeta{
			APIVersion: v1.SchemeGroupVersion.String(),
			Kind:       k8.Deployment,
		},
		ObjectMeta: metaV1.ObjectMeta{
			Namespace:   app.Namespace,
			Labels:      deploymentLabels,
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
	}
}

func FetchJobMangerDeploymentCreateObj(app *v1alpha1.FlinkApplication) *v1.Deployment {
	template := jobmanagerTemplate(app.DeepCopy())
	hash := HashForApplication(app)

	template.Name = getJobManagerName(app, hash)
	template.Labels[FlinkAppHash] = hash
	template.Spec.Template.Labels[FlinkAppHash] = hash
	template.Spec.Selector.MatchLabels[FlinkAppHash] = hash
	template.Spec.Template.Name = getJobManagerPodName(app, hash)

	return template
}

func JobManagerDeploymentMatches(deployment *v1.Deployment, application *v1alpha1.FlinkApplication) bool {
	deploymentFromApp := FetchJobMangerDeploymentCreateObj(application)
	return DeploymentsEqual(deploymentFromApp, deployment)
}

func getJobManagerCount(deployments []v1.Deployment, application *v1alpha1.FlinkApplication) int32 {
	jobManagerDeployment := getJobManagerDeployment(deployments, application)
	if jobManagerDeployment == nil {
		return 0
	}
	return *jobManagerDeployment.Spec.Replicas
}

func getJobManagerDeployment(deployments []v1.Deployment, application *v1alpha1.FlinkApplication) *v1.Deployment {
	jmDeploymentName := getJobManagerName(application, HashForApplication(application))
	return k8.GetDeploymentWithName(deployments, jmDeploymentName)
}
