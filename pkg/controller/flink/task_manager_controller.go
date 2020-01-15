package flink

import (
	"context"
	"fmt"
	"math"

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
)

const (
	TaskManagerNameFormat     = "%s-%s-tm"
	TaskManagerPodNameFormat  = "%s-%s-tm-pod"
	TaskManagerContainerName  = "taskmanager"
	TaskManagerArg            = "taskmanager"
	TaskManagerHostnameEnvVar = "TASKMANAGER_HOSTNAME"
)

type TaskManagerControllerInterface interface {
	CreateIfNotExist(ctx context.Context, application *v1beta1.FlinkApplication) (bool, error)
}

func NewTaskManagerController(k8sCluster k8.ClusterInterface, config config.RuntimeConfig) TaskManagerControllerInterface {
	metrics := newTaskManagerMetrics(config.MetricsScope)
	return &TaskManagerController{
		k8Cluster: k8sCluster,
		metrics:   metrics,
	}
}

type TaskManagerController struct {
	k8Cluster k8.ClusterInterface
	metrics   *taskManagerMetrics
}

func newTaskManagerMetrics(scope promutils.Scope) *taskManagerMetrics {
	taskManagerControllerScope := scope.NewSubScope("task_manager_controller")
	return &taskManagerMetrics{
		scope:                     scope,
		deploymentCreationSuccess: labeled.NewCounter("deployment_create_success", "Task manager deployment created successfully", taskManagerControllerScope),
		deploymentCreationFailure: labeled.NewCounter("deployment_create_failure", "Task manager deployment creation failed", taskManagerControllerScope),
	}
}

type taskManagerMetrics struct {
	scope                     promutils.Scope
	deploymentCreationSuccess labeled.Counter
	deploymentCreationFailure labeled.Counter
}

var TaskManagerDefaultResources = coreV1.ResourceRequirements{
	Requests: coreV1.ResourceList{
		coreV1.ResourceCPU:    resource.MustParse("2"),
		coreV1.ResourceMemory: resource.MustParse("1024Mi"),
	},
	Limits: coreV1.ResourceList{
		coreV1.ResourceCPU:    resource.MustParse("2"),
		coreV1.ResourceMemory: resource.MustParse("1024Mi"),
	},
}

func (t *TaskManagerController) CreateIfNotExist(ctx context.Context, application *v1beta1.FlinkApplication) (bool, error) {
	hash := HashForApplication(application)

	taskManagerDeployment := FetchTaskMangerDeploymentCreateObj(application, hash)
	err := t.k8Cluster.CreateK8Object(ctx, taskManagerDeployment)
	if err != nil {
		if !k8_err.IsAlreadyExists(err) {
			logger.Errorf(ctx, "Taskmanager deployment creation failed %v", err)
			t.metrics.deploymentCreationFailure.Inc(ctx)
			return false, err
		}
		logger.Infof(ctx, "Taskmanager deployment already exists")
	} else {
		t.metrics.deploymentCreationSuccess.Inc(ctx)
		return true, nil
	}

	return false, nil
}

func GetTaskManagerPorts(app *v1beta1.FlinkApplication) []coreV1.ContainerPort {
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
			Name:          FlinkInternalMetricPortName,
			ContainerPort: getInternalMetricsQueryPort(app),
		},
	}
}

func FetchTaskManagerContainerObj(application *v1beta1.FlinkApplication) *coreV1.Container {
	tmConfig := application.Spec.TaskManagerConfig
	ports := GetTaskManagerPorts(application)
	resources := tmConfig.Resources
	if resources == nil {
		resources = &TaskManagerDefaultResources
	}

	operatorEnv := GetFlinkContainerEnv(application)
	operatorEnv = append(operatorEnv, coreV1.EnvVar{
		Name:  FlinkDeploymentTypeEnv,
		Value: FlinkDeploymentTypeTaskmanager,
	})

	operatorEnv = append(operatorEnv, tmConfig.EnvConfig.Env...)

	return &coreV1.Container{
		Name:            getFlinkContainerName(TaskManagerContainerName),
		Image:           application.Spec.Image,
		ImagePullPolicy: ImagePullPolicy(application),
		Resources:       *resources,
		Args:            []string{TaskManagerArg},
		Ports:           ports,
		Env:             operatorEnv,
		EnvFrom:         tmConfig.EnvConfig.EnvFrom,
		VolumeMounts:    application.Spec.VolumeMounts,
	}
}

func getTaskManagerPodName(application *v1beta1.FlinkApplication, hash string) string {
	applicationName := application.Name
	return fmt.Sprintf(TaskManagerPodNameFormat, applicationName, hash)
}

func getTaskManagerName(application *v1beta1.FlinkApplication, hash string) string {
	applicationName := application.Name
	return fmt.Sprintf(TaskManagerNameFormat, applicationName, hash)
}

func computeTaskManagerReplicas(application *v1beta1.FlinkApplication) int32 {
	slots := getTaskmanagerSlots(application)
	parallelism := application.Spec.Parallelism
	return int32(math.Ceil(float64(parallelism) / float64(slots)))
}

func DeploymentIsTaskmanager(deployment *v1.Deployment) bool {
	return deployment.Labels[FlinkDeploymentType] == FlinkDeploymentTypeTaskmanager
}

// Translates a FlinkApplication into a TaskManager deployment. Changes to this function must be
// made very carefully. Any new version v' that causes DeploymentsEqual(v(x), v'(x)) to be false
// will cause redeployments for all applications, and should be considered a breaking change that
// requires a new version of the CRD.
func taskmanagerTemplate(app *v1beta1.FlinkApplication) *v1.Deployment {
	labels := getCommonAppLabels(app)
	labels = common.CopyMap(labels, app.Labels)
	labels[FlinkDeploymentType] = FlinkDeploymentTypeTaskmanager

	podSelector := &metaV1.LabelSelector{
		MatchLabels: labels,
	}

	taskContainer := FetchTaskManagerContainerObj(app)

	replicas := computeTaskManagerReplicas(app)

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
					Labels:      labels,
					Annotations: app.Annotations,
				},
				Spec: coreV1.PodSpec{
					Containers: []coreV1.Container{
						*taskContainer,
					},
					Volumes:          app.Spec.Volumes,
					ImagePullSecrets: app.Spec.ImagePullSecrets,
					NodeSelector:     app.Spec.TaskManagerConfig.NodeSelector,
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

func FetchTaskMangerDeploymentCreateObj(app *v1beta1.FlinkApplication, hash string) *v1.Deployment {
	template := taskmanagerTemplate(app.DeepCopy())

	template.Name = getTaskManagerName(app, hash)
	template.Labels[FlinkAppHash] = hash
	template.Spec.Template.Labels[FlinkAppHash] = hash
	template.Spec.Selector.MatchLabels[FlinkAppHash] = hash
	template.Spec.Template.Name = getTaskManagerPodName(app, hash)

	InjectOperatorCustomizedConfig(template, app, hash, FlinkDeploymentTypeTaskmanager)

	return template
}

func TaskManagerDeploymentMatches(deployment *v1.Deployment, application *v1beta1.FlinkApplication, hash string) bool {
	deploymentName := getTaskManagerName(application, hash)
	return deployment.Name == deploymentName
}
