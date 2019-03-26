package flink

import (
	"context"
	"fmt"
	"math"

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
)

const (
	TaskManagerNameFormat     = "%s-%s-tm"
	TaskManagerPodNameFormat  = "%s-%s-tm-pod"
	TaskManagerContainerName  = "taskmanager"
	TaskManagerArg            = "taskmanager"
	TaskManagerHostnameEnvVar = "TASKMANAGER_HOSTNAME"
)

type FlinkTaskManagerControllerInterface interface {
	CreateIfNotExist(ctx context.Context, application *v1alpha1.FlinkApplication) error
}

func NewFlinkTaskManagerController(scope promutils.Scope) FlinkJobManagerControllerInterface {
	metrics := newFlinkTaskManagerMetrics(scope)
	return &FlinkTaskManagerController{
		k8Cluster: k8.NewK8Cluster(),
		metrics:   metrics,
	}
}

type FlinkTaskManagerController struct {
	k8Cluster k8.K8ClusterInterface
	metrics   *flinkTaskManagerMetrics
}

func newFlinkTaskManagerMetrics(scope promutils.Scope) *flinkTaskManagerMetrics {
	taskManagerControllerScope := scope.NewSubScope("task_manager_controller")
	return &flinkTaskManagerMetrics{
		scope:                     scope,
		deploymentCreationSuccess: labeled.NewCounter("deployment_create_success", "Task manager deployment created successfully", taskManagerControllerScope),
		deploymentCreationFailure: labeled.NewCounter("deployment_create_failure", "Task manager deployment creation failed", taskManagerControllerScope),
	}
}

type flinkTaskManagerMetrics struct {
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

func (t *FlinkTaskManagerController) CreateIfNotExist(ctx context.Context, application *v1alpha1.FlinkApplication) error {
	taskManagerDeployment, err := FetchTaskMangerDeploymentCreateObj(application)
	if err != nil {
		return err
	}
	err = t.k8Cluster.CreateK8Object(ctx, taskManagerDeployment)
	if err != nil {
		if !k8_err.IsAlreadyExists(err) {
			logger.Errorf(ctx, "Taskmanager deployment creation failed %v", err)
			t.metrics.deploymentCreationFailure.Inc(ctx)
			return err
		}
		logger.Infof(ctx, "Taskmanager deployment already exists")
	} else {
		t.metrics.deploymentCreationSuccess.Inc(ctx)

	}

	return nil
}

func getDeploymentWithName(deployments []v1.Deployment, name string) *v1.Deployment {
	if len(deployments) == 0 {
		return nil
	}
	for _, deployment := range deployments {
		if deployment.Name == name {
			return &deployment
		}
	}
	return nil
}

func getTaskManagerDeployment(deployments []v1.Deployment, application *v1alpha1.FlinkApplication) *v1.Deployment {
	tmDeploymentName := getTaskManagerName(*application)
	return getDeploymentWithName(deployments, tmDeploymentName)
}

func getJobManagerDeployment(deployments []v1.Deployment, application *v1alpha1.FlinkApplication) *v1.Deployment {
	jmDeploymentName := getJobManagerName(application)
	return getDeploymentWithName(deployments, jmDeploymentName)
}

func getTaskManagerReplicaCount(deployments []v1.Deployment, application *v1alpha1.FlinkApplication) int32 {
	taskManagerDeployment := getTaskManagerDeployment(deployments, application)
	if taskManagerDeployment == nil {
		return 0
	}
	return *taskManagerDeployment.Spec.Replicas
}

func getJobManagerReplicaCount(deployments []v1.Deployment, application *v1alpha1.FlinkApplication) int32 {
	jobManagerDeployment := getJobManagerDeployment(deployments, application)
	if jobManagerDeployment == nil {
		return 0
	}
	return *jobManagerDeployment.Spec.Replicas
}

func GetTaskManagerPorts(app *v1alpha1.FlinkApplication) []coreV1.ContainerPort {
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
			Name:          FlinkInternalMetricPortName,
			ContainerPort: getInternalMetricsQueryPort(app),
		},
	}
}

func FetchTaskManagerContainerObj(application *v1alpha1.FlinkApplication) (*coreV1.Container, error) {
	tmConfig := application.Spec.TaskManagerConfig
	ports := GetTaskManagerPorts(application)
	resources := tmConfig.Resources
	if resources == nil {
		resources = &TaskManagerDefaultResources
	}
	operatorEnv, err := GetFlinkContainerEnv(application)

	operatorEnv = append(operatorEnv, coreV1.EnvVar{
		Name: TaskManagerHostnameEnvVar,
		ValueFrom: &coreV1.EnvVarSource{
			FieldRef: &coreV1.ObjectFieldSelector{
				FieldPath: "status.podIP",
			},
		},
	})

	if err != nil {
		return nil, err
	}
	operatorEnv = append(operatorEnv, tmConfig.Environment.Env...)

	return &coreV1.Container{
		Name:            getFlinkContainerName(TaskManagerContainerName),
		Image:           application.Spec.Image,
		ImagePullPolicy: application.Spec.ImagePullPolicy,
		Resources:       *resources,
		Args:            []string{TaskManagerArg},
		Ports:           ports,
		Env:             operatorEnv,
		EnvFrom:         tmConfig.Environment.EnvFrom,
		VolumeMounts:    application.Spec.VolumeMounts,
	}, nil
}

func getTaskManagerPodName(application v1alpha1.FlinkApplication) string {
	applicationName := application.Name
	imageKey := k8.GetImageKey(application.Spec.Image)
	return fmt.Sprintf(TaskManagerPodNameFormat, applicationName, imageKey)
}

func getTaskManagerName(application v1alpha1.FlinkApplication) string {
	applicationName := application.Name
	imageKey := k8.GetImageKey(application.Spec.Image)
	return fmt.Sprintf(TaskManagerNameFormat, applicationName, imageKey)
}

func computeTaskManagerReplicas(application *v1alpha1.FlinkApplication) int32 {
	slots := getTaskmanagerSlots(application)
	parallelism := application.Spec.FlinkJob.Parallelism
	return int32(math.Ceil(float64(parallelism) / float64(slots)))
}

func FetchTaskMangerDeploymentCreateObj(app *v1alpha1.FlinkApplication) (*v1.Deployment, error) {
	taskName := getTaskManagerName(*app)
	podName := getTaskManagerPodName(*app)

	commonLabels := getCommonAppLabels(app)
	labels := common.CopyMap(app.Labels, commonLabels)

	podSelector := &metaV1.LabelSelector{
		MatchLabels: labels,
	}

	taskContainer, err := FetchTaskManagerContainerObj(app)
	if err != nil {
		return nil, err
	}

	replicas := computeTaskManagerReplicas(app)
	return &v1.Deployment{
		TypeMeta: metaV1.TypeMeta{
			APIVersion: v1.SchemeGroupVersion.String(),
			Kind:       k8.Deployment,
		},
		ObjectMeta: metaV1.ObjectMeta{
			Name:        taskName,
			Namespace:   app.Namespace,
			Labels:      labels,
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
					Labels:      labels,
					Annotations: app.Annotations,
				},
				Spec: coreV1.PodSpec{
					Containers: []coreV1.Container{
						*taskContainer,
					},
					Volumes: app.Spec.Volumes,
					ImagePullSecrets: app.Spec.ImagePullSecrets,
				},
			},
		},
	}, nil
}
