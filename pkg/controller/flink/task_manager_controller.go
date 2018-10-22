package flink

import (
	"fmt"

	"context"

	"github.com/lyft/flinkk8soperator/pkg/apis/app/v1alpha1"
	"github.com/lyft/flinkk8soperator/pkg/controller/common"
	"github.com/lyft/flinkk8soperator/pkg/controller/k8"
	"k8s.io/api/apps/v1"
	coreV1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metaV1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	TaskManagerNameFormat    = "%s-%s-tm"
	TaskManagerPodNameFormat = "%s-%s-tm-pod"
	TaskManagerContainerName = "taskmanager"
	TaskManagerArg           = "taskmanager"
	TaskManagerProcessRole   = "taskmanager"
)

type FlinkTaskManagerControllerInterface interface {
	CreateIfNotExist(ctx context.Context, application *v1alpha1.FlinkApplication) error
}

func NewFlinkTaskManagerController() FlinkJobManagerControllerInterface {
	return &FlinkTaskManagerController{
		k8Cluster: k8.NewK8Cluster(),
	}
}

type FlinkTaskManagerController struct {
	k8Cluster k8.K8ClusterInterface
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
		if !errors.IsAlreadyExists(err) {
			return err
		}
	}
	return nil
}

func getTaskManagerDeployment(deployments []v1.Deployment, application *v1alpha1.FlinkApplication) *v1.Deployment {
	taskDeploymentName := getTaskManagerName(*application)
	if len(deployments) == 0 {
		return nil
	}
	for _, deployment := range deployments {
		if deployment.Name == taskDeploymentName {
			return &deployment
		}
	}
	return nil
}

func getTaskManagerReplicaCount(deployments []v1.Deployment, application *v1alpha1.FlinkApplication) int32 {
	taskManagerDeployment := getTaskManagerDeployment(deployments, application)
	if taskManagerDeployment == nil {
		return 0
	}

	return *taskManagerDeployment.Spec.Replicas
}

func GetTaskManagerPorts(flinkJob *v1alpha1.FlinkApplicationSpec) []coreV1.ContainerPort {
	return []coreV1.ContainerPort{
		containerPort(FlinkRpcPortName, flinkJob.RpcPort, FlinkRpcDefaultPort),
		containerPort(FlinkBlobPortName, flinkJob.BlobPort, FlinkBlobDefaultPort),
		containerPort(FlinkQueryPortName, flinkJob.QueryPort, FlinkQueryDefaultPort),
	}
}

func FetchTaskManagerContainerObj(application *v1alpha1.FlinkApplication) (*coreV1.Container, error) {
	var env []coreV1.EnvVar

	tmConfig := application.Spec.TaskManagerConfig
	ports := GetTaskManagerPorts(&application.Spec)
	if len(tmConfig.Env) != 0 {
		env = tmConfig.Env
	}

	containerEnv, err := GetFlinkContainerEnv(*application)
	if err != nil {
		return nil, err
	}
	env = append(env, containerEnv...)

	resources := application.Spec.TaskManagerConfig.Resources
	if resources == nil {
		resources = &TaskManagerDefaultResources
	}
	return &coreV1.Container{
		Name:      TaskManagerContainerName,
		Image:     application.Spec.Image,
		Resources: *resources,
		Args:      []string{TaskManagerArg},
		Ports:     ports,
		Env:       env,
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

func FetchTaskMangerDeploymentCreateObj(app *v1alpha1.FlinkApplication) (*v1.Deployment, error) {
	taskName := getTaskManagerName(*app)
	podName := getTaskManagerPodName(*app)
	tmReplicas := app.Spec.NumberTaskManagers

	commonLabels := getCommonAppLabels(*app)
	labels := common.CopyMap(app.Labels, commonLabels)

	podSelector := &metaV1.LabelSelector{
		MatchLabels: labels,
	}

	taskContainer, err := FetchTaskManagerContainerObj(app)
	if err != nil {
		return nil, err
	}

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
			Replicas: &tmReplicas,
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
				},
			},
		},
	}, nil
}
