package helpers

import (
	"fmt"

	"github.com/lyft/flinkk8soperator/pkg/apis/app/v1alpha1"
	"k8s.io/api/apps/v1"
	coreV1 "k8s.io/api/core/v1"
	metaV1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	TaskManagerNameFormat    = "%s-tm"
	TaskManagerPodNameFormat = "%s-tm-pod"
	TaskManagerContainerName = "taskmanager"
	TaskManagerArg           = "taskmanager"
	TaskManagerProcessRole   = "taskmanager"
)

func GetTaskManagerPorts(flinkJob *v1alpha1.FlinkJobSpec) []coreV1.ContainerPort {
	return []coreV1.ContainerPort{
		containerPort(FlinkRpcPortName, flinkJob.RpcPort, FlinkRpcDefaultPort),
		containerPort(FlinkBlobPortName, flinkJob.BlobPort, FlinkBlobDefaultPort),
		containerPort(FlinkQueryPortName, flinkJob.QueryPort, FlinkQueryDefaultPort),
	}
}

func FetchTaskManagerContainerObj(job *v1alpha1.FlinkJob) coreV1.Container {
	var env []coreV1.EnvVar

	tmConfig := job.Spec.TaskManagerConfig
	ports := GetTaskManagerPorts(&job.Spec)
	if len(tmConfig.Env) != 0 {
		env = tmConfig.Env
	}
	// Add Default Values
	env = append(env, GetJobManagerServiceEnv(job.Name))

	return coreV1.Container{
		Name:      TaskManagerContainerName,
		Image:     job.Spec.Image,
		Resources: job.Spec.TaskManagerConfig.Resources,
		Args:      []string{TaskManagerArg},
		Ports:     ports,
		Env:       env,
		// TODO we might want to set imagePullPolicy
		// TODO we might want to have LivenessProbe and ReadinessProbe of type
		// livenessProbe:
		//          exec:
		//            command:
		//            - /etc/init.d/presto status | grep -q 'Running as'
		//          failureThreshold: 3
		//          periodSeconds: 300
		//          timeoutSeconds: 10
	}
}

func GetTaskManagerName(jobName string) string {
	return fmt.Sprintf(TaskManagerNameFormat, jobName)
}

func FetchTaskMangerDeploymentIdentityObj(job *v1alpha1.FlinkJob) *v1.Deployment {
	taskName := GetTaskManagerName(job.Name)

	return &v1.Deployment{
		TypeMeta: metaV1.TypeMeta{
			APIVersion: v1.SchemeGroupVersion.String(),
			Kind:       Deployment,
		},
		ObjectMeta: metaV1.ObjectMeta{
			Name:      taskName,
			Namespace: job.Namespace,
		},
	}
}

func FetchTaskMangerDeploymentCreateObj(job *v1alpha1.FlinkJob) *v1.Deployment {
	taskName := GetTaskManagerName(job.Name)
	podName := fmt.Sprintf(TaskManagerPodNameFormat, job.Name)
	tmReplicas := job.Spec.NumberTaskManagers

	podLabels := CopyMap(job.Labels)
	podLabels[FlinkProcessRoleKey] = TaskManagerProcessRole

	podSelector := &metaV1.LabelSelector{
		MatchLabels: podLabels,
	}

	return &v1.Deployment{
		TypeMeta: metaV1.TypeMeta{
			APIVersion: v1.SchemeGroupVersion.String(),
			Kind:       Deployment,
		},
		ObjectMeta: metaV1.ObjectMeta{
			Name:        taskName,
			Namespace:   job.Namespace,
			Labels:      job.Labels,
			Annotations: job.Annotations,
			OwnerReferences: []metaV1.OwnerReference{
				*metaV1.NewControllerRef(job, job.GroupVersionKind()),
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
					Namespace:   job.Namespace,
					Labels:      podLabels,
					Annotations: job.Annotations,
				},
				Spec: coreV1.PodSpec{
					Containers: []coreV1.Container{
						FetchTaskManagerContainerObj(job),
					},
				},
			},
		},
	}
}
