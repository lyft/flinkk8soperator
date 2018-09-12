package helpers

import (
	"fmt"

	"github.com/lyft/flinkk8soperator/pkg/apis/app/v1alpha1"
	"k8s.io/api/apps/v1"
	coreV1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metaV1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	JobManagerNameFormat        = "%s-jm"
	JobManagerPodNameFormat     = "%s-jm-pod"
	JobManagerContainerName     = "jobmanager"
	JobManagerArg               = "jobmanager"
	JobManagerNumReplicas       = int32(1)
	JobManagerServiceNameFormat = "%s-jm"
	JobManagerServiceEnvVar     = "JOB_MANAGER_RPC_ADDRESS"
	JobManagerProcessRole       = "jobmanager"
)

const (
	FlinkProcessRoleKey   = "flink-component"
	FlinkRpcPortName      = "rpc"
	FlinkQueryPortName    = "query"
	FlinkBlobPortName     = "blob"
	FlinkUIPortName       = "ui"
	FlinkRpcDefaultPort   = 6123
	FlinkQueryDefaultPort = 6124
	FlinkBlobDefaultPort  = 6125
	FlinkUIDefaultPort    = 8081
)

var JobManagerDefaultResources = coreV1.ResourceRequirements{
	Requests: coreV1.ResourceList{
		coreV1.ResourceCPU:    resource.MustParse("250m"),
		coreV1.ResourceMemory: resource.MustParse("1024Mi"),
	},
	Limits: coreV1.ResourceList{
		coreV1.ResourceCPU:    resource.MustParse("250m"),
		coreV1.ResourceMemory: resource.MustParse("1024Mi"),
	},
}

func containerPort(name string, optionalPort *int32, defaultPort int32) coreV1.ContainerPort {
	if optionalPort == nil {
		return coreV1.ContainerPort{
			Name:          name,
			ContainerPort: defaultPort,
		}
	}
	return coreV1.ContainerPort{
		Name:          name,
		ContainerPort: *optionalPort,
	}
}

func GetJobManagerServiceEnv(name string) coreV1.EnvVar {
	return coreV1.EnvVar{
		Name:  JobManagerServiceEnvVar,
		Value: fmt.Sprintf(JobManagerServiceNameFormat, name),
	}
}

func FetchJobManagerServiceIdentityObj(job *v1alpha1.FlinkJob) *coreV1.Service {
	return &coreV1.Service{
		TypeMeta: metaV1.TypeMeta{
			APIVersion: coreV1.SchemeGroupVersion.String(),
			Kind:       Service,
		},
		ObjectMeta: metaV1.ObjectMeta{
			Name:      GetJobManagerName(job.Name),
			Namespace: job.Namespace,
		},
	}
}

func FetchJobManagerServiceCreateObj(job *v1alpha1.FlinkJob) *coreV1.Service {
	podLabels := CopyMap(job.Labels)
	podLabels[FlinkProcessRoleKey] = JobManagerProcessRole

	return &coreV1.Service{
		TypeMeta: metaV1.TypeMeta{
			APIVersion: coreV1.SchemeGroupVersion.String(),
			Kind:       Service,
		},
		ObjectMeta: metaV1.ObjectMeta{
			Name:      GetJobManagerName(job.Name),
			Namespace: job.Namespace,
			OwnerReferences: []metaV1.OwnerReference{
				*metaV1.NewControllerRef(job, job.GroupVersionKind()),
			},
		},
		Spec: coreV1.ServiceSpec{
			Ports:    GetJobManagerServicePorts(&job.Spec),
			Selector: podLabels,
		},
	}
}

func GetJobManagerServicePorts(flinkJob *v1alpha1.FlinkJobSpec) []coreV1.ServicePort {
	ports := GetJobManagerPorts(flinkJob)
	servicePorts := make([]coreV1.ServicePort, 0, len(ports))
	for _, p := range ports {
		servicePorts = append(servicePorts, coreV1.ServicePort{
			Name: p.Name,
			Port: p.ContainerPort,
		})
	}
	return servicePorts
}

func GetJobManagerPorts(flinkJob *v1alpha1.FlinkJobSpec) []coreV1.ContainerPort {
	return []coreV1.ContainerPort{
		containerPort(FlinkRpcPortName, flinkJob.RpcPort, FlinkRpcDefaultPort),
		containerPort(FlinkBlobPortName, flinkJob.BlobPort, FlinkBlobDefaultPort),
		containerPort(FlinkQueryPortName, flinkJob.QueryPort, FlinkQueryDefaultPort),
		containerPort(FlinkUIPortName, flinkJob.UiPort, FlinkUIDefaultPort),
	}
}

func CreateJobManagerContainer(job *v1alpha1.FlinkJob) coreV1.Container {
	resources := &JobManagerDefaultResources
	var env []coreV1.EnvVar

	jmConfig := job.Spec.JobManagerConfig
	ports := GetJobManagerPorts(&job.Spec)
	if jmConfig != nil {
		if jmConfig.Resources != nil {
			resources = job.Spec.JobManagerConfig.Resources
		}
		if len(jmConfig.Env) != 0 {
			env = jmConfig.Env
		}

	}
	// Add Default Values
	env = append(env, GetJobManagerServiceEnv(job.Name))
	return coreV1.Container{
		Name:      JobManagerContainerName,
		Image:     job.Spec.Image,
		Resources: *resources,
		Args:      []string{JobManagerArg},
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

func GetJobManagerName(jobName string) string {
	return fmt.Sprintf(JobManagerNameFormat, jobName)
}

func FetchJobMangerDeploymentIdentityObj(job *v1alpha1.FlinkJob) *v1.Deployment {
	jmName := GetJobManagerName(job.Name)

	return &v1.Deployment{
		TypeMeta: metaV1.TypeMeta{
			APIVersion: v1.SchemeGroupVersion.String(),
			Kind:       Deployment,
		},
		ObjectMeta: metaV1.ObjectMeta{
			Name:      jmName,
			Namespace: job.Namespace,
		},
	}
}

func FetchJobMangerDeploymentCreateObj(job *v1alpha1.FlinkJob) *v1.Deployment {
	jmName := GetJobManagerName(job.Name)
	podName := fmt.Sprintf(JobManagerPodNameFormat, job.Name)
	jmReplicas := JobManagerNumReplicas

	podLabels := CopyMap(job.Labels)
	podLabels[FlinkProcessRoleKey] = JobManagerProcessRole

	podSelector := &metaV1.LabelSelector{
		MatchLabels: podLabels,
	}

	return &v1.Deployment{
		TypeMeta: metaV1.TypeMeta{
			APIVersion: v1.SchemeGroupVersion.String(),
			Kind:       Deployment,
		},
		ObjectMeta: metaV1.ObjectMeta{
			Name:        jmName,
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
			Replicas: &jmReplicas,
			Template: coreV1.PodTemplateSpec{
				ObjectMeta: metaV1.ObjectMeta{
					Name:        podName,
					Namespace:   job.Namespace,
					Labels:      podLabels,
					Annotations: job.Annotations,
				},
				Spec: coreV1.PodSpec{
					Containers: []coreV1.Container{
						CreateJobManagerContainer(job),
					},
				},
			},
		},
	}
}
