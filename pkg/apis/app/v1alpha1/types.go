package v1alpha1

import (
	"fmt"
	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

type FlinkApplicationList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata"`
	Items           []FlinkApplication `json:"items"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +genclient
// +genclient:noStatus
type FlinkApplication struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata"`
	Spec              FlinkApplicationSpec   `json:"spec"`
	Status            FlinkApplicationStatus `json:"status,omitempty"`
}

type FlinkApplicationSpec struct {
	Image             string                    `json:"image,omitempty" protobuf:"bytes,2,opt,name=image"`
	ImagePullPolicy   v1.PullPolicy             `json:"imagePullPolicy,omitempty" protobuf:"bytes,14,opt,name=imagePullPolicy,casttype=PullPolicy"`
	ImagePullSecrets  []v1.LocalObjectReference `json:"imagePullSecrets,omitempty" patchStrategy:"merge" patchMergeKey:"name" protobuf:"bytes,15,rep,name=imagePullSecrets"`
	FlinkConfig       FlinkConfig               `json:"flinkConfig"`
	TaskManagerConfig TaskManagerConfig         `json:"taskManagerConfig,omitempty"`
	JobManagerConfig  JobManagerConfig          `json:"jobManagerConfig,omitempty"`
	FlinkJob          FlinkJobInfo              `json:"flinkJob"`
	DeploymentMode    DeploymentMode            `json:"deploymentMode"`
	RpcPort           *int32                    `json:"rpcPort,omitempty"`
	BlobPort          *int32                    `json:"blobPort,omitempty"`
	QueryPort         *int32                    `json:"queryPort,omitempty"`
	UiPort            *int32                    `json:"uiPort,omitempty"`
	MetricsQueryPort  *int32                    `json:"metricsQueryPort,omitempty"`
	Volumes           []v1.Volume               `json:"volumes,omitempty"`
	VolumeMounts      []v1.VolumeMount          `json:"volumeMounts,omitempty"`
}

type FlinkConfig map[string]interface{}

// Workaround for https://github.com/kubernetes-sigs/kubebuilder/issues/528
func (in *FlinkConfig) DeepCopyInto(out *FlinkConfig) {
	if in == nil {
		*out = nil
	} else {
		*out = make(map[string]interface{}, len(*in))
		for k, v := range *in {
			(*out)[k] = deepCopyJSONValue(v)
		}
	}
}

func deepCopyJSONValue(x interface{}) interface{} {
	switch x := x.(type) {
	case map[string]interface{}:
		clone := make(map[string]interface{}, len(x))
		for k, v := range x {
			clone[k] = deepCopyJSONValue(v)
		}
		return clone
	case []interface{}:
		clone := make([]interface{}, len(x))
		for i, v := range x {
			clone[i] = deepCopyJSONValue(v)
		}
		return clone
	case string, int, uint, int32, uint32, int64, uint64, bool, float32, float64, nil:
		return x
	default:
		panic(fmt.Errorf("cannot deep copy %T", x))
	}
}

func (in *FlinkConfig) DeepCopy() *FlinkConfig {
	if in == nil {
		return nil
	}
	out := new(FlinkConfig)
	in.DeepCopyInto(out)
	return out
}

type FlinkJobInfo struct {
	JarName       string        `json:"jarName"`
	Parallelism   int32         `json:"parallelism"`
	EntryClass    string        `json:"entryClass,omitempty"`
	ProgramArgs   string        `json:"programArgs,omitempty"`
	SavepointInfo SavepointInfo `json:"savepointInfo,omitempty"`
}

type JobManagerConfig struct {
	Resources             *v1.ResourceRequirements `json:"resources,omitempty"`
	Environment           EnvironmentConfig        `json:"envConfig"`
	Replicas              *int32                   `json:"replicas,omitempty"`
	OffHeapMemoryFraction *float64                 `json:"offHeapMemoryFraction,omitempty"`
}

type TaskManagerConfig struct {
	Resources             *v1.ResourceRequirements `json:"resources,omitempty"`
	Environment           EnvironmentConfig        `json:"envConfig"`
	TaskSlots             *int32                   `json:"taskSlots,omitempty"`
	OffHeapMemoryFraction *float64                 `json:"offHeapMemoryFraction,omitempty"`

}

type EnvironmentConfig struct {
	EnvFrom []v1.EnvFromSource `json:"envFrom,omitempty"`
	Env     []v1.EnvVar        `json:"env,omitempty"`
}

type SavepointInfo struct {
	SavepointLocation string `json:"savepointLocation,omitempty"`
	TriggerId         string `json:"triggerId,omitempty"`
}

type FlinkApplicationStatus struct {
	Phase         FlinkApplicationPhase `json:"phase"`
	StartedAt     *metav1.Time          `json:"startedAt,omitempty"`
	StoppedAt     *metav1.Time          `json:"stoppedAt,omitempty"`
	LastUpdatedAt *metav1.Time          `json:"lastUpdatedAt,omitempty"`
	Reason        string                `json:"reason,omitempty"`
	JobId         string                `json:"jobId,omitempty"`
}

func (in *FlinkApplicationStatus) GetPhase() FlinkApplicationPhase {
	return in.Phase
}

func (in *FlinkApplicationStatus) UpdatePhase(phase FlinkApplicationPhase, reason string) {
	now := metav1.Now()
	if in.StartedAt == nil {
		in.StartedAt = &now
		in.LastUpdatedAt = &now
	}
	in.Reason = reason
	if phase.IsTerminal() {
		in.StoppedAt = &now
	}
	in.Phase = phase
}

func (in *FlinkApplicationStatus) TouchResource(reason string) {
	now := metav1.Now()
	in.LastUpdatedAt = &now
	in.Reason = reason
}

type FlinkApplicationPhase string

func (p FlinkApplicationPhase) IsTerminal() bool {
	return p == FlinkApplicationCompleted || p == FlinkApplicationFailed
}

func (p FlinkApplicationPhase) VerboseString() string {
	phaseName := fmt.Sprintf("%s", p)
	if p == FlinkApplicationNew {
		phaseName = "New"
	}
	return phaseName
}

// As you add more ApplicationPhase please add it to FlinkApplicationPhases list
const (
	FlinkApplicationNew             FlinkApplicationPhase = ""
	FlinkApplicationClusterStarting FlinkApplicationPhase = "Starting"
	FlinkApplicationReady           FlinkApplicationPhase = "Ready"
	FlinkApplicationRunning         FlinkApplicationPhase = "Running"
	FlinkApplicationSavepointing    FlinkApplicationPhase = "Savepointing"
	FlinkApplicationUpdating        FlinkApplicationPhase = "Updating"
	FlinkApplicationFailed          FlinkApplicationPhase = "Failed"
	FlinkApplicationCompleted       FlinkApplicationPhase = "Completed"
)

var FlinkApplicationPhases = []FlinkApplicationPhase{
	FlinkApplicationNew,
	FlinkApplicationClusterStarting,
	FlinkApplicationReady,
	FlinkApplicationRunning,
	FlinkApplicationSavepointing,
	FlinkApplicationUpdating,
	FlinkApplicationFailed,
	FlinkApplicationCompleted,
}

type DeploymentMode string

const (
	DeploymentModeSingle DeploymentMode = "Single"
	DeploymentModeDual   DeploymentMode = "Dual"
)
