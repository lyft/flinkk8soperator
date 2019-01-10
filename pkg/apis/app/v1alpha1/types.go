package v1alpha1

import (
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

type FlinkApplication struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata"`
	Spec              FlinkApplicationSpec   `json:"spec"`
	Status            FlinkApplicationStatus `json:"status,omitempty"`
}

type FlinkApplicationSpec struct {
	Image             string            `json:"image,omitempty" protobuf:"bytes,2,opt,name=image"`
	ImagePullPolicy   v1.PullPolicy     `json:"imagePullPolicy,omitempty" protobuf:"bytes,14,opt,name=imagePullPolicy,casttype=PullPolicy"`
	TaskManagerConfig TaskManagerConfig `json:"task_manager_config,omitempty"`
	JobManagerConfig  JobManagerConfig  `json:"job_manager_config,omitempty"`
	FlinkJob          FlinkJobInfo      `json:"flink_job"`
	DeploymentMode    DeploymentMode    `json:"deployment_mode"`
	RpcPort           *int32            `json:"rpc_port,omitempty"`
	BlobPort          *int32            `json:"blob_port,omitempty"`
	QueryPort         *int32            `json:"query_port,omitempty"`
	UiPort            *int32            `json:"ui_port,omitempty"`
}

type FlinkJobInfo struct {
	JarName       string        `json:"jar_name"`
	Parallelism   int32         `json:"parallelism"`
	EntryClass    string        `json:"entry_class,omitempty"`
	ProgramArgs   string        `json:"program_args,omitempty"`
	SavepointInfo SavepointInfo `json:"savepoint_info,omitempty"`
}

type JobManagerConfig struct {
	Resources       *v1.ResourceRequirements `json:"resources,omitempty"`
	Environment     EnvironmentConfig        `json:"env_config"`
	JobManagerCount int32                    `json:"job_manager_count"`
}

type TaskManagerConfig struct {
	Resources        *v1.ResourceRequirements `json:"resources,omitempty"`
	Environment      EnvironmentConfig        `json:"env_config"`
	TaskManagerCount int32                    `json:"task_manager_count"`
}

type EnvironmentConfig struct {
	EnvFrom []v1.EnvFromSource `json:"envFrom,omitempty"`
	Env     []v1.EnvVar        `json:"env,omitempty"`
}

type SavepointInfo struct {
	SavepointLocation string `json:"savepoint_location,omitempty"`
	TriggerId         string `json:"trigger_id,omitempty"`
}

type FlinkApplicationStatus struct {
	Phase         FlinkApplicationPhase `json:"phase"`
	StartedAt     *metav1.Time          `json:"started_at,omitempty"`
	StoppedAt     *metav1.Time          `json:"stopped_at,omitempty"`
	LastUpdatedAt *metav1.Time          `json:"last_updated_at,omitempty"`
	Reason        string                `json:"reason,omitempty"`
	ActiveJobId   string                `json:"job_id,omitempty"`
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
	return p == FlinkApplicationStopped || p == FlinkApplicationFailed
}

const (
	FlinkApplicationNew             FlinkApplicationPhase = ""
	FlinkApplicationClusterStarting FlinkApplicationPhase = "Starting"
	FlinkApplicationReady           FlinkApplicationPhase = "Ready"
	FlinkApplicationRunning         FlinkApplicationPhase = "Running"
	FlinkApplicationSavepointing    FlinkApplicationPhase = "Savepointing"
	FlinkApplicationUpdating        FlinkApplicationPhase = "Updating"
	FlinkApplicationFailed          FlinkApplicationPhase = "Failed"
	FlinkApplicationStopped         FlinkApplicationPhase = "Stopped"
)

type DeploymentMode string

const (
	DeploymentModeSingle DeploymentMode = "Single"
	DeploymentModeDual   DeploymentMode = "Dual"
)
