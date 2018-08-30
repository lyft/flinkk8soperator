package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/api/core/v1"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

type FlinkJobList struct {
	metav1.TypeMeta  `json:",inline"`
	metav1.ListMeta  `json:"metadata"`
	Items []FlinkJob `json:"items"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

type FlinkJob struct {
	metav1.TypeMeta       `json:",inline"`
	metav1.ObjectMeta     `json:"metadata"`
	Spec   FlinkJobSpec   `json:"spec"`
	Status FlinkJobStatus `json:"status,omitempty"`
}

// Actual FinkJobSpec.
// TODO Determine things that can be updated
type FlinkJobSpec struct {
	Image              string            `json:"image,omitempty" protobuf:"bytes,2,opt,name=image"`
	DefaultParallelism int32             `json:"default_parallelism"`
	NumberTaskManagers int32             `json:"number_task_managers"`
	TaskManagerConfig  TaskManagerConfig `json:"task_manager_config,omitempty"`
	JobManagerConfig   *JobManagerConfig `json:"job_manager_config,omitempty"`
	RpcPort            *int32            `json:"rpc_port,omitempty"`
	BlobPort           *int32            `json:"blob_port,omitempty"`
	QueryPort          *int32            `json:"query_port,omitempty"`
	UiPort             *int32            `json:"ui_port,omitempty"`
}

type JobManagerConfig struct {
	Resources *v1.ResourceRequirements `json:"resources,omitempty"`
	Env       []v1.EnvVar              `json:"env"`
}

type TaskManagerConfig struct {
	Resources v1.ResourceRequirements `json:"resources,omitempty"`
	Env       []v1.EnvVar             `json:"env"`
	// TODO Is there a default. Whats the behavior if this is 0
	NumberSlots int32 `json:"number_slots"`
}

type FlinkJobStatus struct {
	Phase         FlinkJobPhase `json:"phase"`
	StartedAt     *metav1.Time  `json:"started_at,omitempty"`
	StoppedAt     *metav1.Time  `json:"stopped_at,omitempty"`
	LastUpdatedAt *metav1.Time  `json:"last_updated_at,omitempty"`
	Reason        string        `json:"reason,omitempty"`
}

func (in *FlinkJobStatus) GetPhase() FlinkJobPhase {
	return in.Phase
}

func (in *FlinkJobStatus) UpdatePhase(phase FlinkJobPhase, reason string) {
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

func (in *FlinkJobStatus) TouchResource(reason string) {
	now := metav1.Now()
	in.LastUpdatedAt = &now
	in.Reason = reason
}

type FlinkJobPhase string

func (p FlinkJobPhase) IsTerminal() bool {
	return p == FlinkJobStopped || p == FlinkJobFailed
}

const (
	FlinkJobNew           FlinkJobPhase = ""
	FlinkJobRunning       FlinkJobPhase = "Running"
	FlinkJobCheckpointing FlinkJobPhase = "Checkpointing"
	FlinkJobUpdating      FlinkJobPhase = "Updating"
	FlinkJobFailed        FlinkJobPhase = "Failed"
	FlinkJobStopped       FlinkJobPhase = "Stopped"
)
