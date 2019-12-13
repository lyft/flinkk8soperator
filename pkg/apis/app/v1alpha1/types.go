package v1alpha1

import (
	"fmt"

	apiv1 "k8s.io/api/core/v1"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

type FlinkApplicationList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata"`
	Items           []FlinkApplication `json:"items"`
}

// +genclient
// +genclient:noStatus
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:defaulter-gen=true
type FlinkApplication struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata"`
	Spec              FlinkApplicationSpec   `json:"spec"`
	Status            FlinkApplicationStatus `json:"status,omitempty"`
}

type FlinkApplicationSpec struct {
	Image                 string                       `json:"image,omitempty" protobuf:"bytes,2,opt,name=image"`
	ImagePullPolicy       apiv1.PullPolicy             `json:"imagePullPolicy,omitempty" protobuf:"bytes,14,opt,name=imagePullPolicy,casttype=PullPolicy"`
	ImagePullSecrets      []apiv1.LocalObjectReference `json:"imagePullSecrets,omitempty" patchStrategy:"merge" patchMergeKey:"name" protobuf:"bytes,15,rep,name=imagePullSecrets"`
	FlinkConfig           FlinkConfig                  `json:"flinkConfig"`
	FlinkVersion          string                       `json:"flinkVersion"`
	TaskManagerConfig     TaskManagerConfig            `json:"taskManagerConfig,omitempty"`
	JobManagerConfig      JobManagerConfig             `json:"jobManagerConfig,omitempty"`
	JarName               string                       `json:"jarName"`
	Parallelism           int32                        `json:"parallelism"`
	EntryClass            string                       `json:"entryClass,omitempty"`
	ProgramArgs           string                       `json:"programArgs,omitempty"`
	SavepointInfo         SavepointInfo                `json:"savepointInfo,omitempty"`
	DeploymentMode        DeploymentMode               `json:"deploymentMode,omitempty"`
	RPCPort               *int32                       `json:"rpcPort,omitempty"`
	BlobPort              *int32                       `json:"blobPort,omitempty"`
	QueryPort             *int32                       `json:"queryPort,omitempty"`
	UIPort                *int32                       `json:"uiPort,omitempty"`
	MetricsQueryPort      *int32                       `json:"metricsQueryPort,omitempty"`
	Volumes               []apiv1.Volume               `json:"volumes,omitempty"`
	VolumeMounts          []apiv1.VolumeMount          `json:"volumeMounts,omitempty"`
	RestartNonce          string                       `json:"restartNonce"`
	DeleteMode            DeleteMode                   `json:"deleteMode,omitempty"`
	AllowNonRestoredState bool                         `json:"allowNonRestoredState,omitempty"`
	ForceRollback         bool                         `json:"forceRollback"`
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

type JobManagerConfig struct {
	Resources             *apiv1.ResourceRequirements `json:"resources,omitempty"`
	EnvConfig             EnvironmentConfig           `json:"envConfig"`
	Replicas              *int32                      `json:"replicas,omitempty"`
	OffHeapMemoryFraction *float64                    `json:"offHeapMemoryFraction,omitempty"`
	NodeSelector          map[string]string           `json:"nodeSelector,omitempty"`
}

type TaskManagerConfig struct {
	Resources             *apiv1.ResourceRequirements `json:"resources,omitempty"`
	EnvConfig             EnvironmentConfig           `json:"envConfig"`
	TaskSlots             *int32                      `json:"taskSlots,omitempty"`
	OffHeapMemoryFraction *float64                    `json:"offHeapMemoryFraction,omitempty"`
	NodeSelector          map[string]string           `json:"nodeSelector,omitempty"`
}

type EnvironmentConfig struct {
	EnvFrom []apiv1.EnvFromSource `json:"envFrom,omitempty"`
	Env     []apiv1.EnvVar        `json:"env,omitempty"`
}

type SavepointInfo struct {
	SavepointLocation string `json:"savepointLocation,omitempty"`
	TriggerID         string `json:"triggerId,omitempty"`
}

type FlinkClusterStatus struct {
	Health               HealthStatus `json:"health,omitempty"`
	NumberOfTaskManagers int32        `json:"numberOfTaskManagers,omitempty"`
	HealthyTaskManagers  int32        `json:"healthyTaskManagers,omitempty"`
	NumberOfTaskSlots    int32        `json:"numberOfTaskSlots,omitempty"`
	AvailableTaskSlots   int32        `json:"availableTaskSlots"`
}

type FlinkJobStatus struct {
	JobID  string       `json:"jobID,omitempty"`
	Health HealthStatus `json:"health,omitempty"`
	State  JobState     `json:"state,omitempty"`

	JarName               string `json:"jarName"`
	Parallelism           int32  `json:"parallelism"`
	EntryClass            string `json:"entryClass,omitempty"`
	ProgramArgs           string `json:"programArgs,omitempty"`
	AllowNonRestoredState bool   `json:"allowNonRestoredState,omitempty"`

	StartTime                *metav1.Time `json:"startTime,omitempty"`
	JobRestartCount          int32        `json:"jobRestartCount,omitempty"`
	CompletedCheckpointCount int32        `json:"completedCheckpointCount,omitempty"`
	FailedCheckpointCount    int32        `json:"failedCheckpointCount,omitempty"`
	LastCheckpointTime       *metav1.Time `json:"lastCheckpointTime,omitempty"`
	RestorePath              string       `json:"restorePath,omitempty"`
	RestoreTime              *metav1.Time `json:"restoreTime,omitempty"`
	LastFailingTime          *metav1.Time `json:"lastFailingTime,omitempty"`
}

type FlinkApplicationStatus struct {
	Phase            FlinkApplicationPhase `json:"phase"`
	StartedAt        *metav1.Time          `json:"startedAt,omitempty"`
	LastUpdatedAt    *metav1.Time          `json:"lastUpdatedAt,omitempty"`
	Reason           string                `json:"reason,omitempty"`
	ClusterStatus    FlinkClusterStatus    `json:"clusterStatus,omitempty"`
	JobStatus        FlinkJobStatus        `json:"jobStatus"`
	FailedDeployHash string                `json:"failedDeployHash,omitempty"`
	RollbackHash     string                `json:"rollbackHash,omitempty"`
	DeployHash       string                `json:"deployHash"`
	RetryCount       int32                 `json:"retryCount,omitempty"`
	LastSeenError    FlinkApplicationError `json:"lastSeenError,omitempty"`
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
	in.Phase = phase
}

func (in *FlinkApplicationStatus) TouchResource(reason string) {
	now := metav1.Now()
	in.LastUpdatedAt = &now
	in.Reason = reason
}

type FlinkApplicationPhase string

func (p FlinkApplicationPhase) VerboseString() string {
	phaseName := string(p)
	if p == FlinkApplicationNew {
		phaseName = "New"
	}
	return phaseName
}

// As you add more ApplicationPhase please add it to FlinkApplicationPhases list
const (
	FlinkApplicationNew             FlinkApplicationPhase = ""
	FlinkApplicationUpdating        FlinkApplicationPhase = "Updating"
	FlinkApplicationClusterStarting FlinkApplicationPhase = "ClusterStarting"
	FlinkApplicationSubmittingJob   FlinkApplicationPhase = "SubmittingJob"
	FlinkApplicationRunning         FlinkApplicationPhase = "Running"
	FlinkApplicationSavepointing    FlinkApplicationPhase = "Savepointing"
	FlinkApplicationDeleting        FlinkApplicationPhase = "Deleting"
	FlinkApplicationRollingBackJob  FlinkApplicationPhase = "RollingBackJob"
	FlinkApplicationDeployFailed    FlinkApplicationPhase = "DeployFailed"
)

var FlinkApplicationPhases = []FlinkApplicationPhase{
	FlinkApplicationNew,
	FlinkApplicationUpdating,
	FlinkApplicationClusterStarting,
	FlinkApplicationSubmittingJob,
	FlinkApplicationRunning,
	FlinkApplicationSavepointing,
	FlinkApplicationDeleting,
	FlinkApplicationDeployFailed,
	FlinkApplicationRollingBackJob,
}

func IsRunningPhase(phase FlinkApplicationPhase) bool {
	return phase == FlinkApplicationRunning || phase == FlinkApplicationDeployFailed
}

type DeploymentMode string

const (
	DeploymentModeSingle DeploymentMode = "Single"
	DeploymentModeDual   DeploymentMode = "Dual"
)

type DeleteMode string

const (
	DeleteModeSavepoint   DeleteMode = "Savepoint"
	DeleteModeForceCancel DeleteMode = "ForceCancel"
	DeleteModeNone        DeleteMode = "None"
)

type HealthStatus string

const (
	Green  HealthStatus = "Green"
	Yellow HealthStatus = "Yellow"
	Red    HealthStatus = "Red"
)

type JobState string

const (
	Created     JobState = "CREATED"
	Running     JobState = "RUNNING"
	Failing     JobState = "FAILING"
	Failed      JobState = "FAILED"
	Cancelling  JobState = "CANCELLING"
	Canceled    JobState = "CANCELED"
	Finished    JobState = "FINISHED"
	Restarting  JobState = "RESTARTING"
	Suspended   JobState = "SUSPENDED"
	Reconciling JobState = "RECONCILING"
)

// FlinkApplicationError implements the error interface to make error handling more structured
type FlinkApplicationError struct {
	AppError            string       `json:"appError,omitempty"`
	Method              FlinkMethod  `json:"method,omitempty"`
	ErrorCode           string       `json:"errorCode,omitempty"`
	IsRetryable         bool         `json:"isRetryable,omitempty"`
	IsFailFast          bool         `json:"isFailFast,omitempty"`
	MaxRetries          int32        `json:"maxRetries,omitempty"`
	LastErrorUpdateTime *metav1.Time `json:"lastErrorUpdateTime,omitempty"`
}

func NewFlinkApplicationError(appError string, method FlinkMethod, errorCode string, isRetryable bool, isFailFast bool, maxRetries int32) *FlinkApplicationError {
	now := metav1.Now()
	return &FlinkApplicationError{AppError: appError, Method: method, ErrorCode: errorCode, IsRetryable: isRetryable, IsFailFast: isFailFast, MaxRetries: maxRetries, LastErrorUpdateTime: &now}
}

func (f *FlinkApplicationError) Error() string {
	return f.AppError
}

type FlinkMethod string

const (
	CancelJobWithSavepoint FlinkMethod = "CancelJobWithSavepoint"
	ForceCancelJob         FlinkMethod = "ForceCancelJob"
	SubmitJob              FlinkMethod = "SubmitJob"
	CheckSavepointStatus   FlinkMethod = "CheckSavepointStatus"
	GetJobs                FlinkMethod = "GetJobs"
	GetClusterOverview     FlinkMethod = "GetClusterOverview"
	GetLatestCheckpoint    FlinkMethod = "GetLatestCheckpoint"
	GetJobConfig           FlinkMethod = "GetJobConfig"
	GetTaskManagers        FlinkMethod = "GetTaskManagers"
	GetCheckpointCounts    FlinkMethod = "GetCheckpointCounts"
	GetJobOverview         FlinkMethod = "GetJobOverview"
)
