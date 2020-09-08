package v1beta1

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
	Image              string                       `json:"image,omitempty" protobuf:"bytes,2,opt,name=image"`
	ImagePullPolicy    apiv1.PullPolicy             `json:"imagePullPolicy,omitempty" protobuf:"bytes,14,opt,name=imagePullPolicy,casttype=PullPolicy"`
	ImagePullSecrets   []apiv1.LocalObjectReference `json:"imagePullSecrets,omitempty" patchStrategy:"merge" patchMergeKey:"name" protobuf:"bytes,15,rep,name=imagePullSecrets"`
	ServiceAccountName string                       `json:"serviceAccountName,omitempty"`
	SecurityContext    *apiv1.PodSecurityContext    `json:"securityContext,omitempty"`
	FlinkConfig        FlinkConfig                  `json:"flinkConfig"`
	FlinkVersion       string                       `json:"flinkVersion"`
	TaskManagerConfig  TaskManagerConfig            `json:"taskManagerConfig,omitempty"`
	JobManagerConfig   JobManagerConfig             `json:"jobManagerConfig,omitempty"`
	JarName            string                       `json:"jarName"`
	Parallelism        int32                        `json:"parallelism"`
	EntryClass         string                       `json:"entryClass,omitempty"`
	ProgramArgs        string                       `json:"programArgs,omitempty"`
	// Deprecated: use SavepointPath instead
	SavepointInfo                  SavepointInfo       `json:"savepointInfo,omitempty"`
	SavepointPath                  string              `json:"savepointPath,omitempty"`
	SavepointDisabled              bool                `json:"savepointDisabled"`
	DeploymentMode                 DeploymentMode      `json:"deploymentMode,omitempty"`
	RPCPort                        *int32              `json:"rpcPort,omitempty"`
	BlobPort                       *int32              `json:"blobPort,omitempty"`
	QueryPort                      *int32              `json:"queryPort,omitempty"`
	UIPort                         *int32              `json:"uiPort,omitempty"`
	MetricsQueryPort               *int32              `json:"metricsQueryPort,omitempty"`
	Volumes                        []apiv1.Volume      `json:"volumes,omitempty"`
	VolumeMounts                   []apiv1.VolumeMount `json:"volumeMounts,omitempty"`
	RestartNonce                   string              `json:"restartNonce"`
	DeleteMode                     DeleteMode          `json:"deleteMode,omitempty"`
	ScaleMode                      ScaleMode           `json:"scaleMode,omitempty"`
	AllowNonRestoredState          bool                `json:"allowNonRestoredState,omitempty"`
	ForceRollback                  bool                `json:"forceRollback"`
	MaxCheckpointRestoreAgeSeconds *int32              `json:"maxCheckpointRestoreAgeSeconds,omitempty"`
	TearDownVersionHash            string              `json:"tearDownVersionHash,omitempty"`
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
	Resources *apiv1.ResourceRequirements `json:"resources,omitempty"`
	EnvConfig EnvironmentConfig           `json:"envConfig"`
	Replicas  *int32                      `json:"replicas,omitempty"`
	// Deprecated: use SystemMemoryFraction instead
	OffHeapMemoryFraction *float64           `json:"offHeapMemoryFraction,omitempty"`
	SystemMemoryFraction  *float64           `json:"systemMemoryFraction,omitempty"`
	NodeSelector          map[string]string  `json:"nodeSelector,omitempty"`
	Tolerations           []apiv1.Toleration `json:"tolerations,omitempty"`
}

type TaskManagerConfig struct {
	Resources *apiv1.ResourceRequirements `json:"resources,omitempty"`
	EnvConfig EnvironmentConfig           `json:"envConfig"`
	TaskSlots *int32                      `json:"taskSlots,omitempty"`
	// Deprecated: use SystemMemoryFraction instead
	OffHeapMemoryFraction *float64           `json:"offHeapMemoryFraction,omitempty"`
	SystemMemoryFraction  *float64           `json:"systemMemoryFraction,omitempty"`
	NodeSelector          map[string]string  `json:"nodeSelector,omitempty"`
	Tolerations           []apiv1.Toleration `json:"tolerations,omitempty"`
}

type EnvironmentConfig struct {
	EnvFrom []apiv1.EnvFromSource `json:"envFrom,omitempty"`
	Env     []apiv1.EnvVar        `json:"env,omitempty"`
}

type SavepointInfo struct {
	SavepointLocation string `json:"savepointLocation,omitempty"`
}

type FlinkClusterStatus struct {
	ClusterOverviewURL   string       `json:"clusterOverviewURL,omitempty"`
	Health               HealthStatus `json:"health,omitempty"`
	NumberOfTaskManagers int32        `json:"numberOfTaskManagers,omitempty"`
	HealthyTaskManagers  int32        `json:"healthyTaskManagers,omitempty"`
	NumberOfTaskSlots    int32        `json:"numberOfTaskSlots,omitempty"`
	AvailableTaskSlots   int32        `json:"availableTaskSlots"`
}

type FlinkJobStatus struct {
	JobOverviewURL string       `json:"jobOverviewURL,omitempty"`
	JobID          string       `json:"jobID,omitempty"`
	Health         HealthStatus `json:"health,omitempty"`
	State          JobState     `json:"state,omitempty"`

	JarName               string `json:"jarName"`
	Parallelism           int32  `json:"parallelism"`
	EntryClass            string `json:"entryClass,omitempty"`
	ProgramArgs           string `json:"programArgs,omitempty"`
	AllowNonRestoredState bool   `json:"allowNonRestoredState,omitempty"`

	StartTime                *metav1.Time `json:"startTime,omitempty"`
	JobRestartCount          int32        `json:"jobRestartCount,omitempty"`
	CompletedCheckpointCount int32        `json:"completedCheckpointCount,omitempty"`
	FailedCheckpointCount    int32        `json:"failedCheckpointCount,omitempty"`
	RestorePath              string       `json:"restorePath,omitempty"`
	RestoreTime              *metav1.Time `json:"restoreTime,omitempty"`
	LastFailingTime          *metav1.Time `json:"lastFailingTime,omitempty"`

	LastCheckpointPath string       `json:"lastCheckpoint,omitempty"`
	LastCheckpointTime *metav1.Time `json:"lastCheckpointTime,omitempty"`

	RunningTasks int32 `json:"runningTasks,omitempty"`
	TotalTasks   int32 `json:"totalTasks,omitempty"`
}

type FlinkApplicationStatus struct {
	Phase                  FlinkApplicationPhase           `json:"phase"`
	StartedAt              *metav1.Time                    `json:"startedAt,omitempty"`
	LastUpdatedAt          *metav1.Time                    `json:"lastUpdatedAt,omitempty"`
	Reason                 string                          `json:"reason,omitempty"`
	DeployVersion          FlinkApplicationVersion         `json:"deployVersion,omitempty"`
	UpdatingVersion        FlinkApplicationVersion         `json:"updatingVersion,omitempty"`
	ClusterStatus          FlinkClusterStatus              `json:"clusterStatus,omitempty"`
	JobStatus              FlinkJobStatus                  `json:"jobStatus,omitempty"`
	VersionStatuses        []FlinkApplicationVersionStatus `json:"versionStatuses,omitempty"`
	FailedDeployHash       string                          `json:"failedDeployHash,omitempty"`
	RollbackHash           string                          `json:"rollbackHash,omitempty"`
	DeployHash             string                          `json:"deployHash"`
	UpdatingHash           string                          `json:"updatingHash,omitempty"`
	TeardownHash           string                          `json:"teardownHash,omitempty"`
	InPlaceUpdatedFromHash string                          `json:"inPlaceUpdatedFromHash,omitempty"`
	SavepointTriggerID     string                          `json:"savepointTriggerId,omitempty"`
	SavepointPath          string                          `json:"savepointPath,omitempty"`
	RetryCount             int32                           `json:"retryCount,omitempty"`
	LastSeenError          *FlinkApplicationError          `json:"lastSeenError,omitempty"`
	// We store deployment mode in the status to prevent incompatible migrations from
	// Dual --> BlueGreen and BlueGreen --> Dual
	DeploymentMode DeploymentMode `json:"deploymentMode,omitempty"`
}

type FlinkApplicationVersion string

const (
	BlueFlinkApplication  FlinkApplicationVersion = "blue"
	GreenFlinkApplication FlinkApplicationVersion = "green"
)

type FlinkApplicationVersionStatus struct {
	Version       FlinkApplicationVersion `json:"appVersion,omitempty"`
	VersionHash   string                  `json:"versionHash,omitempty"`
	ClusterStatus FlinkClusterStatus      `json:"clusterStatus,omitempty"`
	JobStatus     FlinkJobStatus          `json:"jobStatus,omitempty"`
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
	FlinkApplicationRescaling       FlinkApplicationPhase = "Rescaling"
	FlinkApplicationClusterStarting FlinkApplicationPhase = "ClusterStarting"
	FlinkApplicationSubmittingJob   FlinkApplicationPhase = "SubmittingJob"
	FlinkApplicationRunning         FlinkApplicationPhase = "Running"
	FlinkApplicationSavepointing    FlinkApplicationPhase = "Savepointing"
	FlinkApplicationCancelling      FlinkApplicationPhase = "Cancelling"
	FlinkApplicationDeleting        FlinkApplicationPhase = "Deleting"
	FlinkApplicationRecovering      FlinkApplicationPhase = "Recovering"
	FlinkApplicationRollingBackJob  FlinkApplicationPhase = "RollingBackJob"
	FlinkApplicationDeployFailed    FlinkApplicationPhase = "DeployFailed"
	FlinkApplicationDualRunning     FlinkApplicationPhase = "DualRunning"
)

var FlinkApplicationPhases = []FlinkApplicationPhase{
	FlinkApplicationNew,
	FlinkApplicationUpdating,
	FlinkApplicationRescaling,
	FlinkApplicationClusterStarting,
	FlinkApplicationSubmittingJob,
	FlinkApplicationRunning,
	FlinkApplicationSavepointing,
	FlinkApplicationCancelling,
	FlinkApplicationDeleting,
	FlinkApplicationRecovering,
	FlinkApplicationDeployFailed,
	FlinkApplicationRollingBackJob,
	FlinkApplicationDualRunning,
}

func IsRunningPhase(phase FlinkApplicationPhase) bool {
	return phase == FlinkApplicationRunning || phase == FlinkApplicationDeployFailed
}

func IsBlueGreenDeploymentMode(mode DeploymentMode) bool {
	// Backaward compatibility between v1beta1 and v1beta1
	if mode == DeploymentModeDual {
		return false
	}
	return mode == DeploymentModeBlueGreen
}

func GetMaxRunningJobs(mode DeploymentMode) int32 {
	if IsBlueGreenDeploymentMode(mode) {
		return int32(2)
	}
	return int32(1)
}

type DeploymentMode string

const (
	DeploymentModeSingle    DeploymentMode = "Single"
	DeploymentModeDual      DeploymentMode = "Dual"
	DeploymentModeBlueGreen DeploymentMode = "BlueGreen"
)

type DeleteMode string

const (
	DeleteModeSavepoint   DeleteMode = "Savepoint"
	DeleteModeForceCancel DeleteMode = "ForceCancel"
	DeleteModeNone        DeleteMode = "None"
)

type ScaleMode string

const (
	ScaleModeNewCluster ScaleMode = "NewCluster"
	ScaleModeInPlace    ScaleMode = "InPlace"
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
	SavepointJob           FlinkMethod = "SavepointJob"
)
