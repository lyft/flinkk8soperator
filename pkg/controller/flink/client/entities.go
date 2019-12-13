package client

type SavepointStatus string

const (
	SavePointInvalid    SavepointStatus = ""
	SavePointInProgress SavepointStatus = "IN_PROGRESS"
	SavePointCompleted  SavepointStatus = "COMPLETED"
)

type CheckpointStatus string

const (
	CheckpointInProgress CheckpointStatus = "IN_PROGRESS"
	CheckpointFailed     CheckpointStatus = "FAILED"
	CheckpointCompleted  CheckpointStatus = "COMPLETED"
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

type CancelJobRequest struct {
	CancelJob       bool   `json:"cancel-job"`
	TargetDirectory string `json:"target-directory,omitempty"`
}

type SubmitJobRequest struct {
	SavepointPath         string `json:"savepointPath"`
	Parallelism           int32  `json:"parallelism"`
	ProgramArgs           string `json:"programArgs"`
	EntryClass            string `json:"entryClass"`
	AllowNonRestoredState bool   `json:"allowNonRestoredState"`
}

type SavepointResponse struct {
	SavepointStatus SavepointStatusResponse    `json:"status"`
	Operation       SavepointOperationResponse `json:"operation"`
}

type SavepointStatusResponse struct {
	Status SavepointStatus `json:"id"`
}

type SavepointOperationResponse struct {
	Location     string       `json:"location"`
	FailureCause FailureCause `json:"failure-cause"`
}

type FailureCause struct {
	Class      string `json:"class"`
	StackTrace string `json:"stack-trace"`
}

type CancelJobResponse struct {
	TriggerID string `json:"request-id"`
}

type SubmitJobResponse struct {
	JobID string `json:"jobid"`
}

type GetJobsResponse struct {
	Jobs []FlinkJob `json:"jobs"`
}

type JobConfigResponse struct {
	JobID           string             `json:"jid"`
	ExecutionConfig JobExecutionConfig `json:"execution-config"`
}

type JobExecutionConfig struct {
	Parallelism int32 `json:"job-parallelism"`
}

type FlinkJob struct {
	JobID  string   `json:"id"`
	Status JobState `json:"status"`
}

type FlinkJobVertex struct {
	ID          string                 `json:"id"`
	Name        string                 `json:"name"`
	Parallelism int64                  `json:"parallelism"`
	Status      JobState               `json:"status"`
	StartTime   int64                  `json:"start-time"`
	EndTime     int64                  `json:"end-time"`
	Duration    int64                  `json:"duration"`
	Tasks       map[string]int64       `json:"tasks"`
	Metrics     map[string]interface{} `json:"metrics"`
}

type FlinkJobOverview struct {
	JobID     string           `json:"jid"`
	State     JobState         `json:"state"`
	StartTime int64            `json:"start-time"`
	EndTime   int64            `json:"end-time"`
	Vertices  []FlinkJobVertex `json:"vertices"`
}

type ClusterOverviewResponse struct {
	TaskManagerCount  int32 `json:"taskmanagers"`
	SlotsAvailable    int32 `json:"slots-available"`
	NumberOfTaskSlots int32 `json:"slots-total"`
}

type CheckpointStatistics struct {
	ID                 uint             `json:"id"`
	Status             CheckpointStatus `json:"status"`
	IsSavepoint        bool             `json:"is_savepoint"`
	TriggerTimestamp   int64            `json:"trigger_timestamp"`
	LatestAckTimestamp int64            `json:"latest_ack_timestamp"`
	StateSize          int64            `json:"state_size"`
	EndToEndDuration   int64            `json:"end_to_end_duration"`
	AlignmentBuffered  int64            `json:"alignment_buffered"`
	NumSubtasks        int64            `json:"num_subtasks"`
	FailureTimestamp   int64            `json:"failure_timestamp"`
	FailureMessage     string           `json:"failure_message"`
	ExternalPath       string           `json:"external_path"`
	Discarded          bool             `json:"discarded"`
	RestoredTimeStamp  int64            `json:"restore_timestamp"`
}

type LatestCheckpoints struct {
	Completed *CheckpointStatistics `json:"completed,omitempty"`
	Savepoint *CheckpointStatistics `json:"savepoint,omitempty"`
	Failed    *CheckpointStatistics `json:"failed,omitempty"`
	Restored  *CheckpointStatistics `json:"restored,omitempty"`
}

type CheckpointResponse struct {
	Counts  map[string]int32       `json:"counts"`
	Latest  LatestCheckpoints      `json:"latest"`
	History []CheckpointStatistics `json:"history"`
}

type TaskManagerStats struct {
	Path                   string `json:"path"`
	DataPort               int32  `json:"dataPort"`
	TimeSinceLastHeartbeat int64  `json:"timeSinceLastHeartbeat"`
	SlotsNumber            int32  `json:"slotsNumber"`
	FreeSlots              int32  `json:"freeSlots"`
}

type TaskManagersResponse struct {
	TaskManagers []TaskManagerStats `json:"taskmanagers"`
}
