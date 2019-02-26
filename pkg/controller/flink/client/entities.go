package client

type SavepointStatus string

const (
	SavePointInvalid    SavepointStatus = ""
	SavePointInProgress SavepointStatus = "IN_PROGRESS"
	SavePointCompleted  SavepointStatus = "COMPLETED"
)

type FlinkJobStatus string

const (
	FlinkJobCreated    FlinkJobStatus = "CREATED"
	FlinkJobRunning    FlinkJobStatus = "RUNNING"
	FlinkJobFailing    FlinkJobStatus = "FAILING"
	FlinkJobFailed     FlinkJobStatus = "FAILED"
	FlinkJobCancelling FlinkJobStatus = "CANCELLING"
	FlinkJobCanceled   FlinkJobStatus = "CANCELED"
	FlinkJobFinished   FlinkJobStatus = "FINISHED"
)

type CancelJobRequest struct {
	CancelJob       bool   `json:"cancel-job"`
	TargetDirectory string `json:"target-directory,omitempty"`
}

type SubmitJobRequest struct {
	SavepointPath string `json:"savepointPath"`
	Parallelism   int32  `json:"parallelism"`
	ProgramArgs   string `json:"programArgs"`
	EntryClass    string `json:"entryClass"`
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
	FailureCause FailureCause `json"failure-cause"`
}

type FailureCause struct {
	Class      string `json:"class"`
	StackTrace string `json:"stack-trace"`
}

type CancelJobResponse struct {
	TriggerId string `json:"request-id"`
}

type SubmitJobResponse struct {
	JobId string `json:"jobid"`
}

type GetJobsResponse struct {
	Jobs []FlinkJob `json:"jobs"`
}

type JobConfigResponse struct {
	JobId           string             `json:"jid"`
	ExecutionConfig JobExecutionConfig `json:"execution-config"`
}

type JobExecutionConfig struct {
	Parallelism int32 `json:"job-parallelism"`
}

type FlinkJob struct {
	JobId  string         `json:"id"`
	Status FlinkJobStatus `json:"status"`
}

type ClusterOverviewResponse struct {
	TaskManagerCount uint `json:"taskmanagers"`
	SlotsAvailable   uint `json:"slots-available"`
}
