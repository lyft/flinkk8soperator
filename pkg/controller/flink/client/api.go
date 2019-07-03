package client

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"net/http"

	"github.com/go-resty/resty"
	"github.com/lyft/flinkk8soperator/pkg/controller/config"
	"github.com/lyft/flytestdlib/logger"
	"github.com/lyft/flytestdlib/promutils"
	"github.com/lyft/flytestdlib/promutils/labeled"
	"github.com/pkg/errors"
)

const submitJobURL = "/jars/%s/run"
const savepointURL = "/jobs/%s/savepoints"
const jobURL = "/jobs/%s"
const checkSavepointStatusURL = "/jobs/%s/savepoints/%s"
const getJobsURL = "/jobs"
const getJobsOverviewURL = "/jobs/%s"
const getJobConfigURL = "/jobs/%s/config"
const getOverviewURL = "/overview"
const checkpointsURL = "/jobs/%s/checkpoints"
const taskmanagersURL = "/taskmanagers"
const httpGet = "GET"
const httpPost = "POST"
const httpPatch = "PATCH"
const retryCount = 3
const httpGetTimeOut = 5 * time.Second
const defaultTimeOut = 1 * time.Minute

type FlinkAPIInterface interface {
	CancelJobWithSavepoint(ctx context.Context, url string, jobID string) (string, error)
	ForceCancelJob(ctx context.Context, url string, jobID string) error
	SubmitJob(ctx context.Context, url string, jarID string, submitJobRequest SubmitJobRequest) (*SubmitJobResponse, error)
	CheckSavepointStatus(ctx context.Context, url string, jobID, triggerID string) (*SavepointResponse, error)
	GetJobs(ctx context.Context, url string) (*GetJobsResponse, error)
	GetClusterOverview(ctx context.Context, url string) (*ClusterOverviewResponse, error)
	GetLatestCheckpoint(ctx context.Context, url string, jobID string) (*CheckpointStatistics, error)
	GetJobConfig(ctx context.Context, url string, jobID string) (*JobConfigResponse, error)
	GetTaskManagers(ctx context.Context, url string) (*TaskManagersResponse, error)
	GetCheckpointCounts(ctx context.Context, url string, jobID string) (*CheckpointResponse, error)
	GetJobOverview(ctx context.Context, url string, jobID string) (*FlinkJobOverview, error)
}

type FlinkJobManagerClient struct {
	metrics *flinkJobManagerClientMetrics
}

type flinkJobManagerClientMetrics struct {
	scope                        promutils.Scope
	submitJobSuccessCounter      labeled.Counter
	submitJobFailureCounter      labeled.Counter
	cancelJobSuccessCounter      labeled.Counter
	cancelJobFailureCounter      labeled.Counter
	forceCancelJobSuccessCounter labeled.Counter
	forceCancelJobFailureCounter labeled.Counter
	checkSavepointSuccessCounter labeled.Counter
	checkSavepointFailureCounter labeled.Counter
	getJobsSuccessCounter        labeled.Counter
	getJobsFailureCounter        labeled.Counter
	getJobConfigSuccessCounter   labeled.Counter
	getJobConfigFailureCounter   labeled.Counter
	getClusterSuccessCounter     labeled.Counter
	getClusterFailureCounter     labeled.Counter
	getCheckpointsSuccessCounter labeled.Counter
	getCheckpointsFailureCounter labeled.Counter
}

func newFlinkJobManagerClientMetrics(scope promutils.Scope) *flinkJobManagerClientMetrics {
	flinkJmClientScope := scope.NewSubScope("flink_jm_client")
	return &flinkJobManagerClientMetrics{
		scope:                        scope,
		submitJobSuccessCounter:      labeled.NewCounter("submit_job_success", "Flink job submission successful", flinkJmClientScope),
		submitJobFailureCounter:      labeled.NewCounter("submit_job_failure", "Flink job submission failed", flinkJmClientScope),
		cancelJobSuccessCounter:      labeled.NewCounter("cancel_job_success", "Flink job cancellation successful", flinkJmClientScope),
		cancelJobFailureCounter:      labeled.NewCounter("cancel_job_failure", "Flink job cancellation failed", flinkJmClientScope),
		forceCancelJobSuccessCounter: labeled.NewCounter("force_cancel_job_success", "Flink forced job cancellation successful", flinkJmClientScope),
		forceCancelJobFailureCounter: labeled.NewCounter("force_cancel_job_failure", "Flink forced job cancellation failed", flinkJmClientScope),
		checkSavepointSuccessCounter: labeled.NewCounter("check_savepoint_status_success", "Flink check savepoint status successful", flinkJmClientScope),
		checkSavepointFailureCounter: labeled.NewCounter("check_savepoint_status_failure", "Flink check savepoint status failed", flinkJmClientScope),
		getJobsSuccessCounter:        labeled.NewCounter("get_jobs_success", "Get flink jobs succeeded", flinkJmClientScope),
		getJobsFailureCounter:        labeled.NewCounter("get_jobs_failure", "Get flink jobs failed", flinkJmClientScope),
		getJobConfigSuccessCounter:   labeled.NewCounter("get_job_config_success", "Get flink job config succeeded", flinkJmClientScope),
		getJobConfigFailureCounter:   labeled.NewCounter("get_job_config_failure", "Get flink job config failed", flinkJmClientScope),
		getClusterSuccessCounter:     labeled.NewCounter("get_cluster_success", "Get cluster overview succeeded", flinkJmClientScope),
		getClusterFailureCounter:     labeled.NewCounter("get_cluster_failure", "Get cluster overview failed", flinkJmClientScope),
		getCheckpointsSuccessCounter: labeled.NewCounter("get_checkpoints_success", "Get checkpoint request succeeded", flinkJmClientScope),
		getCheckpointsFailureCounter: labeled.NewCounter("get_checkpoints_failed", "Get checkpoint request failed", flinkJmClientScope),
	}
}

func (c *FlinkJobManagerClient) GetJobConfig(ctx context.Context, url, jobID string) (*JobConfigResponse, error) {
	path := fmt.Sprintf(getJobConfigURL, jobID)
	url = url + path

	response, err := c.executeRequest(ctx, httpGet, url, nil)
	if err != nil {
		c.metrics.getJobConfigFailureCounter.Inc(ctx)
		return nil, GetError(err, GetJobConfig, GlobalFailure)
	}

	if response != nil && !response.IsSuccess() {
		c.metrics.getJobConfigFailureCounter.Inc(ctx)
		logger.Errorf(ctx, fmt.Sprintf("Get Jobconfig failed with response %v", response))
		return nil, GetError(err, GetJobConfig, response.Status())
	}
	var jobConfigResponse JobConfigResponse
	if err := json.Unmarshal(response.Body(), &jobConfigResponse); err != nil {
		logger.Errorf(ctx, "Unable to Unmarshal jobPlanResponse %v, err: %v", response, err)
		return nil, GetError(err, GetJobConfig, JSONUnmarshalError)
	}
	c.metrics.getJobConfigSuccessCounter.Inc(ctx)
	return &jobConfigResponse, nil
}

func (c *FlinkJobManagerClient) GetClusterOverview(ctx context.Context, url string) (*ClusterOverviewResponse, error) {
	url = url + getOverviewURL
	response, err := c.executeRequest(ctx, httpGet, url, nil)
	if err != nil {
		c.metrics.getClusterFailureCounter.Inc(ctx)
		return nil, GetRetryableError(err, GetClusterOverview, GlobalFailure, defaultRetries)
	}
	if response != nil && !response.IsSuccess() {
		c.metrics.getClusterFailureCounter.Inc(ctx)
		if response.StatusCode() != int(http.StatusNotFound) || response.StatusCode() != int(http.StatusServiceUnavailable) {
			logger.Errorf(ctx, fmt.Sprintf("Get cluster overview failed with response %v", response))
		}
		return nil, GetRetryableError(err, GetClusterOverview, response.Status(), defaultRetries)
	}
	var clusterOverviewResponse ClusterOverviewResponse
	if err = json.Unmarshal(response.Body(), &clusterOverviewResponse); err != nil {
		logger.Errorf(ctx, "Unable to Unmarshal clusterOverviewResponse %v, err: %v", response, err)
		return nil, GetRetryableError(err, GetClusterOverview, JSONUnmarshalError, defaultRetries)
	}
	c.metrics.getClusterSuccessCounter.Inc(ctx)
	return &clusterOverviewResponse, nil
}

// Helper method to execute the requests
func (c *FlinkJobManagerClient) executeRequest(ctx context.Context,
	method string, url string, payload interface{}) (*resty.Response, error) {
	client := resty.SetLogger(logger.GetLogWriter(ctx)).SetTimeout(defaultTimeOut)

	var resp *resty.Response
	var err error
	if method == httpGet {
		client.SetTimeout(httpGetTimeOut).SetRetryCount(retryCount)
		resp, err = client.R().Get(url)
	} else if method == httpPatch {
		resp, err = client.R().Patch(url)
	} else if method == httpPost {
		resp, err = client.R().
			SetHeader("Content-Type", "application/json").
			SetBody(payload).
			Post(url)
	} else {
		return nil, errors.New(fmt.Sprintf("Invalid method %s in request", method))
	}
	return resp, err
}

func (c *FlinkJobManagerClient) CancelJobWithSavepoint(ctx context.Context, url string, jobID string) (string, error) {
	path := fmt.Sprintf(savepointURL, jobID)

	url = url + path
	cancelJobRequest := CancelJobRequest{
		CancelJob: true,
	}
	response, err := c.executeRequest(ctx, httpPost, url, cancelJobRequest)
	if err != nil {
		c.metrics.cancelJobFailureCounter.Inc(ctx)
		return "", GetError(err, CancelJobWithSavepoint, GlobalFailure)
	}
	if response != nil && !response.IsSuccess() {
		c.metrics.cancelJobFailureCounter.Inc(ctx)
		logger.Errorf(ctx, fmt.Sprintf("Cancel job failed with response %v", response))
		return "", GetError(err, CancelJobWithSavepoint, response.Status())
	}
	var cancelJobResponse CancelJobResponse
	if err = json.Unmarshal(response.Body(), &cancelJobResponse); err != nil {
		logger.Errorf(ctx, "Unable to Unmarshal cancelJobResponse %v, err: %v", response, err)
		return "", GetError(err, CancelJobWithSavepoint, JSONUnmarshalError)
	}
	c.metrics.cancelJobSuccessCounter.Inc(ctx)
	return cancelJobResponse.TriggerID, nil
}

func (c *FlinkJobManagerClient) ForceCancelJob(ctx context.Context, url string, jobID string) error {
	path := fmt.Sprintf(jobURL, jobID)

	url = url + path + "?mode=cancel"

	response, err := c.executeRequest(ctx, httpPatch, url, nil)
	if err != nil {
		c.metrics.forceCancelJobFailureCounter.Inc(ctx)
		return GetError(err, ForceCancelJob, GlobalFailure)
	}
	if response != nil && !response.IsSuccess() {
		c.metrics.forceCancelJobFailureCounter.Inc(ctx)
		logger.Errorf(ctx, fmt.Sprintf("Force cancel job failed with response %v", response))
		return GetError(err, ForceCancelJob, response.Status())
	}

	c.metrics.forceCancelJobFailureCounter.Inc(ctx)
	return nil
}

func (c *FlinkJobManagerClient) SubmitJob(ctx context.Context, url string, jarID string, submitJobRequest SubmitJobRequest) (*SubmitJobResponse, error) {
	path := fmt.Sprintf(submitJobURL, jarID)
	url = url + path

	response, err := c.executeRequest(ctx, httpPost, url, submitJobRequest)
	if err != nil {
		c.metrics.submitJobFailureCounter.Inc(ctx)
		return nil, GetRetryableError(err, SubmitJob, GlobalFailure, defaultRetries)
	}
	if response != nil && !response.IsSuccess() {
		c.metrics.submitJobFailureCounter.Inc(ctx)
		logger.Warnf(ctx, fmt.Sprintf("Job submission failed with response %v", response))
		if response.StatusCode() > 400 {
			return nil, GetRetryableError(err, SubmitJob, response.Status(), defaultRetries, string(response.Body()))
		}

		return nil, GetFailfastError(err, SubmitJob, response.Status(), string(response.Body()))
	}

	var submitJobResponse SubmitJobResponse
	if err = json.Unmarshal(response.Body(), &submitJobResponse); err != nil {
		logger.Errorf(ctx, "Unable to Unmarshal submitJobResponse %v, err: %v", response, err)
		return nil, GetRetryableError(err, SubmitJob, response.Status(), defaultRetries, JSONUnmarshalError)
	}

	c.metrics.submitJobSuccessCounter.Inc(ctx)
	return &submitJobResponse, nil
}

func (c *FlinkJobManagerClient) CheckSavepointStatus(ctx context.Context, url string, jobID, triggerID string) (*SavepointResponse, error) {
	path := fmt.Sprintf(checkSavepointStatusURL, jobID, triggerID)
	url = url + path

	response, err := c.executeRequest(ctx, httpGet, url, nil)
	if err != nil {
		c.metrics.checkSavepointFailureCounter.Inc(ctx)
		return nil, GetRetryableError(err, CheckSavepointStatus, GlobalFailure, defaultRetries)
	}
	if response != nil && !response.IsSuccess() {
		c.metrics.checkSavepointFailureCounter.Inc(ctx)
		logger.Errorf(ctx, fmt.Sprintf("Check savepoint status failed with response %v", response))
		return nil, GetRetryableError(err, CheckSavepointStatus, response.Status(), defaultRetries)
	}
	var savepointResponse SavepointResponse
	if err = json.Unmarshal(response.Body(), &savepointResponse); err != nil {
		logger.Errorf(ctx, "Unable to Unmarshal savepointResponse %v, err: %v", response, err)
		return nil, GetRetryableError(err, CheckSavepointStatus, JSONUnmarshalError, defaultRetries)
	}
	c.metrics.cancelJobSuccessCounter.Inc(ctx)
	return &savepointResponse, nil
}

func (c *FlinkJobManagerClient) GetJobs(ctx context.Context, url string) (*GetJobsResponse, error) {
	url = url + getJobsURL
	response, err := c.executeRequest(ctx, httpGet, url, nil)
	if err != nil {
		c.metrics.getJobsFailureCounter.Inc(ctx)
		return nil, GetRetryableError(err, GetJobs, GlobalFailure, defaultRetries)
	}
	if response != nil && !response.IsSuccess() {
		c.metrics.getJobsFailureCounter.Inc(ctx)
		logger.Errorf(ctx, fmt.Sprintf("GetJobs failed with response %v", response))
		return nil, GetRetryableError(err, GetJobs, response.Status(), defaultRetries)
	}
	var getJobsResponse GetJobsResponse
	if err = json.Unmarshal(response.Body(), &getJobsResponse); err != nil {
		logger.Errorf(ctx, "%v", getJobsResponse)
		logger.Errorf(ctx, "Unable to Unmarshal getJobsResponse %v, err: %v", response, err)
		return nil, GetRetryableError(err, GetJobs, response.Status(), defaultRetries)
	}
	c.metrics.getJobsSuccessCounter.Inc(ctx)
	return &getJobsResponse, nil
}

func (c *FlinkJobManagerClient) GetLatestCheckpoint(ctx context.Context, url string, jobID string) (*CheckpointStatistics, error) {
	endpoint := fmt.Sprintf(url+checkpointsURL, jobID)
	response, err := c.executeRequest(ctx, httpGet, endpoint, nil)
	if err != nil {
		c.metrics.getCheckpointsFailureCounter.Inc(ctx)
		return nil, GetRetryableError(err, GetLatestCheckpoint, GlobalFailure, defaultRetries)
	}
	if response != nil && !response.IsSuccess() {
		c.metrics.getCheckpointsFailureCounter.Inc(ctx)
		return nil, GetRetryableError(err, GetLatestCheckpoint, response.Status(), defaultRetries)
	}

	var checkpointResponse CheckpointResponse
	if err = json.Unmarshal(response.Body(), &checkpointResponse); err != nil {
		logger.Errorf(ctx, "Failed to unmarshal checkpointResponse %v, err %v", response, err)
	}

	c.metrics.getCheckpointsSuccessCounter.Inc(ctx)
	return checkpointResponse.Latest.Completed, nil
}

func (c *FlinkJobManagerClient) GetTaskManagers(ctx context.Context, url string) (*TaskManagersResponse, error) {
	endpoint := url + taskmanagersURL
	response, err := c.executeRequest(ctx, httpGet, endpoint, nil)
	if err != nil {
		return nil, GetRetryableError(err, GetTaskManagers, GlobalFailure, defaultRetries)
	}

	if response != nil && !response.IsSuccess() {
		return nil, GetRetryableError(err, GetTaskManagers, response.Status(), defaultRetries)
	}

	var taskmanagerResponse TaskManagersResponse
	if err = json.Unmarshal(response.Body(), &taskmanagerResponse); err != nil {
		logger.Errorf(ctx, "Failed to unmarshal taskmanagerResponse %v, err %v", response, err)
	}

	return &taskmanagerResponse, nil

}

func (c *FlinkJobManagerClient) GetCheckpointCounts(ctx context.Context, url string, jobID string) (*CheckpointResponse, error) {
	endpoint := fmt.Sprintf(url+checkpointsURL, jobID)
	response, err := c.executeRequest(ctx, httpGet, endpoint, nil)
	if err != nil {
		c.metrics.getCheckpointsFailureCounter.Inc(ctx)
		return nil, GetRetryableError(err, GetCheckpointCounts, GlobalFailure, defaultRetries)
	}
	if response != nil && !response.IsSuccess() {
		c.metrics.getCheckpointsFailureCounter.Inc(ctx)
		return nil, GetRetryableError(err, GetCheckpointCounts, response.Status(), defaultRetries)
	}

	var checkpointResponse CheckpointResponse
	if err = json.Unmarshal(response.Body(), &checkpointResponse); err != nil {
		logger.Errorf(ctx, "Failed to unmarshal checkpointResponse %v, err %v", response, err)
	}

	c.metrics.getCheckpointsSuccessCounter.Inc(ctx)
	return &checkpointResponse, nil
}

func (c *FlinkJobManagerClient) GetJobOverview(ctx context.Context, url string, jobID string) (*FlinkJobOverview, error) {
	endpoint := fmt.Sprintf(url+getJobsOverviewURL, jobID)
	response, err := c.executeRequest(ctx, httpGet, endpoint, nil)
	if err != nil {
		return nil, GetRetryableError(err, GetJobOverview, GlobalFailure, defaultRetries)
	}
	if response != nil && !response.IsSuccess() {
		c.metrics.getCheckpointsFailureCounter.Inc(ctx)
		return nil, GetRetryableError(err, GetJobOverview, response.Status(), defaultRetries)
	}

	var jobOverviewResponse FlinkJobOverview
	if err = json.Unmarshal(response.Body(), &jobOverviewResponse); err != nil {
		logger.Errorf(ctx, "Failed to unmarshal FlinkJob %v, err %v", response, err)
	}

	return &jobOverviewResponse, nil
}

func NewFlinkJobManagerClient(config config.RuntimeConfig) FlinkAPIInterface {
	metrics := newFlinkJobManagerClientMetrics(config.MetricsScope)
	return &FlinkJobManagerClient{
		metrics: metrics,
	}
}
