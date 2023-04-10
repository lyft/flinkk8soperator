package client

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/lyft/flinkk8soperator/pkg/apis/app/v1beta1"

	"net/http"

	"github.com/lyft/flinkk8soperator/pkg/controller/config"
	"github.com/lyft/flytestdlib/logger"
	"github.com/lyft/flytestdlib/promutils"
	"github.com/lyft/flytestdlib/promutils/labeled"
	"github.com/pkg/errors"
	resty "gopkg.in/resty.v1"
)

const GetJobsOverviewURL = "/jobs/%s"
const GetClusterOverviewURL = "/overview"
const WebUIAnchor = "/#"

const submitJobURL = "/jars/%s/run"
const savepointURL = "/jobs/%s/savepoints"
const jobURL = "/jobs/%s"
const checkSavepointStatusURL = "/jobs/%s/savepoints/%s"
const getJobsURL = "/jobs"
const getJobConfigURL = "/jobs/%s/config"
const checkpointsURL = "/jobs/%s/checkpoints"
const taskmanagersURL = "/taskmanagers"
const httpGet = "GET"
const httpPost = "POST"
const httpPatch = "PATCH"
const retryCount = 3
const httpGetTimeOut = 5 * time.Second
const defaultTimeOut = 5 * time.Minute
const checkSavepointStatusRetries = 3

// ProgramInvocationException is thrown when the entry class doesn't exist or throws an exception
const programInvocationException = "org.apache.flink.client.program.ProgramInvocationException"

// JobSubmissionException is thrown when there is an error submitting the job (e.g., the savepoint is
// incompatible, classes for parts of the jobgraph cannot be found, jobgraph is invalid, etc.)
const jobSubmissionException = "org.apache.flink.runtime.client.JobSubmissionException"

type FlinkAPIInterface interface {
	CancelJobWithSavepoint(ctx context.Context, url string, jobID string) (string, error)
	SavepointJob(ctx context.Context, url string, jobID string) (string, error)
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
	savepointJobSuccessCounter   labeled.Counter
	savepointJobFailureCounter   labeled.Counter
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
		savepointJobSuccessCounter:   labeled.NewCounter("savepoint_job_success", "Savepoint job request succeeded", flinkJmClientScope),
		savepointJobFailureCounter:   labeled.NewCounter("savepoint_job_failed", "Savepoint job request failed", flinkJmClientScope),
	}
}

func (c *FlinkJobManagerClient) GetJobConfig(ctx context.Context, url, jobID string) (*JobConfigResponse, error) {
	path := fmt.Sprintf(getJobConfigURL, jobID)
	url = url + path

	response, err := c.executeRequest(ctx, httpGet, url, nil)
	if err != nil {
		c.metrics.getJobConfigFailureCounter.Inc(ctx)
		return nil, GetRetryableError(err, v1beta1.GetJobConfig, GlobalFailure, DefaultRetries)
	}

	if response != nil && !response.IsSuccess() {
		c.metrics.getJobConfigFailureCounter.Inc(ctx)
		logger.Errorf(ctx, fmt.Sprintf("Get Jobconfig failed with response %v", response))
		return nil, GetRetryableError(err, v1beta1.GetJobConfig, response.Status(), DefaultRetries)
	}
	var jobConfigResponse JobConfigResponse
	if err := json.Unmarshal(response.Body(), &jobConfigResponse); err != nil {
		logger.Errorf(ctx, "Unable to Unmarshal jobPlanResponse %v, err: %v", response, err)
		return nil, GetRetryableError(err, v1beta1.GetJobConfig, JSONUnmarshalError, DefaultRetries)
	}
	c.metrics.getJobConfigSuccessCounter.Inc(ctx)
	return &jobConfigResponse, nil
}

func (c *FlinkJobManagerClient) GetClusterOverview(ctx context.Context, url string) (*ClusterOverviewResponse, error) {
	url = url + GetClusterOverviewURL
	response, err := c.executeRequest(ctx, httpGet, url, nil)
	if err != nil {
		c.metrics.getClusterFailureCounter.Inc(ctx)
		return nil, GetRetryableError(err, v1beta1.GetClusterOverview, GlobalFailure, DefaultRetries)
	}
	if response != nil && !response.IsSuccess() {
		c.metrics.getClusterFailureCounter.Inc(ctx)
		if response.StatusCode() != int(http.StatusNotFound) && response.StatusCode() != int(http.StatusServiceUnavailable) {
			logger.Errorf(ctx, fmt.Sprintf("Get cluster overview failed with response %v", response))
		}
		return nil, GetRetryableError(err, v1beta1.GetClusterOverview, response.Status(), DefaultRetries)
	}
	var clusterOverviewResponse ClusterOverviewResponse
	if err = json.Unmarshal(response.Body(), &clusterOverviewResponse); err != nil {
		logger.Errorf(ctx, "Unable to Unmarshal clusterOverviewResponse %v, err: %v", response, err)
		return nil, GetRetryableError(err, v1beta1.GetClusterOverview, JSONUnmarshalError, DefaultRetries)
	}
	c.metrics.getClusterSuccessCounter.Inc(ctx)
	return &clusterOverviewResponse, nil
}

// Helper method to execute the requests
func (c *FlinkJobManagerClient) executeRequest(ctx context.Context,
	method string, url string, payload interface{}) (*resty.Response, error) {
	client := resty.New().SetLogger(logger.GetLogWriter(ctx)).SetTimeout(defaultTimeOut)

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
	cancelJobRequest := SavepointJobRequest{
		CancelJob: true,
	}
	response, err := c.executeRequest(ctx, httpPost, url, cancelJobRequest)
	if err != nil {
		c.metrics.cancelJobFailureCounter.Inc(ctx)
		return "", GetRetryableError(err, v1beta1.CancelJobWithSavepoint, GlobalFailure, 5)
	}
	if response != nil && !response.IsSuccess() {
		c.metrics.cancelJobFailureCounter.Inc(ctx)
		logger.Errorf(ctx, fmt.Sprintf("Cancel job failed with response %v", response))
		return "", GetRetryableError(err, v1beta1.CancelJobWithSavepoint, response.Status(), 5)
	}
	var cancelJobResponse SavepointJobResponse
	if err = json.Unmarshal(response.Body(), &cancelJobResponse); err != nil {
		logger.Errorf(ctx, "Unable to Unmarshal cancelJobResponse %v, err: %v", response, err)
		return "", GetRetryableError(err, v1beta1.CancelJobWithSavepoint, JSONUnmarshalError, 5)
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
		logger.Errorf(ctx, fmt.Sprintf("Force cancel job failed with error %v", err))
		return GetRetryableError(err, v1beta1.ForceCancelJob, GlobalFailure, DefaultRetries)
	}
	if response != nil && !response.IsSuccess() {
		c.metrics.forceCancelJobFailureCounter.Inc(ctx)
		logger.Errorf(ctx, fmt.Sprintf("Force cancel job failed with response %v", response))
		return GetRetryableError(err, v1beta1.ForceCancelJob, response.Status(), DefaultRetries)
	}

	c.metrics.forceCancelJobSuccessCounter.Inc(ctx)
	return nil
}

func (c *FlinkJobManagerClient) SubmitJob(ctx context.Context, url string, jarID string, submitJobRequest SubmitJobRequest) (*SubmitJobResponse, error) {
	path := fmt.Sprintf(submitJobURL, jarID)
	url = url + path
	response, err := c.executeRequest(ctx, httpPost, url, submitJobRequest)
	if err != nil {
		c.metrics.submitJobFailureCounter.Inc(ctx)
		if net.Error.Timeout(err.(net.Error)) {
			// If this is a timeout, we want to fail the deploy immediately. A client-side timeout means
			// that the jobmanager may still be working on submitting the job, and it may eventually end
			// up submitted. If we also try submitting again, we may end up with two jobs in the cluster.
			// The only safe thing to do at this point is give up and allow the user to retry the deploy.
			return nil, GetNonRetryableError(errors.Errorf(
				"Job submission timed out after %d seconds, this may be due to the job main method taking too"+
					" long or may be a transient issue", int(defaultTimeOut.Seconds())),
				v1beta1.SubmitJob, "JobSubmissionTimedOut")
		}
		return nil, GetRetryableError(err, v1beta1.SubmitJob, GlobalFailure, DefaultRetries)
	}
	if response != nil && !response.IsSuccess() {
		c.metrics.submitJobFailureCounter.Inc(ctx)
		logger.Warnf(ctx, fmt.Sprintf("Job submission failed with response %v", response))
		if response.StatusCode() > 499 {
			// Flink returns a 500 when the entry class doesn't exist or crashes on start, but we want to fail fast
			// in those cases
			body := response.String()
			if strings.Contains(body, programInvocationException) || strings.Contains(body, jobSubmissionException) {
				return nil, GetNonRetryableErrorWithMessage(err, v1beta1.SubmitJob, response.Status(), body)
			}
			return nil, GetRetryableErrorWithMessage(err, v1beta1.SubmitJob, response.Status(), DefaultRetries, string(response.Body()))
		}

		return nil, GetNonRetryableErrorWithMessage(err, v1beta1.SubmitJob, response.Status(), string(response.Body()))
	}

	var submitJobResponse SubmitJobResponse
	if err = json.Unmarshal(response.Body(), &submitJobResponse); err != nil {
		logger.Errorf(ctx, "Unable to Unmarshal submitJobResponse %v, err: %v", response, err)
		return nil, GetRetryableErrorWithMessage(err, v1beta1.SubmitJob, response.Status(), DefaultRetries, JSONUnmarshalError)
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
		return nil, GetRetryableError(err, v1beta1.CheckSavepointStatus, GlobalFailure, checkSavepointStatusRetries)
	}
	if response != nil && !response.IsSuccess() {
		c.metrics.checkSavepointFailureCounter.Inc(ctx)
		logger.Errorf(ctx, fmt.Sprintf("Check savepoint status failed with response %v", response))
		return nil, GetRetryableError(err, v1beta1.CheckSavepointStatus, response.Status(), checkSavepointStatusRetries)
	}
	var savepointResponse SavepointResponse
	if err = json.Unmarshal(response.Body(), &savepointResponse); err != nil {
		logger.Errorf(ctx, "Unable to Unmarshal savepointResponse %v, err: %v", response, err)
		return nil, GetRetryableError(err, v1beta1.CheckSavepointStatus, JSONUnmarshalError, checkSavepointStatusRetries)
	}
	c.metrics.cancelJobSuccessCounter.Inc(ctx)
	return &savepointResponse, nil
}

func (c *FlinkJobManagerClient) GetJobs(ctx context.Context, url string) (*GetJobsResponse, error) {
	url = url + getJobsURL
	response, err := c.executeRequest(ctx, httpGet, url, nil)
	if err != nil {
		c.metrics.getJobsFailureCounter.Inc(ctx)
		return nil, GetRetryableError(err, v1beta1.GetJobs, GlobalFailure, DefaultRetries)
	}
	if response != nil && !response.IsSuccess() {
		c.metrics.getJobsFailureCounter.Inc(ctx)
		logger.Errorf(ctx, fmt.Sprintf("GetJobs failed with response %v", response))
		return nil, GetRetryableError(err, v1beta1.GetJobs, response.Status(), DefaultRetries)
	}
	var getJobsResponse GetJobsResponse
	if err = json.Unmarshal(response.Body(), &getJobsResponse); err != nil {
		logger.Errorf(ctx, "%v", getJobsResponse)
		logger.Errorf(ctx, "Unable to Unmarshal getJobsResponse %v, err: %v", response, err)
		return nil, GetRetryableError(err, v1beta1.GetJobs, response.Status(), DefaultRetries)
	}
	c.metrics.getJobsSuccessCounter.Inc(ctx)
	return &getJobsResponse, nil
}

func (c *FlinkJobManagerClient) GetLatestCheckpoint(ctx context.Context, url string, jobID string) (*CheckpointStatistics, error) {
	endpoint := fmt.Sprintf(url+checkpointsURL, jobID)
	response, err := c.executeRequest(ctx, httpGet, endpoint, nil)
	if err != nil {
		c.metrics.getCheckpointsFailureCounter.Inc(ctx)
		return nil, GetRetryableError(err, v1beta1.GetLatestCheckpoint, GlobalFailure, DefaultRetries)
	}
	if response != nil && !response.IsSuccess() {
		c.metrics.getCheckpointsFailureCounter.Inc(ctx)
		return nil, GetRetryableError(err, v1beta1.GetLatestCheckpoint, response.Status(), DefaultRetries)
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
		return nil, GetRetryableError(err, v1beta1.GetTaskManagers, GlobalFailure, DefaultRetries)
	}

	if response != nil && !response.IsSuccess() {
		return nil, GetRetryableError(err, v1beta1.GetTaskManagers, response.Status(), DefaultRetries)
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
		return nil, GetRetryableError(err, v1beta1.GetCheckpointCounts, GlobalFailure, DefaultRetries)
	}
	if response != nil && !response.IsSuccess() {
		c.metrics.getCheckpointsFailureCounter.Inc(ctx)
		return nil, GetRetryableError(err, v1beta1.GetCheckpointCounts, response.Status(), DefaultRetries)
	}

	var checkpointResponse CheckpointResponse
	if err = json.Unmarshal(response.Body(), &checkpointResponse); err != nil {
		logger.Errorf(ctx, "Failed to unmarshal checkpointResponse %v, err %v", response, err)
	}

	c.metrics.getCheckpointsSuccessCounter.Inc(ctx)
	return &checkpointResponse, nil
}

func (c *FlinkJobManagerClient) GetJobOverview(ctx context.Context, url string, jobID string) (*FlinkJobOverview, error) {
	endpoint := fmt.Sprintf(url+GetJobsOverviewURL, jobID)
	response, err := c.executeRequest(ctx, httpGet, endpoint, nil)
	if err != nil {
		return nil, GetRetryableError(err, v1beta1.GetJobOverview, GlobalFailure, DefaultRetries)
	}
	if response != nil && !response.IsSuccess() {
		c.metrics.getCheckpointsFailureCounter.Inc(ctx)
		return nil, GetRetryableError(err, v1beta1.GetJobOverview, response.Status(), DefaultRetries)
	}

	var jobOverviewResponse FlinkJobOverview
	if err = json.Unmarshal(response.Body(), &jobOverviewResponse); err != nil {
		logger.Errorf(ctx, "Failed to unmarshal FlinkJob %v, err %v", response, err)
	}

	return &jobOverviewResponse, nil
}

func (c *FlinkJobManagerClient) SavepointJob(ctx context.Context, url string, jobID string) (string, error) {
	path := fmt.Sprintf(savepointURL, jobID)

	url = url + path
	savepointJobRequest := SavepointJobRequest{
		CancelJob: false,
	}
	response, err := c.executeRequest(ctx, httpPost, url, savepointJobRequest)
	if err != nil {
		c.metrics.savepointJobFailureCounter.Inc(ctx)
		return "", GetRetryableError(err, v1beta1.CancelJobWithSavepoint, GlobalFailure, 5)
	}
	if response != nil && !response.IsSuccess() {
		c.metrics.cancelJobFailureCounter.Inc(ctx)
		logger.Errorf(ctx, fmt.Sprintf("Savepointing job failed with response %v", response))
		return "", GetRetryableError(err, v1beta1.SavepointJob, response.Status(), 5)
	}
	var savepointJobResponse SavepointJobResponse
	if err = json.Unmarshal(response.Body(), &savepointJobResponse); err != nil {
		logger.Errorf(ctx, "Unable to Unmarshal savepointJobResponse %v, err: %v", response, err)
		return "", GetRetryableError(err, v1beta1.SavepointJob, JSONUnmarshalError, 5)
	}
	c.metrics.savepointJobSuccessCounter.Inc(ctx)
	return savepointJobResponse.TriggerID, nil
}

func NewFlinkJobManagerClient(config config.RuntimeConfig) FlinkAPIInterface {
	metrics := newFlinkJobManagerClientMetrics(config.MetricsScope)
	return &FlinkJobManagerClient{
		metrics: metrics,
	}
}
