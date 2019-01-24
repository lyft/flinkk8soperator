package client

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/go-resty/resty"
	"github.com/lyft/flinkk8soperator/pkg/controller/logger"
	"regexp"
)

const submitJobUrl = "/jars/%s/run"
const cancelJobUrl = "/jobs/%s/savepoints"
const checkSavepointStatusUrl = "/jobs/%s/savepoints/%s"
const getJobsUrl = "/jobs"
const getJobConfigUrl = "/jobs/%s/config"
const getOverviewUrl = "/overview"
const httpGet = "GET"
const httpPost = "POST"
const retryCount = 3

type FlinkAPIInterface interface {
	CancelJobWithSavepoint(ctx context.Context, url string, jobId string) (string, error)
	SubmitJob(ctx context.Context, url string, jarId string, submitJobRequest SubmitJobRequest) (*SubmitJobResponse, error)
	CheckSavepointStatus(ctx context.Context, url string, jobId, triggerId string) (*SavepointResponse, error)
	GetJobs(ctx context.Context, url string) (*GetJobsResponse, error)
	GetClusterOverview(ctx context.Context, url string) (*ClusterOverviewResponse, error)
	GetJobConfig(ctx context.Context, url string, jobId string) (*JobConfigResponse, error)
}

type FlinkJobManagerClient struct {
	client *resty.Client
}

func (c *FlinkJobManagerClient) GetJobConfig(ctx context.Context, url, jobId string) (*JobConfigResponse, error) {
	path := fmt.Sprintf(getJobConfigUrl, jobId)
	url = url + path

	response, err := c.executeRequest(ctx, httpGet, url, nil)
	if err != nil {
		return nil, err
	}

	var jobPlanResponse JobConfigResponse
	if err := json.Unmarshal(response, &jobPlanResponse); err != nil {
		return nil, err
	}
	return &jobPlanResponse, nil
}

func (c *FlinkJobManagerClient) GetClusterOverview(ctx context.Context, url string) (*ClusterOverviewResponse, error) {
	url = url + getOverviewUrl
	response, err := c.executeRequest(ctx, httpGet, url, nil)
	if err != nil {
		return nil, err
	}
	var clusterOverviewResponse ClusterOverviewResponse
	if err = json.Unmarshal(response, &clusterOverviewResponse); err != nil {
		return nil, err
	}
	return &clusterOverviewResponse, nil
}

// Helper method to execute the requests
func (c *FlinkJobManagerClient) executeRequest(
	ctx context.Context, method string, url string, payload interface{}) ([]byte, error) {
	switch method {
	case httpGet:
		resp, err := c.client.R().Get(url)
		if err != nil {
			return nil, err
		}
		return resp.Body(), nil
	case httpPost:
		resp, err := c.client.R().
			SetHeader("Content-Type", "application/json").
			SetBody(payload).
			Post(url)
		if err != nil {
			logger.Errorf(ctx, "%v", err)
			return nil, err
		}
		return resp.Body(), nil
	}
	return nil, nil
}

var inputRegex = regexp.MustCompile("{{\\s*[$]jobCluster\\s*}}")

func (c *FlinkJobManagerClient) CancelJobWithSavepoint(ctx context.Context, url string, jobId string) (string, error) {
	path := fmt.Sprintf(cancelJobUrl, jobId)

	url = url + path
	cancelJobRequest := CancelJobRequest{
		CancelJob: true,
	}
	response, err := c.executeRequest(ctx, httpPost, url, cancelJobRequest)
	if err != nil {
		return "", err
	}
	var cancelJobResponse CancelJobResponse
	if err = json.Unmarshal(response, &cancelJobResponse); err != nil {
		return "", err
	}
	return cancelJobResponse.TriggerId, nil
}

func (c *FlinkJobManagerClient) SubmitJob(ctx context.Context, url string, jarId string, submitJobRequest SubmitJobRequest) (*SubmitJobResponse, error) {
	path := fmt.Sprintf(submitJobUrl, jarId)
	url = url + path

	response, err := c.executeRequest(ctx, httpPost, url, submitJobRequest)
	if err != nil {
		return nil, err
	}
	var submitJobResponse SubmitJobResponse
	if err = json.Unmarshal(response, &submitJobResponse); err != nil {
		return nil, err
	}

	return &submitJobResponse, nil
}

func (c *FlinkJobManagerClient) CheckSavepointStatus(ctx context.Context, url string, jobId, triggerId string) (*SavepointResponse, error) {
	path := fmt.Sprintf(checkSavepointStatusUrl, jobId, triggerId)
	url = url + path

	response, err := c.executeRequest(ctx, httpGet, url, nil)
	if err != nil {
		return nil, err
	}

	var savepointResponse SavepointResponse
	if err = json.Unmarshal(response, &savepointResponse); err != nil {
		return nil, err
	}
	return &savepointResponse, nil
}

func (c *FlinkJobManagerClient) GetJobs(ctx context.Context, url string) (*GetJobsResponse, error) {
	url = url + getJobsUrl
	response, err := c.executeRequest(ctx, httpGet, url, nil)
	if err != nil {
		return nil, err
	}
	var getJobsResponse GetJobsResponse
	if err = json.Unmarshal(response, &getJobsResponse); err != nil {
		return nil, err
	}
	return &getJobsResponse, nil
}

func NewFlinkJobManagerClient() FlinkAPIInterface {
	client := resty.SetRetryCount(retryCount)
	return &FlinkJobManagerClient{
		client: client,
	}
}
