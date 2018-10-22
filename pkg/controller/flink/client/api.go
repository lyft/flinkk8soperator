package client

import (
	"context"
	"encoding/json"
	"fmt"

	"regexp"

	"github.com/go-resty/resty"
	"github.com/lyft/flinkk8soperator/pkg/config"
	"github.com/lyft/flinkk8soperator/pkg/controller/logger"
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

const port = 80

type FlinkAPIInterface interface {
	CancelJobWithSavepoint(ctx context.Context, serviceName, jobId string) (string, error)
	SubmitJob(ctx context.Context, serviceName, jarId, savepointPath string, parallelism int32) (*SubmitJobResponse, error)
	CheckSavepointStatus(ctx context.Context, serviceName, jobId, triggerId string) (*SavepointResponse, error)
	GetJobs(ctx context.Context, serviceName string) (*GetJobsResponse, error)
	GetClusterOverview(ctx context.Context, serviceName string) (*ClusterOverviewResponse, error)
	GetJobConfig(ctx context.Context, serviceName, jobId string) (*JobConfigResponse, error)
}

type FlinkJobManagerClient struct {
	client *resty.Client
}

func (c *FlinkJobManagerClient) GetJobConfig(ctx context.Context, serviceName, jobId string) (*JobConfigResponse, error) {
	url := c.getUrlFromServiceName(serviceName)
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

func (c *FlinkJobManagerClient) GetClusterOverview(ctx context.Context, serviceName string) (*ClusterOverviewResponse, error) {
	url := c.getUrlFromServiceName(serviceName)
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

func getIngressHack(serviceName string) string {
	appName := serviceName[:len(serviceName)-3]
	return inputRegex.ReplaceAllString(config.FlinkIngressUrlFormat, appName)
}

func (c *FlinkJobManagerClient) getUrlFromServiceName(serviceName string) string {
	return fmt.Sprintf("http://%s:%d", getIngressHack(serviceName), port)
}

func (c *FlinkJobManagerClient) CancelJobWithSavepoint(ctx context.Context, serviceName, jobId string) (string, error) {
	url := c.getUrlFromServiceName(serviceName)
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

func (c *FlinkJobManagerClient) SubmitJob(ctx context.Context, serviceName, jarId, savepointPath string, parallelism int32) (*SubmitJobResponse, error) {
	url := c.getUrlFromServiceName(serviceName)
	path := fmt.Sprintf(submitJobUrl, jarId)
	url = url + path

	submitJobRequest := SubmitJobRequest{
		SavepointPath: savepointPath,
		Parallelism:   parallelism,
	}

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

func (c *FlinkJobManagerClient) CheckSavepointStatus(ctx context.Context, serviceName, jobId, triggerId string) (*SavepointResponse, error) {
	url := c.getUrlFromServiceName(serviceName)
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

func (c *FlinkJobManagerClient) GetJobs(ctx context.Context, serviceName string) (*GetJobsResponse, error) {
	url := c.getUrlFromServiceName(serviceName)
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
