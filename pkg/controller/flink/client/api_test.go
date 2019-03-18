package client

import (
	"context"
	"github.com/go-resty/resty"
	"github.com/jarcoal/httpmock"
	mockScope "github.com/lyft/flytestdlib/promutils"
	"github.com/lyft/flytestdlib/promutils/labeled"
	"github.com/stretchr/testify/assert"
	"testing"

	"github.com/lyft/flinkk8soperator/pkg/controller/common"
	"strings"
)

func getTestClient() FlinkJobManagerClient {
	client := resty.SetRetryCount(1)
	return FlinkJobManagerClient{
		client: client,
	}
}

func getTestJobManagerClient() FlinkAPIInterface {
	testScope := mockScope.NewTestScope()
	labeled.SetMetricKeys(common.GetValidLabelNames()...)
	return NewFlinkJobManagerClient(testScope)
}

func TestGetJobsHappyCase(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	ctx := context.Background()
	response := GetJobsResponse{
		Jobs: []FlinkJob{
			{
				JobId: "j1",
			},
		},
	}
	responder, _ := httpmock.NewJsonResponder(200, response)
	fakeUrl := "http://abc.com/jobs"
	httpmock.RegisterResponder("GET", fakeUrl, responder)

	client := getTestJobManagerClient()
	resp, err := client.GetJobs(ctx, "http://abc.com")
	assert.Equal(t, response, *resp)
	assert.NoError(t, err)
}

func TestGetJobsInvalidResponse(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	ctx := context.Background()
	response := "invalid response"
	responder, _ := httpmock.NewJsonResponder(200, response)
	fakeUrl := "http://abc.com/jobs"
	httpmock.RegisterResponder("GET", fakeUrl, responder)

	client := getTestJobManagerClient()
	_, err := client.GetJobs(ctx, "http://abc.com")
	assert.NotNil(t, err)
}

func TestGetJobs500Response(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	ctx := context.Background()
	responder, _ := httpmock.NewJsonResponder(500, nil)
	fakeUrl := "http://abc.com/jobs"
	httpmock.RegisterResponder("GET", fakeUrl, responder)

	client := getTestJobManagerClient()
	resp, err := client.GetJobs(ctx, "http://abc.com")
	assert.Nil(t, resp)
	assert.EqualError(t, err, "GetJobs request failed with status 500")
}

func TestGetJobsError(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	ctx := context.Background()
	fakeUrl := "http://abc.com/jobs"
	httpmock.RegisterResponder("GET", fakeUrl, nil)

	client := getTestJobManagerClient()
	resp, err := client.GetJobs(ctx, "http://abc.com")
	assert.Nil(t, resp)
	assert.NotNil(t, err)
	assert.True(t, strings.HasPrefix(err.Error(), "Get jobs API request failed"))
}

func TestGetClusterOverviewHappyCase(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	ctx := context.Background()
	response := ClusterOverviewResponse{
		TaskManagerCount: 4,
	}
	responder, _ := httpmock.NewJsonResponder(200, response)
	fakeUrl := "http://abc.com/overview"
	httpmock.RegisterResponder("GET", fakeUrl, responder)

	client := getTestJobManagerClient()
	resp, err := client.GetClusterOverview(ctx, "http://abc.com")
	assert.Equal(t, response, *resp)
	assert.NoError(t, err)
}

func TestGetClusterOverviewInvalidResponse(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	ctx := context.Background()
	response := "invalid response"
	responder, _ := httpmock.NewJsonResponder(200, response)
	fakeUrl := "http://abc.com/overview"
	httpmock.RegisterResponder("GET", fakeUrl, responder)

	client := getTestJobManagerClient()
	resp, err := client.GetClusterOverview(ctx, "http://abc.com")
	assert.Nil(t, resp)
	assert.NotNil(t, err)
}

func TestGetCluster500Response(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	ctx := context.Background()
	responder, _ := httpmock.NewJsonResponder(500, nil)
	fakeUrl := "http://abc.com/overview"
	httpmock.RegisterResponder("GET", fakeUrl, responder)

	client := getTestJobManagerClient()
	resp, err := client.GetClusterOverview(ctx, "http://abc.com")
	assert.Nil(t, resp)
	assert.EqualError(t, err, "Get cluster overview failed with status 500")
}

func TestGetClusterOverviewError(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	ctx := context.Background()
	fakeUrl := "http://abc.com/overview"
	httpmock.RegisterResponder("GET", fakeUrl, nil)

	client := getTestJobManagerClient()
	resp, err := client.GetClusterOverview(ctx, "http://abc.com")
	assert.Nil(t, resp)
	assert.NotNil(t, err)
	assert.True(t, strings.HasPrefix(err.Error(), "GetClusterOverview API request failed"))
}

func TestGetJobConfigHappyCase(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	ctx := context.Background()
	response := JobConfigResponse{
		JobId: "j1",
	}
	responder, _ := httpmock.NewJsonResponder(200, response)
	fakeUrl := "http://abc.com/jobs/1/config"
	httpmock.RegisterResponder("GET", fakeUrl, responder)

	client := getTestJobManagerClient()
	resp, err := client.GetJobConfig(ctx, "http://abc.com", "1")
	assert.Equal(t, response, *resp)
	assert.NoError(t, err)
}

func TestGetJobConfigInvalidResponse(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	ctx := context.Background()
	response := "invalid response"
	responder, _ := httpmock.NewJsonResponder(200, response)
	fakeUrl := "http://abc.com/jobs/1/config"
	httpmock.RegisterResponder("GET", fakeUrl, responder)

	client := getTestJobManagerClient()
	resp, err := client.GetJobConfig(ctx, "http://abc.com", "1")
	assert.Nil(t, resp)
	assert.NotNil(t, err)
}

func TestGetJobConfig500Response(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	ctx := context.Background()
	responder, _ := httpmock.NewJsonResponder(500, nil)
	fakeUrl := "http://abc.com/jobs/1/config"
	httpmock.RegisterResponder("GET", fakeUrl, responder)

	client := getTestJobManagerClient()
	resp, err := client.GetJobConfig(ctx, "http://abc.com", "1")
	assert.Nil(t, resp)
	assert.EqualError(t, err, "Get Jobconfig failed with status 500")
}

func TestGetJobConfigError(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	ctx := context.Background()
	fakeUrl := "http://abc.com/jobs/1/config"
	httpmock.RegisterResponder("GET", fakeUrl, nil)

	client := getTestJobManagerClient()
	resp, err := client.GetJobConfig(ctx, "http://abc.com", "1")
	assert.Nil(t, resp)
	assert.NotNil(t, err)
	assert.True(t, strings.HasPrefix(err.Error(), "GetJobConfig API request failed"))
}

func TestCheckSavepointHappyCase(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	ctx := context.Background()
	response := SavepointResponse{
		SavepointStatus: SavepointStatusResponse{
			Status: SavePointInProgress,
		},
	}
	responder, _ := httpmock.NewJsonResponder(200, response)
	fakeUrl := "http://abc.com/jobs/1/savepoints/2"
	httpmock.RegisterResponder("GET", fakeUrl, responder)

	client := getTestJobManagerClient()
	resp, err := client.CheckSavepointStatus(ctx, "http://abc.com", "1", "2")
	assert.Equal(t, response, *resp)
	assert.NoError(t, err)
}

func TestCheckSavepointInvalidResponse(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	ctx := context.Background()
	response := "invalid response"
	responder, _ := httpmock.NewJsonResponder(200, response)
	fakeUrl := "http://abc.com/jobs/1/savepoints/2"
	httpmock.RegisterResponder("GET", fakeUrl, responder)

	client := getTestJobManagerClient()
	resp, err := client.CheckSavepointStatus(ctx, "http://abc.com", "1", "2")
	assert.Nil(t, resp)
	assert.NotNil(t, err)
}

func TestCheckSavepoint500Response(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	ctx := context.Background()
	responder, _ := httpmock.NewJsonResponder(500, nil)
	fakeUrl := "http://abc.com/jobs/1/savepoints/2"
	httpmock.RegisterResponder("GET", fakeUrl, responder)

	client := getTestJobManagerClient()
	resp, err := client.CheckSavepointStatus(ctx, "http://abc.com", "1", "2")
	assert.Nil(t, resp)
	assert.EqualError(t, err, "Check savepoint status failed with status 500")
}

func TestCheckSavepointError(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	ctx := context.Background()
	fakeUrl := "http://abc.com/jobs/1/savepoints/2"
	httpmock.RegisterResponder("GET", fakeUrl, nil)

	client := getTestJobManagerClient()
	resp, err := client.CheckSavepointStatus(ctx, "http://abc.com", "1", "2")
	assert.Nil(t, resp)
	assert.NotNil(t, err)
	assert.True(t, strings.HasPrefix(err.Error(), "Check savepoint status API request failed"))
}

func TestSubmitJobHappyCase(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	ctx := context.Background()
	response := SubmitJobResponse{
		JobId: "1",
	}
	responder, _ := httpmock.NewJsonResponder(200, response)
	fakeUrl := "http://abc.com/jars/1/run"
	httpmock.RegisterResponder("POST", fakeUrl, responder)

	client := getTestJobManagerClient()
	resp, err := client.SubmitJob(ctx, "http://abc.com", "1", SubmitJobRequest{
		Parallelism: 10,
	})
	assert.Equal(t, response, *resp)
	assert.NoError(t, err)
}

func TestSubmitJobInvalidResponse(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	ctx := context.Background()
	response := "invalid response"
	responder, _ := httpmock.NewJsonResponder(200, response)
	fakeUrl := "http://abc.com/jars/1/run"
	httpmock.RegisterResponder("POST", fakeUrl, responder)

	client := getTestJobManagerClient()
	resp, err := client.SubmitJob(ctx, "http://abc.com", "1", SubmitJobRequest{
		Parallelism: 10,
	})
	assert.Nil(t, resp)
	assert.NotNil(t, err)
}

func TestSubmitJob500Response(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	ctx := context.Background()
	responder, _ := httpmock.NewJsonResponder(500, nil)
	fakeUrl := "http://abc.com/jars/1/run"
	httpmock.RegisterResponder("POST", fakeUrl, responder)

	client := getTestJobManagerClient()
	resp, err := client.SubmitJob(ctx, "http://abc.com", "1", SubmitJobRequest{
		Parallelism: 10,
	})
	assert.Nil(t, resp)
	assert.EqualError(t, err, "Job submission failed with status 500")
}

func TestSubmitJobError(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	ctx := context.Background()
	fakeUrl := "http://abc.com/jars/1/run"
	httpmock.RegisterResponder("POST", fakeUrl, nil)

	client := getTestJobManagerClient()
	resp, err := client.SubmitJob(ctx, "http://abc.com", "1", SubmitJobRequest{
		Parallelism: 10,
	})
	assert.Nil(t, resp)
	assert.NotNil(t, err)
	assert.True(t, strings.HasPrefix(err.Error(), "Submit job API request failed"))
}

func TestCancelJobHappyCase(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	ctx := context.Background()
	response := CancelJobResponse{
		TriggerId: "133",
	}
	responder, _ := httpmock.NewJsonResponder(203, response)
	fakeUrl := "http://abc.com/jobs/1/savepoints"
	httpmock.RegisterResponder("POST", fakeUrl, responder)

	client := getTestJobManagerClient()
	resp, err := client.CancelJobWithSavepoint(ctx, "http://abc.com", "1")
	assert.Equal(t, response.TriggerId, resp)
	assert.NoError(t, err)
}

func TestCancelJobInvalidResponse(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	ctx := context.Background()
	response := "invalid response"
	responder, _ := httpmock.NewJsonResponder(200, response)
	fakeUrl := "http://abc.com/jobs/1/savepoints"
	httpmock.RegisterResponder("POST", fakeUrl, responder)

	client := getTestJobManagerClient()
	resp, err := client.CancelJobWithSavepoint(ctx, "http://abc.com", "1")
	assert.Empty(t, resp)
	assert.NotNil(t, err)
}

func TestCancelJob500Response(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	ctx := context.Background()
	responder, _ := httpmock.NewJsonResponder(500, nil)
	fakeUrl := "http://abc.com/jobs/1/savepoints"
	httpmock.RegisterResponder("POST", fakeUrl, responder)

	client := getTestJobManagerClient()
	resp, err := client.CancelJobWithSavepoint(ctx, "http://abc.com", "1")
	assert.Empty(t, resp)
	assert.EqualError(t, err, "Cancel job failed with status 500")
}

func TestCancelJobError(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	ctx := context.Background()
	fakeUrl := "http://abc.com/jobs/1/savepoints"
	httpmock.RegisterResponder("POST", fakeUrl, nil)

	client := getTestJobManagerClient()
	resp, err := client.CancelJobWithSavepoint(ctx, "http://abc.com", "1")
	assert.Empty(t, resp)
	assert.NotNil(t, err)
	assert.True(t, strings.HasPrefix(err.Error(), "Cancel job API request failed"))
}

func TestHttpGetNon200Response(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	ctx := context.Background()
	response := GetJobsResponse{
		Jobs: []FlinkJob{
			{
				JobId: "j1",
			},
		},
	}
	responder, _ := httpmock.NewJsonResponder(500, response)
	fakeUrl := "http://abc.com/jobs"
	httpmock.RegisterResponder("GET", fakeUrl, responder)

	client := getTestJobManagerClient()
	_, err := client.GetJobs(ctx, "http://abc.com")
	assert.NotNil(t, err)
	assert.EqualError(t, err, "GetJobs request failed with status 500")
}

func TestClientInvalidMethod(t *testing.T) {
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	ctx := context.Background()

	client := getTestClient()
	_, err := client.executeRequest(ctx, "random", "http://abc.com", nil)
	assert.NotNil(t, err)
	assert.EqualError(t, err, "Invalid method random in request")
}
