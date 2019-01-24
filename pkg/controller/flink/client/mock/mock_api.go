package mock

import (
	"context"

	"github.com/lyft/flinkk8soperator/pkg/controller/flink/client"
)

type CancelJobWithSavepointFunc func(ctx context.Context, url string, jobId string) (string, error)
type SubmitJobFunc func(ctx context.Context, url string, jarId string, submitJobRequest client.SubmitJobRequest) (*client.SubmitJobResponse, error)
type CheckSavepointStatusFunc func(ctx context.Context, url string, jobId, triggerId string) (*client.SavepointResponse, error)
type GetJobsFunc func(ctx context.Context, url string) (*client.GetJobsResponse, error)
type GetClusterOverviewFunc func(ctx context.Context, url string) (*client.ClusterOverviewResponse, error)
type GetJobConfigFunc func(ctx context.Context, url string, jobId string) (*client.JobConfigResponse, error)

type MockJobManagerClient struct {
	CancelJobWithSavepointFunc CancelJobWithSavepointFunc
	SubmitJobFunc              SubmitJobFunc
	CheckSavepointStatusFunc   CheckSavepointStatusFunc
	GetJobsFunc                GetJobsFunc
	GetClusterOverviewFunc     GetClusterOverviewFunc
	GetJobConfigFunc           GetJobConfigFunc
}

func (m *MockJobManagerClient) SubmitJob(ctx context.Context, url string, jarId string, submitJobRequest client.SubmitJobRequest) (*client.SubmitJobResponse, error) {
	if m.SubmitJobFunc != nil {
		return m.SubmitJobFunc(ctx, url, jarId, submitJobRequest)
	}
	return nil, nil
}

func (m *MockJobManagerClient) CancelJobWithSavepoint(ctx context.Context, url string, jobId string) (string, error) {
	if m.CancelJobWithSavepointFunc != nil {
		return m.CancelJobWithSavepointFunc(ctx, url, jobId)
	}
	return "", nil
}

func (m *MockJobManagerClient) CheckSavepointStatus(ctx context.Context, url string, jobId, triggerId string) (*client.SavepointResponse, error) {
	if m.CheckSavepointStatusFunc != nil {
		return m.CheckSavepointStatusFunc(ctx, url, jobId, triggerId)
	}
	return nil, nil
}

func (m *MockJobManagerClient) GetJobs(ctx context.Context, url string) (*client.GetJobsResponse, error) {
	if m.GetJobsFunc != nil {
		return m.GetJobsFunc(ctx, url)
	}
	return nil, nil
}

func (m *MockJobManagerClient) GetClusterOverview(ctx context.Context, url string) (*client.ClusterOverviewResponse, error) {
	if m.GetClusterOverviewFunc != nil {
		return m.GetClusterOverviewFunc(ctx, url)
	}
	return nil, nil
}

func (m *MockJobManagerClient) GetJobConfig(ctx context.Context, url string, jobId string) (*client.JobConfigResponse, error) {
	if m.GetJobConfigFunc != nil {
		return m.GetJobConfigFunc(ctx, url, jobId)
	}
	return nil, nil
}
