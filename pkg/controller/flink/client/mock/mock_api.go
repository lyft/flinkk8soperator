package mock

import (
	"context"

	"github.com/lyft/flinkk8soperator/pkg/controller/flink/client"
)

type CancelJobWithSavepointFunc func(ctx context.Context, serviceName, jobId string) (string, error)
type SubmitJobFunc func(ctx context.Context, serviceName, jarId string, submitJobRequest client.SubmitJobRequest) (*client.SubmitJobResponse, error)
type CheckSavepointStatusFunc func(ctx context.Context, serviceName, jobId, triggerId string) (*client.SavepointResponse, error)
type GetJobsFunc func(ctx context.Context, serviceName string) (*client.GetJobsResponse, error)
type GetClusterOverviewFunc func(ctx context.Context, serviceName string) (*client.ClusterOverviewResponse, error)
type GetJobConfigFunc func(ctx context.Context, serviceName, jobId string) (*client.JobConfigResponse, error)

type MockJobManagerClient struct {
	CancelJobWithSavepointFunc CancelJobWithSavepointFunc
	SubmitJobFunc              SubmitJobFunc
	CheckSavepointStatusFunc   CheckSavepointStatusFunc
	GetJobsFunc                GetJobsFunc
	GetClusterOverviewFunc     GetClusterOverviewFunc
	GetJobConfigFunc           GetJobConfigFunc
}

func (m *MockJobManagerClient) SubmitJob(ctx context.Context, serviceName, jarId string, submitJobRequest client.SubmitJobRequest) (*client.SubmitJobResponse, error) {
	if m.SubmitJobFunc != nil {
		return m.SubmitJobFunc(ctx, serviceName, jarId, submitJobRequest)
	}
	return nil, nil
}

func (m *MockJobManagerClient) CancelJobWithSavepoint(ctx context.Context, serviceName, jobId string) (string, error) {
	if m.CancelJobWithSavepointFunc != nil {
		return m.CancelJobWithSavepointFunc(ctx, serviceName, jobId)
	}
	return "", nil
}

func (m *MockJobManagerClient) CheckSavepointStatus(ctx context.Context, serviceName, jobId, triggerId string) (*client.SavepointResponse, error) {
	if m.CheckSavepointStatusFunc != nil {
		return m.CheckSavepointStatusFunc(ctx, serviceName, jobId, triggerId)
	}
	return nil, nil
}

func (m *MockJobManagerClient) GetJobs(ctx context.Context, serviceName string) (*client.GetJobsResponse, error) {
	if m.GetJobsFunc != nil {
		return m.GetJobsFunc(ctx, serviceName)
	}
	return nil, nil
}

func (m *MockJobManagerClient) GetClusterOverview(ctx context.Context, serviceName string) (*client.ClusterOverviewResponse, error) {
	if m.GetClusterOverviewFunc != nil {
		return m.GetClusterOverviewFunc(ctx, serviceName)
	}
	return nil, nil
}

func (m *MockJobManagerClient) GetJobConfig(ctx context.Context, serviceName, jobId string) (*client.JobConfigResponse, error) {
	if m.GetJobConfigFunc != nil {
		return m.GetJobConfigFunc(ctx, serviceName, jobId)
	}
	return nil, nil
}
