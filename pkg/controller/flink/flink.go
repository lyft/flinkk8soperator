package flink

import (
	"context"
	"errors"
	"github.com/lyft/flinkk8soperator/pkg/controller/logger"

	"github.com/lyft/flinkk8soperator/pkg/apis/app/v1alpha1"
	"github.com/lyft/flinkk8soperator/pkg/controller/flink/client"
	"github.com/lyft/flinkk8soperator/pkg/controller/k8"
	"k8s.io/api/apps/v1"
)

// Interface to manage Flink Application in Kubernetes
type FlinkInterface interface {
	// Creates a Flink cluster with necessary Job Manager, Task Managers and services for UI
	CreateCluster(ctx context.Context, application *v1alpha1.FlinkApplication) error

	// Deletes a Flink cluster that does not match the spec of the Application if there is one
	DeleteOldCluster(ctx context.Context, application *v1alpha1.FlinkApplication) (bool, error)

	// Cancels the running/active jobs in the Cluster for the Application after savepoint is created
	CancelWithSavepoint(ctx context.Context, application *v1alpha1.FlinkApplication) (string, error)

	// Starts the Job in the Flink Cluster based on values in the Application
	StartFlinkJob(ctx context.Context, application *v1alpha1.FlinkApplication) (string, error)

	// Savepoint creation is asynchronous.
	// Polls the status of the Savepoint, using the triggerId
	GetSavepointStatus(ctx context.Context, application *v1alpha1.FlinkApplication) (*client.SavepointResponse, error)

	// Check if the Flink Kubernetes Cluster is Ready.
	// Checks if all the pods of task and job managers are ready.
	IsClusterReady(ctx context.Context, application *v1alpha1.FlinkApplication) (bool, error)

	// Checks to see if the Flink Cluster is ready to handle API requests
	IsServiceReady(ctx context.Context, application *v1alpha1.FlinkApplication) (bool, error)

	// Compares the Application Spec with the underlying Flink Cluster.
	// Return true if the spec does not match the underlying Flink cluster.
	HasApplicationChanged(ctx context.Context, application *v1alpha1.FlinkApplication) (bool, error)

	// Indicates if a new Flink cluster needs to spinned up for the Application
	IsClusterChangeNeeded(ctx context.Context, application *v1alpha1.FlinkApplication) (bool, error)

	// Ensure that correct resources are running for the Application.
	CheckAndUpdateClusterResources(ctx context.Context, application *v1alpha1.FlinkApplication) (bool, error)

	// Check if the Job details in the application has changed corresponding to one running in the cluster
	HasApplicationJobChanged(ctx context.Context, application *v1alpha1.FlinkApplication) (bool, error)

	// Returns the list of Jobs running on the Flink Cluster for the Application
	GetJobsForApplication(ctx context.Context, application *v1alpha1.FlinkApplication) ([]client.FlinkJob, error)

	// For the application, a deployment corresponds to an image. This returns the current and older deployments for the app.
	GetCurrentAndOldDeploymentsForApp(ctx context.Context, application *v1alpha1.FlinkApplication) ([]v1.Deployment, []v1.Deployment, error)
}

func NewFlinkController() FlinkInterface {
	return &FlinkController{
		k8Cluster:        k8.NewK8Cluster(),
		flinkJobManager:  NewFlinkJobManagerController(),
		FlinkTaskManager: NewFlinkTaskManagerController(),
		flinkClient:      client.NewFlinkJobManagerClient(),
	}
}

type FlinkController struct {
	k8Cluster        k8.K8ClusterInterface
	flinkJobManager  FlinkJobManagerControllerInterface
	FlinkTaskManager FlinkTaskManagerControllerInterface
	flinkClient      client.FlinkAPIInterface
}

func GetActiveFlinkJob(jobs []client.FlinkJob) *client.FlinkJob {
	if len(jobs) == 0 {
		return nil
	}
	for _, job := range jobs {
		if job.Status == client.FlinkJobRunning ||
			job.Status == client.FlinkJobCreated {
			return &job
		}
	}
	return nil
}

func (f *FlinkController) HasApplicationJobChanged(ctx context.Context, application *v1alpha1.FlinkApplication) (bool, error) {
	serviceName := GetJobManagerExternalServiceName(*application)
	jobId, err := f.getJobIdForApplication(ctx, application)
	if err != nil {
		return false, err
	}
	jobConfig, err := f.flinkClient.GetJobConfig(ctx, serviceName, jobId)
	if application.Spec.FlinkJob.Parallelism != jobConfig.ExecutionConfig.Parallelism {
		return true, nil
	}
	// More checks here for job change monitoring.
	return false, nil
}

func (f *FlinkController) CheckAndUpdateClusterResources(ctx context.Context, application *v1alpha1.FlinkApplication) (bool, error) {
	logger.Infof(ctx, "Checking and updating cluster resources: %s", application)

	currentAppDeployments, err := f.getDeploymentsForApp(ctx, application)
	if err != nil {
		return false, err
	}
	hasUpdated := false
	taskManagerDeployment := getTaskManagerDeployment(currentAppDeployments.Items, application)
	if *taskManagerDeployment.Spec.Replicas != application.Spec.TaskManagerConfig.Replicas {
		taskManagerDeployment.Spec.Replicas = &application.Spec.TaskManagerConfig.Replicas
		err := f.k8Cluster.UpdateK8Object(ctx, taskManagerDeployment)
		if err != nil {
			return false, err
		}
		hasUpdated = true
	}

	jobManagerDeployment := getJobManagerDeployment(currentAppDeployments.Items, application)
	if *jobManagerDeployment.Spec.Replicas != application.Spec.JobManagerConfig.Replicas {
		jobManagerDeployment.Spec.Replicas = &application.Spec.JobManagerConfig.Replicas
		err := f.k8Cluster.UpdateK8Object(ctx, jobManagerDeployment)
		if err != nil {
			return false, err
		}
		hasUpdated = true
	}
	return hasUpdated, nil
}

func (f *FlinkController) IsMultipleClusterPresent(ctx context.Context, application *v1alpha1.FlinkApplication) (bool, error) {
	currentDeployments, oldDeployments, err := f.GetCurrentAndOldDeploymentsForApp(ctx, application)
	if err != nil {
		return false, err
	}
	if len(currentDeployments) > 0 && len(oldDeployments) > 0 {
		return true, nil
	}
	return false, nil
}

func (f *FlinkController) getDeploymentsForApp(ctx context.Context, application *v1alpha1.FlinkApplication) (*v1.DeploymentList, error) {
	appLabels := k8.GetAppLabel(application.Name)
	return f.k8Cluster.GetDeploymentsWithLabel(ctx, application.Namespace, appLabels)
}

func (f *FlinkController) getDeploymentsForImage(ctx context.Context, application *v1alpha1.FlinkApplication) (*v1.DeploymentList, error) {
	imageLabels := k8.GetImageLabel(k8.GetImageKey(application.Spec.Image))
	return f.k8Cluster.GetDeploymentsWithLabel(ctx, application.Namespace, imageLabels)
}

func (f *FlinkController) GetJobsForApplication(ctx context.Context, application *v1alpha1.FlinkApplication) ([]client.FlinkJob, error) {
	serviceName := GetJobManagerExternalServiceName(*application)
	jobResponse, err := f.flinkClient.GetJobs(ctx, serviceName)
	if err != nil {
		return nil, err
	}
	return jobResponse.Jobs, nil
}

// The operator for now assumes and is intended to run single application per Flink Cluster.
// Once we move to run multiple applications, this has to be removed/updated
func (f *FlinkController) getJobIdForApplication(ctx context.Context, application *v1alpha1.FlinkApplication) (string, error) {
	if application.Status.JobId != "" {
		return application.Status.JobId, nil
	}

	return "", errors.New("active job id not available")
}

func (f *FlinkController) CancelWithSavepoint(ctx context.Context, application *v1alpha1.FlinkApplication) (string, error) {
	serviceName := GetJobManagerExternalServiceName(*application)
	jobId, err := f.getJobIdForApplication(ctx, application)
	if err != nil {
		return "", err
	}
	return f.flinkClient.CancelJobWithSavepoint(ctx, serviceName, jobId)
}

func (f *FlinkController) CreateCluster(ctx context.Context, application *v1alpha1.FlinkApplication) error {
	err := f.flinkJobManager.CreateIfNotExist(ctx, application)
	if err != nil {
		return err
	}
	err = f.FlinkTaskManager.CreateIfNotExist(ctx, application)
	if err != nil {
		return err
	}
	return nil
}

func (f *FlinkController) StartFlinkJob(ctx context.Context, application *v1alpha1.FlinkApplication) (string, error) {
	serviceName := GetJobManagerExternalServiceName(*application)
	response, err := f.flinkClient.SubmitJob(
		ctx,
		serviceName,
		application.Spec.FlinkJob.JarName,
		client.SubmitJobRequest{
			Parallelism:   application.Spec.FlinkJob.Parallelism,
			SavepointPath: application.Spec.FlinkJob.SavepointInfo.SavepointLocation,
			EntryClass:    application.Spec.FlinkJob.EntryClass,
			ProgramArgs:   application.Spec.FlinkJob.ProgramArgs,
		})
	if err != nil {
		return "", err
	}
	if response.JobId == "" {
		return "", errors.New("unable to submit job: invalid job id")
	}
	return response.JobId, nil
}

func (f *FlinkController) GetSavepointStatus(ctx context.Context, application *v1alpha1.FlinkApplication) (*client.SavepointResponse, error) {
	serviceName := GetJobManagerExternalServiceName(*application)
	jobId, err := f.getJobIdForApplication(ctx, application)
	if err != nil {
		return nil, err
	}
	return f.flinkClient.CheckSavepointStatus(ctx, serviceName, jobId, application.Spec.FlinkJob.SavepointInfo.TriggerId)
}

func (f *FlinkController) DeleteOldCluster(ctx context.Context, application *v1alpha1.FlinkApplication) (bool, error) {
	_, oldDeployments, err := f.GetCurrentAndOldDeploymentsForApp(ctx, application)
	if err != nil {
		return false, err
	}
	if len(oldDeployments) == 0 {
		return true, nil
	}
	err = f.k8Cluster.DeleteDeployments(ctx, v1.DeploymentList{
		Items: oldDeployments,
	})
	if err != nil {
		return false, err
	}
	return true, nil
}

func (f *FlinkController) IsClusterReady(ctx context.Context, application *v1alpha1.FlinkApplication) (bool, error) {
	appImageLabel := k8.GetImageLabel(k8.GetImageKey(application.Spec.Image))
	return f.k8Cluster.IsAllPodsRunning(ctx, application.Namespace, appImageLabel)
}

func (f *FlinkController) IsServiceReady(ctx context.Context, application *v1alpha1.FlinkApplication) (bool, error) {
	serviceName := GetJobManagerExternalServiceName(*application)
	_, err := f.flinkClient.GetClusterOverview(ctx, serviceName)
	if err != nil {
		logger.Infof(ctx, "Failed to start application %s: %s", application.Name, err)
		return false, err
	}
	return true, nil
}

func (f *FlinkController) GetCurrentAndOldDeploymentsForApp(ctx context.Context, application *v1alpha1.FlinkApplication) ([]v1.Deployment, []v1.Deployment, error) {
	currentAppDeployments, err := f.getDeploymentsForApp(ctx, application)
	if err != nil {
		return nil, nil, err
	}
	appImageLabel := k8.GetImageLabel(k8.GetImageKey(application.Spec.Image))
	currentDeployments, oldDeployments := k8.MatchDeploymentsByLabel(*currentAppDeployments, appImageLabel)
	return currentDeployments, oldDeployments, nil
}

func (f *FlinkController) HasApplicationChanged(ctx context.Context, application *v1alpha1.FlinkApplication) (bool, error) {
	clusterChangeNeeded, err := f.IsClusterChangeNeeded(ctx, application)
	if err != nil {
		return false, err
	}
	if clusterChangeNeeded {
		return true, nil
	}

	clusterUpdateNeeded, err := f.isClusterUpdateNeeded(ctx, application)
	if err != nil {
		return false, err
	}

	if clusterUpdateNeeded {
		return true, nil
	}
	return false, nil
}

func (f *FlinkController) IsClusterChangeNeeded(ctx context.Context, application *v1alpha1.FlinkApplication) (bool, error) {
	currentDeployments, err := f.getDeploymentsForImage(ctx, application)
	if err != nil {
		return false, err
	}
	if len(currentDeployments.Items) == 0 {
		return true, nil
	}
	return false, nil
}

func (f *FlinkController) isClusterUpdateNeeded(ctx context.Context, application *v1alpha1.FlinkApplication) (bool, error) {
	currentAppDeployments, err := f.getDeploymentsForApp(ctx, application)
	if err != nil {
		return false, err
	}
	taskManagerReplicaCount := getTaskManagerReplicaCount(currentAppDeployments.Items, application)
	if taskManagerReplicaCount != application.Spec.TaskManagerConfig.Replicas {
		return true, nil
	}

	jobManagerReplicaCount := getJobManagerReplicaCount(currentAppDeployments.Items, application)
	if jobManagerReplicaCount != application.Spec.JobManagerConfig.Replicas {
		return true, nil
	}
	return f.HasApplicationJobChanged(ctx, application)
}
