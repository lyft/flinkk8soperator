package flink

import (
	"context"
	"errors"
	"fmt"

	"github.com/lyft/flinkk8soperator/pkg/config"
	"github.com/lyft/flytestdlib/logger"

	"github.com/lyft/flinkk8soperator/pkg/apis/app/v1alpha1"
	"github.com/lyft/flinkk8soperator/pkg/controller/flink/client"
	"github.com/lyft/flinkk8soperator/pkg/controller/k8"
	"k8s.io/api/apps/v1"
)

const proxyUrl = "http://localhost:%d/api/v1/namespaces/%s/services/%s-jm:8081/proxy"
const port = 8081

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

func getUrlFromApp(application *v1alpha1.FlinkApplication) string {
	if config.UseProxy {
		return fmt.Sprintf(proxyUrl, config.ProxyPort, application.Namespace, application.Name)
	} else {
		return fmt.Sprintf("http://%s:%d", GetJobManagerExternalServiceName(application), port)
	}
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
	jobId, err := f.getJobIdForApplication(ctx, application)
	if err != nil {
		return false, err
	}
	jobConfig, err := f.flinkClient.GetJobConfig(ctx, getUrlFromApp(application), jobId)
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

	replicas := computeTaskManagerReplicas(application)

	taskManagerDeployment := getTaskManagerDeployment(currentAppDeployments.Items, application)
	if *taskManagerDeployment.Spec.Replicas != replicas {
		taskManagerDeployment.Spec.Replicas = &replicas
		err := f.k8Cluster.UpdateK8Object(ctx, taskManagerDeployment)
		if err != nil {
			logger.Errorf(ctx, "Taskmanager deployment update failed %v", err)
			return false, err
		}
		hasUpdated = true
	}

	jobManagerDeployment := getJobManagerDeployment(currentAppDeployments.Items, application)
	if *jobManagerDeployment.Spec.Replicas != application.Spec.JobManagerConfig.Replicas {
		jobManagerDeployment.Spec.Replicas = &application.Spec.JobManagerConfig.Replicas
		err := f.k8Cluster.UpdateK8Object(ctx, jobManagerDeployment)
		if err != nil {
			logger.Errorf(ctx, "Jobmanager deployment update failed %v", err)
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
	jobResponse, err := f.flinkClient.GetJobs(ctx, getUrlFromApp(application))
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
	jobId, err := f.getJobIdForApplication(ctx, application)
	if err != nil {
		return "", err
	}
	return f.flinkClient.CancelJobWithSavepoint(ctx, getUrlFromApp(application), jobId)
}

func (f *FlinkController) CreateCluster(ctx context.Context, application *v1alpha1.FlinkApplication) error {
	err := f.flinkJobManager.CreateIfNotExist(ctx, application)
	if err != nil {
		logger.Errorf(ctx, "Job manager cluster creation did not succeed %v", err)
		return err
	}
	err = f.FlinkTaskManager.CreateIfNotExist(ctx, application)
	if err != nil {
		logger.Errorf(ctx, "Task manager cluster creation did not succeed %v", err)
		return err
	}
	return nil
}

func (f *FlinkController) StartFlinkJob(ctx context.Context, application *v1alpha1.FlinkApplication) (string, error) {
	response, err := f.flinkClient.SubmitJob(
		ctx,
		getUrlFromApp(application),
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
		logger.Errorf(ctx, "Job id in the submit job response was empty")
		return "", errors.New("unable to submit job: invalid job id")
	}
	return response.JobId, nil
}

func (f *FlinkController) GetSavepointStatus(ctx context.Context, application *v1alpha1.FlinkApplication) (*client.SavepointResponse, error) {
	jobId, err := f.getJobIdForApplication(ctx, application)
	if err != nil {
		return nil, err
	}
	return f.flinkClient.CheckSavepointStatus(ctx, getUrlFromApp(application), jobId, application.Spec.FlinkJob.SavepointInfo.TriggerId)
}

func (f *FlinkController) DeleteOldCluster(ctx context.Context, application *v1alpha1.FlinkApplication) (bool, error) {
	_, oldDeployments, err := f.GetCurrentAndOldDeploymentsForApp(ctx, application)
	if err != nil {
		return false, err
	}
	if len(oldDeployments) == 0 {
		logger.Infof(ctx, "No old deployments found for the cluster to delete")
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
	_, err := f.flinkClient.GetClusterOverview(ctx, getUrlFromApp(application))
	if err != nil {
		logger.Infof(ctx, "Error response indicating flink API is not ready to handle request %v", err)
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
		logger.Infof(ctx, "Flink cluster for the application needs to be changed")
		return true, nil
	}

	clusterUpdateNeeded, err := f.isClusterUpdateNeeded(ctx, application)
	if err != nil {
		return false, err
	}

	if clusterUpdateNeeded {
		logger.Infof(ctx, "Flink cluster for the application needs to be updated")
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
		logger.Infof(ctx, "No deployments found matching the current application spec")
		return true, nil
	}
	return false, nil
}

func (f *FlinkController) isClusterUpdateNeeded(ctx context.Context, application *v1alpha1.FlinkApplication) (bool, error) {
	currentAppDeployments, err := f.getDeploymentsForApp(ctx, application)
	if err != nil {
		return false, err
	}
	replicasNeeded := computeTaskManagerReplicas(application)
	taskManagerReplicaCount := getTaskManagerReplicaCount(currentAppDeployments.Items, application)
	if taskManagerReplicaCount != replicasNeeded {
		return true, nil
	}

	jobManagerReplicaCount := getJobManagerReplicaCount(currentAppDeployments.Items, application)
	if jobManagerReplicaCount != application.Spec.JobManagerConfig.Replicas {
		return true, nil
	}
	return f.HasApplicationJobChanged(ctx, application)
}
