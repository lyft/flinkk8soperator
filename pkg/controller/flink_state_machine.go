package controller

import (
	"context"
	"time"

	"github.com/lyft/flinkk8soperator/pkg/apis/app/v1alpha1"
	"github.com/lyft/flinkk8soperator/pkg/controller/flink"
	"github.com/lyft/flinkk8soperator/pkg/controller/flink/client"
	"github.com/lyft/flinkk8soperator/pkg/controller/k8"
	"github.com/lyft/flytestdlib/logger"
	"github.com/spf13/viper"
	"k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/clock"
)

type FlinkHandlerInterface interface {
	Handle(ctx context.Context, application *v1alpha1.FlinkApplication) error
}

const (
	StatemachineStalenessDurationKey = "statemachineStalenessDuration"
)

// State Machine Transition for Flink Application
// Every state in the state machine can move to failed state.
//+-----------------------------------------------------------------------------------------------------------------+
//|                                                                                                                 |
//|                +---------+                                                                                      |
//|                |         |                                                                                      |
//|                |         |                                                                                      |
//|                |Updating |                                                                                      |
//|                |         <-----+--------------------------------------------+                                   |
//|                |         |                                                  |                                   |
//|                |         |                                                  |                                   |
//|                +----^----+                                                  |                                   |
//|                     |                                                       |                                   |
//|                     |                                                       |                                   |
//|      +---------+    |    +----------+          +----------+           +-----+-----+          +-----------+      |
//|      |         |    |    |          |          |          |           |           |          |           |      |
//|      |         |    |    |          |          |          |           |           |          |           |      |
//|      |   New   +---------> Starting +----------> Ready    +----------->  Running  +----------> Completed |      |
//|      |         |    |    |          |          |          |           |           |          |           |      |
//|      |         |    |    |          <          |          |           |           |          |           |      |
//|      |         |    |    |          |          |          |           |           |          |           |      |
//|      +----^----+    |    +----------+          +-----^----+           +-----+-----+          +-----------+      |
//|           |         |         |                      |                                                          |
//|           |         |         |                      |                                                          |
//|           |         |         |                      |                                                          |
//|           |         |   +-----v------+               |                                                          |
//|           |         |   |            |               |                                                          |
//|           |         +--->Savepointing|               |                                                          |
//|           +-------------+            +---------------+                                                          |
//|                         |            |                                                                          |
//|                         |            |                                                                          |
//|                         +------------+                                                                          |
//|                                                                                                                 |
//|                                                                                                                 |
//+-----------------------------------------------------------------------------------------------------------------+
type FlinkStateMachine struct {
	flinkController               flink.FlinkInterface
	k8Cluster                     k8.K8ClusterInterface
	clock                         clock.Clock
	statemachineStalenessDuration time.Duration
}

func (s *FlinkStateMachine) updateApplicationPhase(ctx context.Context, application *v1alpha1.FlinkApplication, phase v1alpha1.FlinkApplicationPhase) error {
	application.Status.Phase = phase
	now := v1.NewTime(s.clock.Now())
	application.Status.LastUpdatedAt = &now
	return s.k8Cluster.UpdateK8Object(ctx, application)
}

func (s *FlinkStateMachine) isApplicationStuck(ctx context.Context, application *v1alpha1.FlinkApplication) bool {
	appLastUpdated := application.Status.LastUpdatedAt
	if appLastUpdated != nil && application.Status.Phase != v1alpha1.FlinkApplicationFailed &&
		application.Status.Phase != v1alpha1.FlinkApplicationRunning {
		elapsedTime := s.clock.Since(appLastUpdated.Time)
		if elapsedTime > s.statemachineStalenessDuration {
			logger.Errorf(ctx, "Flink Application stuck for %v", elapsedTime)
			return true
		}
	}
	return false
}

func (s *FlinkStateMachine) Handle(ctx context.Context, application *v1alpha1.FlinkApplication) error {
	if s.isApplicationStuck(ctx, application) {
		return s.updateApplicationPhase(ctx, application, v1alpha1.FlinkApplicationFailed)
	}
	logger.Infof(ctx, "Handling state %s for application", application.Status.Phase)
	switch application.Status.Phase {
	case v1alpha1.FlinkApplicationNew:
		// In this state, we need a new flink cluster to be created or existing cluster to be updated
		return s.handleNewOrCreating(ctx, application)
	case v1alpha1.FlinkApplicationClusterStarting:
		return s.handleClusterStarting(ctx, application)
	case v1alpha1.FlinkApplicationRunning:
		return s.handleApplicationRunning(ctx, application)
	case v1alpha1.FlinkApplicationReady:
		return s.handleApplicationReady(ctx, application)
	case v1alpha1.FlinkApplicationUpdating:
		return s.handleApplicationUpdating(ctx, application)
	case v1alpha1.FlinkApplicationSavepointing:
		return s.handleApplicationSavepointing(ctx, application)
	case v1alpha1.FlinkApplicationFailed:
		return s.handleApplicationFailed(ctx, application)
	case v1alpha1.FlinkApplicationCompleted:
		return nil
	}
	return nil
}

// This indicates that this is a brand new CRD.
// Create the cluster for the Flink Application
func (s *FlinkStateMachine) handleNewOrCreating(ctx context.Context, application *v1alpha1.FlinkApplication) error {
	err := s.flinkController.CreateCluster(ctx, application)
	if err != nil {
		logger.Errorf(ctx, "Cluster creation failed with error: %v", err)
		return err
	}
	logger.Infof(ctx, "New cluster created")
	return s.updateApplicationPhase(ctx, application, v1alpha1.FlinkApplicationClusterStarting)
}

func (s *FlinkStateMachine) cancelWithSavepointAndTransitionState(ctx context.Context, application *v1alpha1.FlinkApplication) error {
	triggerId, err := s.flinkController.CancelWithSavepoint(ctx, application)
	if err != nil {
		return err
	}
	logger.Infof(ctx, "Flink job cancelled with savepoint, trigger id: %s", triggerId)
	application.Spec.FlinkJob.SavepointInfo.TriggerId = triggerId
	return s.updateApplicationPhase(ctx, application, v1alpha1.FlinkApplicationSavepointing)
}

// If the cluster is starting,
// This state has two cases
// 1) Primary Cluster starting - wait for active and kick off the job
// 2) Switch over cluster starting - wait for cluster to be running and issue cancel with savepoint to original cluster.
// Kick off the job using the REST API
func (s *FlinkStateMachine) handleClusterStarting(ctx context.Context, application *v1alpha1.FlinkApplication) error {
	// Wait for all to be running
	ready, err := s.flinkController.IsClusterReady(ctx, application)
	if err != nil {
		return err
	}
	if !ready {
		// Return and check and proceed state in the next callback
		return nil
	}
	logger.Infof(ctx, "Flink cluster has started successfully")
	_, oldDeployments, err := s.flinkController.GetCurrentAndOldDeploymentsForApp(ctx, application)
	if err != nil {
		return err
	}
	if len(oldDeployments) > 0 {
		logger.Infof(ctx, "Deployment for an older version of the application detected")
		return s.cancelWithSavepointAndTransitionState(ctx, application)
	}
	return s.updateApplicationPhase(ctx, application, v1alpha1.FlinkApplicationReady)
}

// This indicates that the cluster is ready for application with that latest image and configuration.
// Kick off the the flink application
func (s *FlinkStateMachine) handleApplicationReady(ctx context.Context, application *v1alpha1.FlinkApplication) error {
	isReady, _ := s.flinkController.IsServiceReady(ctx, application)
	// Ignore errors
	if !isReady {
		return nil
	}
	logger.Infof(ctx, "Flink cluster is ready to start the job")
	// Check that there are no jobs running before starting the job
	jobs, err := s.flinkController.GetJobsForApplication(ctx, application)
	if err != nil {
		return err
	}
	activeJob := flink.GetActiveFlinkJob(jobs)
	if activeJob == nil {
		logger.Infof(ctx, "No active job found for the application %v", jobs)
		jobId, err := s.flinkController.StartFlinkJob(ctx, application)
		if err != nil {
			return err
		}
		logger.Infof(ctx, "New flink job submitted successfully, jobId: %s", jobId)
		application.Status.JobId = jobId
	} else {
		logger.Infof(ctx, "Active job found for the application, job info: %v", activeJob)
		application.Status.JobId = activeJob.JobId
	}

	// Clear the savepoint info
	application.Spec.FlinkJob.SavepointInfo = v1alpha1.SavepointInfo{}
	return s.updateApplicationPhase(ctx, application, v1alpha1.FlinkApplicationRunning)
}

// Check if the application is Running.
// This is a stable state. Keep monitoring if the underlying CRD reflects the Flink cluster
func (s *FlinkStateMachine) handleApplicationRunning(ctx context.Context, application *v1alpha1.FlinkApplication) error {
	jobs, err := s.flinkController.GetJobsForApplication(ctx, application)
	if err != nil {
		return err
	}
	// The jobid in Flink can change if there is a Job manager failover.
	// The Operator needs to update it's state with the right value.
	// In the Running state, there must be a job already kickedoff in the cluster.
	activeJob := flink.GetActiveFlinkJob(jobs)
	if activeJob != nil {
		application.Status.JobId = activeJob.JobId

		if activeJob.Status == client.FlinkJobFinished {
			return s.updateApplicationPhase(ctx, application, v1alpha1.FlinkApplicationCompleted)
		}
	}
	logger.Infof(ctx, "Application running with job %v", activeJob)
	hasAppChanged, err := s.flinkController.HasApplicationChanged(ctx, application)
	if err != nil {
		return err
	}
	// If application has changed, we can perform all the updates here itself.
	// Pushing the state machine to Updating helps in monitoring and debuggability.
	if hasAppChanged {
		logger.Infof(ctx, "Application resource has changed. Moving to Updating")
		return s.updateApplicationPhase(ctx, application, v1alpha1.FlinkApplicationUpdating)
	}
	return nil
}

// This is only triggered when the current job is cancelled with Savepoint.
// Once the savepoint is ready, switch the cluster and tear the first cluster.
func (s *FlinkStateMachine) handleApplicationSavepointing(ctx context.Context, application *v1alpha1.FlinkApplication) error {
	savepointStatusResponse, err := s.flinkController.GetSavepointStatus(ctx, application)
	if err != nil {
		return err
	}

	if savepointStatusResponse.Operation.Location == "" &&
		savepointStatusResponse.SavepointStatus.Status != client.SavePointInProgress {
		// Savepointing failed
		return s.updateApplicationPhase(ctx, application, v1alpha1.FlinkApplicationFailed)
	}

	if savepointStatusResponse.SavepointStatus.Status == client.SavePointCompleted {
		logger.Infof(ctx, "Application savepoint operation succeeded %v", savepointStatusResponse)
		isDeleted, err := s.flinkController.DeleteOldCluster(ctx, application)
		if err != nil {
			return err
		}
		if !isDeleted {
			return nil
		}
		application.Spec.FlinkJob.SavepointInfo.SavepointLocation = savepointStatusResponse.Operation.Location
		// In single deployment mode, after the cluster is deleted, no extra cluster exists
		if application.Spec.DeploymentMode == v1alpha1.DeploymentModeSingle {
			return s.updateApplicationPhase(ctx, application, v1alpha1.FlinkApplicationNew)
		}
		return s.updateApplicationPhase(ctx, application, v1alpha1.FlinkApplicationReady)
	}
	return nil
}

// This state is triggered by an update to the CRD, and application needs to be updated.
// Updating has two flows
// (1) Bring up a new cluster, cancel the current job with savepoint, and restart the new job from the savepoint
// (2) Update configuration in the existing cluster.
func (s *FlinkStateMachine) handleApplicationUpdating(ctx context.Context, application *v1alpha1.FlinkApplication) error {
	hasApplicationChanged, err := s.flinkController.HasApplicationChanged(ctx, application)
	if err != nil {
		return err
	}

	if hasApplicationChanged {
		logger.Infof(ctx, "Flink cluster for the application requires modification")
		if application.Spec.DeploymentMode == v1alpha1.DeploymentModeDual {
			logger.Infof(ctx, "Creating a new flink cluster for application as dual mode is enabled")
			err := s.flinkController.CreateCluster(ctx, application)
			if err != nil {
				return err
			}
		}
		return s.cancelWithSavepointAndTransitionState(ctx, application)
	}
	return s.updateApplicationPhase(ctx, application, v1alpha1.FlinkApplicationClusterStarting)
}

// In the Failed state, the image of the application has to be updated for automatic recovery.
// If the application is in Failed state, due to a particular reason, and the underlying issue has been fixed,
// then the phase needs to be updated manually. In the failed state, the operator will not monitor the underlying
// cluster.
// Dual mode does not apply during the failed state.
func (s *FlinkStateMachine) handleApplicationFailed(ctx context.Context, application *v1alpha1.FlinkApplication) error {
	currentDeployments, _, err := s.flinkController.GetCurrentAndOldDeploymentsForApp(ctx, application)
	if err != nil {
		return err
	}
	// If current deployments is zero, it indicates that image changed.
	if len(currentDeployments) == 0 {
		isDeleted, err := s.flinkController.DeleteOldCluster(ctx, application)
		if err != nil {
			return err
		}
		logger.Infof(ctx, "Deleting the existing flink cluster for the application")
		if isDeleted {
			return s.updateApplicationPhase(ctx, application, v1alpha1.FlinkApplicationNew)
		}
	}
	return nil
}

func NewFlinkStateMachine() FlinkHandlerInterface {
	statemachineStalenessDuration, err := time.ParseDuration(viper.GetString(StatemachineStalenessDurationKey))
	if err != nil {
		return nil
	}
	return &FlinkStateMachine{
		k8Cluster:                     k8.NewK8Cluster(),
		flinkController:               flink.NewFlinkController(),
		statemachineStalenessDuration: statemachineStalenessDuration,
		clock: clock.RealClock{},
	}
}
