package controller

import (
	"context"

	"github.com/lyft/flinkk8soperator/pkg/apis/app/v1alpha1"
	"github.com/lyft/flinkk8soperator/pkg/controller/flink"
	"github.com/lyft/flinkk8soperator/pkg/controller/flink/client"
	"github.com/lyft/flinkk8soperator/pkg/controller/k8"
)

type FlinkHandlerInterface interface {
	Handle(ctx context.Context, application *v1alpha1.FlinkApplication) error
}

// State Machine Transition for Flink Application:
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
//|      +---------+    |    +----------+          +----------+           +-----+-----+          +----------+       |
//|      |         |    |    |          |          |          |           |           |          |          |       |
//|      |         |    |    |          |          |          |           |           |          |          |       |
//|      |   New   +---------> Starting +----------> Ready    +----------->  Running  +---------->  Failed  |       |
//|      |         |    |    |          |          |          |           |           |          |          |       |
//|      |         |    |    |          <-----+    |          |           |           |          |          |       |
//|      |         |    |    |          |     |    |          |           |           |          |          |       |
//|      +----^----+    |    +----------+     |    +-----^----+           +-----+-----+          +----------+       |
//|           |         |         |           |          |                                                          |
//|           |         |         |           |          |                                                          |
//|           |         |         |           |          |                                                          |
//|           |         |   +-----v-----+     |          |                                                          |
//|           |         |   |           +------          |                                                          |
//|           |         +--->Savepointing                |                                                          |
//|           +-------------+           +----------------+                                                          |
//|                         |           |                                                                           |
//|                         |           |                                                                           |
//|                         +-----------+                                                                           |
//|                                                                                                                 |
//|                                                                                                                 |
//+-----------------------------------------------------------------------------------------------------------------+
type FlinkStateMachine struct {
	flinkController flink.FlinkInterface
	k8Cluster       k8.K8ClusterInterface
}

func (s *FlinkStateMachine) updateApplicationPhase(ctx context.Context, application *v1alpha1.FlinkApplication, phase v1alpha1.FlinkApplicationPhase) error {
	application.Status.Phase = phase
	return s.k8Cluster.UpdateK8Object(ctx, application)
}

func (s *FlinkStateMachine) Handle(ctx context.Context, application *v1alpha1.FlinkApplication) error {
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
	case v1alpha1.FlinkApplicationStopped:
		return nil
	}
	return nil
}

// This indicates that this is a brand new CRD.
// Create the cluster for the Flink Application
func (s *FlinkStateMachine) handleNewOrCreating(ctx context.Context, application *v1alpha1.FlinkApplication) error {
	err := s.flinkController.CreateCluster(ctx, application)
	if err != nil {
		return err
	}
	return s.updateApplicationPhase(ctx, application, v1alpha1.FlinkApplicationClusterStarting)
}

func (s *FlinkStateMachine) cancelWithSavepointAndTransitionState(ctx context.Context, application *v1alpha1.FlinkApplication) error {
	triggerId, err := s.flinkController.CancelWithSavepoint(ctx, application)
	if err != nil {
		return err
	}
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
	multiClusterPresent, err := s.flinkController.IsMultipleClusterPresent(ctx, application)
	if err != nil {
		return err
	}
	if multiClusterPresent {
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
	jobs, err := s.flinkController.GetJobsForApplication(ctx, application)
	if err != nil {
		return err
	}

	activeJob := flink.GetActiveFlinkJob(jobs)
	if activeJob == nil {
		jobId, err := s.flinkController.StartFlinkJob(ctx, application)
		if err != nil {
			return err
		}
		application.Status.ActiveJobId = jobId
	} else {
		application.Status.ActiveJobId = activeJob.JobId

	}

	// Clear the savepoint info
	application.Spec.FlinkJob.SavepointInfo = v1alpha1.SavepointInfo{}
	return s.updateApplicationPhase(ctx, application, v1alpha1.FlinkApplicationRunning)
}

// Check if the application is Running.
// This is a stable state. Keep monitoring if the underlying CRD reflects the Flink cluster
func (s *FlinkStateMachine) handleApplicationRunning(ctx context.Context, application *v1alpha1.FlinkApplication) error {
	hasAppChanged, err := s.flinkController.HasApplicationChanged(ctx, application)
	if err != nil {
		return err
	}
	// If application has changed, we can perform all the updates here itself.
	// Pushing the state machine to Updating helps in monitoring and debuggability.
	if hasAppChanged {
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
		return s.updateApplicationPhase(ctx, application, v1alpha1.FlinkApplicationClusterStarting)
	}

	if savepointStatusResponse.SavepointStatus.Status == client.SavePointCompleted {
		err := s.flinkController.DeleteOldCluster(ctx, application, false)
		if err != nil {
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
	isClusterChangeNeeded, err := s.flinkController.IsClusterChangeNeeded(ctx, application)
	if err != nil {
		return err
	}
	if isClusterChangeNeeded {
		if application.Spec.DeploymentMode == v1alpha1.DeploymentModeDual {
			err := s.flinkController.CreateCluster(ctx, application)
			if err != nil {
				return err
			}
		}
		return s.updateApplicationPhase(ctx, application, v1alpha1.FlinkApplicationClusterStarting)
	}

	_, err = s.flinkController.CheckAndUpdateClusterResources(ctx, application)
	if err != nil {
		return err
	}
	differentParallelism, err := s.flinkController.HasApplicationJobChanged(ctx, application)
	if err != nil {
		return err
	}

	if differentParallelism {
		return s.cancelWithSavepointAndTransitionState(ctx, application)
	}
	return s.updateApplicationPhase(ctx, application, v1alpha1.FlinkApplicationClusterStarting)
}

func (s *FlinkStateMachine) handleApplicationFailed(ctx context.Context, application *v1alpha1.FlinkApplication) error {
	return nil
}

func NewFlinkStateMachine() FlinkHandlerInterface {
	return &FlinkStateMachine{
		k8Cluster:       k8.NewK8Cluster(),
		flinkController: flink.NewFlinkController(),
	}
}
