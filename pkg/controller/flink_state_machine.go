package controller

import (
	"context"
	"time"

	"fmt"

	"github.com/lyft/flinkk8soperator/pkg/apis/app/v1alpha1"
	"github.com/lyft/flinkk8soperator/pkg/controller/config"
	"github.com/lyft/flinkk8soperator/pkg/controller/flink"
	"github.com/lyft/flinkk8soperator/pkg/controller/flink/client"
	"github.com/lyft/flinkk8soperator/pkg/controller/k8"
	"github.com/lyft/flytestdlib/logger"
	"github.com/lyft/flytestdlib/promutils"
	"github.com/lyft/flytestdlib/promutils/labeled"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/clock"
)

const (
	jobFinalizer = "job.finalizers.flink.k8s.io"
)

type FlinkHandlerInterface interface {
	Handle(ctx context.Context, application *v1alpha1.FlinkApplication) error
}

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
	flinkController flink.ControllerInterface
	k8Cluster       k8.ClusterInterface
	clock           clock.Clock
	metrics         *stateMachineMetrics
}

type stateMachineMetrics struct {
	scope                             promutils.Scope
	stateMachineHandlePhaseMap        map[v1alpha1.FlinkApplicationPhase]labeled.StopWatch
	stateMachineHandleSuccessPhaseMap map[v1alpha1.FlinkApplicationPhase]labeled.StopWatch
	errorCounterPhaseMap              map[v1alpha1.FlinkApplicationPhase]labeled.Counter
}

func newStateMachineMetrics(scope promutils.Scope) *stateMachineMetrics {
	stateMachineScope := scope.NewSubScope("state_machine")
	stateMachineHandlePhaseMap := map[v1alpha1.FlinkApplicationPhase]labeled.StopWatch{}
	stateMachineHandleSuccessPhaseMap := map[v1alpha1.FlinkApplicationPhase]labeled.StopWatch{}
	errorCounterPhaseMap := map[v1alpha1.FlinkApplicationPhase]labeled.Counter{}

	for _, phase := range v1alpha1.FlinkApplicationPhases {
		phaseName := phase.VerboseString()
		stateMachineHandleSuccessPhaseMap[phase] = labeled.NewStopWatch(phaseName+"_"+"handle_time_success",
			fmt.Sprintf("Total time to handle the %s application state on success", phaseName), time.Millisecond, stateMachineScope)
		stateMachineHandlePhaseMap[phase] = labeled.NewStopWatch(phaseName+"_"+"handle_time",
			fmt.Sprintf("Total time to handle the %s application state", phaseName), time.Millisecond, stateMachineScope)
		errorCounterPhaseMap[phase] = labeled.NewCounter(phaseName+"_"+"error",
			fmt.Sprintf("Failure to handle the %s application state", phaseName), stateMachineScope)
	}
	return &stateMachineMetrics{
		scope:                             scope,
		stateMachineHandlePhaseMap:        stateMachineHandlePhaseMap,
		stateMachineHandleSuccessPhaseMap: stateMachineHandleSuccessPhaseMap,
		errorCounterPhaseMap:              errorCounterPhaseMap,
	}
}

func (s *FlinkStateMachine) updateApplicationPhase(ctx context.Context, application *v1alpha1.FlinkApplication, phase v1alpha1.FlinkApplicationPhase) error {
	application.Status.Phase = phase
	now := v1.NewTime(s.clock.Now())
	application.Status.LastUpdatedAt = &now

	// Update status of the cluster everytime there is a change in phase.
	_, err := s.flinkController.CompareAndUpdateClusterStatus(ctx, application)
	if err != nil {
		logger.Errorf(ctx, "Updating cluster status failed with %v", err)
	}

	// Update status of the job everytime there is a change in phase.
	_, err = s.flinkController.CompareAndUpdateJobStatus(ctx, application)
	if err != nil {
		logger.Errorf(ctx, "Updating job status failed with %v", err)
	}
	return s.k8Cluster.UpdateK8Object(ctx, application)
}

func (s *FlinkStateMachine) isApplicationStuck(ctx context.Context, application *v1alpha1.FlinkApplication) bool {
	appLastUpdated := application.Status.LastUpdatedAt
	if appLastUpdated != nil && application.Status.Phase != v1alpha1.FlinkApplicationFailed &&
		application.Status.Phase != v1alpha1.FlinkApplicationRunning {
		elapsedTime := s.clock.Since(appLastUpdated.Time)
		if s.getStalenessDuration() > 0 && elapsedTime > s.getStalenessDuration() {
			s.flinkController.LogEvent(ctx, application, "", corev1.EventTypeWarning,
				"Update", fmt.Sprintf("Application stuck for %v", elapsedTime))

			return true
		}
	}
	return false
}
func (s *FlinkStateMachine) Handle(ctx context.Context, application *v1alpha1.FlinkApplication) error {
	currentPhase := application.Status.Phase
	timer := s.metrics.stateMachineHandlePhaseMap[currentPhase].Start(ctx)
	successTimer := s.metrics.stateMachineHandleSuccessPhaseMap[currentPhase].Start(ctx)

	defer timer.Stop()
	err := s.handle(ctx, application)
	if err != nil {
		s.metrics.errorCounterPhaseMap[currentPhase].Inc(ctx)
	} else {
		successTimer.Stop()
	}
	return err
}

func (s *FlinkStateMachine) handle(ctx context.Context, application *v1alpha1.FlinkApplication) error {
	if s.isApplicationStuck(ctx, application) {
		return s.updateApplicationPhase(ctx, application, v1alpha1.FlinkApplicationFailed)
	}

	if !application.ObjectMeta.DeletionTimestamp.IsZero() && application.Status.Phase != v1alpha1.FlinkApplicationDeleting {
		// move to the deleting phase
		if err := s.updateApplicationPhase(ctx, application, v1alpha1.FlinkApplicationDeleting); err != nil {
			return err
		}
	}

	if application.Status.Phase != v1alpha1.FlinkApplicationRunning {
		logger.Infof(ctx, "Handling state %s for application", application.Status.Phase)
	}

	switch application.Status.Phase {
	case v1alpha1.FlinkApplicationNew:
		// In this state, we need a new flink cluster to be created or existing cluster to be updated
		return s.handleNewOrCreating(ctx, application)
	case v1alpha1.FlinkApplicationClusterStarting:
		return s.handleClusterStarting(ctx, application)
	case v1alpha1.FlinkApplicationReady:
		return s.handleApplicationReady(ctx, application)
	case v1alpha1.FlinkApplicationRunning:
		return s.handleApplicationRunning(ctx, application)
	case v1alpha1.FlinkApplicationUpdating:
		return s.handleApplicationUpdating(ctx, application)
	case v1alpha1.FlinkApplicationSavepointing:
		return s.handleApplicationSavepointing(ctx, application)
	case v1alpha1.FlinkApplicationDeleting:
		return s.handleApplicationDeleting(ctx, application)
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
	triggerID, err := s.flinkController.CancelWithSavepoint(ctx, application)
	if err != nil {
		return err
	}
	logger.Infof(ctx, "Flink job cancelled with savepoint, trigger id: %s", triggerID)
	application.Spec.SavepointInfo.TriggerID = triggerID
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

	// add the job running finalizer if necessary
	if err := s.addFinalizerIfMissing(ctx, application, jobFinalizer); err != nil {
		return err
	}

	s.flinkController.LogEvent(ctx, application, "", corev1.EventTypeNormal, "Update",
		"Flink cluster is ready")

	// Check that there are no jobs running before starting the job
	jobs, err := s.flinkController.GetJobsForApplication(ctx, application)
	if err != nil {
		return err
	}
	activeJob := flink.GetActiveFlinkJob(jobs)
	if activeJob == nil {
		logger.Infof(ctx, "No active job found for the application %v", jobs)
		jobID, err := s.flinkController.StartFlinkJob(ctx, application)
		if err != nil {
			s.flinkController.LogEvent(ctx, application, "", corev1.EventTypeWarning,
				"Update", fmt.Sprintf("Failed to submit job to cluster: %v", err))

			return err
		}

		s.flinkController.LogEvent(ctx, application, "", corev1.EventTypeNormal,
			"Update", "Flink job submitted to cluster")
		application.Status.JobStatus.JobID = jobID
	} else {
		logger.Infof(ctx, "Active job found for the application, job info: %v", activeJob)
		application.Status.JobStatus.JobID = activeJob.JobID
	}

	// Clear the savepoint info
	application.Spec.SavepointInfo = v1alpha1.SavepointInfo{}
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
	// The Operator needs to update its state with the right value.
	// In the Running state, there must be a job already started in the cluster.
	activeJob := flink.GetActiveFlinkJob(jobs)
	if activeJob != nil {
		application.Status.JobStatus.JobID = activeJob.JobID

		if activeJob.Status == client.Finished {
			return s.updateApplicationPhase(ctx, application, v1alpha1.FlinkApplicationCompleted)
		}
	}

	logger.Debugf(ctx, "Application running with job %v", activeJob)
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
	// Update status of the cluster
	hasClusterStatusChanged, clusterErr := s.flinkController.CompareAndUpdateClusterStatus(ctx, application)
	if clusterErr != nil {
		logger.Errorf(ctx, "Updating cluster status failed with %v", clusterErr)
	}

	// Update status of jobs on the cluster
	hasJobStatusChanged, jobsErr := s.flinkController.CompareAndUpdateJobStatus(ctx, application)
	if jobsErr != nil {
		logger.Errorf(ctx, "Updating jobs status failed with %v", jobsErr)
	}

	// Update k8s object if either job or cluster status has changed
	if hasJobStatusChanged || hasClusterStatusChanged {
		updateErr := s.k8Cluster.UpdateK8Object(ctx, application)
		if updateErr != nil {
			logger.Errorf(ctx, "Failed to update object %v", updateErr)
		}
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

	var restorePath string
	if savepointStatusResponse.Operation.Location == "" &&
		savepointStatusResponse.SavepointStatus.Status != client.SavePointInProgress {
		// Savepointing failed
		s.flinkController.LogEvent(ctx, application, "", corev1.EventTypeWarning,
			"Update", fmt.Sprintf("Failed to take savepoint: %v",
				savepointStatusResponse.Operation.FailureCause))

		// try to find an externalized checkpoint
		path, err := s.flinkController.FindExternalizedCheckpoint(ctx, application)
		if err != nil {
			logger.Infof(ctx, "error while fetching externalized checkpoint path: %v", err)
			return s.updateApplicationPhase(ctx, application, v1alpha1.FlinkApplicationFailed)
		} else if path == "" {
			logger.Infof(ctx, "no externalized checkpoint found")
			return s.updateApplicationPhase(ctx, application, v1alpha1.FlinkApplicationFailed)
		}

		s.flinkController.LogEvent(ctx, application, "", corev1.EventTypeNormal,
			"Update", fmt.Sprintf("Restoring from externalized checkpoint %s", path))

		restorePath = path
	} else if savepointStatusResponse.SavepointStatus.Status == client.SavePointCompleted {
		s.flinkController.LogEvent(ctx, application, "", corev1.EventTypeNormal,
			"Update", fmt.Sprintf("Canceled job with savepoint %s",
				savepointStatusResponse.Operation.Location))
		restorePath = savepointStatusResponse.Operation.Location
	}

	if restorePath != "" {
		isDeleted, err := s.flinkController.DeleteOldCluster(ctx, application)
		if err != nil {
			return err
		}
		if !isDeleted {
			return nil
		}

		application.Spec.SavepointInfo.SavepointLocation = restorePath
		application.Status.JobStatus.RestorePath = restorePath
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
	// TODO: allow recovering from externalized checkpoints (STRMCMP-252)
	return nil
}

func (s *FlinkStateMachine) getStalenessDuration() time.Duration {
	return config.GetConfig().StatemachineStalenessDuration.Duration
}

func (s *FlinkStateMachine) addFinalizerIfMissing(ctx context.Context, application *v1alpha1.FlinkApplication, finalizer string) error {
	for _, f := range application.Finalizers {
		if f == finalizer {
			return nil
		}
	}

	// finalizer not present; add
	application.Finalizers = append(application.Finalizers, finalizer)
	return s.k8Cluster.UpdateK8Object(ctx, application)
}

func removeString(list []string, target string) []string {
	ret := make([]string, 0)
	for _, s := range list {
		if s != target {
			ret = append(ret, s)
		}
	}

	return ret
}

func (s *FlinkStateMachine) clearFinalizers(ctx context.Context, app *v1alpha1.FlinkApplication) error {
	app.Finalizers = removeString(app.Finalizers, jobFinalizer)
	return s.k8Cluster.UpdateK8Object(ctx, app)
}

func jobFinished(jobs []client.FlinkJob, id string) bool {
	for _, job := range jobs {
		if job.JobID == id {
			return job.Status == client.Canceled ||
				job.Status == client.Failed ||
				job.Status == client.Finished
		}
	}

	return true
}

func (s *FlinkStateMachine) handleApplicationDeleting(ctx context.Context, app *v1alpha1.FlinkApplication) error {
	// There should be a way for the user to force deletion (e.g., if the job is failing and they can't
	// savepoint). However, this seems dangerous to do automatically.
	// If https://github.com/kubernetes/kubernetes/issues/56567 is fixed users will be able to use
	// kubectl delete --force, but for now they will need to update the DeleteMode.

	if app.Spec.DeleteMode == v1alpha1.DeleteModeNone {
		// just delete the finalizer so the cluster can be torn down
		return s.clearFinalizers(ctx, app)
	}

	jobs, err := s.flinkController.GetJobsForApplication(ctx, app)
	if err != nil {
		return err
	}

	finished := jobFinished(jobs, app.Status.JobStatus.JobID)

	switch app.Spec.DeleteMode {
	case v1alpha1.DeleteModeForceCancel:
		if finished {
			// the job has already been cancelled, so clear the finalizer
			return s.clearFinalizers(ctx, app)
		}

		logger.Infof(ctx, "Force cancelling job as part of cleanup")
		return s.flinkController.ForceCancel(ctx, app)
	case v1alpha1.DeleteModeSavepoint, "":
		if app.Spec.SavepointInfo.SavepointLocation != "" {
			if finished {
				return s.clearFinalizers(ctx, app)
			}
			// we've already created the savepoint, now just waiting for the job to be cancelled
			return nil
		}

		if app.Spec.SavepointInfo.TriggerID == "" {
			// delete with savepoint
			triggerID, err := s.flinkController.CancelWithSavepoint(ctx, app)
			if err != nil {
				return err
			}

			s.flinkController.LogEvent(ctx, app, "", corev1.EventTypeNormal, "Delete",
				"Cancelling job with savepoint")
			app.Spec.SavepointInfo.TriggerID = triggerID
		} else {
			// we've already started savepointing; check the status
			status, err := s.flinkController.GetSavepointStatus(ctx, app)
			if err != nil {
				return err
			}

			if status.Operation.Location == "" && status.SavepointStatus.Status != client.SavePointInProgress {
				// savepointing failed
				s.flinkController.LogEvent(ctx, app, "", corev1.EventTypeWarning, "Delete",
					fmt.Sprintf("Failed to take savepoint %v", status.Operation.FailureCause))
				// clear the trigger id so that we can try again
				app.Spec.SavepointInfo.TriggerID = ""
			} else if status.SavepointStatus.Status == client.SavePointCompleted {
				// we're done, clean up
				s.flinkController.LogEvent(ctx, app, "", corev1.EventTypeNormal, "Delete",
					fmt.Sprintf("Cancelled job with savepoint '%s'", status.Operation.Location))
				app.Spec.SavepointInfo.SavepointLocation = status.Operation.Location
				app.Spec.SavepointInfo.TriggerID = ""
			}
		}

		return s.k8Cluster.UpdateK8Object(ctx, app)
	default:
		logger.Errorf(ctx, "Unsupported DeleteMode %s", app.Spec.DeleteMode)
	}

	return nil
}

func NewFlinkStateMachine(scope promutils.Scope) FlinkHandlerInterface {

	metrics := newStateMachineMetrics(scope)
	return &FlinkStateMachine{
		k8Cluster:       k8.NewK8Cluster(),
		flinkController: flink.NewController(scope),
		clock:           clock.RealClock{},
		metrics:         metrics,
	}
}
