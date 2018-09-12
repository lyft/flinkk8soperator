package controller

import (
	"context"

	"github.com/lyft/flinkk8soperator/pkg/apis/app/v1alpha1"
	flinkErrors "github.com/lyft/flinkk8soperator/pkg/controller/errors"
	"github.com/lyft/flinkk8soperator/pkg/controller/helpers"
	"github.com/lyft/flinkk8soperator/pkg/controller/logger"
	"github.com/operator-framework/operator-sdk/pkg/sdk"
	"k8s.io/api/apps/v1"
	coreV1 "k8s.io/api/core/v1"
	"k8s.io/api/extensions/v1beta1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
)

var foregroundDeletion = metav1.DeletePropagationForeground
var defaultDeleteOptions = sdk.WithDeleteOptions(&metav1.DeleteOptions{PropagationPolicy: &foregroundDeletion})

type FlinkHandlerIface interface {
	StartJob(ctx context.Context, job *v1alpha1.FlinkJob) error
	StopJob(ctx context.Context, job *v1alpha1.FlinkJob) error
	UpdateJob(ctx context.Context, job *v1alpha1.FlinkJob) error
	CheckpointJob(ctx context.Context, job *v1alpha1.FlinkJob) error
}

func NewFlinkHandler(recorder record.EventRecorder) FlinkHandlerIface {
	return &flinkHandler{
		recorder: recorder,
	}
}

type flinkHandler struct {
	recorder record.EventRecorder
}

func (f *flinkHandler) getJobManagerDeployment(ctx context.Context, job *v1alpha1.FlinkJob) (*v1.Deployment, error) {
	currentDeployment := helpers.FetchJobMangerDeploymentIdentityObj(job)
	err := sdk.Get(currentDeployment)
	if err != nil {
		logger.Warningf(ctx, "Failed to get JobManager deployment, [%v]", err)
		return nil, err
	}
	return currentDeployment, nil
}

func (f *flinkHandler) getJobManagerService(ctx context.Context, job *v1alpha1.FlinkJob) (*coreV1.Service, error) {
	currentService := helpers.FetchJobManagerServiceIdentityObj(job)
	err := sdk.Get(currentService)
	if err != nil {
		logger.Warningf(ctx, "Failed to get JobManager service [%v]", err)
		return nil, err
	}
	return currentService, nil
}

func (f *flinkHandler) getJobManagerIngress(ctx context.Context, job *v1alpha1.FlinkJob) (*v1beta1.Ingress, error) {
	currentService := helpers.FetchJobManagerIngressIdentityObj(job)
	err := sdk.Get(currentService)
	if err != nil {
		logger.Warningf(ctx, "Failed to get JobManager Ingress [%v]", err)
		return nil, err
	}
	return currentService, nil
}

func (f *flinkHandler) getTaskManagerDeployment(ctx context.Context, job *v1alpha1.FlinkJob) (*v1.Deployment, error) {
	currentDeployment := helpers.FetchTaskMangerDeploymentIdentityObj(job)
	err := sdk.Get(currentDeployment)
	if err != nil {
		logger.Warningf(ctx, "Failed to get TaskManager deployment [%v]", err)
		return nil, err
	}
	return currentDeployment, nil
}

func (f *flinkHandler) deleteJobManagerDeployment(ctx context.Context, job *v1alpha1.FlinkJob) error {
	currentDeployment := helpers.FetchJobMangerDeploymentIdentityObj(job)
	err := sdk.Delete(currentDeployment, defaultDeleteOptions)
	if err != nil {
		logger.Warningf(ctx, "Failed to get JobManager deployment, [%v]", err)
		return err
	}
	return nil
}

func (f *flinkHandler) deleteJobManagerService(ctx context.Context, job *v1alpha1.FlinkJob) error {
	currentService := helpers.FetchJobManagerServiceIdentityObj(job)
	err := sdk.Delete(currentService, defaultDeleteOptions)
	if err != nil {
		logger.Warningf(ctx, "Failed to get JobManager service [%v]", err)
		return err
	}
	return nil
}

func (f *flinkHandler) deleteJobManagerIngress(ctx context.Context, job *v1alpha1.FlinkJob) error {
	currentService := helpers.FetchJobManagerIngressIdentityObj(job)
	err := sdk.Delete(currentService, defaultDeleteOptions)
	if err != nil {
		logger.Warningf(ctx, "Failed to get JobManager Ingress [%v]", err)
		return err
	}
	return nil
}

func (f *flinkHandler) deleteTaskManagerDeployment(ctx context.Context, job *v1alpha1.FlinkJob) error {
	currentDeployment := helpers.FetchTaskMangerDeploymentIdentityObj(job)
	err := sdk.Delete(currentDeployment, defaultDeleteOptions)
	if err != nil {
		logger.Warningf(ctx, "Failed to get TaskManager deployment [%v]", err)
		return err
	}
	return nil
}

func (f *flinkHandler) createJobManagerDeployment(ctx context.Context, job *v1alpha1.FlinkJob) (*v1.Deployment, error) {
	newDeployment := helpers.FetchJobMangerDeploymentCreateObj(job)
	err := sdk.Create(newDeployment)
	if err != nil {
		return nil, err
		// TODO add what should the reconciliation should look like
		//if err := sdk.Update(newDeployment); err != nil {
		//	return nil, err
		//}
	}
	return newDeployment, nil
}

func (f *flinkHandler) createJobManagerService(ctx context.Context, job *v1alpha1.FlinkJob) (*coreV1.Service, error) {
	newServiceSpec := helpers.FetchJobManagerServiceCreateObj(job)
	err := sdk.Create(newServiceSpec)
	if err != nil {
		return nil, err
	}
	return newServiceSpec, nil
}

func (f *flinkHandler) createJobManagerIngress(ctx context.Context, job *v1alpha1.FlinkJob) (*v1beta1.Ingress, error) {
	newIngressSpec := helpers.FetchJobManagerIngressCreateObj(job)
	err := sdk.Create(newIngressSpec)
	if err != nil {
		return nil, err
	}
	return newIngressSpec, nil
}

func (f *flinkHandler) createTaskManagerDeployment(ctx context.Context, job *v1alpha1.FlinkJob) (*v1.Deployment, error) {
	newDeployment := helpers.FetchTaskMangerDeploymentCreateObj(job)
	err := sdk.Create(newDeployment)
	if err != nil {
		return nil, err
	}
	return newDeployment, nil
}

func (f *flinkHandler) create(ctx context.Context, job *v1alpha1.FlinkJob) error {
	logger.Infof(ctx, "Creating Flink Job Cluster[%v]", job.Name)
	if _, err := f.createJobManagerDeployment(ctx, job); err != nil {
		if !errors.IsAlreadyExists(err) {
			return err
		}
		logger.Infof(ctx, "JobManager Deployment already exists. Moving on")
	}
	logger.Infof(ctx, "JobManager Deployment created")

	if _, err := f.createJobManagerService(ctx, job); err != nil {
		if !errors.IsAlreadyExists(err) {
			return err
		}
		logger.Infof(ctx, "JobManagerService already exists. Moving on")
	}
	logger.Infof(ctx, "JobManager Service created")

	if _, err := f.createJobManagerIngress(ctx, job); err != nil {
		if !errors.IsAlreadyExists(err) {
			return err
		}
		logger.Infof(ctx, "JobManagerIngress already exists. Moving on")
	}
	logger.Infof(ctx, "JobManager Ingress created")

	if _, err := f.createTaskManagerDeployment(ctx, job); err != nil {
		if !errors.IsAlreadyExists(err) {
			return err
		}
		logger.Infof(ctx, "TaskManager already exists. Moving on")
	}
	logger.Infof(ctx, "TaskManagers created")

	job.Status.TouchResource("Updated FlinkJob")
	return nil
}

// Function checks if all the components exist.
// Returns true, if they exist, false otherwise. Error indicates any error in processing
func (f *flinkHandler) allComponentsExist(ctx context.Context, job *v1alpha1.FlinkJob) (bool, error) {
	if _, err := f.getJobManagerDeployment(ctx, job); err != nil {
		if errors.IsNotFound(err) {
			return false, nil
		}
		return false, flinkErrors.WrapErrorf(flinkErrors.CausedByError, err, "Failed to Get Existing Job Manager deployment")
	}
	if _, err := f.getJobManagerService(ctx, job); err != nil {
		if errors.IsNotFound(err) {
			return false, nil
		}
		return false, flinkErrors.WrapErrorf(flinkErrors.CausedByError, err, "Failed to Get Existing Job Manager Service")
	}
	if _, err := f.getJobManagerIngress(ctx, job); err != nil {
		if errors.IsNotFound(err) {
			return false, nil
		}
		return false, flinkErrors.WrapErrorf(flinkErrors.CausedByError, err, "Failed to Get Existing Job Manager Ingress")
	}
	if _, err := f.getTaskManagerDeployment(ctx, job); err != nil {
		if errors.IsNotFound(err) {
			return false, nil
		}
		return false, flinkErrors.WrapErrorf(flinkErrors.CausedByError, err, "Failed to Get Existing Task Manager deployment")
	}
	// All the components Exist
	return true, nil
}

// In the case when all components exist, check if they differ from the expected state
// Return true, if the actual and intended state differ
// Return false otherwise
// Error indicates problems in the processing
func (f *flinkHandler) isReconciliationNeeded(ctx context.Context, job *v1alpha1.FlinkJob) (bool, error) {
	tm, err := f.getTaskManagerDeployment(ctx, job)
	if err != nil {
		if errors.IsNotFound(err) {
			return false, nil
		}
		return false, flinkErrors.WrapErrorf(flinkErrors.CausedByError, err, "Failed to Get Existing Job Manager deployment")
	}
	if len(tm.Spec.Template.Spec.Containers) == 0 {
		logger.Infof(ctx, "No Containers found in Task Manager. Reconciliation needed.")
		return true, nil
	}
	if tm.Spec.Template.Spec.Containers[0].Image != job.Spec.Image {
		return true, nil
	}
	if tm.Spec.Replicas == nil || *tm.Spec.Replicas != job.Spec.NumberTaskManagers {
		return true, nil
	}
	return false, nil
}

// We only restart Job Manager when the image has changed. Nothing else changes the job manager
func (f *flinkHandler) jobManagerRestartRequired(ctx context.Context, job *v1alpha1.FlinkJob) (bool, error) {
	jm, err := f.getJobManagerDeployment(ctx, job)
	if err != nil {
		if errors.IsNotFound(err) {
			return true, nil
		}
		// Error lets fail this round
		return true, err
	}
	if len(jm.Spec.Template.Spec.Containers) > 0 && jm.Spec.Template.Spec.Containers[0].Image == job.Spec.Image {
		// Image is the same, skip restarting
		return false, nil
	}
	return true, nil
}

// This is the core reconciliation algorithm
// Do this for JobManager first and then for TaskManager
// STEP 1: Check if the resource already exists.
//         If it does not, go to startJob
// STEP 2: If it exists, compare the Image for the deployment in TaskManager and the replicas size in JobManager
// STEP 3: If they are the same return
// STEP 4: If not the same, patch and update
func (f *flinkHandler) StartJob(ctx context.Context, job *v1alpha1.FlinkJob) error {
	// This is a quick cache check to avoid reconciling and calling KubeAPI create in a steady state.

	ok, err := f.allComponentsExist(ctx, job)
	if err != nil {
		return err
	}
	if !ok {
		return f.create(ctx, job)
	}

	reconcileNeeded, err := f.isReconciliationNeeded(ctx, job)
	if err != nil {
		logger.Infof(ctx, "Reconcilition needed check failed")
		return err
	}
	if reconcileNeeded {
		return flinkErrors.Errorf(flinkErrors.ReconciliationNeeded, "Reconciliation needed.")
	}
	//TODO update should be a separate rounds
	return nil
}

func (f *flinkHandler) CheckpointJob(ctx context.Context, job *v1alpha1.FlinkJob) error {
	return nil
}

func (f *flinkHandler) UpdateJob(ctx context.Context, job *v1alpha1.FlinkJob) error {
	// We should Checkpoint before and then enter
	if err := f.StopJob(ctx, job); err != nil {
		return err
	}
	yes, err := f.jobManagerRestartRequired(ctx, job)
	if err != nil {
		return err
	}
	if yes {
		return f.create(ctx, job)
	}
	if _, err := f.createTaskManagerDeployment(ctx, job); err != nil {
		// This should never happen?
		if !errors.IsAlreadyExists(err) {
			return err
		}
		logger.Infof(ctx, "TaskManagerService already exists. Moving on.")
	}
	return nil
}

func (f *flinkHandler) StopJob(ctx context.Context, job *v1alpha1.FlinkJob) error {

	restartJobManger, err := f.jobManagerRestartRequired(ctx, job)
	if err != nil {
		return err
	}
	if err := f.deleteTaskManagerDeployment(ctx, job); err != nil {
		if errors.IsNotFound(err) {
			return nil
		}
		return err
	}
	if restartJobManger {
		if err := f.deleteJobManagerService(ctx, job); err != nil {
			if errors.IsNotFound(err) {
				return nil
			}
			return err
		}
		if err := f.deleteJobManagerDeployment(ctx, job); err != nil {
			if errors.IsNotFound(err) {
				return nil
			}
			return err
		}
		if err := f.deleteJobManagerIngress(ctx, job); err != nil {
			if errors.IsNotFound(err) {
				return nil
			}
			return err
		}
	}
	return nil
}
