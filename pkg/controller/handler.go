package controller

import (
	"context"

	"github.com/lyft/flinkk8soperator/pkg/apis/app/v1alpha1"

	"reflect"

	"github.com/lyft/flinkk8soperator/pkg/controller/errors"
	"github.com/lyft/flinkk8soperator/pkg/controller/logger"
	"github.com/operator-framework/operator-sdk/pkg/sdk"
	"k8s.io/client-go/tools/record"
)

func NewHandler() sdk.Handler {
	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(logger.InfofNoCtx)
	//eventBroadcaster.StartRecordingToSink(&corev1.EventSinkImpl{Interface: client.CoreV1().Events("")})
	//recorder := eventBroadcaster.NewRecorder(scheme.Scheme, corev1.EventSource{Component: controllerAgentName})
	// TODO implement event sink logic
	return &Handler{
		recorder:     nil,
		flinkHandler: NewFlinkHandler(nil),
	}
}

type Handler struct {
	flinkHandler FlinkHandlerIface
	recorder     record.EventRecorder
}

var jobObjectType = reflect.TypeOf(&v1alpha1.FlinkJob{}).String()

func (h *Handler) Handle(ctx context.Context, event sdk.Event) error {
	switch o := event.Object.(type) {
	case *v1alpha1.FlinkJob:
		if event.Deleted {
			logger.Infof(ctx, "Flink Job deleted, we will let the owner references cascade downstream")
			return nil
		}
		err := h.HandleFlinkJob(ctx, o)
		if err != nil {
			logger.Errorf(ctx, "Failed to Handle Flink Job: %v", err)
			return err
		}
	default:
		return errors.Errorf(errors.IllegalStateError, "Operator can only handle jobs of type [%v]. Received [%v]", jobObjectType, reflect.TypeOf(o).String())
	}
	return nil
}

func (h *Handler) HandleFlinkJob(ctx context.Context, job *v1alpha1.FlinkJob) error {
	switch job.Status.Phase {
	case v1alpha1.FlinkJobNew:
		logger.Infof(ctx, "New FlinkJob [%s] received.", job.Name)
		fallthrough
	case v1alpha1.FlinkJobRunning:
		// Now that the FlinkJob is accepted start setting it up
		err := h.flinkHandler.StartJob(ctx, job)
		if err != nil {
			logger.Errorf(ctx, "%v", err)
			if errors.IsReconciliationNeeded(err) {
				job.Status.UpdatePhase(v1alpha1.FlinkJobCheckpointing, err.Error())
				logger.Infof(ctx, "New FlinkJob [%s] Updating.", job.Name)
			} else {
				job.Status.UpdatePhase(v1alpha1.FlinkJobFailed, err.Error())
				logger.Infof(ctx, "New FlinkJob [%s] Failed.", job.Name)
			}
		} else {
			logger.Infof(ctx, "New FlinkJob [%s] Running.", job.Name)
			job.Status.UpdatePhase(v1alpha1.FlinkJobRunning, "")
		}
		return sdk.Update(job)
	case v1alpha1.FlinkJobCheckpointing:
		err := h.flinkHandler.CheckpointJob(ctx, job)
		if err != nil {
			logger.Infof(ctx, "Failed to Checkpoint. Error [%v]", err)
			return err
		}
		job.Status.UpdatePhase(v1alpha1.FlinkJobUpdating, "Checkpoint successfully taken")
		return sdk.Update(job)
	case v1alpha1.FlinkJobUpdating:
		err := h.flinkHandler.UpdateJob(ctx, job)
		if err != nil {
			logger.Infof(ctx, "Failed to Update Job. Error [%v]", err)
			return err
		}
		job.Status.UpdatePhase(v1alpha1.FlinkJobRunning, "Checkpoint successfully taken")
		return sdk.Update(job)
	case v1alpha1.FlinkJobStopped:
		// This job is no longer used, but has not been deleted
		logger.Infof(ctx, "Job [%s] has been stopped. Ignoring", job.Name)
		// TODO, we may want to go from stopped to Accepted Again
		return nil
	case v1alpha1.FlinkJobFailed:
		// This job has failed and is no longer updated by the system
		logger.Infof(ctx, "Job [%s] has failed. Ignoring", job.Name)
		return nil

	}
	return nil
}
