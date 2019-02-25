package controller

import (
	"context"

	"github.com/lyft/flinkk8soperator/pkg/apis/app/v1alpha1"

	"reflect"

	"github.com/lyft/flinkk8soperator/pkg/controller/errors"
	"github.com/lyft/flytestdlib/logger"
	"github.com/operator-framework/operator-sdk/pkg/sdk"
	"k8s.io/client-go/tools/record"
	"github.com/lyft/flytestdlib/contextutils"
	"fmt"
)

func NewHandler() sdk.Handler {
	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(logger.InfofNoCtx)
	flinkHandler := NewFlinkStateMachine()
	if flinkHandler == nil {
		panic("unable to create flink handler")
	}
	//eventBroadcaster.StartRecordingToSink(&corev1.EventSinkImpl{Interface: client.CoreV1().Events("")})
	//recorder := eventBroadcaster.NewRecorder(scheme.Scheme, corev1.EventSource{Component: controllerAgentName})
	// TODO implement event sink logic
	return &Handler{
		recorder:     nil,
		flinkHandler: flinkHandler,
	}
}

type Handler struct {
	flinkHandler FlinkHandlerInterface
	recorder     record.EventRecorder
}

var jobObjectType = reflect.TypeOf(&v1alpha1.FlinkApplication{}).String()

func (h *Handler) Handle(ctx context.Context, event sdk.Event) error {
	switch o := event.Object.(type) {
	case *v1alpha1.FlinkApplication:
		if event.Deleted {
			logger.Infof(ctx, "Flink Job deleted, we will let the owner references cascade downstream")
			return nil
		}
		ctx = contextutils.WithNamespace(ctx, o.Namespace)
		ctx = contextutils.WithAppName(ctx, o.Name)
		ctx = contextutils.WithPhase(ctx, fmt.Sprintf("%s", o.Status.Phase))
		err := h.flinkHandler.Handle(ctx, o)
		if err != nil {
			logger.Errorf(ctx, "Failed to Handle Flink Job: %v", err)
			return nil
		}
	default:
		return errors.Errorf(errors.IllegalStateError, "Operator can only handle jobs of type [%v]. Received [%v]", jobObjectType, reflect.TypeOf(o).String())
	}
	return nil
}
