package mock

import (
	"context"

	"github.com/lyft/flinkk8soperator/pkg/apis/app/v1alpha1"
)

type MockJobManagerController struct {
	CreateIfNotExistFunc CreateIfNotExistFunc
}

func (m *MockJobManagerController) CreateIfNotExist(
	ctx context.Context,
	application *v1alpha1.FlinkApplication) error {
	if m.CreateIfNotExistFunc != nil {
		return m.CreateIfNotExistFunc(ctx, application)
	}
	return nil
}
