package controller

import (
	"github.com/lyft/flinkk8soperator/pkg/controller/flinkapplication"
)

func init() {
	// AddToManagerFuncs is a list of functions to create controllers and add them to a manager.
	AddToManagerFuncs = append(AddToManagerFuncs, flinkapplication.Add)
}
