/*
 * Copyright (c) 2018 Lyft. All rights reserved.
 */

package apis

import (
	"github.com/lyft/flinkk8soperator/pkg/apis/app/v1beta1"
)

func init() {
	// Register the types with the Scheme so the components can map objects to GroupVersionKinds and back
	AddToSchemes = append(AddToSchemes, v1beta1.SchemeBuilder.AddToScheme)
}
