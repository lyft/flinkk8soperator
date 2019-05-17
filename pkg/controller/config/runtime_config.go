package config

import (
	"github.com/lyft/flytestdlib/promutils"
)

type RuntimeConfig struct {
	MetricsScope promutils.Scope
}
