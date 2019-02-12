package flink

import (
	"testing"

	"github.com/lyft/flinkk8soperator/pkg/config"
	"github.com/stretchr/testify/assert"
)

func TestReplaceJobUrl(t *testing.T) {
	assert.Equal(t,
		"ABC.lyft.xyz",
		ReplaceJobUrl("{{$jobCluster}}.lyft.xyz", "ABC"))
}

func TestGetFlinkUIIngressURL(t *testing.T) {
	config.Init("")
	assert.Equal(t,
		"ABC.lyft.xyz",
		GetFlinkUIIngressURL("ABC"))
}
